package readadapter

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPhase16A_PlanCacheKeyScopingAndTenantStatsIsolation(t *testing.T) {
	pc := NewQueryPlanCache(64)
	sc := NewStatsCache(64)

	var calls atomic.Int32
	fn := func(q string) (string, int, []uint32, []uint32) {
		calls.Add(1)
		return strings.ReplaceAll(q, "$1", "?1"), 1, []uint32{1001}, []uint32{2001}
	}

	// 1. Seed tenant_a with int4 param, search_path=public.
	sA := PlanScope{
		TenantID:      "tenant_a",
		Generation:    "g1",
		DatabaseID:    1,
		SearchPath:    "public",
		SchemaVersion: 1,
		StatsVersion:  sc.TenantVersion("tenant_a"),
		CapabilityVer: 1,
		ParamOIDs:     []uint32{PGOIDInt4},
		SettingsHash:  ExecSettingsHash("public", "UTC", "C"),
	}
	_, hit := pc.GetOrTranslateScoped(sA, "SELECT id FROM users WHERE id = $1", fn)
	if hit {
		t.Fatalf("expected initial miss")
	}

	// Repeat with same scope -> hit.
	_, hit = pc.GetOrTranslateScoped(sA, "SELECT id FROM users WHERE id = $1", fn)
	if !hit {
		t.Fatalf("expected cache hit for identical scope")
	}

	// 2. Different SearchPath -> miss.
	sSearch := sA
	sSearch.SearchPath = "analytics"
	sSearch.SettingsHash = ExecSettingsHash("analytics", "UTC", "C")
	if _, hit = pc.GetOrTranslateScoped(sSearch, "SELECT id FROM users WHERE id = $1", fn); hit {
		t.Fatalf("expected miss when search_path changes")
	}

	// 3. Different DatabaseID -> miss.
	sDB := sA
	sDB.DatabaseID = 2
	if _, hit = pc.GetOrTranslateScoped(sDB, "SELECT id FROM users WHERE id = $1", fn); hit {
		t.Fatalf("expected miss when DatabaseID changes")
	}

	// 4. Different Parameter Type Signature -> miss.
	sParam := sA
	sParam.ParamOIDs = []uint32{PGOIDText}
	if _, hit = pc.GetOrTranslateScoped(sParam, "SELECT id FROM users WHERE id = $1", fn); hit {
		t.Fatalf("expected miss when parameter OID signature changes")
	}

	// 5. Different Execution Settings (timezone) -> miss.
	sTZ := sA
	sTZ.SettingsHash = ExecSettingsHash("public", "America/New_York", "C")
	if _, hit = pc.GetOrTranslateScoped(sTZ, "SELECT id FROM users WHERE id = $1", fn); hit {
		t.Fatalf("expected miss when timezone setting changes")
	}

	// 6. Per-tenant StatsCache isolation: updating tenant_b stats must NOT invalidate tenant_a.
	sc.Put(&TableStats{TenantID: "tenant_b", Generation: "g1", RelOID: 5001, RelPages: 10, RelTuples: 500})
	sA.StatsVersion = sc.TenantVersion("tenant_a")
	if _, hit = pc.GetOrTranslateScoped(sA, "SELECT id FROM users WHERE id = $1", fn); !hit {
		t.Fatalf("tenant_b stats update must not invalidate tenant_a plan cache")
	}

	// Updating tenant_a stats MUST bump tenant_a StatsVersion and miss.
	sc.Put(&TableStats{TenantID: "tenant_a", Generation: "g1", RelOID: 1001, RelPages: 20, RelTuples: 1000})
	sA.StatsVersion = sc.TenantVersion("tenant_a")
	if _, hit = pc.GetOrTranslateScoped(sA, "SELECT id FROM users WHERE id = $1", fn); hit {
		t.Fatalf("expected tenant_a stats update to invalidate tenant_a plan")
	}
}

func TestPhase16A_TargetedInvalidation_TableIndexAndCatalogReconcile(t *testing.T) {
	pc := NewQueryPlanCache(128)
	reg := NewSchemaVersionRegistry()

	tblUsers := DiscoveredTable{
		Schema: "public", Name: "users", RelOID: 1001,
		Columns: []DiscoveredColumn{{Name: "id", AttNum: 1, PGTypeOID: PGOIDInt4}},
		Indexes: []DiscoveredIndex{{Name: "idx_users_id", IndexRelOID: 2001, TableRelOID: 1001, IsUnique: true, KeyAttNums: []int16{1}}},
	}
	tblOrders := DiscoveredTable{
		Schema: "public", Name: "orders", RelOID: 1002,
		Columns: []DiscoveredColumn{{Name: "id", AttNum: 1, PGTypeOID: PGOIDInt4}},
		Indexes: []DiscoveredIndex{{Name: "idx_orders_id", IndexRelOID: 2002, TableRelOID: 1002, IsUnique: true, KeyAttNums: []int16{1}}},
	}

	_, _, changed := reg.ReconcileCatalog("t1", 0, []DiscoveredTable{tblUsers, tblOrders})
	if !changed {
		t.Fatalf("expected initial catalog reconcile to report schemaChanged=true")
	}
	sv1 := reg.SchemaVersion("t1", 0)

	// Reconciling identical schema (e.g. after DML) must NOT bump schema version.
	chTbl, chIdx, changed2 := reg.ReconcileCatalog("t1", 0, []DiscoveredTable{tblUsers, tblOrders})
	if changed2 || len(chTbl) != 0 || len(chIdx) != 0 || reg.SchemaVersion("t1", 0) != sv1 {
		t.Fatalf("expected unchanged catalog to not bump versions (changed=%v sv=%d/%d)", changed2, sv1, reg.SchemaVersion("t1", 0))
	}

	// Cache plans for users, orders, and another tenant t2.
	translateFor := func(q string) (string, int, []uint32, []uint32) {
		tdeps, ideps := reg.ResolveQueryDeps("t1", 0, q)
		return q, 0, tdeps, ideps
	}
	sT1 := PlanScope{TenantID: "t1", Generation: "g1", SchemaVersion: sv1}
	pc.GetOrTranslateScoped(sT1, "SELECT id FROM users", translateFor)
	pc.GetOrTranslateScoped(sT1, "SELECT id FROM orders", translateFor)
	pc.GetOrTranslateScoped(PlanScope{TenantID: "t2", Generation: "g1", SchemaVersion: 1}, "SELECT id FROM orders",
		func(q string) (string, int, []uint32, []uint32) { return q, 0, []uint32{1002}, nil })

	// Alter ONLY orders (add a column) -> only orders table/schema changes, users plan must remain!
	tblOrdersV2 := tblOrders
	tblOrdersV2.Columns = append(tblOrdersV2.Columns, DiscoveredColumn{Name: "total", AttNum: 2, PGTypeOID: PGOIDInt8})
	chTbl, _, changed3 := reg.ReconcileCatalog("t1", 0, []DiscoveredTable{tblUsers, tblOrdersV2})
	if !changed3 || len(chTbl) != 1 || chTbl[0] != 1002 {
		t.Fatalf("expected only orders (1002) to change, got %v", chTbl)
	}
	evicted := pc.InvalidateTable("t1", 1002)
	if evicted != 1 {
		t.Fatalf("expected exactly 1 plan (t1/orders) invalidated, got %d", evicted)
	}

	// t1/users at sv1 is still cached if queried at sv1, and t2/orders is untouched!
	if _, hitUsers := pc.GetOrTranslateScoped(sT1, "SELECT id FROM users", translateFor); !hitUsers {
		t.Fatalf("expected t1/users plan to survive targeted invalidation of orders")
	}
	if _, hitT2 := pc.GetOrTranslateScoped(PlanScope{TenantID: "t2", Generation: "g1", SchemaVersion: 1}, "SELECT id FROM orders",
		func(q string) (string, int, []uint32, []uint32) { return q, 0, []uint32{1002}, nil }); !hitT2 {
		t.Fatalf("expected t2/orders plan to survive t1 table invalidation")
	}

	// Index-targeted invalidation on idx_users_id (2001).
	if evictedIdx := pc.InvalidateIndex("t1", 2001); evictedIdx != 1 {
		t.Fatalf("expected 1 plan invalidated by index 2001, got %d", evictedIdx)
	}
	if _, hitUsersAfter := pc.GetOrTranslateScoped(sT1, "SELECT id FROM users", translateFor); hitUsersAfter {
		t.Fatalf("expected t1/users plan to be invalidated after index 2001 invalidation")
	}
}

func TestPhase16A_ConcurrentInvalidationAndAccess(t *testing.T) {
	pc := NewQueryPlanCache(256)
	reg := NewSchemaVersionRegistry()
	sc := NewStatsCache(256)

	var wg sync.WaitGroup
	// 8 reader/translator workers across 4 tenants and 2 tables
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tid := fmt.Sprintf("tenant_%d", id%4)
			for i := 0; i < 200; i++ {
				tblOID := uint32(1000 + (i % 2))
				q := fmt.Sprintf("SELECT id FROM tbl_%d WHERE id = $1", i%2)
				sv := reg.SchemaVersion(tid, 0)
				stv := sc.TenantVersion(tid)
				scope := PlanScope{
					TenantID:      tid,
					Generation:    "g1",
					SchemaVersion: sv,
					StatsVersion:  stv,
					ParamOIDs:     []uint32{PGOIDInt4},
				}
				p, _ := pc.GetOrTranslateScoped(scope, q, func(s string) (string, int, []uint32, []uint32) {
					return strings.ReplaceAll(s, "$1", "?1"), 1, []uint32{tblOID}, nil
				})
				if p == nil || p.Key.TenantID != tid {
					t.Errorf("invalid or cross-tenant plan returned: %+v", p)
					return
				}
			}
		}(w)
	}

	// 2 invalidator workers performing concurrent DDL/stats invalidations
	for inv := 0; inv < 2; inv++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				tid := fmt.Sprintf("tenant_%d", (id+i)%4)
				reg.BumpSchema(tid, 0)
				pc.InvalidateTable(tid, uint32(1000+(i%2)))
				sc.Put(&TableStats{
					TenantID: tid, Generation: "g1", RelOID: uint32(1000 + (i % 2)),
					RelPages: int32(10 + i), RelTuples: float32(100 + i),
				})
			}
		}(inv)
	}
	wg.Wait()

	st := pc.Stats()
	if st.Entries > 256 {
		t.Fatalf("plan cache exceeded bounded capacity: entries=%d", st.Entries)
	}
}

func TestPhase16A_PreparedStatementInvalidation(t *testing.T) {
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	sess := &sqliteClientSession{
		srv:         srv,
		tenantID:    "tenant_prep",
		sessionVars: map[string]string{"search_path": "public"},
		stmts:       make(map[string]*sqlitePreparedStmt),
		portals:     make(map[string]*sqlitePortal),
	}

	ts, err := srv.getOrCreateTenant("tenant_prep")
	if err != nil {
		t.Fatal(err)
	}

	// Initial schema version 1
	srv.schemaVersions.BumpSchema("tenant_prep", 0)
	sq, pcnt, _ := srv.translateCachedForSession(sess, ts, "SELECT id FROM users WHERE id = $1", []uint32{PGOIDInt4})
	ps := &sqlitePreparedStmt{
		Name:          "stmt_1",
		OrigSQL:       "SELECT id FROM users WHERE id = $1",
		SQLiteSQL:     sq,
		ParamCount:    pcnt,
		ParamOIDs:     []uint32{PGOIDInt4},
		SchemaVersion: srv.schemaVersions.SchemaVersion("tenant_prep", 0),
		Generation:    ts.Generation,
		SearchPath:    "public",
	}

	// Bump schema version (simulating ALTER TABLE / CREATE INDEX)
	newVer := srv.schemaVersions.BumpSchema("tenant_prep", 0)
	if err := sess.revalidatePreparedStmt(ps); err != nil {
		t.Fatalf("revalidatePreparedStmt: %v", err)
	}
	if ps.SchemaVersion != newVer {
		t.Fatalf("expected prepared stmt SchemaVersion updated to %d, got %d", newVer, ps.SchemaVersion)
	}
}
