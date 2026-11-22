package readadapter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func TestPhase15_StatsCache_BoundedLRUAndTenantIsolation(t *testing.T) {
	sc := NewStatsCache(2)
	v0 := sc.Version()

	sc.Put(&TableStats{
		TenantID:   "tenant_a",
		Generation: "gen-1",
		RelOID:     101,
		RelPages:   100,
		RelTuples:  5000,
		Columns: map[int16]ColumnStats{
			1: {AttNum: 1, NullFrac: 0.0, Distinct: -1.0}, // unique
			2: {AttNum: 2, NullFrac: 0.1, Distinct: 20},   // 20 distinct values
		},
	})
	if sc.Version() <= v0 {
		t.Fatalf("expected StatsCache.Version() to increment on Put")
	}

	tsA, ok := sc.Get("tenant_a", "gen-1", 101)
	if !ok || tsA.RelPages != 100 {
		t.Fatalf("expected tenant_a stats hit")
	}
	if _, ok := sc.Get("tenant_b", "gen-1", 101); ok {
		t.Fatalf("cross-tenant stats leakage detected")
	}

	// Selectivity checks
	if sel := tsA.EstimateSelectivity(1, sqliteConstraintEQ, 5000, true); sel > 0.001 {
		t.Fatalf("expected very small selectivity for unique col EQ, got %f", sel)
	}
	if sel := tsA.EstimateSelectivity(2, sqliteConstraintEQ, 5000, false); sel < 0.04 || sel > 0.06 {
		t.Fatalf("expected ~0.045 selectivity for 20 distinct with 0.1 nullfrac, got %f", sel)
	}
	if sel := tsA.EstimateSelectivity(2, sqliteConstraintISNULL, 5000, false); sel < 0.09 || sel > 0.11 {
		t.Fatalf("expected ~0.10 selectivity for ISNULL, got %f", sel)
	}

	// Bounded LRU eviction: maxEntries=2
	sc.Put(&TableStats{TenantID: "tenant_a", Generation: "gen-1", RelOID: 102, RelPages: 10, RelTuples: 100})
	sc.Put(&TableStats{TenantID: "tenant_a", Generation: "gen-1", RelOID: 103, RelPages: 20, RelTuples: 200})
	if _, ok := sc.Get("tenant_a", "gen-1", 101); ok {
		t.Fatalf("expected oldest entry (101) to be evicted from bounded LRU")
	}
}

func TestPhase15_QueryOptimizer_CostSelectionAndCacheTiers(t *testing.T) {
	sc := NewStatsCache(128)
	at := NewAdaptiveTracker(128)
	cfg := DefaultCostModelConfig()
	opt := NewQueryOptimizer(cfg, sc, at, nil)

	// 1. Known tiny 1-page table -> SeqScan must beat IndexScan.
	sc.Put(&TableStats{
		TenantID:   "t1",
		Generation: "g1",
		RelOID:     2001,
		RelPages:   1,
		RelTuples:  12,
		Columns: map[int16]ColumnStats{
			1: {AttNum: 1, Distinct: -1.0},
		},
	})
	tinyTable := DiscoveredTable{
		Name:   "tiny_tbl",
		RelOID: 2001,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: PGOIDInt4},
			{Name: "val", AttNum: 2, PGTypeOID: PGOIDText},
		},
	}
	tinyPlan := []btreeIndexPlan{{
		Index: DiscoveredIndex{
			Name: "tiny_idx", IndexRelOID: 3001, IsUnique: true, IndNAtts: 1, IndNKeyAtts: 1, KeyAttNums: []int16{1},
		},
		KeyAllCols: []int{0},
		KeyVisCols: []int{0},
	}}
	tinyAllCols := []ColumnType{ColInt4, ColText}
	decTiny := opt.ChooseBestPlan(
		tinyTable, tinyPlan, tinyAllCols,
		[]FilterConstraint{{AllColIdx: 0, AttNum: 1, Op: sqliteConstraintEQ}},
		nil, 2, 1, "t1", "g1",
	)
	if decTiny.Selected.Strategy != StrategySeqScan {
		t.Fatalf("expected SeqScan for 1-page tiny table, got %s (cost=%.3f)", decTiny.Selected.Strategy, decTiny.Selected.EstimatedCost)
	}

	// 2. Large table (500 pages, 25000 rows) with equality + ORDER BY on composite index -> IndexScan with OrderByConsumed.
	sc.Put(&TableStats{
		TenantID:   "t1",
		Generation: "g1",
		RelOID:     2002,
		RelPages:   500,
		RelTuples:  25000,
		Columns: map[int16]ColumnStats{
			1: {AttNum: 1, Distinct: 1000},
			2: {AttNum: 2, Distinct: 5000},
		},
	})
	bigTable := DiscoveredTable{
		Name:   "big_tbl",
		RelOID: 2002,
		Columns: []DiscoveredColumn{
			{Name: "tenant_id", AttNum: 1, PGTypeOID: PGOIDInt4},
			{Name: "created_at", AttNum: 2, PGTypeOID: PGOIDInt8},
			{Name: "payload", AttNum: 3, PGTypeOID: PGOIDText},
		},
	}
	bigPlans := []btreeIndexPlan{{
		Index: DiscoveredIndex{
			Name: "idx_big_comp", IndexRelOID: 3002, IsUnique: false, IndNAtts: 2, IndNKeyAtts: 2, KeyAttNums: []int16{1, 2},
		},
		KeyAllCols: []int{0, 1},
		KeyVisCols: []int{0, 1},
		Orderings:  []btreeKeyOrdering{{Desc: false}, {Desc: false}},
	}}
	bigAllCols := []ColumnType{ColInt4, ColInt8, ColText}
	decBig := opt.ChooseBestPlan(
		bigTable, bigPlans, bigAllCols,
		[]FilterConstraint{{AllColIdx: 0, AttNum: 1, Op: sqliteConstraintEQ}},
		[]OrderBySpec{{AllColIdx: 1, Desc: false}},
		1, 500, "t1", "g1",
	)
	if decBig.Selected.Strategy != StrategyIndexScan && decBig.Selected.Strategy != StrategyIndexScanOrdered {
		t.Fatalf("expected IndexScan for 500-page selective query, got %s", decBig.Selected.Strategy)
	}
	if !decBig.Selected.OrderByConsumed {
		t.Fatalf("expected composite index (col 0 EQ, ORDER BY col 1) to consume ORDER BY")
	}
	if !decBig.ProjectionPushdown || decBig.NeededCols != 1 || decBig.TotalCols != 3 {
		t.Fatalf("expected projection pushdown 1/3, got %+v", decBig)
	}

	// 3. L1-heavy vs L3-heavy cost sensitivity.
	coldCfg := cfg
	coldCfg.L1PageCost = 0.01
	coldCfg.L3PageCost = 100.0
	coldOpt := NewQueryOptimizer(coldCfg, sc, at, nil)
	decCold := coldOpt.ChooseBestPlan(
		bigTable, bigPlans, bigAllCols,
		[]FilterConstraint{{AllColIdx: 0, AttNum: 1, Op: sqliteConstraintEQ}},
		nil, 1, 500, "t1", "g1",
	)
	if decCold.Selected.EstimatedCost <= decBig.Selected.EstimatedCost {
		t.Fatalf("expected higher L3PageCost to raise estimated plan cost: cold=%.2f warm=%.2f", decCold.Selected.EstimatedCost, decBig.Selected.EstimatedCost)
	}
}

func TestPhase15_QueryPlanCache_InvalidationIsolationAndStampede(t *testing.T) {
	pc := NewQueryPlanCache(2)
	var translates atomic.Int32
	fn := func(pgSQL string) (string, int) {
		translates.Add(1)
		time.Sleep(20 * time.Millisecond)
		return strings.ReplaceAll(pgSQL, "$1", "?1"), 1
	}

	// Stampede test: 8 concurrent callers for the same query should invoke translateFn once.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _ := pc.GetOrTranslate("tenant_a", "gen-1", 100, 1, 1, "SELECT id FROM users WHERE id = $1", fn)
			if p == nil || p.SQLiteSQL == "" {
				t.Errorf("expected translated plan")
			}
		}()
	}
	wg.Wait()
	if got := translates.Load(); got != 1 {
		t.Fatalf("expected singleflight to coalesce stampede to 1 translate call, got %d", got)
	}

	// Cache hit for same tenant/LSN/stats/cap
	_, hit := pc.GetOrTranslate("tenant_a", "gen-1", 100, 1, 1, "SELECT id FROM users WHERE id = $1", fn)
	if !hit {
		t.Fatalf("expected cache hit on repeat query")
	}

	// Cross-tenant isolation: tenant_b must miss
	_, hitB := pc.GetOrTranslate("tenant_b", "gen-1", 100, 1, 1, "SELECT id FROM users WHERE id = $1", fn)
	if hitB {
		t.Fatalf("expected cross-tenant miss")
	}

	// Stats version bump must miss
	_, hitStats := pc.GetOrTranslate("tenant_a", "gen-1", 100, 2, 1, "SELECT id FROM users WHERE id = $1", fn)
	if hitStats {
		t.Fatalf("expected statsVersion bump to invalidate plan")
	}

	// InvalidateTenant
	pc.InvalidateTenant("tenant_a")
	_, hitAfterInv := pc.GetOrTranslate("tenant_a", "gen-1", 100, 2, 1, "SELECT id FROM users WHERE id = $1", fn)
	if hitAfterInv {
		t.Fatalf("expected InvalidateTenant to evict tenant_a plans")
	}
}

func TestPhase15_AdaptiveTracker_BoundedEMA(t *testing.T) {
	at := NewAdaptiveTracker(2)
	at.Record("t1", 100, 100, 400, 5, 4, 1, 50000)
	at.Record("t1", 100, 100, 400, 5, 4, 1, 50000)
	fb, ok := at.Get("t1", 100)
	if !ok || fb.SampleCount != 2 || fb.SelectivityFactor <= 1.0 {
		t.Fatalf("expected upward EMA SelectivityFactor > 1.0 with 2 samples, got %+v (ok=%v)", fb, ok)
	}
	at.Record("t1", 101, 10, 10, 1, 1, 0, 1000)
	at.Record("t1", 102, 10, 10, 1, 1, 0, 1000)
	if _, okOld := at.Get("t1", 100); okOld {
		t.Fatalf("expected bounded LRU eviction of oldest adaptive entry")
	}
}

func TestPhase15_ExplainDiagnostics_FormatAndNoSecrets(t *testing.T) {
	lines := FormatAIRQueryPlan(ExplainDiagnostics{
		Strategy:           StrategyIndexScan,
		SelectedIndex:      "idx_users_email",
		EstimatedCost:      2.45,
		EstimatedRows:      1,
		ActualRows:         1,
		HasAnalyze:         true,
		ExecTimeMs:         0.312,
		PagesRead:          2,
		CacheTier:          "L1",
		PredicatePushdown:  true,
		ProjectionPushdown: true,
		NeededCols:         2,
		TotalCols:          6,
		PlanCacheHit:       true,
	})
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "AIR QUERY PLAN") ||
		!strings.Contains(joined, string(StrategyIndexScan)) ||
		!strings.Contains(joined, "idx_users_email") ||
		!strings.Contains(joined, "actual rows=1") ||
		!strings.Contains(joined, "Projection Pushdown: Enabled (2/6 cols)") {
		t.Fatalf("unexpected EXPLAIN output:\n%s", joined)
	}
}

func TestPhase15_LivePG17_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live PostgreSQL 17 integration test in -short mode")
	}
	initdbMatches, _ := filepath.Glob("/usr/lib/postgresql/*/bin/initdb")
	if len(initdbMatches) == 0 {
		t.Skip("PostgreSQL server binaries not installed")
	}
	initdbBin := initdbMatches[len(initdbMatches)-1]
	pgBin := filepath.Join(filepath.Dir(initdbBin), "postgres")

	tmpDir := t.TempDir()
	pgData := filepath.Join(tmpDir, "pgdata")
	sockDir := filepath.Join(tmpDir, "pgsock")
	_ = os.MkdirAll(sockDir, 0o755)

	if out, err := exec.Command(initdbBin, "-D", pgData, "-U", "postgres", "-A", "trust", "--no-sync").CombinedOutput(); err != nil {
		t.Fatalf("initdb failed: %v\n%s", err, out)
	}
	confPath := filepath.Join(pgData, "postgresql.conf")
	f, err := os.OpenFile(confPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\n")
	_ = f.Close()

	hbaPath := filepath.Join(pgData, "pg_hba.conf")
	hf, err := os.OpenFile(hbaPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55449", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55449 user=postgres dbname=postgres sslmode=disable", sockDir)
	var pgConn *pgx.Conn
	for i := 0; i < 50; i++ {
		pgConn, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pgConn == nil {
		t.Fatalf("could not connect to PostgreSQL")
	}
	defer pgConn.Close(ctx)

	if _, err := pgConn.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatalf("CHECKPOINT: %v", err)
	}
	var lsnStr string
	if err := pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr); err != nil {
		t.Fatalf("pg_current_wal_lsn: %v", err)
	}
	var hi, lo uint32
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	initLSN := (uint64(hi) << 32) | uint64(lo)

	psSock := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: psSock,
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}

	const tenantID = "tenant_p15_e2e"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatalf("BootstrapTenantFromPGData: %v", err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() { _ = rx.Run(rxCtx) }()

	if _, err := pgConn.Exec(ctx, `
		CREATE TABLE items (id int4 PRIMARY KEY, cat int4 NOT NULL, seq int4 NOT NULL, label text NOT NULL);
		CREATE TABLE cats (cat int4 PRIMARY KEY, title text NOT NULL);
		INSERT INTO cats SELECT g, 'cat_' || g FROM generate_series(1, 10) g;
		INSERT INTO items SELECT g, (g % 10) + 1, g * 10, 'item_' || g FROM generate_series(1, 500) g;
		CREATE INDEX idx_items_cat_seq ON items (cat, seq);
		ANALYZE cats;
		ANALYZE items;
		CHECKPOINT;
	`); err != nil {
		t.Fatalf("PG schema/data setup failed: %v", err)
	}

	var curLSNStr string
	_ = pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&curLSNStr)
	_, _ = fmt.Sscanf(curLSNStr, "%X/%X", &hi, &lo)
	targetLSN := (uint64(hi) << 32) | uint64(lo)

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if tl.LastLSN() >= targetLSN-8 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir:          filepath.Join(tmpDir, "sqlite_srv"),
		PGPort:           0,
		PageserverSocket: psSock,
		L1CacheBytes:     16 * 1024 * 1024,
		CacheDir:         filepath.Join(tmpDir, "l2cache"),
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start: %v", err)
	}
	defer srv.Stop()

	airDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv.Port(), tenantID)
	airConn, err := pgx.Connect(ctx, airDSN)
	if err != nil {
		t.Fatalf("connect to SQLiteReadServer: %v", err)
	}
	defer airConn.Close(ctx)

	// 1. Point lookup
	var label string
	if err := airConn.QueryRow(ctx, "SELECT label FROM items WHERE id = 42").Scan(&label); err != nil {
		t.Fatalf("point lookup failed: %v", err)
	}
	if label != "item_42" {
		t.Fatalf("expected item_42, got %q", label)
	}

	// 2. Range scan + ORDER BY + LIMIT
	rows, err := airConn.Query(ctx, "SELECT id, seq FROM items WHERE cat = 3 AND seq >= 100 ORDER BY seq LIMIT 5")
	if err != nil {
		t.Fatalf("composite range query failed: %v", err)
	}
	var count int
	for rows.Next() {
		var id, seq int32
		if err := rows.Scan(&id, &seq); err != nil {
			t.Fatal(err)
		}
		count++
	}
	rows.Close()
	if count != 5 {
		t.Fatalf("expected 5 rows from composite range+limit, got %d", count)
	}

	// 3. Join + Aggregation
	var catTitle string
	var cnt int64
	if err := airConn.QueryRow(ctx, "SELECT c.title, COUNT(*) FROM items i JOIN cats c ON i.cat = c.cat WHERE c.cat = 3 GROUP BY c.title").Scan(&catTitle, &cnt); err != nil {
		t.Fatalf("join+agg failed: %v", err)
	}
	if catTitle != "cat_3" || cnt != 50 {
		t.Fatalf("expected (cat_3, 50), got (%q, %d)", catTitle, cnt)
	}

	// 4. EXPLAIN ANALYZE diagnostics
	exRows, err := airConn.Query(ctx, "EXPLAIN ANALYZE SELECT label FROM items WHERE id = 42")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE failed: %v", err)
	}
	var exLines []string
	for exRows.Next() {
		var line string
		if err := exRows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		exLines = append(exLines, line)
	}
	exRows.Close()
	exJoined := strings.Join(exLines, "\n")
	if !strings.Contains(exJoined, "AIR QUERY PLAN") {
		t.Fatalf("expected AIR QUERY PLAN in EXPLAIN ANALYZE output, got:\n%s", exJoined)
	}

	m := srv.DetailedMetrics()
	if m.PlanCache.Hits == 0 && m.PlanCache.Misses == 0 {
		t.Fatalf("expected PlanCache metrics to be populated")
	}
}
