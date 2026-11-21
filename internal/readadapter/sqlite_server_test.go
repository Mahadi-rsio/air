package readadapter

import (
	"context"
	"encoding/binary"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func TestSQLiteReadServer_EnforcementAndPGWireCompatibility(t *testing.T) {
	rootDir := t.TempDir()
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost: "127.0.0.1",
		PGPort:     0,
		DataDir:    rootDir,
		CacheKB:    16384,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	const tenantID = "tenant-sqlite-ro"
	if err := srv.EnsureTenantDB(tenantID); err != nil {
		t.Fatalf("EnsureTenantDB: %v", err)
	}

	// Seed table via internal writer connection at LSN 0x2000400.
	seedSQL := `
CREATE TABLE items (id INT4 PRIMARY KEY, val TEXT);
INSERT INTO items VALUES (1, 'alpha'), (2, 'beta');
`
	if err := srv.ExecInternal(tenantID, seedSQL, 0x2000400); err != nil {
		t.Fatalf("ExecInternal seed: %v", err)
	}

	ctx := context.Background()
	dsn := fmt.Sprintf("postgres://app_user@127.0.0.1:%d/%s?sslmode=disable", srv.Port(), tenantID)

	// 1. Test via pgx (Extended Query protocol Parse/Bind/Describe/Execute/Sync)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer conn.Close(ctx)

	var count int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM items").Scan(&count); err != nil || count != 2 {
		t.Fatalf("SELECT count(*): got=%d err=%v", count, err)
	}

	var val string
	if err := conn.QueryRow(ctx, "SELECT val FROM items WHERE id = $1", 2).Scan(&val); err != nil || val != "beta" {
		t.Fatalf("SELECT parameterized: got=%q err=%v", val, err)
	}

	var lsnStr string
	if err := conn.QueryRow(ctx, "SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn())::text").Scan(&lsnStr); err != nil || lsnStr != "0/02000400" {
		t.Fatalf("LSN introspection: got=%q want=0/02000400 err=%v", lsnStr, err)
	}

	var sqVer string
	if err := conn.QueryRow(ctx, "SELECT sqlite_version()").Scan(&sqVer); err != nil || sqVer == "" {
		t.Fatalf("sqlite_version(): got=%q err=%v", sqVer, err)
	}

	// 2. Test via psql over Unix socket and TCP (Simple Query protocol + COPY TO STDOUT + PREPARE/EXECUTE)
	psqlBin, err := exec.LookPath("psql")
	if err != nil {
		psqlBin = "/usr/bin/psql"
	}
	runPSQL := func(sql string) (string, error) {
		cmd := exec.Command(psqlBin,
			"-h", srv.SockDir(),
			"-p", strconv.Itoa(srv.Port()),
			"-U", "app_user",
			"-d", tenantID,
			"-v", "ON_ERROR_STOP=1",
			"-At", "-c", sql,
		)
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}

	allowed := []struct {
		name string
		sql  string
		want string
	}{
		{"select_count", "SELECT count(*) FROM items;", "2"},
		{"select_filter", "SELECT val FROM items WHERE id = 1;", "alpha"},
		{"explain_analyze", "EXPLAIN (ANALYZE, COSTS OFF) SELECT * FROM items WHERE id = 2;", ""},
		{"prepare_exec", "PREPARE p1 (int) AS SELECT val FROM items WHERE id = $1; EXECUTE p1(2);", "PREPARE\nbeta"},
		{"tx_readonly", "BEGIN READ ONLY; SELECT count(*) FROM items; COMMIT;", "BEGIN\n2\nCOMMIT"},
		{"copy_to", "COPY (SELECT id, val FROM items ORDER BY id) TO STDOUT;", "1\talpha\n2\tbeta"},
		{"set_safe_guc", "SET work_mem = '8MB'; SHOW work_mem;", "SET\n8MB"},
	}
	for _, tc := range allowed {
		out, err := runPSQL(tc.sql)
		if err != nil {
			t.Fatalf("allowed query %s (%q) failed: %v\n%s", tc.name, tc.sql, err, out)
		}
		if tc.want != "" && out != tc.want {
			t.Fatalf("allowed query %s: got %q, want %q", tc.name, out, tc.want)
		}
	}

	// 3. Write / mutating / lock queries MUST fail with read-only rejection
	blocked := []struct {
		name string
		sql  string
	}{
		{"insert", "INSERT INTO items VALUES (3, 'gamma');"},
		{"update", "UPDATE items SET val = 'x' WHERE id = 1;"},
		{"delete", "DELETE FROM items WHERE id = 1;"},
		{"merge", "MERGE INTO items t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE;"},
		{"modifying_cte", "WITH d AS (DELETE FROM items WHERE id = 1 RETURNING id) SELECT * FROM d;"},
		{"for_update", "SELECT * FROM items WHERE id = 1 FOR UPDATE;"},
		{"for_share", "SELECT * FROM items WHERE id = 1 FOR SHARE;"},
		{"create_table", "CREATE TABLE evil (id int);"},
		{"create_temp_table", "CREATE TEMP TABLE tmp_evil (id int);"},
		{"select_into", "SELECT * INTO items_copy FROM items;"},
		{"alter_table", "ALTER TABLE items ADD COLUMN extra int;"},
		{"drop_table", "DROP TABLE items;"},
		{"truncate", "TRUNCATE items;"},
		{"copy_from", "COPY items FROM STDIN;"},
		{"do_block", "DO $$ BEGIN INSERT INTO items VALUES (9, 'do'); END $$;"},
		{"begin_read_write", "BEGIN READ WRITE; SELECT 1; COMMIT;"},
		{"set_ro_off", "SET transaction_read_only = off;"},
		{"set_default_ro_off", "SET default_transaction_read_only = off;"},
		{"pragma_writable", "PRAGMA query_only = OFF;"},
		{"attach_db", "ATTACH DATABASE ':memory:' AS aux;"},
	}
	for _, tc := range blocked {
		out, err := runPSQL(tc.sql)
		if err == nil {
			t.Fatalf("blocked query %s (%q) unexpectedly SUCCEEDED with output: %s", tc.name, tc.sql, out)
		}
	}

	// 4. Verify air_ro_status() metrics and intact data
	statusOut, err := runPSQL("SELECT enabled, queries_allowed, writes_blocked, allow_temp_tables FROM air_ro_status();")
	if err != nil {
		t.Fatalf("air_ro_status failed: %v\n%s", err, statusOut)
	}
	parts := strings.Split(statusOut, "|")
	if len(parts) != 4 || parts[0] != "t" || parts[3] != "f" {
		t.Fatalf("unexpected air_ro_status output: %q", statusOut)
	}
	qAllowed, _ := strconv.ParseInt(parts[1], 10, 64)
	wBlocked, _ := strconv.ParseInt(parts[2], 10, 64)
	if qAllowed < 6 {
		t.Fatalf("expected queries_allowed >= 6, got %d", qAllowed)
	}
	if wBlocked < int64(len(blocked)) {
		t.Fatalf("expected writes_blocked >= %d, got %d", len(blocked), wBlocked)
	}

	countOut, err := runPSQL("SELECT count(*) FROM items;")
	if err != nil || countOut != "2" {
		t.Fatalf("expected 2 intact rows, got %q (err=%v)", countOut, err)
	}
}

func TestSQLiteReadServer_MaterializeFromPageserverAndConcurrentMix(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: t.TempDir() + "/ps.sock",
		LayerDir:   t.TempDir() + "/layers",
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID  = "tenant-ps-sqlite"
		gen       = "gen-1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(41000)
		forkNum   = uint8(0)
		snapLSN   = uint64(0x3000500)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)

	// Build a valid PG17 8 KiB heap page with 3 tuples: 2 committed (id=10,val="ten" and id=20,val="twenty")
	// and 1 aborted/invisible (id=30,val="aborted").
	page := buildSyntheticPG17HeapPage(t, snapLSN)
	ctx := context.Background()
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, 0, 1, snapLSN, page); err != nil {
		t.Fatalf("PutPageBatch: %v", err)
	}

	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: 16 * PGPageSize,
		L2Dir:      t.TempDir() + "/l2",
		L2MaxBytes: 64 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend: %v", err)
	}
	coords := NewSnapshotCoordinator()
	coords.Publish(tenantID, gen, SnapshotInfo{LSN: snapLSN, Xmin: 100, Xmax: 200})

	adapter := NewAdapter(cache, coords)
	snap, err := adapter.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	defer adapter.ReleaseSnapshot(snap)

	rel, err := adapter.OpenRelation(ctx, RelationSpec{
		TenantID: tenantID, Generation: gen, SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum,
	})
	if err != nil {
		t.Fatalf("OpenRelation: %v", err)
	}
	defer adapter.CloseRelation(rel)

	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost: "127.0.0.1",
		PGPort:     0,
		DataDir:    t.TempDir(),
		Adapter:    adapter,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	nRows, err := srv.MaterializeRelation(
		ctx, adapter, rel, snap, nil,
		"accounts", []string{"id", "label"}, []ColumnType{ColInt4, ColText},
	)
	if err != nil {
		t.Fatalf("MaterializeRelation: %v", err)
	}
	if nRows != 2 {
		t.Fatalf("expected 2 visible rows materialized, got %d", nRows)
	}

	dsn := fmt.Sprintf("postgres://app_user@127.0.0.1:%d/%s?sslmode=disable", srv.Port(), tenantID)

	// Concurrent readers and blocked writers against SQLiteReadServer
	var wg sync.WaitGroup
	var readFails atomic.Int64
	var writeLeaks atomic.Int64

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := pgx.Connect(ctx, dsn)
			if err != nil {
				readFails.Add(1)
				return
			}
			defer c.Close(ctx)
			for i := 0; i < 10; i++ {
				var sum int
				if err := c.QueryRow(ctx, "SELECT sum(id) FROM accounts").Scan(&sum); err != nil || sum != 30 {
					readFails.Add(1)
				}
				if _, err := c.Exec(ctx, "INSERT INTO accounts VALUES (99, 'leak')"); err == nil {
					writeLeaks.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	if readFails.Load() != 0 {
		t.Fatalf("observed %d read failures under concurrent SQLite load", readFails.Load())
	}
	if writeLeaks.Load() != 0 {
		t.Fatalf("observed %d write leaks under concurrent SQLite load", writeLeaks.Load())
	}
}

func buildSyntheticPG17HeapPage(t *testing.T, lsn uint64) []byte {
	t.Helper()
	page := make([]byte, PGPageSize)
	binary.LittleEndian.PutUint32(page[0:4], uint32(lsn>>32))
	binary.LittleEndian.PutUint32(page[4:8], uint32(lsn&0xFFFFFFFF))

	encodeTuple := func(xmin uint32, infomask uint16, id int32, label string) []byte {
		const hoff = 24 // 23B header padded to 24B (4-byte aligned)
		shortHdr := byte((len(label)+1)<<1 | 0x01)
		tup := make([]byte, hoff+4+1+len(label))
		binary.LittleEndian.PutUint32(tup[0:4], xmin)
		binary.LittleEndian.PutUint32(tup[4:8], 0) // xmax = 0
		binary.LittleEndian.PutUint16(tup[18:20], 2)
		binary.LittleEndian.PutUint16(tup[20:22], infomask)
		tup[22] = hoff
		binary.LittleEndian.PutUint32(tup[hoff:hoff+4], uint32(id))
		tup[hoff+4] = shortHdr
		copy(tup[hoff+5:], []byte(label))
		return tup
	}

	t1 := encodeTuple(101, HeapXminCommitted, 10, "ten")
	t2 := encodeTuple(102, HeapXminCommitted, 20, "twenty")
	t3 := encodeTuple(103, HeapXminInvalid, 30, "aborted") // rolled back

	tuples := [][]byte{t1, t2, t3}
	upper := uint16(PGPageSize)
	lower := uint16(PGPageHeaderSize)

	for i, tup := range tuples {
		tlen := uint16(len(tup))
		upper -= tlen
		upper &= ^uint16(7) // 8-byte align
		copy(page[upper:upper+tlen], tup)
		lp := uint32(upper&0x7FFF) | (uint32(LPNormal&0x03) << 15) | (uint32(tlen&0x7FFF) << 17)
		binary.LittleEndian.PutUint32(page[PGPageHeaderSize+i*4:PGPageHeaderSize+(i+1)*4], lp)
		lower += 4
	}

	binary.LittleEndian.PutUint16(page[12:14], lower)
	binary.LittleEndian.PutUint16(page[14:16], upper)
	binary.LittleEndian.PutUint16(page[16:18], PGPageSize)
	binary.LittleEndian.PutUint16(page[18:20], PGExpectedPageSizeVer)
	return page
}
