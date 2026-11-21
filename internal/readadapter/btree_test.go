package readadapter

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

// buildSyntheticBTreePages constructs a valid single-leaf PostgreSQL B-tree index
// (block 0 = metapage pointing to root=1, block 1 = leaf page with int4 keys -> TIDs).
func buildSyntheticBTreePages(entries []struct {
	Key   int32
	Block uint32
	Off   uint16
}) (metaPage []byte, leafPage []byte) {
	metaPage = make([]byte, PGPageSize)
	// PageHeader for metapage: lower=24+40=64, upper=8176, special=8176
	binary.LittleEndian.PutUint16(metaPage[12:14], 64)
	binary.LittleEndian.PutUint16(metaPage[14:16], 8176)
	binary.LittleEndian.PutUint16(metaPage[16:18], 8176)
	binary.LittleEndian.PutUint16(metaPage[18:20], PGExpectedPageSizeVer)
	// BTMetaPageData at offset 24
	binary.LittleEndian.PutUint32(metaPage[24:28], PGBTMetaMagic) // btm_magic
	binary.LittleEndian.PutUint32(metaPage[28:32], 4)             // btm_version
	binary.LittleEndian.PutUint32(metaPage[32:36], 1)             // btm_root = block 1
	binary.LittleEndian.PutUint32(metaPage[36:40], 0)             // btm_level = 0 (leaf)
	binary.LittleEndian.PutUint32(metaPage[40:44], 1)             // btm_fastroot = block 1
	binary.LittleEndian.PutUint32(metaPage[44:48], 0)             // btm_fastlevel = 0
	// BTPageOpaqueData at 8176: btpo_flags = BTP_META (8)
	binary.LittleEndian.PutUint32(metaPage[8176:8180], BTPNone)
	binary.LittleEndian.PutUint32(metaPage[8180:8184], BTPNone)
	binary.LittleEndian.PutUint16(metaPage[8188:8190], BTPMeta)

	leafPage = make([]byte, PGPageSize)
	specialOff := uint16(8176)
	upper := specialOff
	lower := uint16(PGPageHeaderSize)

	for i, e := range entries {
		// IndexTupleData: 6B t_tid + 2B t_info (size=12) + 4B int4 key = 12 bytes (MAXALIGN(12)=16)
		tupLen := uint16(12)
		upper -= 16
		tup := leafPage[upper : upper+tupLen]
		// t_tid: bi_hi, bi_lo, ip_posid
		binary.LittleEndian.PutUint16(tup[0:2], uint16(e.Block>>16))
		binary.LittleEndian.PutUint16(tup[2:4], uint16(e.Block&0xFFFF))
		binary.LittleEndian.PutUint16(tup[4:6], e.Off)
		// t_info: size=12 (no nulls, no varwidths)
		binary.LittleEndian.PutUint16(tup[6:8], tupLen)
		// int4 key
		binary.LittleEndian.PutUint32(tup[8:12], uint32(e.Key))

		// Line pointer
		lp := (uint32(upper) & 0x7FFF) | (uint32(LPNormal) << 15) | ((uint32(tupLen) & 0x7FFF) << 17)
		lpOff := PGPageHeaderSize + i*4
		binary.LittleEndian.PutUint32(leafPage[lpOff:lpOff+4], lp)
		lower = uint16(lpOff + 4)
	}

	binary.LittleEndian.PutUint16(leafPage[12:14], lower)
	binary.LittleEndian.PutUint16(leafPage[14:16], upper)
	binary.LittleEndian.PutUint16(leafPage[16:18], specialOff)
	binary.LittleEndian.PutUint16(leafPage[18:20], PGExpectedPageSizeVer)
	// BTPageOpaqueData at 8176: prev=NONE, next=NONE, level=0, flags=BTP_LEAF|BTP_ROOT
	binary.LittleEndian.PutUint32(leafPage[8176:8180], BTPNone)
	binary.LittleEndian.PutUint32(leafPage[8180:8184], BTPNone)
	binary.LittleEndian.PutUint32(leafPage[8184:8188], 0)
	binary.LittleEndian.PutUint16(leafPage[8188:8190], BTPLeaf|BTPRoot)

	return metaPage, leafPage
}

func makeInt4TextPayload(id int32, text string) []byte {
	var tb tupleBuilder
	tb.addInt4(id)
	tb.addText(text)
	return tb.buf
}

// TestPhase14A_StaleIndexEntryAndHOTValidation verifies that B-tree index scan
// never trusts a stale index entry whose referenced heap tuple has a different
// key value or is invisible under MVCC, and that it follows HOT chains on the
// same page to find the visible updated tuple.
func TestPhase14A_StaleIndexEntryAndHOTValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}
	defer ps.Close()

	const (
		tenantID = "t_btree"
		gen      = "gen-1"
		lsn      = uint64(0x2000000)
		spcOID   = uint32(1663)
		dbOID    = uint32(16384)
		heapRel  = uint32(16500)
		idxRel   = uint32(16501)
	)
	ps.SetTenantGeneration(tenantID, gen)
	_ = ps.EnsureTenant(tenantID, 1)

	// Build a heap page (rel 16500, block 0) with:
	// - Off 1: id=10, val="stale_mismatch" (index claims key=999 points to (0,1), which actually has id=10! Must be rejected!)
	// - Off 2: id=20, val="v1_old" (updated in-place via HOT to Off 3 with xmax=100, t_ctid=(0,3), HEAP_HOT_UPDATED)
	// - Off 3: id=20, val="v2_hot_updated" (xmin=100, HEAP_ONLY_TUPLE)
	// - Off 4: id=30, val="deleted_tuple" (xmin=100, xmax=101 committed delete -> invisible)
	t1 := buildHeapTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, makeInt4TextPayload(10, "stale_mismatch"))
	t2 := buildHeapTuple(100, 100, HeapXminCommitted|HeapXmaxCommitted, makeInt4TextPayload(20, "v1_old"))
	// Mark t2 as HEAP_HOT_UPDATED (0x4000) in Infomask2 and set t_ctid = (0, 3)
	binary.LittleEndian.PutUint16(t2[12:14], 0)
	binary.LittleEndian.PutUint16(t2[14:16], 0)
	binary.LittleEndian.PutUint16(t2[16:18], 3) // t_ctid.ip_posid = 3
	binary.LittleEndian.PutUint16(t2[18:20], HeapHOTUpdate)

	t3 := buildHeapTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, makeInt4TextPayload(20, "v2_hot_updated"))
	binary.LittleEndian.PutUint16(t3[18:20], HeapOnlyTuple)

	t4 := buildHeapTuple(100, 101, HeapXminCommitted|HeapXmaxCommitted, makeInt4TextPayload(30, "deleted_tuple"))

	heapPage := buildPG17HeapPage(lsn, [][]byte{t1, t2, t3, t4})
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, heapRel, 0, 0, 1, lsn, heapPage); err != nil {
		t.Fatalf("PutPageBatch heap: %v", err)
	}

	// Build B-tree index on id (col 0):
	// - key 20 -> (0, 2) (points to root of HOT chain, should return "v2_hot_updated" from (0, 3)!)
	// - key 30 -> (0, 4) (points to deleted tuple, should return 0 rows!)
	// - key 999 -> (0, 1) (stale index entry pointing to id=10, should be rejected by predicate re-validation!)
	metaPg, leafPg := buildSyntheticBTreePages([]struct {
		Key   int32
		Block uint32
		Off   uint16
	}{
		{Key: 20, Block: 0, Off: 2},
		{Key: 30, Block: 0, Off: 4},
		{Key: 999, Block: 0, Off: 1},
	})
	idxBatch := append(append([]byte{}, metaPg...), leafPg...)
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, idxRel, 0, 0, 2, lsn, idxBatch); err != nil {
		t.Fatalf("PutPageBatch index: %v", err)
	}

	snapProv := NewStaticSnapshotProvider()
	snapProv.SetSnapshot(tenantID, gen, SnapshotInfo{LSN: lsn})
	adapter := NewAdapter(ps, snapProv)
	clog := NewCLOGResolver(ps)

	tbl := DiscoveredTable{
		Schema:    "public",
		RelOID:    heapRel,
		Name:      "items",
		SpcOID:    spcOID,
		DbOID:     dbOID,
		RelNumber: heapRel,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "val", AttNum: 2, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
		},
		Indexes: []DiscoveredIndex{
			{
				IndexRelOID: idxRel,
				TableRelOID: heapRel,
				Name:        "items_pkey",
				SpcOID:      spcOID,
				DbOID:       dbOID,
				RelNumber:   idxRel,
				IndNAtts:    1,
				IndNKeyAtts: 1,
				IsUnique:    true,
				IsPrimary:   true,
				KeyAttNums:  []int16{1},
				KeyOptions:  []int16{0},
			},
		},
	}

	conn, err := OpenSQLiteConn(":memory:", false, 2000)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer conn.Close()
	if err := conn.RegisterAirPGModule(); err != nil {
		t.Fatalf("RegisterAirPGModule: %v", err)
	}

	vtabID, err := conn.RegisterNativeTable(tenantID, gen, tbl, adapter, clog)
	if err != nil {
		t.Fatalf("RegisterNativeTable: %v", err)
	}
	defer UnregisterNativeTable(vtabID)

	_, scansBefore := VTabIndexStats()
	// 1. Stale index entry (key=999 points to id=10): must return 0 rows!
	resStale, err := conn.Query("SELECT id, val FROM items WHERE id = 999;", nil)
	if err != nil {
		t.Fatalf("query stale key 999: %v", err)
	}
	if len(resStale.Rows) != 0 {
		t.Fatalf("expected 0 rows for stale index entry key=999, got %+v", resStale.Rows)
	}

	// 2. Deleted tuple (key=30 points to deleted tuple): must return 0 rows!
	resDel, err := conn.Query("SELECT id, val FROM items WHERE id = 30;", nil)
	if err != nil {
		t.Fatalf("query deleted key 30: %v", err)
	}
	if len(resDel.Rows) != 0 {
		t.Fatalf("expected 0 rows for deleted tuple key=30, got %+v", resDel.Rows)
	}

	// 3. HOT updated tuple (key=20 points to (0,2) -> HOT chain -> (0,3)): must return "v2_hot_updated"!
	resHOT, err := conn.Query("SELECT id, val FROM items WHERE id = 20;", nil)
	if err != nil {
		t.Fatalf("query HOT key 20: %v", err)
	}
	if len(resHOT.Rows) != 1 || resHOT.Rows[0][1].Text != "v2_hot_updated" {
		t.Fatalf("expected HOT updated row [20 v2_hot_updated], got %+v", resHOT.Rows)
	}
	_, scansAfter := VTabIndexStats()
	if scansAfter-scansBefore != 3 {
		t.Fatalf("expected 3 B-tree index scans in TestPhase14A_StaleIndexEntryAndHOTValidation, got %d", scansAfter-scansBefore)
	}
}

// TestPhase14A_LivePG15_BTreeIndexPushdownSuite runs the full Phase 14A B-tree
// index scan verification matrix against a live PostgreSQL 15 instance with
// WAL streaming into Pageserver, TieredCacheBackend (L1 RAM + L2 NVMe), and
// SQLiteReadServer, comparing every query result against PostgreSQL 15.
func TestPhase14A_LivePG15_BTreeIndexPushdownSuite(t *testing.T) {
	initdbBin := "/usr/lib/postgresql/15/bin/initdb"
	pgBin := "/usr/lib/postgresql/15/bin/postgres"
	if _, err := os.Stat(initdbBin); err != nil {
		t.Skip("PostgreSQL 15 binaries not installed")
	}

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
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\nautovacuum = off\n")
	_ = f.Close()

	hf, err := os.OpenFile(filepath.Join(pgData, "pg_hba.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	const pgPort = "55445"
	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", pgPort, "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=%s user=postgres dbname=postgres sslmode=disable", sockDir, pgPort)
	var pgConn *pgx.Conn
	for i := 0; i < 40; i++ {
		pgConn, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pgConn == nil {
		t.Fatalf("could not connect to PG15")
	}
	defer pgConn.Close(ctx)

	if _, err := pgConn.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatalf("CHECKPOINT: %v", err)
	}
	var lsnStr string
	if err := pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr); err != nil {
		t.Fatalf("pg_current_wal_lsn: %v", err)
	}
	initLSN := ParsePGLSN(lsnStr)

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

	const tenantID = "tenant_p14_btree"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatalf("BootstrapTenantFromPGData: %v", err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() {
		_ = rx.Run(rxCtx)
	}()

	waitForWAL := func() uint64 {
		var cur string
		if err := pgConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&cur); err != nil {
			t.Fatalf("pg_current_wal_lsn: %v", err)
		}
		target := ParsePGLSN(cur)
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if tl.LastLSN() >= target-8 {
				return tl.LastLSN()
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatalf("WALReceiver did not reach target LSN %s (got %s)", cur, FormatPGLSN(tl.LastLSN()))
		return 0
	}

	// Create schema with:
	// 1. Primary key index (users_pkey on id)
	// 2. Secondary index with duplicate values (idx_users_category on category -> B-tree v4 posting lists)
	// 3. Composite index with ASC and DESC columns (idx_users_region_score on (region ASC, score DESC))
	// 4. Index on nullable column (idx_users_tag on nullable_tag)
	// Insert 2,000 rows so the B-tree splits across multiple leaf pages and creates a root internal page (level >= 1)!
	if _, err := pgConn.Exec(ctx, `
		CREATE TABLE users (
			id int4 PRIMARY KEY,
			category text NOT NULL,
			region text NOT NULL,
			score int4 NOT NULL,
			nullable_tag text,
			payload text NOT NULL
		);
		CREATE INDEX idx_users_category ON users (category);
		CREATE INDEX idx_users_region_score ON users (region ASC, score DESC);
		CREATE INDEX idx_users_tag ON users (nullable_tag);

		INSERT INTO users (id, category, region, score, nullable_tag, payload)
		SELECT
			i,
			'cat_' || to_char(i % 20, 'FM00'),
			CASE WHEN i % 3 = 0 THEN 'us-east' WHEN i % 3 = 1 THEN 'us-west' ELSE 'eu-central' END,
			i * 10,
			CASE WHEN i % 50 = 0 THEN NULL WHEN i % 25 = 0 THEN 'special_tag' ELSE 'tag_' || i::text END,
			repeat('payload_', 12) || i::text
		FROM generate_series(1, 2000) AS s(i);
	`); err != nil {
		t.Fatalf("create and populate users: %v", err)
	}
	waitForWAL()

	// Start SQLiteReadServer backed by PageserverSocketBackend + TieredCacheBackend
	readSrv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          filepath.Join(tmpDir, "readsrv"),
		PageserverSocket: psSock,
		CacheDir:         filepath.Join(tmpDir, "l2cache"),
		L1CacheBytes:     32 * 1024 * 1024,
		ReadersPerTenant: 4,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := readSrv.Start(); err != nil {
		t.Fatalf("readSrv.Start: %v", err)
	}
	defer readSrv.Stop()

	airDSN := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s?sslmode=disable", readSrv.Port(), tenantID)
	airConn, err := pgx.Connect(ctx, airDSN)
	if err != nil {
		t.Fatalf("connect AIR read server: %v", err)
	}
	defer airConn.Close(ctx)

	queryBothAndCompare := func(name, sqlQuery string) [][]string {
		t.Helper()
		fetchRows := func(c *pgx.Conn) ([][]string, error) {
			rows, qerr := c.Query(ctx, sqlQuery)
			if qerr != nil {
				return nil, qerr
			}
			defer rows.Close()
			var out [][]string
			for rows.Next() {
				vals, verr := rows.Values()
				if verr != nil {
					return nil, verr
				}
				row := make([]string, len(vals))
				for i, v := range vals {
					if v == nil {
						row[i] = "<NULL>"
					} else {
						row[i] = fmt.Sprint(v)
					}
				}
				out = append(out, row)
			}
			return out, rows.Err()
		}

		pgRows, err := fetchRows(pgConn)
		if err != nil {
			t.Fatalf("[%s] PG query failed (%s): %v", name, sqlQuery, err)
		}
		airRows, err := fetchRows(airConn)
		if err != nil {
			t.Fatalf("[%s] AIR query failed (%s): %v", name, sqlQuery, err)
		}
		if !reflect.DeepEqual(pgRows, airRows) {
			t.Fatalf("[%s] result mismatch for %q:\nPG (%d rows):  %+v\nAIR (%d rows): %+v",
				name, sqlQuery, len(pgRows), pgRows, len(airRows), airRows)
		}
		return airRows
	}

	// Warm catalog on all 4 reader slots so subsequent page read counts measure pure table/index execution
	for i := 0; i < 4; i++ {
		queryBothAndCompare("warmup_pk", "SELECT id, category, region, score, payload FROM users WHERE id = 1")
	}

	// 1. Primary-key point lookup & page count verification (target: 1-4 pages read vs 45+ heap pages)
	heapPagesBefore, _, _, _ := VTabStats()
	idxPagesBefore, idxScansBefore := VTabIndexStats()
	rows := queryBothAndCompare("pk_lookup", "SELECT id, category, region, score, payload FROM users WHERE id = 1234")
	heapPagesAfter, _, _, _ := VTabStats()
	idxPagesAfter, idxScansAfter := VTabIndexStats()

	if len(rows) != 1 || rows[0][0] != "1234" {
		t.Fatalf("unexpected pk_lookup rows: %+v", rows)
	}
	heapDelta := (heapPagesAfter - heapPagesBefore) - (idxPagesAfter - idxPagesBefore)
	idxDelta := idxPagesAfter - idxPagesBefore
	scansDelta := idxScansAfter - idxScansBefore
	if scansDelta != 1 {
		t.Fatalf("expected 1 B-tree index scan for PK point lookup, got %d", scansDelta)
	}
	if heapDelta != 1 || idxDelta < 2 || idxDelta > 4 {
		t.Fatalf("expected 1 heap page + 2..4 index pages for PK lookup on 2000-row split B-tree, got heap=%d index=%d", heapDelta, idxDelta)
	}
	t.Logf("PK point lookup on 2,000-row table: index_pages=%d, heap_pages=%d (total=%d pages vs ~45 heap pages seq scan)",
		idxDelta, heapDelta, idxDelta+heapDelta)

	// 2. Missing key lookup
	rows = queryBothAndCompare("missing_key", "SELECT id, category FROM users WHERE id = 999999")
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows for missing key, got %d", len(rows))
	}

	// 3. Secondary-index lookup with duplicate values (B-tree v4 deduplicated posting lists: 100 rows per category)
	rows = queryBothAndCompare("secondary_idx_duplicates", "SELECT id, category, score FROM users WHERE category = 'cat_07' ORDER BY id")
	if len(rows) != 100 {
		t.Fatalf("expected 100 rows for category='cat_07', got %d", len(rows))
	}

	// 4. Composite-index lookup (full key: region EQ + score EQ, and prefix key + DESC range)
	rows = queryBothAndCompare("composite_full_eq", "SELECT id, region, score FROM users WHERE region = 'us-east' AND score = 3000")
	if len(rows) != 1 || rows[0][0] != "300" {
		t.Fatalf("expected id=300 for composite full eq, got %+v", rows)
	}
	rows = queryBothAndCompare("composite_prefix_range_desc", "SELECT id, region, score FROM users WHERE region = 'us-west' AND score >= 19900 AND score <= 20000 ORDER BY id")
	if len(rows) == 0 {
		t.Fatalf("expected rows for composite prefix + DESC column range")
	}

	// 5. Indexed range scan (BETWEEN / >= AND <=)
	rows = queryBothAndCompare("pk_range_scan", "SELECT id, category, score FROM users WHERE id >= 500 AND id <= 515 ORDER BY id")
	if len(rows) != 16 {
		t.Fatalf("expected 16 rows for range 500..515, got %d", len(rows))
	}

	// 6. NULL values on indexed column
	rows = queryBothAndCompare("null_index_lookup", "SELECT id FROM users WHERE nullable_tag IS NULL ORDER BY id")
	if len(rows) != 40 {
		t.Fatalf("expected 40 rows with nullable_tag IS NULL, got %d", len(rows))
	}

	// 7. Index page cache hit verification
	cacheBefore := readSrv.DetailedMetrics().Cache
	for i := 0; i < 20; i++ {
		queryBothAndCompare("cached_pk_lookup", "SELECT id, payload FROM users WHERE id = 1234")
	}
	cacheAfter := readSrv.DetailedMetrics().Cache
	l1HitsDelta := cacheAfter.L1Hits - cacheBefore.L1Hits
	l3FetchDelta := cacheAfter.L3Fetches - cacheBefore.L3Fetches
	if l1HitsDelta < 20 || l3FetchDelta != 0 {
		t.Fatalf("expected all index + heap pages served from L1 cache (l1HitsDelta=%d, l3FetchDelta=%d)", l1HitsDelta, l3FetchDelta)
	}

	// 8. HOT update (updating unindexed column `payload`), non-HOT update (updating indexed column `category`),
	// DELETE visibility, INSERT visibility, and Snapshot Consistency!
	// Pin a snapshot BEFORE the mutations to verify snapshot isolation on B-tree index scan:
	gen := readSrv.nativeSock.GetTenantGeneration(tenantID)
	oldSnap, err := readSrv.nativeAdapter.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatalf("GetSnapshot oldSnap: %v", err)
	}
	defer readSrv.nativeAdapter.ReleaseSnapshot(oldSnap)

	if _, err := pgConn.Exec(ctx, `
		-- HOT update on id=100 (only non-indexed payload changed)
		UPDATE users SET payload = 'hot_updated_payload_100' WHERE id = 100;
		-- Non-HOT update on id=200 (indexed category and score changed)
		UPDATE users SET category = 'cat_updated', score = 999999 WHERE id = 200;
		-- Delete id=300
		DELETE FROM users WHERE id = 300;
		-- Insert new row id=2500
		INSERT INTO users VALUES (2500, 'cat_new', 'us-east', 25000, 'tag_2500', 'inserted_payload_2500');
	`); err != nil {
		t.Fatalf("mutations failed: %v", err)
	}
	waitForWAL()

	// Verify latest state matches PostgreSQL via B-tree index lookups:
	rows = queryBothAndCompare("after_hot_update", "SELECT id, payload FROM users WHERE id = 100")
	if len(rows) != 1 || rows[0][1] != "hot_updated_payload_100" {
		t.Fatalf("HOT update mismatch: %+v", rows)
	}
	rows = queryBothAndCompare("after_non_hot_update_by_pk", "SELECT id, category, score FROM users WHERE id = 200")
	if len(rows) != 1 || rows[0][1] != "cat_updated" || rows[0][2] != "999999" {
		t.Fatalf("non-HOT update by PK mismatch: %+v", rows)
	}
	rows = queryBothAndCompare("after_non_hot_update_by_new_sec_idx", "SELECT id, category FROM users WHERE category = 'cat_updated'")
	if len(rows) != 1 || rows[0][0] != "200" {
		t.Fatalf("non-HOT update by new secondary index key mismatch: %+v", rows)
	}
	// Old secondary index entry for id=200 ('cat_00') must NOT return id=200!
	rows = queryBothAndCompare("after_non_hot_update_by_old_sec_idx", "SELECT id FROM users WHERE category = 'cat_00' AND id = 200")
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows for old secondary index key of id=200, got %+v", rows)
	}
	rows = queryBothAndCompare("after_delete", "SELECT id FROM users WHERE id = 300")
	if len(rows) != 0 {
		t.Fatalf("expected deleted id=300 to be invisible, got %+v", rows)
	}
	rows = queryBothAndCompare("after_insert", "SELECT id, category, payload FROM users WHERE id = 2500")
	if len(rows) != 1 || rows[0][2] != "inserted_payload_2500" {
		t.Fatalf("expected inserted id=2500 to be visible, got %+v", rows)
	}

	// Verify oldSnap (pinned at pre-mutation LSN) still sees pre-mutation values when scanned!
	oldTables, err := DiscoverCatalog(ctx, readSrv.nativeAdapter, readSrv.nativeCLOG, oldSnap, tenantID)
	if err != nil {
		t.Fatalf("DiscoverCatalog at oldSnap: %v", err)
	}
	var oldUsersTbl DiscoveredTable
	for _, tbl := range oldTables {
		if tbl.Name == "users" {
			oldUsersTbl = tbl
			break
		}
	}
	snapConn, err := OpenSQLiteConn(":memory:", false, 2000)
	if err != nil {
		t.Fatal(err)
	}
	defer snapConn.Close()
	if err := snapConn.RegisterAirPGModule(); err != nil {
		t.Fatal(err)
	}
	snapVTabID, err := snapConn.RegisterNativeTable(tenantID, gen, oldUsersTbl, readSrv.nativeAdapter, readSrv.nativeCLOG)
	if err != nil {
		t.Fatal(err)
	}
	defer UnregisterNativeTable(snapVTabID)
	oldRes, err := snapConn.QueryWithContext(ctx, oldSnap, "SELECT id, payload FROM users WHERE id = 300;", nil)
	if err != nil {
		t.Fatalf("oldSnap query id=300: %v", err)
	}
	if len(oldRes.Rows) != 1 || oldRes.Rows[0][0].Int64 != 300 {
		t.Fatalf("expected oldSnap to still see deleted id=300 via B-tree index scan, got %+v", oldRes.Rows)
	}

	// 9. Reindex behavior (REINDEX TABLE users changes relfilenode of all indexes on users)
	if _, err := pgConn.Exec(ctx, "REINDEX TABLE users;"); err != nil {
		t.Fatalf("REINDEX TABLE users: %v", err)
	}
	waitForWAL()

	rows = queryBothAndCompare("after_reindex_pk", "SELECT id, category, payload FROM users WHERE id = 100")
	if len(rows) != 1 || rows[0][2] != "hot_updated_payload_100" {
		t.Fatalf("after REINDEX PK lookup failed: %+v", rows)
	}
	rows = queryBothAndCompare("after_reindex_sec", "SELECT id, category FROM users WHERE category = 'cat_updated'")
	if len(rows) != 1 || rows[0][0] != "200" {
		t.Fatalf("after REINDEX secondary index lookup failed: %+v", rows)
	}
}

// BenchmarkPhase14A_BTreePointLookupVsSeqScan benchmarks a point lookup
// (`SELECT id, val FROM bench_items WHERE id = 2500`) on a 5,000-row table
// across 100 heap pages with B-tree index pushdown enabled vs sequential heap scan.
func BenchmarkPhase14A_BTreePointLookupVsSeqScan(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := b.TempDir()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		b.Fatal(err)
	}
	defer ps.Close()

	const (
		tenantID    = "t_bench"
		gen         = "gen-1"
		lsn         = uint64(0x2000000)
		spcOID      = uint32(1663)
		dbOID       = uint32(16384)
		heapRel     = uint32(17000)
		idxRel      = uint32(17001)
		numPages    = 100
		rowsPerPage = 50
	)
	ps.SetTenantGeneration(tenantID, gen)
	_ = ps.EnsureTenant(tenantID, 1)

	var idxEntries []struct {
		Key   int32
		Block uint32
		Off   uint16
	}
	for blk := uint32(0); blk < numPages; blk++ {
		var tuples [][]byte
		for r := 0; r < rowsPerPage; r++ {
			id := int32(int(blk)*rowsPerPage + r + 1)
			tup := buildHeapTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, makeInt4TextPayload(id, fmt.Sprintf("bench_payload_%05d", id)))
			tuples = append(tuples, tup)
			idxEntries = append(idxEntries, struct {
				Key   int32
				Block uint32
				Off   uint16
			}{Key: id, Block: blk, Off: uint16(r + 1)})
		}
		pg := buildPG17HeapPage(lsn, tuples)
		if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, heapRel, 0, blk, 1, lsn, pg); err != nil {
			b.Fatal(err)
		}
	}

	// Build a 2-level B-tree (block 0 = meta -> root=1, block 1 = internal root pointing to leaf blocks 2..21)
	const entriesPerLeaf = 250
	numLeaves := len(idxEntries) / entriesPerLeaf
	metaPage := make([]byte, PGPageSize)
	binary.LittleEndian.PutUint16(metaPage[12:14], 64)
	binary.LittleEndian.PutUint16(metaPage[14:16], 8176)
	binary.LittleEndian.PutUint16(metaPage[16:18], 8176)
	binary.LittleEndian.PutUint16(metaPage[18:20], PGExpectedPageSizeVer)
	binary.LittleEndian.PutUint32(metaPage[24:28], PGBTMetaMagic)
	binary.LittleEndian.PutUint32(metaPage[28:32], 4)
	binary.LittleEndian.PutUint32(metaPage[32:36], 1) // root = block 1
	binary.LittleEndian.PutUint32(metaPage[36:40], 1) // level = 1
	binary.LittleEndian.PutUint32(metaPage[40:44], 1)
	binary.LittleEndian.PutUint32(metaPage[44:48], 1)
	binary.LittleEndian.PutUint32(metaPage[8176:8180], BTPNone)
	binary.LittleEndian.PutUint32(metaPage[8180:8184], BTPNone)
	binary.LittleEndian.PutUint16(metaPage[8188:8190], BTPMeta)
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, idxRel, 0, 0, 1, lsn, metaPage); err != nil {
		b.Fatal(err)
	}

	rootPage := make([]byte, PGPageSize)
	rootUpper := uint16(8176)
	rootLower := uint16(PGPageHeaderSize)

	for l := 0; l < numLeaves; l++ {
		leafBlk := uint32(2 + l)
		slice := idxEntries[l*entriesPerLeaf : (l+1)*entriesPerLeaf]
		leafPage := make([]byte, PGPageSize)
		upper := uint16(8176)
		lower := uint16(PGPageHeaderSize)
		lpIdx := 0
		if l+1 < numLeaves {
			// Non-rightmost leaf pages in PostgreSQL B-tree have P_HIKEY at OffsetNumber 1 (lp[0]).
			hkKey := idxEntries[(l+1)*entriesPerLeaf].Key
			tupLen := uint16(12)
			upper -= 16
			tup := leafPage[upper : upper+tupLen]
			binary.LittleEndian.PutUint16(tup[4:6], 1)
			binary.LittleEndian.PutUint16(tup[6:8], tupLen|IndexAltTIDMask)
			binary.LittleEndian.PutUint32(tup[8:12], uint32(hkKey))
			lp := (uint32(upper) & 0x7FFF) | (uint32(LPNormal) << 15) | ((uint32(tupLen) & 0x7FFF) << 17)
			lpOff := PGPageHeaderSize + lpIdx*4
			binary.LittleEndian.PutUint32(leafPage[lpOff:lpOff+4], lp)
			lpIdx++
			lower = uint16(lpOff + 4)
		}
		for _, e := range slice {
			tupLen := uint16(12)
			upper -= 16
			tup := leafPage[upper : upper+tupLen]
			binary.LittleEndian.PutUint16(tup[0:2], uint16(e.Block>>16))
			binary.LittleEndian.PutUint16(tup[2:4], uint16(e.Block&0xFFFF))
			binary.LittleEndian.PutUint16(tup[4:6], e.Off)
			binary.LittleEndian.PutUint16(tup[6:8], tupLen)
			binary.LittleEndian.PutUint32(tup[8:12], uint32(e.Key))
			lp := (uint32(upper) & 0x7FFF) | (uint32(LPNormal) << 15) | ((uint32(tupLen) & 0x7FFF) << 17)
			lpOff := PGPageHeaderSize + lpIdx*4
			binary.LittleEndian.PutUint32(leafPage[lpOff:lpOff+4], lp)
			lpIdx++
			lower = uint16(lpOff + 4)
		}
		binary.LittleEndian.PutUint16(leafPage[12:14], lower)
		binary.LittleEndian.PutUint16(leafPage[14:16], upper)
		binary.LittleEndian.PutUint16(leafPage[16:18], 8176)
		binary.LittleEndian.PutUint16(leafPage[18:20], PGExpectedPageSizeVer)
		prevBlk := BTPNone
		if l > 0 {
			prevBlk = uint32(1 + l)
		}
		nextBlk := BTPNone
		if l+1 < numLeaves {
			nextBlk = uint32(3 + l)
		}
		binary.LittleEndian.PutUint32(leafPage[8176:8180], prevBlk)
		binary.LittleEndian.PutUint32(leafPage[8180:8184], nextBlk)
		binary.LittleEndian.PutUint32(leafPage[8184:8188], 0)
		binary.LittleEndian.PutUint16(leafPage[8188:8190], BTPLeaf)
		if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, idxRel, 0, leafBlk, 1, lsn, leafPage); err != nil {
			b.Fatal(err)
		}

		// Add downlink to rootPage (first downlink is minus-infinity with 0 key attrs)
		tupLen := uint16(12)
		rootUpper -= 16
		rtup := rootPage[rootUpper : rootUpper+tupLen]
		binary.LittleEndian.PutUint16(rtup[0:2], uint16(leafBlk>>16))
		binary.LittleEndian.PutUint16(rtup[2:4], uint16(leafBlk&0xFFFF))
		if l == 0 {
			// minus-infinity downlink: 0 key attrs
			binary.LittleEndian.PutUint16(rtup[4:6], 0)
			binary.LittleEndian.PutUint16(rtup[6:8], 8|IndexAltTIDMask)
		} else {
			binary.LittleEndian.PutUint16(rtup[4:6], 1)
			binary.LittleEndian.PutUint16(rtup[6:8], tupLen|IndexAltTIDMask)
			binary.LittleEndian.PutUint32(rtup[8:12], uint32(slice[0].Key))
		}
		rlp := (uint32(rootUpper) & 0x7FFF) | (uint32(LPNormal) << 15) | ((uint32(tupLen) & 0x7FFF) << 17)
		rlpOff := PGPageHeaderSize + l*4
		binary.LittleEndian.PutUint32(rootPage[rlpOff:rlpOff+4], rlp)
		rootLower = uint16(rlpOff + 4)
	}
	binary.LittleEndian.PutUint16(rootPage[12:14], rootLower)
	binary.LittleEndian.PutUint16(rootPage[14:16], rootUpper)
	binary.LittleEndian.PutUint16(rootPage[16:18], 8176)
	binary.LittleEndian.PutUint16(rootPage[18:20], PGExpectedPageSizeVer)
	binary.LittleEndian.PutUint32(rootPage[8176:8180], BTPNone)
	binary.LittleEndian.PutUint32(rootPage[8180:8184], BTPNone)
	binary.LittleEndian.PutUint32(rootPage[8184:8188], 1) // level = 1
	binary.LittleEndian.PutUint16(rootPage[8188:8190], BTPRoot)
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, idxRel, 0, 1, 1, lsn, rootPage); err != nil {
		b.Fatal(err)
	}

	cache, err := NewTieredCacheBackend(ps, CacheConfig{L1MaxBytes: 256 * PGPageSize})
	if err != nil {
		b.Fatal(err)
	}
	snapProv := NewStaticSnapshotProvider()
	snapProv.SetSnapshot(tenantID, gen, SnapshotInfo{LSN: lsn})
	adapter := NewAdapter(cache, snapProv)
	clog := NewCLOGResolver(cache)

	cols := []DiscoveredColumn{
		{Name: "id", AttNum: 1, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
		{Name: "val", AttNum: 2, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
	}

	b.Run("BTreeIndexPushdown_5000rows_100pages", func(b *testing.B) {
		tbl := DiscoveredTable{
			Schema:    "public",
			RelOID:    heapRel,
			Name:      "bench_items",
			SpcOID:    spcOID,
			DbOID:     dbOID,
			RelNumber: heapRel,
			Columns:   cols,
			Indexes: []DiscoveredIndex{
				{
					IndexRelOID: idxRel,
					TableRelOID: heapRel,
					Name:        "bench_items_pkey",
					SpcOID:      spcOID,
					DbOID:       dbOID,
					RelNumber:   idxRel,
					IndNAtts:    1,
					IndNKeyAtts: 1,
					IsUnique:    true,
					IsPrimary:   true,
					KeyAttNums:  []int16{1},
					KeyOptions:  []int16{0},
				},
			},
		}
		conn, err := OpenSQLiteConn(":memory:", false, 2000)
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()
		if err := conn.RegisterAirPGModule(); err != nil {
			b.Fatal(err)
		}
		vid, err := conn.RegisterNativeTable(tenantID, gen, tbl, adapter, clog)
		if err != nil {
			b.Fatal(err)
		}
		defer UnregisterNativeTable(vid)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, err := conn.Query("SELECT id, val FROM bench_items WHERE id = 2500;", nil)
			if err != nil || len(res.Rows) != 1 {
				b.Fatalf("query failed: err=%v rows=%d", err, len(res.Rows))
			}
		}
	})

	b.Run("SequentialHeapScan_5000rows_100pages", func(b *testing.B) {
		tbl := DiscoveredTable{
			Schema:    "public",
			RelOID:    heapRel,
			Name:      "bench_items_no_idx",
			SpcOID:    spcOID,
			DbOID:     dbOID,
			RelNumber: heapRel,
			Columns:   cols,
		}
		conn, err := OpenSQLiteConn(":memory:", false, 2000)
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()
		if err := conn.RegisterAirPGModule(); err != nil {
			b.Fatal(err)
		}
		vid, err := conn.RegisterNativeTable(tenantID, gen, tbl, adapter, clog)
		if err != nil {
			b.Fatal(err)
		}
		defer UnregisterNativeTable(vid)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, err := conn.Query("SELECT id, val FROM bench_items_no_idx WHERE id = 2500;", nil)
			if err != nil || len(res.Rows) != 1 {
				b.Fatalf("query failed: err=%v rows=%d", err, len(res.Rows))
			}
		}
	})
}
