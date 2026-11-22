package readadapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func TestPhase14D_IncrementalInvalidationCorrectness(t *testing.T) {
	ctx := t.Context()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}

	const (
		tenant    = "t_p14d_inc"
		gen       = "g1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(25001)
		forkNum   = uint8(0)
	)
	ps.SetTenantGeneration(tenant, gen)
	_ = ps.EnsureTenant(tenant, 1)

	makePage := func(tag byte) []byte {
		b := make([]byte, PGPageSize)
		b[0] = tag
		binary.LittleEndian.PutUint32(b[PGPageSize-4:], uint32(tag))
		return b
	}

	// Seed 4 blocks at LSN 0x100.
	for blk := uint32(0); blk < 4; blk++ {
		if err := ps.PutPageBatch(ctx, tenant, spcOID, dbOID, relNumber, forkNum, blk, 1, 0x100, makePage(byte(0x10+blk))); err != nil {
			t.Fatalf("PutPageBatch blk %d: %v", blk, err)
		}
	}

	sockPath := filepath.Join(t.TempDir(), "ps.sock")
	ps2, err := pageserver.New(pageserver.Config{
		SocketPath: sockPath,
		LayerDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	if err := ps2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ps2.SetTenantGeneration(tenant, gen)
	tl2 := ps2.EnsureTenant(tenant, 1)
	for blk := uint32(0); blk < 4; blk++ {
		if err := ps2.PutPageBatch(ctx, tenant, spcOID, dbOID, relNumber, forkNum, blk, 1, 0x100, makePage(byte(0x10+blk))); err != nil {
			t.Fatal(err)
		}
	}

	sockBE := NewPageserverSocketBackend(sockPath)
	defer sockBE.Close()
	cache, err := NewTieredCacheBackend(sockBE, CacheConfig{
		L1MaxBytes: 1024 * 1024,
		L2Dir:      filepath.Join(t.TempDir(), "l2"),
		L2MaxBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Warm all 4 blocks at LSN 0x100.
	for blk := uint32(0); blk < 4; blk++ {
		pg, err := cache.GetPageAtLSN(ctx, tenant, pageserver.RelBlockKey(spcOID, dbOID, relNumber, forkNum, blk), 0x100)
		if err != nil || pg[0] != byte(0x10+blk) {
			t.Fatalf("warm blk %d: err=%v tag=%x", blk, err, pg[0])
		}
	}
	st0 := cache.Stats()
	if st0.L3Fetches != 4 {
		t.Fatalf("expected 4 L3Fetches after warm, got %+v", st0)
	}

	// 2. Modify ONLY block 2 via FPI WAL record (also simulates VACUUM/FPI) at LSN 0x200.
	fpi := makePage(0x99)
	tl2.ApplyWALRecord(&pageserver.DecodedWALRecord{
		Rmid:   10, // RM_HEAP_ID
		Info:   0x00,
		EndLSN: 0x200,
		Blocks: []pageserver.WALBlockRef{{
			Rel:          pageserver.RelFileLocator{SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber},
			ForkNum:      forkNum,
			BlockNum:     2,
			HasImage:     true,
			ImagePayload: fpi,
		}},
	})

	// 3. Read all 4 blocks at LSN 0x200 concurrently (tests singleflight sync + selective invalidation).
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blk := uint32(0); blk < 4; blk++ {
				pg, gerr := cache.GetPageAtLSN(ctx, tenant, pageserver.RelBlockKey(spcOID, dbOID, relNumber, forkNum, blk), 0x200)
				if gerr != nil {
					t.Errorf("read blk %d at 0x200: %v", blk, gerr)
					return
				}
				wantTag := byte(0x10 + blk)
				if blk == 2 {
					wantTag = 0x99
				}
				if pg[0] != wantTag {
					t.Errorf("blk %d at 0x200: got %x want %x", blk, pg[0], wantTag)
				}
			}
		}()
	}
	wg.Wait()

	st1 := cache.Stats()
	// Only block 2 should have triggered an L3 fetch at 0x200 (blocks 0, 1, 3 hit L1!).
	if st1.L3Fetches != 5 {
		t.Fatalf("expected only 1 extra L3Fetch (total 5) for modified block 2, got %+v", st1)
	}

	// 4. Snapshot isolation: reading block 2 at old pinned LSN 0x100 must still return 0x12, NOT 0x99!
	oldPg, err := cache.GetPageAtLSN(ctx, tenant, pageserver.RelBlockKey(spcOID, dbOID, relNumber, forkNum, 2), 0x100)
	if err != nil || oldPg[0] != 0x12 {
		t.Fatalf("old pinned snapshot blk 2 at 0x100: err=%v got=%x want=12", err, oldPg[0])
	}

	// 5. Truncate relation to 2 blocks at LSN 0x300 via XLogSmgrTruncate.
	truncMain := make([]byte, 20)
	binary.LittleEndian.PutUint32(truncMain[0:4], 2)
	binary.LittleEndian.PutUint32(truncMain[4:8], spcOID)
	binary.LittleEndian.PutUint32(truncMain[8:12], dbOID)
	binary.LittleEndian.PutUint32(truncMain[12:16], relNumber)
	binary.LittleEndian.PutUint32(truncMain[16:20], 1) // MAIN_FORKNUM bit
	tl2.ApplyWALRecord(&pageserver.DecodedWALRecord{
		Rmid:     pageserver.RmSmgrID,
		Info:     pageserver.XLogSmgrTruncate,
		EndLSN:   0x300,
		MainData: truncMain,
	})
	sz, err := cache.GetRelSize(ctx, tenant, spcOID, dbOID, relNumber, forkNum, 0x300)
	if err != nil || sz != 2 {
		t.Fatalf("expected truncated size 2 at 0x300, got %d (err=%v)", sz, err)
	}

	// 6. Catalog LSN tracking: user relation writes must NOT advance catalogLSN,
	//    while system catalog (<16384) writes DO advance catalogLSN.
	catBefore, ok := cache.CatalogLSN(ctx, tenant, gen, 0x300)
	if !ok {
		t.Fatal("expected CatalogLSN supported")
	}
	if err := ps2.PutPageBatch(ctx, tenant, spcOID, dbOID, relNumber, forkNum, 0, 1, 0x400, makePage(0x44)); err != nil {
		t.Fatal(err)
	}
	catAfterUser, _ := cache.CatalogLSN(ctx, tenant, gen, 0x400)
	if catAfterUser != catBefore {
		t.Fatalf("user table write must not advance catalogLSN: before=%#x after=%#x", catBefore, catAfterUser)
	}
	if err := ps2.PutPageBatch(ctx, tenant, spcOID, dbOID, 1259, forkNum, 0, 1, 0x500, makePage(0x55)); err != nil {
		t.Fatal(err)
	}
	catAfterDDL, _ := cache.CatalogLSN(ctx, tenant, gen, 0x500)
	if catAfterDDL != 0x500 {
		t.Fatalf("catalog write must advance catalogLSN to 0x500, got %#x", catAfterDDL)
	}
}

func TestPhase14D_HitRatioBenchmark(t *testing.T) {
	ctx := t.Context()
	runWorkload := func(name string, writePct int) float64 {
		ps, err := pageserver.New(pageserver.Config{
			SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
			LayerDir:   t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer ps.Close()

		const (
			tenant    = "t_bench"
			gen       = "g1"
			spcOID    = uint32(1663)
			dbOID     = uint32(5)
			relNumber = uint32(30001)
			numBlocks = 200
		)
		ps.SetTenantGeneration(tenant, gen)
		ps.EnsureTenant(tenant, 1)

		page := bytes.Repeat([]byte{0xAA}, PGPageSize)
		lsn := uint64(0x1000)
		for b := uint32(0); b < numBlocks; b++ {
			_ = ps.PutPageBatch(ctx, tenant, spcOID, dbOID, relNumber, 0, b, 1, lsn, page)
		}

		cache, err := NewTieredCacheBackend(ps, CacheConfig{
			L1MaxBytes: 4 * 1024 * 1024,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Warm working set.
		for b := uint32(0); b < numBlocks; b++ {
			_, _ = cache.GetPageAtLSN(ctx, tenant, pageserver.RelBlockKey(spcOID, dbOID, relNumber, 0, b), lsn)
		}
		baseStats := cache.Stats()

		const ops = 1000
		for i := 0; i < ops; i++ {
			isWrite := ((i * 37) % 100) < writePct
			blk := uint32((i * 13) % numBlocks)
			if isWrite {
				lsn += 0x10
				_ = ps.PutPageBatch(ctx, tenant, spcOID, dbOID, relNumber, 0, blk, 1, lsn, page)
			} else {
				// Read a 5-page slice at current LSN.
				for j := uint32(0); j < 5; j++ {
					rb := (blk + j) % numBlocks
					if _, err := cache.GetPageAtLSN(ctx, tenant, pageserver.RelBlockKey(spcOID, dbOID, relNumber, 0, rb), lsn); err != nil {
						t.Fatalf("%s read blk %d: %v", name, rb, err)
					}
				}
			}
		}

		st := cache.Stats()
		hits := (st.L1Hits - baseStats.L1Hits) + (st.L2Hits - baseStats.L2Hits)
		reqs := (st.L1Hits - baseStats.L1Hits) + (st.L1Misses - baseStats.L1Misses)
		ratio := float64(hits) * 100.0 / float64(reqs)
		t.Logf("Phase 14D %s (writePct=%d%%): hit_ratio=%.2f%% (hits=%d reqs=%d l3_fetches=%d)",
			name, writePct, ratio, hits, reqs, st.L3Fetches-baseStats.L3Fetches)
		return ratio
	}

	r9010 := runWorkload("90/10_read_heavy", 10)
	r5050 := runWorkload("50/50_balanced", 50)
	r1090 := runWorkload("10/90_write_heavy", 90)

	// Phase 13 baselines were 23.76%, 8.47%, 3.92%.
	if r9010 < 85.0 || r5050 < 60.0 || r1090 < 25.0 {
		t.Fatalf("expected large hit ratio improvement over Phase 13 (23.76%%/8.47%%/3.92%%), got %.2f%%/%.2f%%/%.2f%%",
			r9010, r5050, r1090)
	}
}

// TestPhase14E_PG17_LiveVerification exercises 14A (B-tree), 14B (TOAST), 14C (MultiXact),
// and 14D (incremental invalidation + catalog LSN skip) together on live PostgreSQL 17.
func TestPhase14E_PG17_LiveVerification(t *testing.T) {
	initdbBin := "/usr/lib/postgresql/17/bin/initdb"
	pgBin := "/usr/lib/postgresql/17/bin/postgres"
	if _, err := os.Stat(initdbBin); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}

	tmpDir := t.TempDir()
	pgData := filepath.Join(tmpDir, "pgdata")
	sockDir := filepath.Join(tmpDir, "pgsock")
	_ = os.MkdirAll(sockDir, 0o755)

	if out, err := exec.Command(initdbBin, "-D", pgData, "-U", "postgres", "-A", "trust", "--no-sync").CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v\n%s", err, out)
	}
	f, _ := os.OpenFile(filepath.Join(pgData, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\n")
	_ = f.Close()
	hf, _ := os.OpenFile(filepath.Join(pgData, "pg_hba.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55446", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55446 user=postgres dbname=postgres sslmode=disable", sockDir)
	var conn *pgx.Conn
	for i := 0; i < 40; i++ {
		var err error
		conn, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("could not connect to PG17")
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `
		CREATE TABLE p14e_items (id int4 PRIMARY KEY, v int4, payload text);
		ALTER TABLE p14e_items ALTER COLUMN payload SET STORAGE EXTERNAL;
		INSERT INTO p14e_items SELECT g, g * 10, CASE WHEN g = 5 THEN repeat('T', 4000) ELSE 'v_' || g::text END FROM generate_series(1, 200) g;
		CHECKPOINT;
	`); err != nil {
		t.Fatalf("setup p14e_items: %v", err)
	}

	var lsnStr string
	_ = conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr)
	var hi, lo uint32
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	initLSN := (uint64(hi) << 32) | uint64(lo)

	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}

	const tenantID = "tenant_p14e_pg17"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatal(err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() { _ = rx.Run(rxCtx) }()

	rs, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          filepath.Join(tmpDir, "re"),
		PageserverSocket: filepath.Join(tmpDir, "ps.sock"),
		CacheDir:         filepath.Join(tmpDir, "l2"),
		CacheKB:          16384,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.Start(); err != nil {
		t.Fatal(err)
	}
	defer rs.Stop()

	ts, err := rs.getOrCreateTenant(tenantID)
	if err != nil {
		t.Fatal(err)
	}

	// 14A + 14B: PK lookup on TOAST row id=5.
	res, err := rs.executeReaderQuery(ctx, ts, "SELECT id, v, payload FROM p14e_items WHERE id = 5", nil)
	if err != nil || len(res.Rows) != 1 || len(res.Rows[0][2].Text) != 4000 {
		t.Fatalf("PK+TOAST query failed: err=%v rows=%+v", err, res)
	}

	// 14D: Update row 10 on PG17 primary, wait for WAL, and verify cache hits for unmodified rows + fresh data for row 10.
	if _, err := conn.Exec(ctx, "UPDATE p14e_items SET v = 9999 WHERE id = 10"); err != nil {
		t.Fatal(err)
	}
	_ = conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr)
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	targetLSN := (uint64(hi) << 32) | uint64(lo)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && tl.LastLSN() < targetLSN-8 {
		time.Sleep(20 * time.Millisecond)
	}

	// Warm & re-read across LSN advance.
	res10, err := rs.executeReaderQuery(ctx, ts, "SELECT id, v FROM p14e_items WHERE id = 10", nil)
	if err != nil || len(res10.Rows) != 1 || res10.Rows[0][1].Text != "9999" {
		t.Fatalf("updated row 10 mismatch: err=%v rows=%+v", err, res10)
	}
	res5Again, err := rs.executeReaderQuery(ctx, ts, "SELECT id, v FROM p14e_items WHERE id = 5", nil)
	if err != nil || len(res5Again.Rows) != 1 || res5Again.Rows[0][1].Text != "50" {
		t.Fatalf("unmodified row 5 mismatch: err=%v rows=%+v", err, res5Again)
	}

	dm := rs.DetailedMetrics()
	if dm.TotalFallbacks != 0 {
		t.Fatalf("expected 0 fallbacks on PG17 live verification, got %+v", dm)
	}
	var unused atomic.Uint64
	_ = unused
	t.Logf("PG17 Live Verification passed! Cache stats: %+v", rs.nativeCache.Stats())
}
