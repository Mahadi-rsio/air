package readadapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/gateway"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

// --- 1. SQLite Cancellation Isolation Test ---

type slowBlockBackend struct {
	*pageserver.Pageserver
	blockRelNum uint32
	entered     chan struct{}
	once        sync.Once
}

func (b *slowBlockBackend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	if key.RelNumber == b.blockRelNum {
		b.once.Do(func() { close(b.entered) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return b.Pageserver.GetPageAtLSN(ctx, tenantID, key, lsn)
}

func (b *slowBlockBackend) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	if relNumber == b.blockRelNum {
		b.once.Do(func() { close(b.entered) })
		<-ctx.Done()
		return nil, 0, ctx.Err()
	}
	return b.Pageserver.GetPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, lsn)
}

func TestAudit_SQLiteCancellationIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID = "tenant-cancel-iso"
		gen      = "gen-1"
		dbOID    = uint32(16384)
		lsn      = uint64(0x20000)
		slowRel  = uint32(31001)
		fastRel  = uint32(31002)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)

	// Seed minimal catalog + slow_tbl + fast_tbl
	putRel := func(spc, db, rel uint32, page []byte) {
		if err := ps.PutPageBatch(context.Background(), tenantID, spc, db, rel, 0, 0, 1, lsn, page); err != nil {
			t.Fatalf("PutPageBatch: %v", err)
		}
	}
	var dbRow tupleBuilder
	dbRow.addOID(dbOID)
	dbRow.addName(tenantID)
	dbRow.addOID(10)
	dbRow.addInt4(6)
	dbRow.addChar('c')
	dbRow.addBool(false)
	dbRow.addBool(true)
	dbRow.addBool(false)
	dbRow.addInt4(-1)
	dbRow.addOID(726)
	dbRow.addOID(1)
	dbRow.addOID(PGDefaultTablespaceOID)
	putRel(PGGlobalTablespaceOID, 0, PGDatabaseRelID, buildPG17HeapPage(lsn, [][]byte{
		buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, dbRow.buf),
	}))

	var nsRow tupleBuilder
	nsRow.addOID(2200)
	nsRow.addName("public")
	putRel(PGDefaultTablespaceOID, dbOID, PGNamespaceRelID, buildPG17HeapPage(lsn, [][]byte{
		buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, nsRow.buf),
	}))

	mkCls := func(oid uint32, name string) []byte {
		var c tupleBuilder
		c.addOID(oid)
		c.addName(name)
		c.addOID(2200)
		c.addOID(oid + 1)
		c.addOID(0)
		c.addOID(10)
		c.addOID(2)
		c.addOID(oid)
		c.addOID(0)
		c.addInt4(1)
		c.addFloat4(1)
		c.addInt4(0)
		c.addOID(0)
		c.addBool(false)
		c.addBool(false)
		c.addChar('p')
		c.addChar('r')
		return buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, c.buf)
	}
	putRel(PGDefaultTablespaceOID, dbOID, PGClassRelID, buildPG17HeapPage(lsn, [][]byte{
		mkCls(slowRel, "slow_tbl"),
		mkCls(fastRel, "fast_tbl"),
	}))

	mkAttr := func(relOID uint32, name string) []byte {
		var b tupleBuilder
		b.addOID(relOID)
		b.addName(name)
		b.addOID(23)
		b.addInt2(4)
		b.addInt2(1)
		b.addInt4(-1)
		b.addInt4(-1)
		b.addInt2(0)
		b.addBool(true)
		b.addChar('i')
		b.addChar('p')
		b.addChar(0)
		b.addBool(false)
		b.addBool(false)
		b.addBool(false)
		b.addChar(0)
		b.addChar(0)
		b.addBool(false)
		return buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, b.buf)
	}
	putRel(PGDefaultTablespaceOID, dbOID, PGAttributeRelID, buildPG17HeapPage(lsn, [][]byte{
		mkAttr(slowRel, "id"),
		mkAttr(fastRel, "id"),
	}))

	var r1, r2 tupleBuilder
	r1.addInt4(1)
	r2.addInt4(42)
	putRel(PGDefaultTablespaceOID, dbOID, slowRel, buildPG17HeapPage(lsn, [][]byte{
		buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, r1.buf),
	}))
	putRel(PGDefaultTablespaceOID, dbOID, fastRel, buildPG17HeapPage(lsn, [][]byte{
		buildHeapTuple(FrozenTransactionID, 0, HeapXminFrozen|HeapXmaxInvalid, r2.buf),
	}))

	sb := &slowBlockBackend{
		Pageserver:  ps,
		blockRelNum: slowRel,
		entered:     make(chan struct{}),
	}
	snaps := NewStaticSnapshotProvider()
	snaps.SetSnapshot(tenantID, gen, SnapshotInfo{LSN: lsn})

	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          filepath.Join(tmpDir, "re"),
		NativeBackend:    sb,
		NativeSnapProv:   snaps,
		ReadersPerTenant: 2,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	ts, err := srv.getOrCreateTenant(tenantID)
	if err != nil {
		t.Fatalf("getOrCreateTenant: %v", err)
	}

	// Start Query A (blocked on slow_tbl) on the same tenant.
	ctxA, cancelA := context.WithCancel(context.Background())
	errChA := make(chan error, 1)
	go func() {
		_, qerr := srv.executeReaderQuery(ctxA, ts, "SELECT id FROM slow_tbl", nil)
		errChA <- qerr
	}()

	select {
	case <-sb.entered: // Query A is actively executing inside SQLite + ReadPages
	case qerr := <-errChA:
		t.Fatalf("Query A exited before entering slowBlockBackend: %v", qerr)
	}

	// Execute Query B concurrently on the SAME tenant while Query A is blocked, then cancel Query A.
	ctxB, cancelB := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelB()

	var wg sync.WaitGroup
	var resB *SQLiteResult
	var errB error
	wg.Add(1)
	go func() {
		defer wg.Done()
		resB, errB = srv.executeReaderQuery(ctxB, ts, "SELECT id FROM fast_tbl", nil)
	}()

	// Cancel Query A while Query B is running.
	cancelA()
	if qerr := <-errChA; !errors.Is(qerr, context.Canceled) {
		t.Fatalf("expected Query A to fail with context.Canceled, got %v", qerr)
	}

	wg.Wait()
	if errB != nil {
		t.Fatalf("Query B was affected by Query A cancellation: %v", errB)
	}
	if len(resB.Rows) != 1 || resB.Rows[0][0].Int64 != 42 {
		t.Fatalf("Query B unexpected rows: %+v", resB.Rows)
	}
}

// --- 2. Pageserver Protocol Stress Test (500 concurrent requests, multi-tenant, restarts, overload) ---

func TestAudit_PageserverSocketStress500AndRestart(t *testing.T) {
	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "ps.sock")

	startPS := func() *pageserver.Pageserver {
		ps, err := pageserver.New(pageserver.Config{
			SocketPath: sockPath,
			LayerDir:   filepath.Join(tmpDir, "layers"),
		})
		if err != nil {
			t.Fatalf("pageserver.New: %v", err)
		}
		if err := ps.Start(context.Background()); err != nil {
			t.Fatalf("ps.Start: %v", err)
		}
		for _, tid := range []string{"tenant-s1", "tenant-s2", "tenant-s3"} {
			ps.EnsureTenant(tid, 1)
			ps.SetTenantGeneration(tid, "gen-1")
			for blk := uint32(0); blk < 10; blk++ {
				pg := make([]byte, pageserver.PageSize)
				copy(pg[0:], tid)
				binary.LittleEndian.PutUint32(pg[32:36], blk)
				if err := ps.PutPageBatch(context.Background(), tid, 1663, 5, 40001, 0, blk, 1, 0x10000, pg); err != nil {
					t.Fatalf("PutPageBatch: %v", err)
				}
			}
		}
		return ps
	}

	ps := startPS()
	sb := NewPageserverSocketBackendWithConfig(PageserverSocketConfig{
		SocketPath:     sockPath,
		MaxConns:       8,
		MaxInFlight:    8,
		QueueCapacity:  512,
		RequestTimeout: 5 * time.Second,
	})
	defer sb.Close()

	// 500 concurrent requests across 3 tenants with random delays and some cancellations.
	const total = 500
	var wg sync.WaitGroup
	var mismatch atomic.Int64
	var succeeded atomic.Int64

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tenants := []string{"tenant-s1", "tenant-s2", "tenant-s3"}
			tid := tenants[idx%3]
			blk := uint32(idx % 10)
			if idx%17 == 0 {
				time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if idx%50 == 49 {
				// Pre-cancelled context must fail cleanly without corrupting pool.
				cancel()
			}
			pg, err := sb.GetPageAtLSN(ctx, tid, pageserver.RelBlockKey(1663, 5, 40001, 0, blk), 0x10000)
			if idx%50 == 49 {
				return
			}
			if err != nil {
				mismatch.Add(1)
				return
			}
			gotTid := string(pg[:len(tid)])
			gotBlk := binary.LittleEndian.Uint32(pg[32:36])
			if gotTid != tid || gotBlk != blk {
				mismatch.Add(1)
				return
			}
			succeeded.Add(1)
		}(i)
	}
	wg.Wait()

	if mismatch.Load() != 0 {
		t.Fatalf("detected %d mismatched or failed responses out of 500", mismatch.Load())
	}
	if succeeded.Load() != 490 {
		t.Fatalf("expected 490 succeeded, got %d", succeeded.Load())
	}

	// Simulate server restart: close PS, start a fresh PS on the same socket, verify reconnect.
	_ = ps.Close()
	ps2 := startPS()
	defer ps2.Close()

	pg, err := sb.GetPageAtLSN(context.Background(), "tenant-s2", pageserver.RelBlockKey(1663, 5, 40001, 0, 7), 0x10000)
	if err != nil {
		t.Fatalf("post-restart GetPageAtLSN failed: %v", err)
	}
	if string(pg[:9]) != "tenant-s2" || binary.LittleEndian.Uint32(pg[32:36]) != 7 {
		t.Fatalf("post-restart page mismatch")
	}

	// Queue saturation test: QueueCapacity=1, MaxConns=1
	sbSat := NewPageserverSocketBackendWithConfig(PageserverSocketConfig{
		SocketPath:    sockPath,
		MaxConns:      1,
		MaxInFlight:   1,
		QueueCapacity: 1,
	})
	defer sbSat.Close()
	var overloadHits atomic.Int64
	var wgSat sync.WaitGroup
	for i := 0; i < 32; i++ {
		wgSat.Add(1)
		go func() {
			defer wgSat.Done()
			_, err := sbSat.GetPageAtLSN(context.Background(), "tenant-s1", pageserver.RelBlockKey(1663, 5, 40001, 0, 0), 0x10000)
			if errors.Is(err, ErrPageserverOverloaded) {
				overloadHits.Add(1)
			}
		}()
	}
	wgSat.Wait()
	// Just ensure no panic/race under saturation.
	t.Logf("queue saturation overloadHits=%d", overloadHits.Load())
}

// --- 3. Cache Historical LSN & Singleflight Cancellation Audit ---

type cancelSafeMockL3 struct {
	*pageserver.Pageserver
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *cancelSafeMockL3) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	if key.BlockNum == 99 {
		m.once.Do(func() { close(m.entered) })
		select {
		case <-m.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return m.Pageserver.GetPageAtLSN(ctx, tenantID, key, lsn)
}

func TestAudit_CacheHistoricalLSNAndSingleflightCancel(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const tid = "tenant-hist"
	ps.EnsureTenant(tid, 1)
	ps.SetTenantGeneration(tid, "gen-1")

	pg1 := bytes.Repeat([]byte{0x11}, PGPageSize)
	pg2 := bytes.Repeat([]byte{0x22}, PGPageSize)
	_ = ps.PutPageBatch(context.Background(), tid, 1663, 5, 50001, 0, 0, 1, 100, pg1)
	_ = ps.PutPageBatch(context.Background(), tid, 1663, 5, 50001, 0, 0, 1, 200, pg2)
	_ = ps.PutPageBatch(context.Background(), tid, 1663, 5, 50001, 0, 99, 1, 200, pg2)

	mockL3 := &cancelSafeMockL3{
		Pageserver: ps,
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	cache, err := NewTieredCacheBackend(mockL3, CacheConfig{
		L1MaxBytes: 16 * PGPageSize,
		L2Dir:      filepath.Join(t.TempDir(), "l2"),
		L2MaxBytes: 16 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend: %v", err)
	}

	k0 := pageserver.RelBlockKey(1663, 5, 50001, 0, 0)
	// Read at LSN 200 first, then LSN 100: must return pg2 and pg1 respectively (no stale cross-LSN hit!).
	got200, err := cache.GetPageAtLSN(context.Background(), tid, k0, 200)
	if err != nil || !bytes.Equal(got200, pg2) {
		t.Fatalf("LSN 200 mismatch: err=%v", err)
	}
	got100, err := cache.GetPageAtLSN(context.Background(), tid, k0, 100)
	if err != nil || !bytes.Equal(got100, pg1) {
		t.Fatalf("LSN 100 mismatch (stale LSN 200 returned?): err=%v", err)
	}

	// Sentinel LSN ^uint64(0) must NOT be cached in L1/L2.
	beforeHits := cache.Stats().L1Hits
	_, _ = cache.GetPageAtLSN(context.Background(), tid, k0, ^uint64(0))
	_, _ = cache.GetPageAtLSN(context.Background(), tid, k0, ^uint64(0))
	if cache.Stats().L1Hits != beforeHits {
		t.Fatalf("sentinel LSN ^uint64(0) must bypass L1 cache")
	}

	// Singleflight cancellation isolation: caller A cancels, caller B must still succeed!
	k99 := pageserver.RelBlockKey(1663, 5, 50001, 0, 99)
	ctxA, cancelA := context.WithCancel(context.Background())
	errA := make(chan error, 1)
	go func() {
		_, e := cache.GetPageAtLSN(ctxA, tid, k99, 200)
		errA <- e
	}()
	<-mockL3.entered

	errB := make(chan error, 1)
	go func() {
		pg, e := cache.GetPageAtLSN(context.Background(), tid, k99, 200)
		if e == nil && !bytes.Equal(pg, pg2) {
			e = fmt.Errorf("wrong page")
		}
		errB <- e
	}()
	time.Sleep(10 * time.Millisecond)
	cancelA()
	if e := <-errA; !errors.Is(e, context.Canceled) {
		t.Fatalf("expected caller A context.Canceled, got %v", e)
	}
	close(mockL3.release)
	if e := <-errB; e != nil {
		t.Fatalf("caller B poisoned by caller A cancellation: %v", e)
	}
}

// --- 4. SQL Routing Security Audit ---

func TestAudit_SQLRoutingSecurityAndEscapeStrings(t *testing.T) {
	mustWrite := []string{
		"BEGIN",
		"BEGIN READ ONLY",
		"START TRANSACTION",
		"COMMIT",
		"ROLLBACK",
		"SELECT 1; INSERT INTO t VALUES (1)",
		"SELECT * FROM t FOR UPDATE",
		"SELECT * FROM t FOR SHARE",
		"WITH c AS(DELETE FROM t RETURNING *) SELECT * FROM c",
		"CREATE TABLE x (id int)",
		"ALTER TABLE x ADD COLUMN y int",
		"DROP TABLE x",
		"SELECT nextval('seq')",
		"SELECT pg_advisory_lock(1)",
		"UNKNOWN_COMMAND foo bar",
		"/* comment */ INSERT INTO t VALUES (1)",
		// Escape-string literal containing escaped quote: must NOT confuse tokenizer
		`SELECT E'hello\' ; INSERT INTO t VALUES (1); --' FROM t; DELETE FROM t`,
	}
	for _, q := range mustWrite {
		if d := gateway.ClassifySQLForRouting(q); d.Target != gateway.RouteTargetWriteEngine {
			t.Fatalf("expected WriteEngine for %q, got %+v", q, d)
		}
	}

	mustRead := []string{
		"SELECT 1",
		`SELECT E'hello\' ; DELETE FROM t' FROM users`,
		"/* INSERT INTO t */ SELECT * FROM users WHERE id = 1",
		"WITH cte AS (SELECT id FROM users) SELECT * FROM cte",
	}
	for _, q := range mustRead {
		if d := gateway.ClassifySQLForRouting(q); d.Target != gateway.RouteTargetReadEngine {
			t.Fatalf("expected ReadEngine for %q, got %+v", q, d)
		}
	}
}

// --- 5. Real PostgreSQL 15 Live MVCC & Concurrent Transaction Verification ---

func TestAudit_RealPostgreSQL15_MVCC_And_Joins(t *testing.T) {
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

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55432", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55432 user=postgres dbname=postgres sslmode=disable", sockDir)
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
		t.Fatalf("could not connect to ephemeral PostgreSQL 15")
	}
	defer conn.Close(ctx)

	// Create users and orders tables, perform COMMIT, ROLLBACK, UPDATE, DELETE, SAVEPOINT subtransactions, and HOT updates.
	_, err := conn.Exec(ctx, `
		CREATE TABLE users (id int4, name text, bal int4);
		CREATE TABLE orders (id int4, user_id int4, amount int4);
		INSERT INTO users VALUES (1, 'alice', 100), (2, 'bob', 200), (3, 'carol', 300);
		INSERT INTO orders VALUES (10, 1, 50), (20, 2, 75);
		UPDATE users SET bal = 150 WHERE id = 1;
		DELETE FROM users WHERE id = 3;
	`)
	if err != nil {
		t.Fatalf("setup DML: %v", err)
	}

	// Aborted transaction
	txAbort, _ := conn.Begin(ctx)
	_, _ = txAbort.Exec(ctx, "INSERT INTO users VALUES (99, 'aborted', 999)")
	_ = txAbort.Rollback(ctx)

	// Subtransaction (SAVEPOINT): sp1 committed, sp2 rolled back
	txSub, _ := conn.Begin(ctx)
	_, _ = txSub.Exec(ctx, "SAVEPOINT sp1; INSERT INTO users VALUES (4, 'dave', 400); RELEASE SAVEPOINT sp1;")
	_, _ = txSub.Exec(ctx, "SAVEPOINT sp2; INSERT INTO users VALUES (5, 'rolledback_sub', 500); ROLLBACK TO SAVEPOINT sp2;")
	if err := txSub.Commit(ctx); err != nil {
		t.Fatalf("subtx commit: %v", err)
	}

	// Open an uncommitted in-progress transaction (must NOT be visible to SQLite!)
	conn2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect conn2: %v", err)
	}
	defer conn2.Close(ctx)
	txInProg, _ := conn2.Begin(ctx)
	_, _ = txInProg.Exec(ctx, "INSERT INTO users VALUES (88, 'in_progress', 888)")
	defer txInProg.Rollback(ctx)

	// Flush PG dirty buffers to disk so PGDATA files contain the exact heap & pg_xact pages.
	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatalf("CHECKPOINT: %v", err)
	}

	// Query authoritative PostgreSQL result for comparison.
	pgRows, err := conn.Query(ctx, "SELECT u.id, u.name, u.bal, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id")
	if err != nil {
		t.Fatalf("pg query: %v", err)
	}
	var want []string
	for pgRows.Next() {
		var id, bal int32
		var name string
		var amt *int32
		if err := pgRows.Scan(&id, &name, &bal, &amt); err != nil {
			t.Fatalf("pg scan: %v", err)
		}
		amtStr := "NULL"
		if amt != nil {
			amtStr = strconv.Itoa(int(*amt))
		}
		want = append(want, fmt.Sprintf("%d|%s|%d|%s", id, name, bal, amtStr))
	}
	pgRows.Close()

	// Locate PGDATA relfilenodes and load actual PostgreSQL 15 heap + pg_xact pages into Pageserver!
	var dbOID, usersNode, ordersNode uint32
	_ = conn.QueryRow(ctx, "SELECT oid FROM pg_database WHERE datname='postgres'").Scan(&dbOID)
	_ = conn.QueryRow(ctx, "SELECT relfilenode FROM pg_class WHERE relname='users'").Scan(&usersNode)
	_ = conn.QueryRow(ctx, "SELECT relfilenode FROM pg_class WHERE relname='orders'").Scan(&ordersNode)

	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "air_ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "air_layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}

	const tid = "pg15-live"
	const gen = "gen-1"
	const snapLSN = uint64(0x100000)
	tl := ps.EnsureTenant(tid, 1)
	ps.SetTenantGeneration(tid, gen)

	// Load pg_xact/0000 from PGDATA into Pageserver SLRU.
	xactBytes, err := os.ReadFile(filepath.Join(pgData, "pg_xact", "0000"))
	if err != nil {
		t.Fatalf("read pg_xact/0000: %v", err)
	}
	for blk := 0; (blk+1)*PGPageSize <= len(xactBytes); blk++ {
		pg := make([]byte, PGPageSize)
		copy(pg, xactBytes[blk*PGPageSize:(blk+1)*PGPageSize])
		tl.SeedSLRUPage(SLRUKey(SLRUXactKind, uint32(blk)), pg)
	}

	// Load actual PG15 relation files into Pageserver.
	loadRelFile := func(relNode uint32) {
		p := filepath.Join(pgData, "base", strconv.Itoa(int(dbOID)), strconv.Itoa(int(relNode)))
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read relfile %s: %v", p, err)
		}
		nblk := uint32(len(raw) / PGPageSize)
		if nblk > 0 {
			if err := ps.PutPageBatch(ctx, tid, PGDefaultTablespaceOID, dbOID, relNode, 0, 0, nblk, snapLSN, raw[:int(nblk)*PGPageSize]); err != nil {
				t.Fatalf("PutPageBatch %d: %v", relNode, err)
			}
		}
	}
	loadRelFile(usersNode)
	loadRelFile(ordersNode)

	sb := NewPageserverSocketBackend(filepath.Join(tmpDir, "air_ps.sock"))
	defer sb.Close()
	snaps := NewStaticSnapshotProvider()
	snaps.SetSnapshot(tid, gen, SnapshotInfo{LSN: snapLSN})

	ad := NewAdapter(sb, snaps)
	clog := NewCLOGResolver(sb)

	sqConn, err := OpenSQLiteConn(":memory:", false, 4096)
	if err != nil {
		t.Fatalf("OpenSQLiteConn: %v", err)
	}
	defer sqConn.Close()
	_ = sqConn.RegisterAirPGModule()

	usersTbl := DiscoveredTable{
		Schema: "public", Name: "users", RelOID: usersNode, SpcOID: PGDefaultTablespaceOID, DbOID: dbOID, RelNumber: usersNode,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "name", AttNum: 2, PGTypeOID: 25, ColType: ColText, SQLiteType: "TEXT"},
			{Name: "bal", AttNum: 3, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
		},
	}
	ordersTbl := DiscoveredTable{
		Schema: "public", Name: "orders", RelOID: ordersNode, SpcOID: PGDefaultTablespaceOID, DbOID: dbOID, RelNumber: ordersNode,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "user_id", AttNum: 2, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "amount", AttNum: 3, PGTypeOID: 23, ColType: ColInt4, SQLiteType: "INTEGER"},
		},
	}
	uid, err := sqConn.RegisterNativeTable(tid, gen, usersTbl, ad, clog)
	if err != nil {
		t.Fatalf("RegisterNativeTable users: %v", err)
	}
	defer UnregisterNativeTable(uid)
	oid, err := sqConn.RegisterNativeTable(tid, gen, ordersTbl, ad, clog)
	if err != nil {
		t.Fatalf("RegisterNativeTable orders: %v", err)
	}
	defer UnregisterNativeTable(oid)
	_ = sqConn.Exec("PRAGMA query_only=ON;")
	sqConn.SetReadOnlyAuthorizer(true)

	snap, err := ad.GetSnapshot(ctx, tid, gen)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	defer ad.ReleaseSnapshot(snap)

	res, err := sqConn.QueryWithContext(ctx, snap, "SELECT u.id, u.name, u.bal, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id", nil)
	if err != nil {
		t.Fatalf("SQLite query over real PG15 pages failed: %v", err)
	}
	var got []string
	for _, r := range res.Rows {
		amtStr := "NULL"
		if !r[3].IsNil {
			amtStr = r[3].Text
		}
		got = append(got, fmt.Sprintf("%s|%s|%s|%s", r[0].Text, r[1].Text, r[2].Text, amtStr))
	}

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("PG15 vs SQLite Read Engine mismatch!\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	t.Logf("Real PostgreSQL 15 vs SQLite Read Engine exact match (%d rows):\n%s", len(got), strings.Join(got, "\n"))
}
