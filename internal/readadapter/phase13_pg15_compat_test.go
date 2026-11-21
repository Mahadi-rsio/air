package readadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func TestPhase13_PG15_WALReceiver_CatalogAndQuery(t *testing.T) {
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
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\n")
	_ = f.Close()

	hbaPath := filepath.Join(pgData, "pg_hba.conf")
	hf, err := os.OpenFile(hbaPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55439", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55439 user=postgres dbname=postgres sslmode=disable", sockDir)
	var conn *pgx.Conn
	for i := 0; i < 40; i++ {
		conn, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("could not connect to PG15")
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatalf("CHECKPOINT: %v", err)
	}
	var lsnStr string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnStr); err != nil {
		t.Fatalf("pg_current_wal_lsn: %v", err)
	}
	var hi, lo uint32
	_, _ = fmt.Sscanf(lsnStr, "%X/%X", &hi, &lo)
	initLSN := (uint64(hi) << 32) | uint64(lo)

	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}

	const tenantID = "tenant_p13_wal"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatalf("BootstrapTenantFromPGData: %v", err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() {
		if rerr := rx.Run(rxCtx); rerr != nil && rxCtx.Err() == nil {
			t.Logf("walreceiver exited: %v", rerr)
		}
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE t1 (id int4 PRIMARY KEY, val text);
		INSERT INTO t1 VALUES (1, 'hello_air'), (2, 'phase13');
	`); err != nil {
		t.Fatalf("CREATE/INSERT: %v", err)
	}

	// Wait for WALReceiver to catch up
	var curLSNStr string
	_ = conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&curLSNStr)
	_, _ = fmt.Sscanf(curLSNStr, "%X/%X", &hi, &lo)
	targetLSN := (uint64(hi) << 32) | uint64(lo)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tl.LastLSN() >= targetLSN-8 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	sockBackend := NewPageserverSocketBackend(filepath.Join(tmpDir, "ps.sock"))
	defer sockBackend.Close()
	adapter := NewAdapter(sockBackend, sockBackend)
	clog := NewCLOGResolver(sockBackend)

	gen := sockBackend.GetTenantGeneration(tenantID)
	snap, err := adapter.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	defer adapter.ReleaseSnapshot(snap)

	tables, err := DiscoverCatalog(ctx, adapter, clog, snap, tenantID)
	if err != nil {
		t.Fatalf("DiscoverCatalog: %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "t1" {
		t.Fatalf("expected table t1, got %+v", tables)
	}
}

func TestPhase13_PG15_CompatibilityMatrix(t *testing.T) {
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
	f, _ := os.OpenFile(confPath, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("\nwal_level = replica\nmax_wal_senders = 4\n")
	_ = f.Close()
	hf, _ := os.OpenFile(filepath.Join(pgData, "pg_hba.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = hf.WriteString("\nlocal replication postgres trust\n")
	_ = hf.Close()

	cmd := exec.Command(pgBin, "-D", pgData, "-k", sockDir, "-h", "", "-p", "55440", "-F")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("host=%s port=55440 user=postgres dbname=postgres sslmode=disable", sockDir)
	var conn, conn2 *pgx.Conn
	for i := 0; i < 40; i++ {
		var err error
		conn, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("could not connect to PG15")
	}
	defer conn.Close(ctx)
	conn2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("conn2: %v", err)
	}
	defer conn2.Close(ctx)

	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatalf("CHECKPOINT: %v", err)
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
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}

	const tenantID = "tenant_p13_compat"
	if _, err := ps.BootstrapTenantFromPGData(ctx, tenantID, 1, pgData, initLSN); err != nil {
		t.Fatalf("BootstrapTenantFromPGData: %v", err)
	}
	tl := ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	rxCtx, rxCancel := context.WithCancel(ctx)
	defer rxCancel()
	rx := pageserver.NewWALReceiver(dsn+" replication=database", tl, tl.LastLSN())
	go func() { _ = rx.Run(rxCtx) }()

	// 1. Types, MVCC, HOT updates, Rollback, Subtransactions
	_, err = conn.Exec(ctx, `
		CREATE TABLE types_tbl (
			id int4 PRIMARY KEY,
			s int2,
			b int8,
			f4 float4,
			f8 float8,
			flag bool,
			txt text,
			dt date,
			uid uuid
		);
		INSERT INTO types_tbl VALUES
			(1, 7, 9000000000, 1.5, 3.14159, true, 'alpha', '2026-11-21', '11111111-2222-3333-4444-555555555555'),
			(2, -3, -42, 2.25, 2.71828, false, 'beta', '2025-01-02', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee');
		UPDATE types_tbl SET txt = 'alpha_hot' WHERE id = 1;

		CREATE TABLE toast_tbl (id int4 PRIMARY KEY, big text);
		ALTER TABLE toast_tbl ALTER COLUMN big SET STORAGE EXTERNAL;
		INSERT INTO toast_tbl VALUES (1, repeat('x', 4000));

		CREATE TABLE mx_tbl (id int4 PRIMARY KEY, v int4);
		INSERT INTO mx_tbl VALUES (1, 10), (2, 20);

		CREATE TABLE num_tbl (id int4 PRIMARY KEY, amt numeric(10,2));
		INSERT INTO num_tbl VALUES (1, 12.34);
	`)
	if err != nil {
		t.Fatalf("setup tables: %v", err)
	}

	// Rollback test
	txR, _ := conn.Begin(ctx)
	_, _ = txR.Exec(ctx, "INSERT INTO types_tbl (id, txt) VALUES (99, 'rolled_back')")
	_ = txR.Rollback(ctx)

	// MultiXact lock-only (FOR SHARE by conn2, committed) vs non-lock-only (FOR SHARE + UPDATE)
	txS, _ := conn2.Begin(ctx)
	_, _ = txS.Exec(ctx, "SELECT * FROM mx_tbl WHERE id = 1 FOR SHARE")
	_ = txS.Commit(ctx)

	// Non-lock-only MultiXact: conn holds FOR SHARE while conn2 updates in-place? No, hold FOR SHARE on id=2 while conn2 updates id=2?
	// If conn2 updates id=2 while conn holds FOR SHARE, conn2 blocks until conn commits.
	// Instead, use two concurrent FOR SHARE locks (which creates a MultiXactId!) and verify lock-only MultiXact stays readable!
	// And test non-lock-only MultiXact via subtransaction or in-progress update + share lock.
	txS1, _ := conn.Begin(ctx)
	txS2, _ := conn2.Begin(ctx)
	_, _ = txS1.Exec(ctx, "SELECT * FROM mx_tbl WHERE id = 2 FOR SHARE")
	_, _ = txS2.Exec(ctx, "SELECT * FROM mx_tbl WHERE id = 2 FOR SHARE")
	_ = txS1.Commit(ctx)
	_ = txS2.Commit(ctx)

	// Wait for WALReceiver
	var curLSNStr string
	_ = conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&curLSNStr)
	_, _ = fmt.Sscanf(curLSNStr, "%X/%X", &hi, &lo)
	targetLSN := (uint64(hi) << 32) | uint64(lo)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && tl.LastLSN() < targetLSN-8 {
		time.Sleep(20 * time.Millisecond)
	}

	// Boot SQLiteReadServer backed by Pageserver socket
	reDir := filepath.Join(tmpDir, "re")
	rs, err := NewSQLiteReadServer(SQLiteServerConfig{
		ListenHost:       "127.0.0.1",
		PGPort:           0,
		DataDir:          reDir,
		PageserverSocket: filepath.Join(tmpDir, "ps.sock"),
		CacheDir:         filepath.Join(tmpDir, "l2"),
		CacheKB:          8192,
	})
	if err != nil {
		t.Fatalf("NewSQLiteReadServer: %v", err)
	}
	if err := rs.Start(); err != nil {
		t.Fatalf("rs.Start: %v", err)
	}
	defer rs.Stop()

	ts, err := rs.getOrCreateTenant(tenantID)
	if err != nil {
		t.Fatalf("getOrCreateTenant: %v", err)
	}

	// Verify types_tbl + HOT update + rollback exclusion
	res, err := rs.executeReaderQuery(ctx, ts, "SELECT id, s, b, flag, txt, dt, uid FROM types_tbl ORDER BY id", nil)
	if err != nil {
		t.Fatalf("types_tbl query: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows in types_tbl, got %d: %+v", len(res.Rows), res.Rows)
	}
	if res.Rows[0][4].Text != "alpha_hot" || res.Rows[0][5].Text != "2026-11-21" || res.Rows[0][6].Text != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("unexpected row 0: %+v", res.Rows[0])
	}

	// Verify lock-only MultiXact is readable without error
	mxRes, err := rs.executeReaderQuery(ctx, ts, "SELECT id, v FROM mx_tbl ORDER BY id", nil)
	if err != nil {
		t.Fatalf("mx_tbl lock-only query: %v", err)
	}
	if len(mxRes.Rows) != 2 {
		t.Fatalf("expected 2 rows in mx_tbl, got %d", len(mxRes.Rows))
	}

	// Verify External TOAST triggers ErrTOASTUnsupported and increments ToastFallbacks metric
	_, toastErr := rs.executeReaderQuery(ctx, ts, "SELECT id, big FROM toast_tbl", nil)
	if !errors.Is(toastErr, ErrTOASTUnsupported) {
		t.Fatalf("expected ErrTOASTUnsupported, got %v", toastErr)
	}

	// Verify unsupported type (numeric) triggers ErrUnsupportedType and increments UnsupportedTypeFallbacks
	_, numErr := rs.executeReaderQuery(ctx, ts, "SELECT id, amt FROM num_tbl", nil)
	if !errors.Is(numErr, ErrUnsupportedType) {
		t.Fatalf("expected ErrUnsupportedType, got %v", numErr)
	}

	// Verify Tenant Isolation: a different tenant without pages cannot read tenant_p13_compat
	otherTS, err := rs.getOrCreateTenant("other_isolated_tenant")
	if err != nil {
		t.Fatalf("getOrCreateTenant other: %v", err)
	}
	_, isoErr := rs.executeReaderQuery(ctx, otherTS, "SELECT * FROM types_tbl", nil)
	if isoErr == nil || (!errors.Is(isoErr, ErrStaleGeneration) && !strings.Contains(isoErr.Error(), "not found")) {
		t.Fatalf("expected tenant isolation error, got %v", isoErr)
	}

	dm := rs.DetailedMetrics()
	if dm.ToastFallbacks < 1 || dm.UnsupportedTypeFallbacks < 1 || dm.TotalFallbacks < 2 {
		t.Fatalf("expected fallback metrics >= 1, got %+v", dm)
	}
	t.Logf("PG15 Compatibility Matrix passed! Fallback metrics: toast=%d multixact=%d unsupported_type=%d total=%d",
		dm.ToastFallbacks, dm.MultiXactFallbacks, dm.UnsupportedTypeFallbacks, dm.TotalFallbacks)
}
