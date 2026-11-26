package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func setupRealPGSchema(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	ddl := `
DROP TABLE IF EXISTS tenant_usage;
CREATE TABLE tenant_usage (
	tenant_id TEXT PRIMARY KEY,
	bandwidth_bytes BIGINT NOT NULL DEFAULT 0,
	read_queries BIGINT NOT NULL DEFAULT 0,
	write_queries BIGINT NOT NULL DEFAULT 0,
	storage_bytes BIGINT NOT NULL DEFAULT 0,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
DROP TABLE IF EXISTS metering_batch_receipts;
CREATE TABLE metering_batch_receipts (
	batch_id TEXT PRIMARY KEY,
	gateway_id TEXT NOT NULL,
	payload_sha256 TEXT NOT NULL,
	entry_count BIGINT NOT NULL,
	tenant_count INT NOT NULL,
	committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_metering_batch_receipts_gateway ON metering_batch_receipts(gateway_id, committed_at DESC);
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
		CREATE ROLE app_user WITH LOGIN SUPERUSER PASSWORD 'airpass';
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'air_meta') THEN
		CREATE ROLE air_meta WITH LOGIN SUPERUSER PASSWORD 'airpass';
	END IF;
END$$;
`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("setup schema: %v", err)
	}
	return pool
}

func TestRealPG_MeteringReceiptIdempotencyAndConcurrentGateways(t *testing.T) {
	dsn := os.Getenv("AIR_TEST_META_DSN")
	if dsn == "" {
		t.Skip("AIR_TEST_META_DSN not set")
	}
	ctx := context.Background()
	pool := setupRealPGSchema(t, dsn)

	saver := NewPGMeteringSaver(dsn)
	if err := saver.Ping(ctx); err != nil {
		t.Fatalf("saver.Ping: %v", err)
	}

	now := time.Now().UTC().Truncate(24 * time.Hour)
	entries := []MeteringEntry{
		{Date: now, DBID: "tenant-pg-1", BandwidthBytes: 100, QueryTimeUs: 50, QueryType: "SELECT"},
		{Date: now, DBID: "tenant-pg-1", BandwidthBytes: 200, QueryTimeUs: 80, QueryType: "INSERT"},
	}
	sum := sha256.Sum256([]byte("batch-payload-1"))
	hash1 := hex.EncodeToString(sum[:])

	// First commit succeeds.
	if err := saver.SaveMeteringBatchWithReceipt(ctx, "batch-idem-1", "gw-a", hash1, entries); err != nil {
		t.Fatalf("first SaveMeteringBatchWithReceipt: %v", err)
	}
	// Retry with same batch_id & hash (simulating crash after commit before unlink) is idempotent.
	if err := saver.SaveMeteringBatchWithReceipt(ctx, "batch-idem-1", "gw-a", hash1, entries); err != nil {
		t.Fatalf("idempotent retry SaveMeteringBatchWithReceipt: %v", err)
	}
	// Same batch_id with different hash must fail with ErrBatchPayloadMismatch.
	sum2 := sha256.Sum256([]byte("tampered-payload"))
	hash2 := hex.EncodeToString(sum2[:])
	err := saver.SaveMeteringBatchWithReceipt(ctx, "batch-idem-1", "gw-a", hash2, entries)
	if !errors.Is(err, ErrBatchPayloadMismatch) {
		t.Fatalf("expected ErrBatchPayloadMismatch, got %v", err)
	}

	// Verify tenant_usage was only counted once.
	var bw, rq, wq int64
	if err := pool.QueryRow(ctx, `SELECT bandwidth_bytes, read_queries, write_queries FROM tenant_usage WHERE tenant_id='tenant-pg-1'`).Scan(&bw, &rq, &wq); err != nil {
		t.Fatalf("query tenant_usage: %v", err)
	}
	if bw != 300 || rq != 1 || wq != 1 {
		t.Fatalf("expected bw=300,rq=1,wq=1; got bw=%d,rq=%d,wq=%d", bw, rq, wq)
	}

	// Now run two MeteringLoggers (Gateway A and Gateway B) with distinct volumes flushing concurrently.
	dirA := filepath.Join(nonTmpDir(t), "vol-a")
	dirB := filepath.Join(nonTmpDir(t), "vol-b")
	mlA, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          dirA,
		GatewayID:       "gw-a",
		BillingEnabled:  true,
		RequireWritable: true,
		Saver:           NewPGMeteringSaver(dsn),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mlA.Stop()
	mlB, err := NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:          dirB,
		GatewayID:       "gw-b",
		BillingEnabled:  true,
		RequireWritable: true,
		Saver:           NewPGMeteringSaver(dsn),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mlB.Stop()

	for i := 0; i < 5; i++ {
		if err := mlA.LogQueryErr(MeteringEntry{Date: now, DBID: "tenant-concurrent", BandwidthBytes: 10, QueryTimeUs: 5, QueryType: "SELECT"}); err != nil {
			t.Fatal(err)
		}
		if err := mlB.LogQueryErr(MeteringEntry{Date: now, DBID: "tenant-concurrent", BandwidthBytes: 20, QueryTimeUs: 7, QueryType: "INSERT"}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var errA, errB error
	go func() { defer wg.Done(); errA = mlA.SealAndFlush(ctx) }()
	go func() { defer wg.Done(); errB = mlB.SealAndFlush(ctx) }()
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("concurrent SealAndFlush errors: A=%v B=%v", errA, errB)
	}

	if err := pool.QueryRow(ctx, `SELECT bandwidth_bytes, read_queries, write_queries FROM tenant_usage WHERE tenant_id='tenant-concurrent'`).Scan(&bw, &rq, &wq); err != nil {
		t.Fatalf("query concurrent tenant_usage: %v", err)
	}
	if bw != 150 || rq != 5 || wq != 5 {
		t.Fatalf("expected bw=150,rq=5,wq=5; got bw=%d,rq=%d,wq=%d", bw, rq, wq)
	}
}

func TestRealPG_TwoGateways_TxCommitWhileScaleOutStarts(t *testing.T) {
	dsn := os.Getenv("AIR_TEST_META_DSN")
	if dsn == "" {
		t.Skip("AIR_TEST_META_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create a test table in real Postgres.
	adminPool := setupRealPGSchema(t, dsn)
	if _, err := adminPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS gw_tx_test (id INT PRIMARY KEY, val TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `TRUNCATE TABLE gw_tx_test`); err != nil {
		t.Fatal(err)
	}

	// Parse host/port from DSN.
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	// Start Gateway A listener + PoolManager pointing at real Postgres.
	startGW := func(gwID string) (string, *PoolManager, *MetricsRegistry, func()) {
		gwCfg := DefaultConfig()
		gwCfg.GatewayID = gwID
		gwCfg.RequireTLS = false
		gwCfg.BillingEnabled = false

		route := TenantRoute{
			TenantID:    cfg.Database,
			State:       "ACTIVE",
			Host:        cfg.Host,
			Port:        int(cfg.Port),
			AppPassword: cfg.Password,
		}
		pools := NewPoolManager(10, 60)
		pools.GetOrCreate(route, nil)
		metrics := NewMetricsRegistry(100, 10, 10)
		disc := &TenantDiscovery{
			cfg:   gwCfg,
			cache: map[string]TenantRoute{cfg.Database: route},
		}
		disc.MarkSynced()
		cancelReg := NewCancelRegistry()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go HandleClientConnection(c, gwCfg, nil, disc, pools, metrics, cancelReg)
			}
		}()
		return ln.Addr().String(), pools, metrics, func() {
			_ = ln.Close()
			<-done
		}
	}

	addrA, _, _, stopA := startGW("gw-a")
	defer stopA()

	// Connect real pgx client to Gateway A and open a transaction.
	gwADSN := "postgres://app_user:" + cfg.Password + "@" + addrA + "/" + cfg.Database + "?sslmode=disable&default_query_exec_mode=simple_protocol"
	connA, err := pgx.Connect(ctx, gwADSN)
	if err != nil {
		t.Fatalf("connect Gateway A: %v", err)
	}
	defer connA.Close(ctx)

	txA, err := connA.Begin(ctx)
	if err != nil {
		t.Fatalf("connA.Begin: %v", err)
	}
	if _, err := txA.Exec(ctx, `INSERT INTO gw_tx_test (id, val) VALUES (1, 'from-gw-a')`); err != nil {
		t.Fatalf("txA.Exec: %v", err)
	}

	// While txA is open on Gateway A, start Gateway B and run a query on Gateway B.
	addrB, _, _, stopB := startGW("gw-b")
	defer stopB()

	gwBDSN := "postgres://app_user:" + cfg.Password + "@" + addrB + "/" + cfg.Database + "?sslmode=disable&default_query_exec_mode=simple_protocol"
	connB, err := pgx.Connect(ctx, gwBDSN)
	if err != nil {
		t.Fatalf("connect Gateway B: %v", err)
	}
	defer connB.Close(ctx)

	if _, err := connB.Exec(ctx, `INSERT INTO gw_tx_test (id, val) VALUES (2, 'from-gw-b')`); err != nil {
		t.Fatalf("connB.Exec: %v", err)
	}

	// Commit original transaction on Gateway A.
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("txA.Commit: %v", err)
	}

	var count int
	if err := adminPool.QueryRow(ctx, `SELECT count(*) FROM gw_tx_test`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 committed rows across Gateway A and B, got %d", count)
	}
}
