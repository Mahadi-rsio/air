package readadapter

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

type failingStorageBackend struct {
	StorageBackend
	fail atomic.Bool
}

func (f *failingStorageBackend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	if f.fail.Load() {
		return nil, errors.New("injected pageserver/storage outage")
	}
	return f.StorageBackend.GetPageAtLSN(ctx, tenantID, key, lsn)
}

func (f *failingStorageBackend) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	if f.fail.Load() {
		return nil, 0, errors.New("injected pageserver/storage outage")
	}
	return f.StorageBackend.GetPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, lsn)
}

func TestPhase17E_FailureRecovery_StorageOutageAndClientDisconnect(t *testing.T) {
	tmpDir := t.TempDir()
	psSock := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: psSock,
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	const tid = "tenant_17e"
	_ = ps.EnsureTenant(tid, 1)
	ps.SetTenantGeneration(tid, "gen-1")

	// Seed minimal catalog + 1 data page via pageserver
	sockBackend := NewPageserverSocketBackend(psSock)
	fb := &failingStorageBackend{StorageBackend: sockBackend}

	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir:          filepath.Join(tmpDir, "srv"),
		PGPort:           0,
		NativeBackend:    fb,
		NativeSnapProv:   sockBackend,
		L1CacheBytes:     4 * 1024 * 1024,
		CacheDir:         filepath.Join(tmpDir, "l2"),
		QueryTimeout:     2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// 1. Inject storage outage -> query must return explicit error (no panic, no hang, no leaked budget/slots)
	fb.fail.Store(true)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv.Port(), tid))
	if err != nil {
		t.Fatal(err)
	}
	var dummy int
	if qerr := conn.QueryRow(ctx, "SELECT 1").Scan(&dummy); qerr == nil {
		t.Fatalf("expected explicit error during storage outage before catalog sync")
	}

	// 2. Recover storage -> verify memory and scheduler have zero leaked slots/bytes
	fb.fail.Store(false)
	m := srv.DetailedMetrics()
	if m.ActiveQueries != 0 {
		t.Fatalf("expected 0 active queries after error, got %d", m.ActiveQueries)
	}
	if m.Memory.GlobalUsedBytes != 0 {
		t.Fatalf("expected 0 leaked global memory bytes, got %d", m.Memory.GlobalUsedBytes)
	}
	if m.Scheduler.ActiveGlobal != 0 {
		t.Fatalf("expected 0 leaked scheduler slots, got %d", m.Scheduler.ActiveGlobal)
	}

	// 3. Abrupt client disconnect mid-session -> verify clean server state
	_ = conn.PgConn().Conn().Close()
	time.Sleep(50 * time.Millisecond)

	// New client must still connect cleanly without hanging
	conn2, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv.Port(), tid))
	if err != nil {
		t.Fatalf("reconnect after abrupt client close failed: %v", err)
	}
	defer conn2.Close(ctx)

	// 4. Context cancellation during query -> verify zero leaked memory/slots
	cctx, ccancel := context.WithCancel(ctx)
	ccancel()
	var got int
	_ = conn2.QueryRow(cctx, "SELECT 1").Scan(&got)
	m2 := srv.DetailedMetrics()
	if m2.Memory.GlobalUsedBytes != 0 || m2.Scheduler.ActiveGlobal != 0 {
		t.Fatalf("leaked resources after cancel: mem=%d sched=%d", m2.Memory.GlobalUsedBytes, m2.Scheduler.ActiveGlobal)
	}
}
