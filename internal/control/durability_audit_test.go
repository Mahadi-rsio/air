package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/jackc/pgx/v5"
)

func setupAuditEnv(t *testing.T) (context.Context, string, *cell.Agent, *ControlPlane, *pageserver.Pageserver) {
	t.Helper()
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}
	t.Setenv("AIR_WARM_POOL_SIZE", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	tmpDir, err := os.MkdirTemp("/tmp", "air-audit-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	ag, err := cell.NewAgent(filepath.Join(tmpDir, "vol"), "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	t.Cleanup(ag.Close)

	cp, err := NewControlPlane(ctx, filepath.Join(tmpDir, "meta"), "/usr/lib/postgresql/17/bin", freeTCPPort(t), ag, kms)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	t.Cleanup(cp.Close)

	ps, err := pageserver.New(pageserver.Config{
		SocketPath:    filepath.Join(tmpDir, "ps.sock"),
		LayerDir:      filepath.Join(tmpDir, "ps_layers"),
		CacheDir:      filepath.Join(tmpDir, "ps_cache"),
		CacheMaxBytes: 32 * 1024 * 1024,
		KMS:           kms,
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close() })
	cp.SetPageserver(ps)
	return ctx, tmpDir, ag, cp, ps
}

func TestAuditSuspendDuringActiveWritesAndDurability(t *testing.T) {
	ctx, _, ag, cp, _ := setupAuditEnv(t)
	const acct = "acct_audit"
	const tid = "t_audit_dur"

	resp, err := cp.ProvisionDatabase(ctx, acct, "idem-audit-1", tid)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}
	dsn := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", resp.Password, resp.DirectPort, resp.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE ledger (id int PRIMARY KEY, val text); INSERT INTO ledger VALUES (1, 'committed_before_tx');`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Open uncommitted transaction to verify SuspendTenant vetoes during active writes.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ledger VALUES (2, 'in_flight')`); err != nil {
		t.Fatalf("tx Exec: %v", err)
	}
	if sErr := cp.SuspendTenant(ctx, tid, 500*time.Millisecond, nil); !errors.Is(sErr, cell.ErrDrainVetoed) {
		t.Fatalf("expected ErrDrainVetoed during active tx, got: %v", sErr)
	}

	// Commit transaction, close client, then suspend cleanly.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_ = conn.Close(ctx)

	if err := cp.SuspendTenant(ctx, tid, 2*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant after commit: %v", err)
	}

	// Verify local pgdata is preserved (ACTIVE -> SUSPENDED) and wake preserves all committed rows.
	if !ag.HasLocalPGData(tid) {
		t.Fatalf("expected HasLocalPGData=true after warm suspend")
	}
	port, _, pw, err := cp.WakeTenant(ctx, tid)
	if err != nil {
		t.Fatalf("WakeTenant (warm): %v", err)
	}
	conn2, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", pw, port, tid))
	if err != nil {
		t.Fatalf("pgx.Connect after warm wake: %v", err)
	}
	var cnt int
	if err := conn2.QueryRow(ctx, `SELECT count(*) FROM ledger`).Scan(&cnt); err != nil || cnt != 2 {
		t.Fatalf("row count after warm wake = %d (err=%v), want 2", cnt, err)
	}
	_ = conn2.Close(ctx)

	// Now suspend again, simulate local disk loss (EVICTED: remove local pgdata + lfc), and verify Pageserver skeleton recovery if air_smgr is installed.
	if ag.HasAirSmgr() {
		if err := cp.SuspendTenant(ctx, tid, 2*time.Second, nil); err != nil {
			t.Fatalf("SuspendTenant 2: %v", err)
		}
		if err := ag.EvictTenantLocalData(tid); err != nil {
			t.Fatalf("EvictTenantLocalData: %v", err)
		}
		lfcDir := filepath.Join(ag.VolDir(), "lfc", tid)
		_ = os.RemoveAll(lfcDir)

		if ag.HasLocalPGData(tid) {
			t.Fatalf("expected HasLocalPGData=false after eviction")
		}
		port2, _, pw2, err := cp.WakeTenant(ctx, tid)
		if err != nil {
			t.Fatalf("WakeTenant (evicted/skeleton): %v", err)
		}
		conn3, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", pw2, port2, tid))
		if err != nil {
			t.Fatalf("pgx.Connect after skeleton wake: %v", err)
		}
		defer conn3.Close(ctx)
		var cnt2 int
		if err := conn3.QueryRow(ctx, `SELECT count(*) FROM ledger`).Scan(&cnt2); err != nil || cnt2 != 2 {
			t.Fatalf("row count after skeleton wake = %d (err=%v), want 2", cnt2, err)
		}
	} else {
		t.Log("air_smgr.so not installed on host; skeleton-eviction test covered in Docker")
	}
}

func TestAuditAbruptCrashRecovery(t *testing.T) {
	ctx, _, ag, cp, _ := setupAuditEnv(t)
	const acct = "acct_crash"
	const tid = "t_audit_crash"

	resp, err := cp.ProvisionDatabase(ctx, acct, "idem-crash-1", tid)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}
	dsn := fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", resp.Password, resp.DirectPort, resp.Database)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE crash_t (id int PRIMARY KEY); INSERT INTO crash_t SELECT generate_series(1, 500);`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	_ = conn.Close(ctx)

	rt, ok := ag.GetTenant(tid)
	if !ok || rt.PID <= 0 {
		t.Fatalf("expected active tenant PID")
	}
	// Simulate abrupt container/process termination (SIGKILL) while ACTIVE.
	if err := syscall.Kill(rt.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	// Wait briefly for process exit.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(rt.PID, 0) != nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Verify state in control plane is still ACTIVE (stale state after abrupt crash).
	state, _, err := cp.GetTenantRoute(ctx, tid)
	if err != nil {
		t.Fatalf("GetTenantRoute: %v", err)
	}
	if state != "ACTIVE" {
		t.Fatalf("expected stale ACTIVE state after SIGKILL, got %s", state)
	}

	// Calling WakeTenant directly recovers PG via local WAL crash recovery.
	port, _, pw, err := cp.WakeTenant(ctx, tid)
	if err != nil {
		t.Fatalf("WakeTenant after SIGKILL: %v", err)
	}
	conn2, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app_user:%s@127.0.0.1:%d/%s?sslmode=disable", pw, port, tid))
	if err != nil {
		t.Fatalf("pgx.Connect after crash recovery: %v", err)
	}
	defer conn2.Close(ctx)
	var cnt int
	if err := conn2.QueryRow(ctx, `SELECT count(*) FROM crash_t`).Scan(&cnt); err != nil || cnt != 500 {
		t.Fatalf("crash_t count = %d (err=%v), want 500", cnt, err)
	}
}
