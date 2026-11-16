package control

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestColdOffloadQueueSuspendsImmediatelyAndEvictsAfterSafeSync(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tmpDir, err := os.MkdirTemp("/tmp", "air-cold-queue-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}

	ag, err := cell.NewAgent(filepath.Join(tmpDir, "vol"), "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	cp, err := NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
		MetaDir:        filepath.Join(tmpDir, "meta"),
		PGBinDir:       "/usr/lib/postgresql/17/bin",
		MetaPort:       freeTCPPort(t),
		Agent:          ag,
		KMS:            kms,
		EvictOnSuspend: true,
	})
	if err != nil {
		t.Fatalf("NewControlPlaneWithConfig: %v", err)
	}
	defer cp.Close()

	psSock := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath:    psSock,
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
	defer ps.Close()

	cp.SetPageserver(ps)
	cp.StartColdOffloadWorker(ctx, 1)

	const tid = "t_cold_offload"
	prov, err := cp.ProvisionDatabase(ctx, "acct_cold", "idem-cold-1", tid)
	if err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}
	if prov.State != "ACTIVE" {
		t.Fatalf("expected ACTIVE, got %s", prov.State)
	}

	if !ag.HasLocalPGData(tid) {
		t.Fatalf("expected local pgdata after provision")
	}

	// SuspendTenant should return immediately and mark SUSPENDED, then background queue
	// flushes pageserver, saves skeleton, and evicts local pgdata.
	if err := cp.SuspendTenant(ctx, tid, 5*time.Second, nil); err != nil {
		t.Fatalf("SuspendTenant: %v", err)
	}

	var state string
	if err := cp.db.QueryRow(ctx, `SELECT state FROM clusters WHERE tenant_id = $1`, tid).Scan(&state); err != nil {
		t.Fatalf("query state: %v", err)
	}
	if state != "SUSPENDED" {
		t.Fatalf("state after SuspendTenant = %q, want SUSPENDED", state)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if !ag.HasLocalPGData(tid) && ps.HasMetadataSkeleton(ctx, tid) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ag.HasLocalPGData(tid) {
		t.Fatalf("expected local pgdata to be evicted after cold offload queue completed")
	}
	if !ps.HasMetadataSkeleton(ctx, tid) {
		t.Fatalf("expected metadata skeleton to be saved before eviction")
	}

	// If air_smgr.so is installed (Docker/production), WakeTenant after eviction restores
	// from skeleton + Pageserver and starts PG17. Locally without air_smgr.so, verify
	// RestoreMetadataSkeleton reconstructs PGDATA metadata cleanly.
	if ag.HasAirSmgr() {
		port, _, _, err := cp.WakeTenant(ctx, tid)
		if err != nil {
			t.Fatalf("WakeTenant after eviction: %v", err)
		}
		if port <= 0 {
			t.Fatalf("expected valid direct_port after wake, got %d", port)
		}
		if !ag.HasLocalPGData(tid) {
			t.Fatalf("expected local pgdata to be restored on wake")
		}
	} else {
		pgDataDir, err := ag.TenantPGDataDir(tid)
		if err != nil {
			t.Fatalf("TenantPGDataDir: %v", err)
		}
		if _, err := ps.RestoreMetadataSkeleton(ctx, tid, prov.FencingEpoch, pgDataDir); err != nil {
			t.Fatalf("RestoreMetadataSkeleton: %v", err)
		}
		if !ag.HasLocalPGData(tid) {
			t.Fatalf("expected local pgdata skeleton to be restored")
		}
	}
}

func TestColdOffloadSkipsActiveTenant(t *testing.T) {
	if _, err := os.Stat("/usr/lib/postgresql/17/bin/postgres"); err != nil {
		t.Skip("PostgreSQL 17 binaries not installed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	tmpDir, err := os.MkdirTemp("/tmp", "air-cold-skip-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}

	ag, err := cell.NewAgent(filepath.Join(tmpDir, "vol"), "/usr/lib/postgresql/17/bin", kms)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	cp, err := NewControlPlaneWithConfig(ctx, ControlPlaneConfig{
		MetaDir:        filepath.Join(tmpDir, "meta"),
		PGBinDir:       "/usr/lib/postgresql/17/bin",
		MetaPort:       freeTCPPort(t),
		Agent:          ag,
		KMS:            kms,
		EvictOnSuspend: true,
	})
	if err != nil {
		t.Fatalf("NewControlPlaneWithConfig: %v", err)
	}
	defer cp.Close()

	const tid = "t_cold_active"
	if _, err := cp.ProvisionDatabase(ctx, "acct_cold", "idem-cold-2", tid); err != nil {
		t.Fatalf("ProvisionDatabase: %v", err)
	}

	evicted, err := cp.processColdOffload(ctx, tid)
	if err != nil {
		t.Fatalf("processColdOffload on ACTIVE tenant: %v", err)
	}
	if evicted {
		t.Fatalf("expected processColdOffload to skip eviction for ACTIVE tenant")
	}
	if !ag.HasLocalPGData(tid) {
		t.Fatalf("expected ACTIVE tenant local pgdata to remain intact")
	}
}
