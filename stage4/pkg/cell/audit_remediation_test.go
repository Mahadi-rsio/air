package cell

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"air-stage4/pkg/crypto"
)

func TestRemediation_PathTraversalAndSymlinkContainment(t *testing.T) {
	volDir := t.TempDir()
	kms, _ := crypto.NewEnvelopeKMS()
	a, err := NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	victimDir := filepath.Join(volDir, "victim")
	_ = os.MkdirAll(victimDir, 0o755)
	victimFile := filepath.Join(victimDir, "critical.dat")
	_ = os.WriteFile(victimFile, []byte("intact"), 0o600)

	// Symlink escape attempt inside tenants/claiming and tenants/active
	_ = os.Symlink(victimDir, filepath.Join(volDir, "tenants/claiming/sym_escape"))
	_ = os.Symlink(victimDir, filepath.Join(volDir, "tenants/active/sym_escape"))

	badIDs := []string{
		"", ".", "..", "../victim", "../../victim", "/etc/passwd",
		"tenant/sub", "tenant\\sub", "sym_escape", "bad;rm -rf /", "a b",
	}
	for _, id := range badIDs {
		if _, err := a.ClaimCluster(id); err == nil {
			t.Fatalf("ClaimCluster accepted unsafe tenantID %q", id)
		}
		if _, err := a.ActivateCluster(id, "pw", 1); err == nil {
			t.Fatalf("ActivateCluster accepted unsafe tenantID %q", id)
		}
		if err := a.StopAndPurgeTenant(id); err == nil {
			t.Fatalf("StopAndPurgeTenant accepted unsafe tenantID %q", id)
		}
	}

	if _, err := os.Stat(victimFile); err != nil {
		t.Fatalf("victim file was harmed: %v", err)
	}

	// Concurrent path validation stress test
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				_, err := a.safeTenantSubpath("tenants/claiming", "../escape")
				if !errors.Is(err, ErrInvalidTenantID) && !errors.Is(err, ErrPathEscape) {
					t.Errorf("expected rejection, got %v", err)
				}
			} else {
				p, err := a.safeTenantSubpath("tenants/claiming", "valid_tenant-1")
				if err != nil || filepath.Base(p) != "valid_tenant-1" {
					t.Errorf("valid tenant failed: p=%s err=%v", p, err)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestRemediation_ReadoptFlockAndEpochAndPIDReuse(t *testing.T) {
	if _, err := os.Stat(filepath.Join(pgBin, "initdb")); err != nil {
		t.Skip("PG17 binary not installed on host")
	}
	volDir, err := os.MkdirTemp("/tmp", "air_cell_rem_")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(volDir, 0o755)
	defer os.RemoveAll(volDir)

	kms, _ := crypto.NewEnvelopeKMS()
	a1, err := NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a1.ClaimCluster("t_dual_own"); err != nil {
		t.Fatal(err)
	}
	rt, err := a1.ActivateCluster("t_dual_own", "pw_dual", 1)
	if err != nil {
		t.Fatal(err)
	}

	// Simultaneous second agent on same volDir MUST NOT readopt while a1 holds agent.lock!
	a2, err := NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a2.GetTenant("t_dual_own"); ok {
		a2.Close()
		a1.Close()
		t.Fatalf("Agent 2 illegally readopted t_dual_own while Agent 1 held flock!")
	}
	a2.Close()

	// Release a1's lockFD without stopping PG (simulating a1 crash where PG survived)
	a1.tenantMu.Lock()
	a1.releaseFlockLocked("t_dual_own")
	delete(a1.tenants, "t_dual_own")
	a1.tenantMu.Unlock()
	a1.Close()

	// Now test stale epoch rejection: if control-plane authoritative epoch is 2 > local epoch 1, refuse readopt!
	a3, err := NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	// First verify normal readopt works when epoch matches
	if _, ok := a3.GetTenant("t_dual_own"); !ok {
		t.Fatalf("expected a3 to readopt surviving PG17 cluster when lock was released")
	}
	a3.tenantMu.Lock()
	a3.releaseFlockLocked("t_dual_own")
	delete(a3.tenants, "t_dual_own")
	a3.tenantMu.Unlock()

	a3.SetEpochChecker(func(tenantID string) (int64, error) {
		return 2, nil // control plane moved to epoch 2!
	})
	if cnt := a3.ReadoptRunningTenants(); cnt != 0 {
		t.Fatalf("expected ReadoptRunningTenants to reject stale epoch 1 < 2, got count=%d", cnt)
	}

	// Test PID reuse detection: corrupt agent.pidmeta StartTime
	a3.SetEpochChecker(nil)
	activeBase := filepath.Join(volDir, "tenants/active/t_dual_own")
	writePIDMeta(activeBase, pidMeta{PID: rt.PID, StartTime: 999999999, Epoch: 1, PGData: rt.DataDir})
	if cnt := a3.ReadoptRunningTenants(); cnt != 0 {
		t.Fatalf("expected ReadoptRunningTenants to reject mismatched StartTime (PID reuse), got count=%d", cnt)
	}

	// Clean up surviving PG process
	SignalProcessTree(rt.PID, syscall.SIGKILL)
	a3.Close()
}

func TestRemediation_ProcessSupervision_NestedGrandchildKill(t *testing.T) {
	// Spawn a parent shell that spawns a nested grandchild sleep process
	cmd := exec.Command("sh", "-c", "sh -c 'sleep 30' & wait")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	rootPID := cmd.Process.Pid
	time.Sleep(150 * time.Millisecond)

	tree := collectProcessTree(rootPID)
	if len(tree) < 2 {
		SignalProcessTree(rootPID, syscall.SIGKILL)
		_ = cmd.Wait()
		t.Fatalf("expected nested child/grandchild in process tree, got %v", tree)
	}

	SignalProcessTree(rootPID, syscall.SIGKILL)
	_ = cmd.Wait()

	for _, p := range tree {
		if _, alive := readProcStartTime(p); alive {
			t.Fatalf("nested descendant PID %d survived SignalProcessTree SIGKILL!", p)
		}
	}
}
