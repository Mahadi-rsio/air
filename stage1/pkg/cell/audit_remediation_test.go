package cell

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"air-stage1/pkg/crypto"
)

func TestStage1_PathTraversalAndSymlinkContainment(t *testing.T) {
	volDir := t.TempDir()
	kms, _ := crypto.NewEnvelopeKMS()
	a, err := NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	victimDir := filepath.Join(volDir, "victim")
	_ = os.MkdirAll(victimDir, 0o755)
	victimFile := filepath.Join(victimDir, "keep.dat")
	_ = os.WriteFile(victimFile, []byte("intact"), 0o600)

	_ = os.Symlink(victimDir, filepath.Join(volDir, "tenants/claiming/sym_escape"))
	_ = os.Symlink(victimDir, filepath.Join(volDir, "tenants/active/sym_escape"))

	badIDs := []string{
		"", ".", "..", "../victim", "../../victim", "/etc/passwd",
		"tenant/sub", "tenant\\sub", "sym_escape", "bad;rm", "a b",
	}
	for _, id := range badIDs {
		if _, err := a.ClaimCluster(id); err == nil {
			t.Fatalf("ClaimCluster accepted unsafe tenantID %q", id)
		}
		if _, err := a.ActivateCluster(id, "pw", 1); err == nil {
			t.Fatalf("ActivateCluster accepted unsafe tenantID %q", id)
		}
		if _, err := a.StartOrAdoptTenant(id, 1); err == nil {
			t.Fatalf("StartOrAdoptTenant accepted unsafe tenantID %q", id)
		}
		if err := a.StopAndPurgeTenant(id); err == nil {
			t.Fatalf("StopAndPurgeTenant accepted unsafe tenantID %q", id)
		}
		if err := a.StopTenantOnly(id); err == nil {
			t.Fatalf("StopTenantOnly accepted unsafe tenantID %q", id)
		}
		a.RollbackClaim(id)
	}

	if _, err := os.Stat(victimFile); err != nil {
		t.Fatalf("victim file was harmed: %v", err)
	}
}

func TestStage1_ReadoptFlockContention(t *testing.T) {
	volDir := t.TempDir()
	kms, _ := crypto.NewEnvelopeKMS()
	a1, err := NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer a1.Close()

	// Create a tenant directory with an active flock held by an external FD
	tDir := filepath.Join(volDir, "tenants/active/t_locked")
	_ = os.MkdirAll(filepath.Join(tDir, "pgdata"), 0o700)
	lockPath := filepath.Join(tDir, "agent.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	// acquireFlockLocked must fail when another process/FD holds agent.lock
	a1.tenantMu.Lock()
	err = a1.acquireFlockLocked("t_locked", lockPath)
	a1.tenantMu.Unlock()
	if err == nil {
		t.Fatalf("expected acquireFlockLocked to fail when flock is held")
	}
}
