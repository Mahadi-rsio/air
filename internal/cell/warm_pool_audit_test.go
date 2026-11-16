package cell

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWarmPoolConcurrentClaimAtomicity(t *testing.T) {
	pgBin := "/usr/lib/postgresql/17/bin"
	if _, err := os.Stat(filepath.Join(pgBin, "initdb")); err != nil {
		t.Skipf("PG17 initdb not available at %s: %v", pgBin, err)
	}

	t.Setenv("AIR_WARM_POOL_SIZE", "3")
	volDir := t.TempDir()
	ag, err := NewAgent(volDir, pgBin, nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	// Wait up to 15s for at least 2 warm slots to be ready before concurrent burst.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ag.warmPoolMu.Lock()
		n := len(ag.warmPoolSlots)
		ag.warmPoolMu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	const workers = 6
	var wg sync.WaitGroup
	sysIDs := make([]string, workers)
	errs := make([]error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tid := fmt.Sprintf("t_conc_%d", idx)
			sysIDs[idx], errs[idx] = ag.ClaimCluster(tid)
		}(i)
	}
	wg.Wait()

	seen := make(map[string]int, workers)
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d ClaimCluster failed: %v", i, errs[i])
		}
		if sysIDs[i] == "" {
			t.Fatalf("worker %d returned empty system_identifier", i)
		}
		if prev, dup := seen[sysIDs[i]]; dup {
			t.Fatalf("duplicate system_identifier %s between worker %d and %d", sysIDs[i], prev, i)
		}
		seen[sysIDs[i]] = i

		tid := fmt.Sprintf("t_conc_%d", i)
		pgData, err := ag.TenantClaimingPGDataDir(tid)
		if err != nil {
			t.Fatalf("TenantClaimingPGDataDir(%s): %v", tid, err)
		}
		verBytes, err := os.ReadFile(filepath.Join(pgData, "PG_VERSION"))
		if err != nil {
			t.Fatalf("read PG_VERSION for %s: %v", tid, err)
		}
		if got := strings.TrimSpace(string(verBytes)); got != "17" {
			t.Fatalf("PG_VERSION for %s = %q, want 17", tid, got)
		}
		info, err := os.Stat(pgData)
		if err != nil {
			t.Fatalf("stat %s: %v", pgData, err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("pgdata mode for %s = %o, want 0700", tid, info.Mode().Perm())
		}
	}

	// Verify pool replenishes back to target=3 after exhaustion.
	replenishDeadline := time.Now().Add(20 * time.Second)
	replenished := false
	for time.Now().Before(replenishDeadline) {
		ag.warmPoolMu.Lock()
		n := len(ag.warmPoolSlots)
		ag.warmPoolMu.Unlock()
		if n == 3 {
			replenished = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !replenished {
		t.Fatalf("warm pool did not replenish to 3 slots within 20s")
	}
}

func TestWarmPoolStaleSlotCleanupOnRestart(t *testing.T) {
	t.Setenv("AIR_WARM_POOL_SIZE", "0")
	volDir := t.TempDir()
	staleFile := filepath.Join(volDir, "warm_pool", "slot_999", "pgdata", "stale.marker")
	if err := os.MkdirAll(filepath.Dir(staleFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staleFile, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Enable warm pool with nonexistent pgBin so initWarmPool still cleans stale poolDir if pgBin exists;
	// verify behavior when pgBin exists vs missing.
	ag, err := NewAgent(volDir, "/nonexistent/pg/bin", nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	// Note: When AIR_WARM_POOL_SIZE=0 or initdb binary is missing, initWarmPool returns early
	// BEFORE os.RemoveAll(poolDir). Documented in audit: stale slot_999 remains if pool is disabled.
	if _, statErr := os.Stat(staleFile); statErr != nil {
		t.Logf("stale file removed even when pool disabled: %v", statErr)
	}

	// Now test with real PG17 binary (or skip) and AIR_WARM_POOL_SIZE=1: stale slot_999 MUST be removed.
	pgBin := "/usr/lib/postgresql/17/bin"
	if _, err := os.Stat(filepath.Join(pgBin, "initdb")); err != nil {
		t.Skipf("PG17 initdb not available: %v", err)
	}
	t.Setenv("AIR_WARM_POOL_SIZE", "1")
	ag2, err := NewAgent(volDir, pgBin, nil)
	if err != nil {
		t.Fatalf("NewAgent 2: %v", err)
	}
	defer ag2.Close()
	if _, statErr := os.Stat(staleFile); !os.IsNotExist(statErr) {
		t.Fatalf("expected stale slot_999 to be cleaned on initWarmPool, got err=%v", statErr)
	}
}
