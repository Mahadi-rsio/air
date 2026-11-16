package cell

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestClaimWarmPGDataAndReadSystemIdentifier(t *testing.T) {
	t.Setenv("AIR_WARM_POOL_SIZE", "0")
	volDir := t.TempDir()
	ag, err := NewAgent(volDir, "/nonexistent/pg/bin", nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	// Populate a synthetic warm slot and verify claimWarmPGData moves it atomically.
	slotDir := filepath.Join(volDir, "warm_pool", "slot_1")
	srcPG := filepath.Join(slotDir, "pgdata")
	if err := os.MkdirAll(filepath.Join(srcPG, "global"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcPG, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var ctrl [8]byte
	binary.LittleEndian.PutUint64(ctrl[:], 7412345678901234567)
	if err := os.WriteFile(filepath.Join(srcPG, "global", "pg_control"), ctrl[:], 0o600); err != nil {
		t.Fatal(err)
	}

	ag.warmPoolMu.Lock()
	ag.warmPoolSlots = append(ag.warmPoolSlots, slotDir)
	ag.warmPoolMu.Unlock()

	dstPG := filepath.Join(volDir, "tenants", "claiming", "t_warm", "pgdata")
	if !ag.claimWarmPGData(dstPG) {
		t.Fatalf("expected claimWarmPGData to succeed")
	}
	if _, err := os.Stat(filepath.Join(dstPG, "PG_VERSION")); err != nil {
		t.Fatalf("expected claimed PG_VERSION: %v", err)
	}
	if got := ag.readSystemIdentifier(dstPG); got != "7412345678901234567" {
		t.Fatalf("readSystemIdentifier = %q, want 7412345678901234567", got)
	}
	if ag.claimWarmPGData(filepath.Join(volDir, "tenants", "claiming", "t_empty", "pgdata")) {
		t.Fatalf("expected claimWarmPGData on empty pool to return false")
	}
}
