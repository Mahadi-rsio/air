package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"air-stage1/pkg/cell"
	"air-stage1/pkg/crypto"
)

func setupStage1TestDir(t *testing.T) (string, func()) {
	t.Helper()
	base, err := os.MkdirTemp("/tmp", "air_ctrl_rem_")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(base, 0o755)
	return base, func() { _ = os.RemoveAll(base) }
}

func TestRemediation_MetadataPreservedAcrossRestartAndNoPlaintextPasswords(t *testing.T) {
	if _, err := os.Stat(filepath.Join(pgBin, "initdb")); err != nil {
		t.Skip("PG17 binary not installed on host")
	}
	ctx := context.Background()
	base, cleanup := setupStage1TestDir(t)
	defer cleanup()

	metaDir := filepath.Join(base, "meta")
	volDir := filepath.Join(base, "vol")
	kms, _ := crypto.NewEnvelopeKMS()
	ag, err := cell.NewAgent(volDir, 0, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close()

	cp1, err := NewControlPlane(ctx, metaDir, 19532, ag, kms)
	if err != nil {
		t.Fatal(err)
	}
	ver, err := cp1.SchemaVersion(ctx)
	if err != nil || ver < 2 {
		t.Fatalf("expected schema version >= 2, got %d (err=%v)", ver, err)
	}
	resp, err := cp1.ProvisionDatabase(ctx, "idem-persist-1", "tenant_alpha")
	if err != nil {
		t.Fatal(err)
	}
	// Verify no plaintext password in DB or idempotency_records
	var rawPW, rawEnc, rawResp string
	if err := cp1.db.QueryRow(ctx, `SELECT app_password, app_password_enc FROM clusters WHERE tenant_id = 'tenant_alpha'`).Scan(&rawPW, &rawEnc); err != nil {
		t.Fatal(err)
	}
	if rawPW != "[ENCRYPTED]" || rawEnc == "" || strings.Contains(rawEnc, resp.AppPassword) {
		t.Fatalf("expected encrypted password in clusters, got rawPW=%q rawEnc=%q", rawPW, rawEnc)
	}
	if err := cp1.db.QueryRow(ctx, `SELECT response_json FROM idempotency_records WHERE idempotency_key = 'idem-persist-1'`).Scan(&rawResp); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rawResp, resp.AppPassword) {
		t.Fatalf("plaintext password leaked into idempotency_records.response_json!")
	}
	cp1.Close()

	// Restart on same metaDir: must preserve metadata!
	cp2, err := NewControlPlane(ctx, metaDir, 19532, ag, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer cp2.Close()

	state, _, _, decPW, err := cp2.GetTenantAuth(ctx, "tenant_alpha")
	if err != nil || state != "ACTIVE" || decPW != resp.AppPassword {
		t.Fatalf("metadata lost or corrupted across restart: state=%s err=%v pwMatch=%v", state, err, decPW == resp.AppPassword)
	}
}

func TestRemediation_CorruptedMetadataRefusesDestructiveReset(t *testing.T) {
	if _, err := os.Stat(filepath.Join(pgBin, "initdb")); err != nil {
		t.Skip("PG17 binary not installed on host")
	}
	ctx := context.Background()
	base, cleanup := setupStage1TestDir(t)
	defer cleanup()

	metaDir := filepath.Join(base, "meta_corrupt")
	pgData := filepath.Join(metaDir, "pgdata")
	_ = os.MkdirAll(pgData, 0o700)
	_ = os.WriteFile(filepath.Join(pgData, "PG_VERSION"), []byte("17\n"), 0o600)
	canary := filepath.Join(pgData, "important_marker")
	_ = os.WriteFile(canary, []byte("keep-me"), 0o600)

	kms, _ := crypto.NewEnvelopeKMS()
	ag, _ := cell.NewAgent(filepath.Join(base, "vol"), 0, kms)
	defer ag.Close()

	_, err := NewControlPlane(ctx, metaDir, 19533, ag, kms)
	if !errors.Is(err, ErrMetadataCorrupted) {
		t.Fatalf("expected ErrMetadataCorrupted, got %v", err)
	}
	if _, statErr := os.Stat(canary); os.IsNotExist(statErr) {
		t.Fatalf("NewControlPlane destroyed corrupted metaDir instead of failing closed!")
	}
}

func TestRemediation_CrossTenantIdempotencyConcurrentIsolation(t *testing.T) {
	if _, err := os.Stat(filepath.Join(pgBin, "initdb")); err != nil {
		t.Skip("PG17 binary not installed on host")
	}
	ctx := context.Background()
	base, cleanup := setupStage1TestDir(t)
	defer cleanup()

	kms, _ := crypto.NewEnvelopeKMS()
	ag, _ := cell.NewAgent(filepath.Join(base, "vol"), 0, kms)
	defer ag.Close()

	cp, err := NewControlPlane(ctx, filepath.Join(base, "meta"), 19534, ag, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()

	respA, err := cp.ProvisionDatabase(ctx, "shared-key-x", "tenant_a")
	if err != nil {
		t.Fatal(err)
	}

	// Same tenant replay succeeds and returns same credentials
	replay, err := cp.ProvisionDatabase(ctx, "shared-key-x", "tenant_a")
	if err != nil || !replay.IdempotentReplay || replay.AppPassword != respA.AppPassword {
		t.Fatalf("same-tenant replay failed: err=%v replay=%+v", err, replay)
	}

	// Concurrent cross-tenant collision: 5 goroutines for tenant_b..f using tenant_a's key must ALL fail with ErrIdempotencyConflict!
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tid := fmt.Sprintf("tenant_attacker_%d", idx)
			r, pErr := cp.ProvisionDatabase(ctx, "shared-key-x", tid)
			if !errors.Is(pErr, ErrIdempotencyConflict) || r != nil {
				t.Errorf("attacker %s was not rejected with ErrIdempotencyConflict: err=%v r=%+v", tid, pErr, r)
			}
		}(i)
	}
	wg.Wait()
}

func TestRemediation_ProvisioningCrashConsistencyAndRecovery(t *testing.T) {
	if _, err := os.Stat(filepath.Join(pgBin, "initdb")); err != nil {
		t.Skip("PG17 binary not installed on host")
	}
	ctx := context.Background()
	base, cleanup := setupStage1TestDir(t)
	defer cleanup()

	volDir := filepath.Join(base, "vol")
	metaDir := filepath.Join(base, "meta")
	kms, _ := crypto.NewEnvelopeKMS()
	ag, _ := cell.NewAgent(volDir, 0, kms)
	defer ag.Close()

	cp, err := NewControlPlane(ctx, metaDir, 19535, ag, kms)
	if err != nil {
		t.Fatal(err)
	}

	// Inject faults at every transition stage and verify clean rollback
	for _, stage := range []string{"after_requested", "after_claim", "after_db_claim", "after_activate"} {
		cp.SetFaultHook(func(s string) error {
			if s == stage {
				return fmt.Errorf("injected crash at %s", s)
			}
			return nil
		})
		_, pErr := cp.ProvisionDatabase(ctx, "idem-crash-"+stage, "t_crash_"+stage)
		if pErr == nil {
			t.Fatalf("expected error at fault stage %s", stage)
		}
		if _, statErr := os.Stat(filepath.Join(volDir, "tenants/claiming", "t_crash_"+stage)); !os.IsNotExist(statErr) {
			t.Fatalf("orphaned claiming dir left after fault at %s", stage)
		}
	}
	cp.SetFaultHook(nil)

	// Simulate dirty in-flight DB rows + orphan claiming dir left by SIGKILL, then restart ControlPlane
	_ = os.MkdirAll(filepath.Join(volDir, "tenants/claiming/t_sigkill"), 0o700)
	_, err = cp.db.Exec(ctx, `INSERT INTO provisioning_ops (op_id, tenant_id, idempotency_key, state) VALUES ('op_dead', 't_sigkill', 'idem_dead', 'CLAIMED')`)
	if err != nil {
		t.Fatal(err)
	}
	cp.Close()

	cp2, err := NewControlPlane(ctx, metaDir, 19535, ag, kms)
	if err != nil {
		t.Fatal(err)
	}
	defer cp2.Close()

	var opState string
	if err := cp2.db.QueryRow(ctx, `SELECT state FROM provisioning_ops WHERE op_id = 'op_dead'`).Scan(&opState); err != nil || opState != "ROLLED_BACK" {
		t.Fatalf("expected op_dead to be ROLLED_BACK on startup recovery, got %q (err=%v)", opState, err)
	}
	if _, statErr := os.Stat(filepath.Join(volDir, "tenants/claiming/t_sigkill")); !os.IsNotExist(statErr) {
		t.Fatalf("orphan claiming dir was not cleaned up by recovery")
	}

	// Retry provisioning for t_sigkill must now succeed cleanly
	okResp, err := cp2.ProvisionDatabase(ctx, "idem_dead_retry", "t_sigkill")
	if err != nil || okResp.State != "ACTIVE" {
		t.Fatalf("retry after crash recovery failed: err=%v resp=%+v", err, okResp)
	}
}

func TestRemediation_ValidateTenantIDAndPasswordEncryptionUnit(t *testing.T) {
	bad := []string{"", ".", "..", "../etc", "a/b", "a\\b", "a b", "bad;drop"}
	for _, id := range bad {
		if err := ValidateTenantID(id); !errors.Is(err, ErrInvalidTenantID) {
			t.Fatalf("ValidateTenantID(%q) = %v, want ErrInvalidTenantID", id, err)
		}
	}
	if err := ValidateTenantID("tenant_alpha-01"); err != nil {
		t.Fatalf("valid tenantID rejected: %v", err)
	}

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	cp := &ControlPlane{kms: kms}
	enc, err := cp.encryptTenantPassword("tenant_alpha", "pw_secret_123")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(enc, "pw_secret_123") {
		t.Fatalf("encrypted password contains plaintext!")
	}
	dec, err := cp.decryptTenantPassword("tenant_alpha", enc)
	if err != nil || dec != "pw_secret_123" {
		t.Fatalf("decryptTenantPassword failed: dec=%q err=%v", dec, err)
	}
	// Cross-tenant decryption MUST fail AAD check
	if _, err := cp.decryptTenantPassword("tenant_beta", enc); err == nil {
		t.Fatalf("cross-tenant password decryption succeeded!")
	}

	// DeleteDatabase & IngestUsageBatch must reject invalid tenantIDs before touching DB
	if err := cp.DeleteDatabase(context.Background(), "../escape", true); !errors.Is(err, ErrInvalidTenantID) {
		t.Fatalf("DeleteDatabase accepted invalid tenantID: %v", err)
	}
	if _, err := cp.IngestUsageBatch(context.Background(), []UsageEventBatch{{TenantID: "../escape"}}); !errors.Is(err, ErrInvalidTenantID) {
		t.Fatalf("IngestUsageBatch accepted invalid tenantID: %v", err)
	}
}
