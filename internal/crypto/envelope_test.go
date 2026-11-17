package crypto

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestEnvelopeKMS_CacheAndShredIsolation(t *testing.T) {
	kms, err := LoadOrCreateEnvelopeKMS(filepath.Join(t.TempDir(), "master.kek"))
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}

	wrappedA, err := kms.EnsureTenantDEK("tenant_a")
	if err != nil {
		t.Fatalf("EnsureTenantDEK A: %v", err)
	}
	if _, err := kms.EnsureTenantDEK("tenant_b"); err != nil {
		t.Fatalf("EnsureTenantDEK B: %v", err)
	}

	pt := []byte("sensitive-postgres-page-data")
	ct, err := kms.EncryptObject("tenant_a", 1, "obj1", pt)
	if err != nil {
		t.Fatalf("EncryptObject: %v", err)
	}
	got, err := kms.DecryptObject("tenant_a", 1, "obj1", ct)
	if err != nil || !bytes.Equal(got, pt) {
		t.Fatalf("DecryptObject mismatch: err=%v", err)
	}

	// Cross-tenant / cross-epoch / wrong-object AAD must fail.
	if _, err := kms.DecryptObject("tenant_b", 1, "obj1", ct); !errors.Is(err, ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch for tenant_b, got %v", err)
	}
	if _, err := kms.DecryptObject("tenant_a", 2, "obj1", ct); !errors.Is(err, ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch for epoch 2, got %v", err)
	}
	if _, err := kms.DecryptObject("tenant_a", 1, "obj2", ct); !errors.Is(err, ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch for obj2, got %v", err)
	}

	// ShredDEK must invalidate both wrappedDEKs and cached cipher.AEAD immediately.
	kms.ShredDEK("tenant_a")
	if _, err := kms.DecryptObject("tenant_a", 1, "obj1", ct); !errors.Is(err, ErrDEKMissing) {
		t.Fatalf("expected ErrDEKMissing after ShredDEK, got %v", err)
	}

	// Reloading wrapped DEK restores decryption.
	if err := kms.LoadWrappedDEK("tenant_a", wrappedA); err != nil {
		t.Fatalf("LoadWrappedDEK: %v", err)
	}
	got2, err := kms.DecryptObject("tenant_a", 1, "obj1", ct)
	if err != nil || !bytes.Equal(got2, pt) {
		t.Fatalf("DecryptObject after reload failed: err=%v", err)
	}
}
