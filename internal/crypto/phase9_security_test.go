package crypto

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPhase9_EnvelopeKMS_ConcurrentShredAndEncrypt(t *testing.T) {
	kms, err := NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	const tenant = "t-shred-race"
	wrapped, err := kms.EnsureTenantDEK(tenant)
	if err != nil {
		t.Fatalf("EnsureTenantDEK: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var okOps, missingOps atomic.Int64

	// Concurrent encrypt/decrypt/unwrap workers.
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pt := []byte(fmt.Sprintf("payload-%d", id))
			for {
				select {
				case <-stop:
					return
				default:
				}
				ct, err := kms.EncryptObject(tenant, 1, fmt.Sprintf("obj-%d", id), pt)
				if err != nil {
					if errors.Is(err, ErrDEKMissing) {
						missingOps.Add(1)
						continue
					}
					t.Errorf("unexpected EncryptObject error: %v", err)
					return
				}
				got, err := kms.DecryptObject(tenant, 1, fmt.Sprintf("obj-%d", id), ct)
				if err != nil {
					if errors.Is(err, ErrDEKMissing) {
						missingOps.Add(1)
						continue
					}
					t.Errorf("unexpected DecryptObject error: %v", err)
					return
				}
				if !bytes.Equal(got, pt) {
					t.Errorf("roundtrip mismatch")
					return
				}
				if dek, err := kms.UnwrapDEK(tenant); err == nil {
					zeroBytes(dek)
				} else if !errors.Is(err, ErrDEKMissing) {
					t.Errorf("unexpected UnwrapDEK error: %v", err)
					return
				}
				okOps.Add(1)
			}
		}(g)
	}

	// Shredder / restorer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			kms.ShredDEK(tenant)
			if err := kms.LoadWrappedDEK(tenant, wrapped); err != nil {
				t.Errorf("LoadWrappedDEK: %v", err)
				return
			}
		}
		kms.ShredDEK(tenant)
		close(stop)
	}()

	wg.Wait()

	// After final ShredDEK, operations MUST fail with ErrDEKMissing.
	if _, err := kms.EncryptObject(tenant, 1, "post-shred", []byte("x")); !errors.Is(err, ErrDEKMissing) {
		t.Fatalf("EncryptObject after ShredDEK = %v, want ErrDEKMissing", err)
	}
	if _, err := kms.DecryptObject(tenant, 1, "post-shred", []byte("x")); !errors.Is(err, ErrDEKMissing) {
		t.Fatalf("DecryptObject after ShredDEK = %v, want ErrDEKMissing", err)
	}
	if _, err := kms.UnwrapDEK(tenant); !errors.Is(err, ErrDEKMissing) {
		t.Fatalf("UnwrapDEK after ShredDEK = %v, want ErrDEKMissing", err)
	}
	t.Logf("concurrent shred/encrypt completed: okOps=%d missingOps=%d", okOps.Load(), missingOps.Load())
}

func TestPhase9_EnvelopeKMS_ConcurrentRotationAndCacheInvalidation(t *testing.T) {
	kms, err := NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	const tenant = "t-rotate"
	wrapped1, err := kms.EnsureTenantDEK(tenant)
	if err != nil {
		t.Fatalf("EnsureTenantDEK: %v", err)
	}

	// Generate a second distinct wrapped DEK under the same KEK.
	dek2 := make([]byte, 32)
	if _, err := rand.Read(dek2); err != nil {
		t.Fatal(err)
	}
	raw2, err := aeadSeal(kms.kek, dek2, []byte("wrap:"+tenant))
	if err != nil {
		t.Fatal(err)
	}
	wrapped2 := base64.StdEncoding.EncodeToString(raw2)

	// Run 100 rotation races: clear aeadCache, start concurrent tenantAEAD vs LoadWrappedDEK(wrapped2),
	// then verify that after LoadWrappedDEK(wrapped2) finishes, EncryptObject uses dek2 (not stale dek1).
	for iter := 0; iter < 100; iter++ {
		if err := kms.LoadWrappedDEK(tenant, wrapped1); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = kms.EncryptObject(tenant, 1, "race-key", []byte("hello"))
		}()
		go func() {
			defer wg.Done()
			if err := kms.LoadWrappedDEK(tenant, wrapped2); err != nil {
				t.Errorf("LoadWrappedDEK(wrapped2): %v", err)
			}
		}()
		wg.Wait()

		// Now kms MUST have wrapped2 active and cached, NOT stale wrapped1.
		ct, err := kms.EncryptObject(tenant, 1, "verify-key", []byte("secret-v2"))
		if err != nil {
			t.Fatalf("iter %d EncryptObject: %v", iter, err)
		}
		// Verify directly with dek2 that ct was encrypted under dek2.
		if _, err := aeadOpen(dek2, ct, []byte("t-rotate|1|verify-key")); err != nil {
			t.Fatalf("iter %d: EncryptObject used stale DEK1 instead of rotated DEK2: %v", iter, err)
		}
	}
}

func TestPhase9_EnvelopeKMS_InvalidWrappedDEKAndRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "master.kek")

	kms1, err := LoadOrCreateEnvelopeKMS(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateEnvelopeKMS: %v", err)
	}
	bundle, err := kms1.ExportWrappedDEKBundle("tenant-rec")
	if err != nil {
		t.Fatalf("ExportWrappedDEKBundle: %v", err)
	}
	ct, err := kms1.EncryptObject("tenant-rec", 5, "layers/l0.layer", []byte("critical-data"))
	if err != nil {
		t.Fatalf("EncryptObject: %v", err)
	}

	// Invalid base64 / truncated / tampered wrapped DEK must fail.
	if err := kms1.LoadWrappedDEK("tenant-rec", "!!!not-base64!!!"); err == nil {
		t.Fatalf("expected invalid base64 error")
	}
	if err := kms1.LoadWrappedDEK("tenant-rec", base64.StdEncoding.EncodeToString([]byte("short"))); !errors.Is(err, ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch for short wrapped DEK, got %v", err)
	}
	if err := kms1.LoadWrappedDEK("other-tenant", bundle.WrappedDEK); !errors.Is(err, ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch for cross-tenant wrapped DEK, got %v", err)
	}

	// Wrong KEK cannot unwrap bundle.
	otherKMS, err := NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if err := otherKMS.ImportWrappedDEKBundle(bundle); !errors.Is(err, ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch with wrong KEK, got %v", err)
	}

	// Simulated restart with persisted master KEK + bundle recovers ciphertext.
	kms2, err := LoadOrCreateEnvelopeKMS(keyPath)
	if err != nil {
		t.Fatalf("reload LoadOrCreateEnvelopeKMS: %v", err)
	}
	if err := kms2.ImportWrappedDEKBundle(bundle); err != nil {
		t.Fatalf("ImportWrappedDEKBundle: %v", err)
	}
	pt, err := kms2.DecryptObject("tenant-rec", 5, "layers/l0.layer", ct)
	if err != nil {
		t.Fatalf("DecryptObject after restart: %v", err)
	}
	if string(pt) != "critical-data" {
		t.Fatalf("recovered plaintext = %q, want critical-data", pt)
	}
}
