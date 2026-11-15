package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
)

var (
	ErrDEKShredded    = errors.New("crypto: tenant DEK has been crypto-shredded or does not exist")
	ErrAADMismatch    = errors.New("crypto: AES-256-GCM authentication failed (AAD or ciphertext tampered)")
	ErrKEKVersionGone = errors.New("crypto: requested KEK version is missing or revoked from keyring")
)

// WrappedDEKBundle is persisted inside R2 manifest.json so a DR cell in another region
// (sharing 0 RAM and 0 local disk with the primary cell) can unwrap the tenant DEK using
// only the versioned KEK keyring (S4-RISK-07, ADR-014).
type WrappedDEKBundle struct {
	TenantID   string `json:"tenant_id"`
	KeyVersion string `json:"key_version"` // e.g., "kek-v1", "kek-v2"
	WrappedDEK string `json:"wrapped_dek"` // base64(AES-GCM(KEK[KeyVersion], DEK, AAD="wrap:"+TenantID+"|"+KeyVersion))
}

// EnvelopeKMS manages a versioned keyring of 256-bit Master KEKs and per-tenant wrapped 256-bit DEKs.
type EnvelopeKMS struct {
	mu            sync.RWMutex
	activeVersion string
	kekRing       map[string][]byte           // keyVersion -> 32-byte KEK
	bundles       map[string]WrappedDEKBundle // tenantID -> WrappedDEKBundle
}

// NewEnvelopeKMS initializes a KMS with a fresh random "kek-v1".
func NewEnvelopeKMS() (*EnvelopeKMS, error) {
	kek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, kek); err != nil {
		return nil, err
	}
	return NewVersionedKMS("kek-v1", map[string][]byte{"kek-v1": kek})
}

// NewVersionedKMS initializes a KMS with an explicit KEK keyring (used for Cross-Region DR cells).
func NewVersionedKMS(activeVersion string, keks map[string][]byte) (*EnvelopeKMS, error) {
	ring := make(map[string][]byte, len(keks))
	for ver, raw := range keks {
		if len(raw) != 32 {
			return nil, fmt.Errorf("KEK %q must be 32 bytes, got %d", ver, len(raw))
		}
		cp := make([]byte, 32)
		copy(cp, raw)
		ring[ver] = cp
	}
	if _, ok := ring[activeVersion]; !ok {
		return nil, fmt.Errorf("active KEK version %q not in keyring", activeVersion)
	}
	return &EnvelopeKMS{
		activeVersion: activeVersion,
		kekRing:       ring,
		bundles:       make(map[string]WrappedDEKBundle),
	}, nil
}

func wrapAAD(tenantID, keyVersion string) []byte {
	return []byte("wrap:" + tenantID + "|" + keyVersion)
}

// EnsureTenantDEK generates and wraps a unique 256-bit DEK under the active KEK version if not present.
func (k *EnvelopeKMS) EnsureTenantDEK(tenantID string) (WrappedDEKBundle, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if b, ok := k.bundles[tenantID]; ok {
		return b, nil
	}
	kek, ok := k.kekRing[k.activeVersion]
	if !ok {
		return WrappedDEKBundle{}, ErrKEKVersionGone
	}
	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return WrappedDEKBundle{}, err
	}
	wrapped, err := aeadSeal(kek, dek, wrapAAD(tenantID, k.activeVersion))
	for i := range dek {
		dek[i] = 0
	}
	if err != nil {
		return WrappedDEKBundle{}, err
	}
	bundle := WrappedDEKBundle{
		TenantID:   tenantID,
		KeyVersion: k.activeVersion,
		WrappedDEK: base64.StdEncoding.EncodeToString(wrapped),
	}
	k.bundles[tenantID] = bundle
	return bundle, nil
}

// ImportWrappedDEKBundle loads a WrappedDEKBundle fetched from R2 manifest.json into a DR region's KMS
// and verifies it can be unwrapped using the local KEK keyring (zero shared RAM required).
func (k *EnvelopeKMS) ImportWrappedDEKBundle(bundle WrappedDEKBundle) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	kek, ok := k.kekRing[bundle.KeyVersion]
	if !ok {
		return ErrKEKVersionGone
	}
	rawWrapped, err := base64.StdEncoding.DecodeString(bundle.WrappedDEK)
	if err != nil {
		return err
	}
	dek, err := aeadOpen(kek, rawWrapped, wrapAAD(bundle.TenantID, bundle.KeyVersion))
	if err != nil {
		return ErrAADMismatch
	}
	for i := range dek {
		dek[i] = 0
	}
	k.bundles[bundle.TenantID] = bundle
	return nil
}

// ExportWrappedDEKBundle returns the tenant's self-contained WrappedDEKBundle for R2 manifest persistence.
func (k *EnvelopeKMS) ExportWrappedDEKBundle(tenantID string) (WrappedDEKBundle, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	b, ok := k.bundles[tenantID]
	if !ok {
		return WrappedDEKBundle{}, ErrDEKShredded
	}
	return b, nil
}

// UnwrapDEK unwraps and verifies the tenant's 256-bit DEK using the bundle's KEK version.
func (k *EnvelopeKMS) UnwrapDEK(tenantID string) ([]byte, error) {
	k.mu.RLock()
	bundle, ok := k.bundles[tenantID]
	if !ok {
		k.mu.RUnlock()
		return nil, ErrDEKShredded
	}
	kek, kekOK := k.kekRing[bundle.KeyVersion]
	k.mu.RUnlock()
	if !kekOK {
		return nil, ErrKEKVersionGone
	}
	rawWrapped, err := base64.StdEncoding.DecodeString(bundle.WrappedDEK)
	if err != nil {
		return nil, err
	}
	return aeadOpen(kek, rawWrapped, wrapAAD(tenantID, bundle.KeyVersion))
}

// RotateKEK adds newVersion -> newKEK to the keyring, re-wraps every tenant's DEK from its old KEK version
// to newVersion in O(1) metadata time (0 R2 data bytes re-encrypted), and optionally revokes old KEK versions.
func (k *EnvelopeKMS) RotateKEK(newVersion string, newKEK []byte, revokeOldVersions bool) (int, error) {
	if len(newKEK) != 32 {
		return 0, fmt.Errorf("newKEK must be 32 bytes")
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	cpKey := make([]byte, 32)
	copy(cpKey, newKEK)
	k.kekRing[newVersion] = cpKey

	reWrapped := 0
	for tenantID, bundle := range k.bundles {
		oldKEK, ok := k.kekRing[bundle.KeyVersion]
		if !ok {
			return reWrapped, ErrKEKVersionGone
		}
		rawOld, err := base64.StdEncoding.DecodeString(bundle.WrappedDEK)
		if err != nil {
			return reWrapped, err
		}
		dek, err := aeadOpen(oldKEK, rawOld, wrapAAD(tenantID, bundle.KeyVersion))
		if err != nil {
			return reWrapped, err
		}
		newWrapped, err := aeadSeal(cpKey, dek, wrapAAD(tenantID, newVersion))
		for i := range dek {
			dek[i] = 0
		}
		if err != nil {
			return reWrapped, err
		}
		k.bundles[tenantID] = WrappedDEKBundle{
			TenantID:   tenantID,
			KeyVersion: newVersion,
			WrappedDEK: base64.StdEncoding.EncodeToString(newWrapped),
		}
		reWrapped++
	}

	if revokeOldVersions {
		for ver, keyBytes := range k.kekRing {
			if ver != newVersion {
				for i := range keyBytes {
					keyBytes[i] = 0
				}
				delete(k.kekRing, ver)
			}
		}
	}
	k.activeVersion = newVersion
	return reWrapped, nil
}

// ShredDEK destroys the tenant's wrapped DEK in-place (instant GDPR crypto-shredding, Flow 12).
func (k *EnvelopeKMS) ShredDEK(tenantID string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.bundles, tenantID)
}

// BuildAAD constructs the mandatory Additional Authenticated Data binding tenant, epoch, and R2 key (SI-4).
func BuildAAD(tenantID string, epoch int64, objectKey string) []byte {
	return []byte(fmt.Sprintf("%s|%d|%s", tenantID, epoch, objectKey))
}

// EncryptObject encrypts plaintext using the tenant's DEK and AES-256-GCM bound to AAD.
func (k *EnvelopeKMS) EncryptObject(tenantID string, epoch int64, objectKey string, plaintext []byte) ([]byte, error) {
	dek, err := k.UnwrapDEK(tenantID)
	if err != nil {
		return nil, err
	}
	defer func() {
		for i := range dek {
			dek[i] = 0
		}
	}()
	return aeadSeal(dek, plaintext, BuildAAD(tenantID, epoch, objectKey))
}

// DecryptObject decrypts ciphertext using the tenant's DEK and verifies AAD (tenantID|epoch|objectKey).
func (k *EnvelopeKMS) DecryptObject(tenantID string, epoch int64, objectKey string, ciphertext []byte) ([]byte, error) {
	dek, err := k.UnwrapDEK(tenantID)
	if err != nil {
		return nil, err
	}
	defer func() {
		for i := range dek {
			dek[i] = 0
		}
	}()
	pt, err := aeadOpen(dek, ciphertext, BuildAAD(tenantID, epoch, objectKey))
	if err != nil {
		return nil, ErrAADMismatch
	}
	return pt, nil
}

func aeadSeal(key, plaintext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, aad), nil
}

func aeadOpen(key, ciphertext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}
	nonce := ciphertext[:gcm.NonceSize()]
	ct := ciphertext[gcm.NonceSize():]
	return gcm.Open(nil, nonce, ct, aad)
}
