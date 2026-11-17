package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrDEKMissing  = errors.New("crypto: tenant DEK has been shredded or does not exist")
	ErrAADMismatch = errors.New("crypto: AES-256-GCM authentication failed")
)

// WrappedDEKBundle is persisted inside R2 manifest.json so a recovery cell
// can unwrap the tenant DEK using the master KEK.
type WrappedDEKBundle struct {
	TenantID   string `json:"tenant_id"`
	KeyVersion string `json:"key_version"`
	WrappedDEK string `json:"wrapped_dek"`
}

// EnvelopeKMS manages a 256-bit master KEK and per-tenant 256-bit DEKs wrapped with AES-256-GCM.
type EnvelopeKMS struct {
	mu           sync.RWMutex
	kek          []byte
	wrappedDEKs  map[string][]byte
	aeadCache    map[string]cipher.AEAD
	keyEpochs    map[string]uint64
	onInvalidate []func(tenantID string)
}

// OnKeyInvalidated registers a callback invoked whenever a tenant's DEK is rotated, reloaded, or shredded.
func (k *EnvelopeKMS) OnKeyInvalidated(fn func(tenantID string)) {
	if fn == nil {
		return
	}
	k.mu.Lock()
	k.onInvalidate = append(k.onInvalidate, fn)
	k.mu.Unlock()
}

func (k *EnvelopeKMS) notifyInvalidated(tenantID string, hooks []func(string)) {
	for _, fn := range hooks {
		fn(tenantID)
	}
}

// TenantKeyEpoch returns the monotonic key epoch for tenantID, or ErrDEKMissing if not loaded/shredded.
func (k *EnvelopeKMS) TenantKeyEpoch(tenantID string) (uint64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	w, ok := k.wrappedDEKs[tenantID]
	if !ok || len(w) == 0 {
		return 0, ErrDEKMissing
	}
	if k.keyEpochs == nil {
		return 1, nil
	}
	ep := k.keyEpochs[tenantID]
	if ep == 0 {
		return 1, nil
	}
	return ep, nil
}

// NewEnvelopeKMS creates an in-memory KMS with a fresh 256-bit KEK (primarily for tests).
func NewEnvelopeKMS() (*EnvelopeKMS, error) {
	kek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, kek); err != nil {
		return nil, err
	}
	return &EnvelopeKMS{
		kek:         kek,
		wrappedDEKs: make(map[string][]byte),
		aeadCache:   make(map[string]cipher.AEAD),
	}, nil
}

// LoadOrCreateEnvelopeKMS loads a persistent 32-byte hex-encoded KEK from keyPath or creates one atomically (0600).
func LoadOrCreateEnvelopeKMS(keyPath string) (*EnvelopeKMS, error) {
	if raw, err := os.ReadFile(keyPath); err == nil {
		kek, decErr := hex.DecodeString(strings.TrimSpace(string(raw)))
		if decErr != nil || len(kek) != 32 {
			return nil, fmt.Errorf("invalid master key at %s", keyPath)
		}
		return &EnvelopeKMS{kek: kek, wrappedDEKs: make(map[string][]byte), aeadCache: make(map[string]cipher.AEAD)}, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return nil, err
	}
	kek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, kek); err != nil {
		return nil, err
	}
	tmp := keyPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(hex.EncodeToString(kek)+"\n"), 0o600); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, keyPath); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	return &EnvelopeKMS{kek: kek, wrappedDEKs: make(map[string][]byte), aeadCache: make(map[string]cipher.AEAD)}, nil
}

// EnsureTenantDEK generates and wraps a 256-bit DEK for tenantID if not already loaded.
func (k *EnvelopeKMS) EnsureTenantDEK(tenantID string) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if w, ok := k.wrappedDEKs[tenantID]; ok {
		return base64.StdEncoding.EncodeToString(w), nil
	}
	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return "", err
	}
	wrapped, err := aeadSeal(k.kek, dek, []byte("wrap:"+tenantID))
	for i := range dek {
		dek[i] = 0
	}
	if err != nil {
		return "", err
	}
	k.wrappedDEKs[tenantID] = wrapped
	if k.keyEpochs == nil {
		k.keyEpochs = make(map[string]uint64)
	}
	k.keyEpochs[tenantID]++
	return base64.StdEncoding.EncodeToString(wrapped), nil
}

// LoadWrappedDEK restores a previously persisted base64 wrapped DEK into memory after restart.
func (k *EnvelopeKMS) LoadWrappedDEK(tenantID, wrappedB64 string) error {
	raw, err := base64.StdEncoding.DecodeString(wrappedB64)
	if err != nil {
		return err
	}
	// Verify it unwraps cleanly under current KEK before caching.
	dek, err := aeadOpen(k.kek, raw, []byte("wrap:"+tenantID))
	if err != nil {
		return ErrAADMismatch
	}
	for i := range dek {
		dek[i] = 0
	}
	k.mu.Lock()
	k.wrappedDEKs[tenantID] = raw
	if k.aeadCache != nil {
		delete(k.aeadCache, tenantID)
	}
	if k.keyEpochs == nil {
		k.keyEpochs = make(map[string]uint64)
	}
	k.keyEpochs[tenantID]++
	hooks := append(([]func(string))(nil), k.onInvalidate...)
	k.mu.Unlock()
	k.notifyInvalidated(tenantID, hooks)
	return nil
}

// ExportWrappedDEKBundle ensures a tenant DEK exists and returns it as a self-contained WrappedDEKBundle.
func (k *EnvelopeKMS) ExportWrappedDEKBundle(tenantID string) (WrappedDEKBundle, error) {
	wrappedB64, err := k.EnsureTenantDEK(tenantID)
	if err != nil {
		return WrappedDEKBundle{}, err
	}
	return WrappedDEKBundle{
		TenantID:   tenantID,
		KeyVersion: "kek-v1",
		WrappedDEK: wrappedB64,
	}, nil
}

// ImportWrappedDEKBundle loads a WrappedDEKBundle into the KMS after verifying its KEK unwrap.
func (k *EnvelopeKMS) ImportWrappedDEKBundle(b WrappedDEKBundle) error {
	return k.LoadWrappedDEK(b.TenantID, b.WrappedDEK)
}

// UnwrapDEK unwraps the tenant's 256-bit DEK.
func (k *EnvelopeKMS) UnwrapDEK(tenantID string) ([]byte, error) {
	k.mu.RLock()
	wrapped, ok := k.wrappedDEKs[tenantID]
	if !ok || len(wrapped) == 0 {
		k.mu.RUnlock()
		return nil, ErrDEKMissing
	}
	wrappedCopy := append([]byte(nil), wrapped...)
	k.mu.RUnlock()
	defer zeroBytes(wrappedCopy)
	return aeadOpen(k.kek, wrappedCopy, []byte("wrap:"+tenantID))
}

// ShredDEK zeroes and removes the tenant's wrapped DEK and cached AEAD from memory.
// Security semantics: ShredDEK is a linearizable erasure boundary under k.mu. Any
// EncryptObject, DecryptObject, or UnwrapDEK call that acquires k.mu after ShredDEK
// returns ErrDEKMissing. An in-flight Seal/Open that already acquired its cipher.AEAD
// handle before ShredDEK completes that single operation and cannot be reused.
func (k *EnvelopeKMS) ShredDEK(tenantID string) {
	k.mu.Lock()
	if w, ok := k.wrappedDEKs[tenantID]; ok {
		zeroBytes(w)
		delete(k.wrappedDEKs, tenantID)
	}
	if k.aeadCache != nil {
		delete(k.aeadCache, tenantID)
	}
	if k.keyEpochs == nil {
		k.keyEpochs = make(map[string]uint64)
	}
	k.keyEpochs[tenantID]++
	hooks := append(([]func(string))(nil), k.onInvalidate...)
	k.mu.Unlock()
	k.notifyInvalidated(tenantID, hooks)
}

func (k *EnvelopeKMS) tenantAEAD(tenantID string) (cipher.AEAD, error) {
	for {
		k.mu.RLock()
		if k.aeadCache != nil {
			if gcm, ok := k.aeadCache[tenantID]; ok {
				k.mu.RUnlock()
				return gcm, nil
			}
		}
		wrapped, ok := k.wrappedDEKs[tenantID]
		if !ok || len(wrapped) == 0 {
			k.mu.RUnlock()
			return nil, ErrDEKMissing
		}
		wrappedCopy := append([]byte(nil), wrapped...)
		k.mu.RUnlock()

		dek, err := aeadOpen(k.kek, wrappedCopy, []byte("wrap:"+tenantID))
		if err != nil {
			zeroBytes(wrappedCopy)
			return nil, err
		}

		block, err := aes.NewCipher(dek)
		zeroBytes(dek)
		if err != nil {
			zeroBytes(wrappedCopy)
			return nil, err
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			zeroBytes(wrappedCopy)
			return nil, err
		}

		k.mu.Lock()
		cur, ok := k.wrappedDEKs[tenantID]
		if !ok || len(cur) == 0 {
			k.mu.Unlock()
			zeroBytes(wrappedCopy)
			return nil, ErrDEKMissing
		}
		if !bytes.Equal(cur, wrappedCopy) {
			// DEK was rotated concurrently while unwrapping; retry with the current DEK.
			k.mu.Unlock()
			zeroBytes(wrappedCopy)
			continue
		}
		zeroBytes(wrappedCopy)
		if k.aeadCache == nil {
			k.aeadCache = make(map[string]cipher.AEAD)
		}
		k.aeadCache[tenantID] = gcm
		k.mu.Unlock()
		return gcm, nil
	}
}

// EncryptObject encrypts plaintext with the tenant's DEK bound to tenantID|epoch|objectKey AAD.
func (k *EnvelopeKMS) EncryptObject(tenantID string, epoch int64, objectKey string, plaintext []byte) ([]byte, error) {
	gcm, err := k.tenantAEAD(tenantID)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, []byte(fmt.Sprintf("%s|%d|%s", tenantID, epoch, objectKey))), nil
}

// DecryptObject decrypts ciphertext with the tenant's DEK and verifies tenantID|epoch|objectKey AAD.
func (k *EnvelopeKMS) DecryptObject(tenantID string, epoch int64, objectKey string, ciphertext []byte) ([]byte, error) {
	gcm, err := k.tenantAEAD(tenantID)
	if err != nil {
		return nil, err
	}
	ns := gcm.NonceSize()
	if len(ciphertext) < ns {
		return nil, ErrAADMismatch
	}
	pt, err := gcm.Open(nil, ciphertext[:ns], ciphertext[ns:], []byte(fmt.Sprintf("%s|%d|%s", tenantID, epoch, objectKey)))
	if err != nil {
		return nil, ErrAADMismatch
	}
	return pt, nil
}

func zeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
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
	ns := gcm.NonceSize()
	if len(ciphertext) < ns {
		return nil, errors.New("ciphertext too short")
	}
	return gcm.Open(nil, ciphertext[:ns], ciphertext[ns:], aad)
}
