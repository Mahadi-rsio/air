package crypto

import (
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

// EnvelopeKMS manages a 256-bit master KEK and per-tenant 256-bit DEKs wrapped with AES-256-GCM.
type EnvelopeKMS struct {
	mu          sync.RWMutex
	kek         []byte
	wrappedDEKs map[string][]byte
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
	}, nil
}

// LoadOrCreateEnvelopeKMS loads a persistent 32-byte hex-encoded KEK from keyPath or creates one atomically (0600).
func LoadOrCreateEnvelopeKMS(keyPath string) (*EnvelopeKMS, error) {
	if raw, err := os.ReadFile(keyPath); err == nil {
		kek, decErr := hex.DecodeString(strings.TrimSpace(string(raw)))
		if decErr != nil || len(kek) != 32 {
			return nil, fmt.Errorf("invalid master key at %s", keyPath)
		}
		return &EnvelopeKMS{kek: kek, wrappedDEKs: make(map[string][]byte)}, nil
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
	return &EnvelopeKMS{kek: kek, wrappedDEKs: make(map[string][]byte)}, nil
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
	k.mu.Unlock()
	return nil
}

// UnwrapDEK unwraps the tenant's 256-bit DEK.
func (k *EnvelopeKMS) UnwrapDEK(tenantID string) ([]byte, error) {
	k.mu.RLock()
	wrapped, ok := k.wrappedDEKs[tenantID]
	k.mu.RUnlock()
	if !ok || len(wrapped) == 0 {
		return nil, ErrDEKMissing
	}
	return aeadOpen(k.kek, wrapped, []byte("wrap:"+tenantID))
}

// ShredDEK zeroes and removes the tenant's wrapped DEK from memory.
func (k *EnvelopeKMS) ShredDEK(tenantID string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if w, ok := k.wrappedDEKs[tenantID]; ok {
		for i := range w {
			w[i] = 0
		}
		delete(k.wrappedDEKs, tenantID)
	}
}

// EncryptObject encrypts plaintext with the tenant's DEK bound to tenantID|epoch|objectKey AAD.
func (k *EnvelopeKMS) EncryptObject(tenantID string, epoch int64, objectKey string, plaintext []byte) ([]byte, error) {
	dek, err := k.UnwrapDEK(tenantID)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(dek)
	return aeadSeal(dek, plaintext, []byte(fmt.Sprintf("%s|%d|%s", tenantID, epoch, objectKey)))
}

// DecryptObject decrypts ciphertext with the tenant's DEK and verifies tenantID|epoch|objectKey AAD.
func (k *EnvelopeKMS) DecryptObject(tenantID string, epoch int64, objectKey string, ciphertext []byte) ([]byte, error) {
	dek, err := k.UnwrapDEK(tenantID)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(dek)
	pt, err := aeadOpen(dek, ciphertext, []byte(fmt.Sprintf("%s|%d|%s", tenantID, epoch, objectKey)))
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
