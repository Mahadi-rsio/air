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
	ErrDEKShredded = errors.New("crypto: tenant DEK has been crypto-shredded or does not exist")
	ErrAADMismatch = errors.New("crypto: AES-256-GCM authentication failed (AAD or ciphertext tampered)")
)

// EnvelopeKMS manages a 256-bit master KEK and wrapped per-tenant 256-bit DEKs (ADR-002, RISK-24).
type EnvelopeKMS struct {
	mu          sync.RWMutex
	kek         []byte
	wrappedDEKs map[string][]byte // tenantID -> AES-GCM wrapped DEK
}

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

// EnsureTenantDEK generates and wraps a unique 256-bit DEK for tenantID if not already present.
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
	if err != nil {
		return "", err
	}
	// Zero plaintext DEK in memory after wrapping
	for i := range dek {
		dek[i] = 0
	}
	k.wrappedDEKs[tenantID] = wrapped
	return base64.StdEncoding.EncodeToString(wrapped), nil
}

// UnwrapDEK unwraps and verifies the tenant's 256-bit DEK (used in pre-publish backup check & restore).
func (k *EnvelopeKMS) UnwrapDEK(tenantID string) ([]byte, error) {
	k.mu.RLock()
	wrapped, ok := k.wrappedDEKs[tenantID]
	k.mu.RUnlock()
	if !ok || len(wrapped) == 0 {
		return nil, ErrDEKShredded
	}
	return aeadOpen(k.kek, wrapped, []byte("wrap:"+tenantID))
}

// ShredDEK destroys the tenant's wrapped DEK in-place (instant GDPR crypto-shredding, Flow 12).
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

// RotateMasterKEK generates a new 256-bit Master KEK, unwraps every active tenant DEK using the old KEK,
// re-wraps it with the new KEK in O(1) metadata time (0 R2 objects re-encrypted!), and zeroes the old KEK (DR-11).
func (k *EnvelopeKMS) RotateMasterKEK() (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	newKEK := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, newKEK); err != nil {
		return 0, err
	}
	reWrapped := 0
	for tenantID, oldWrapped := range k.wrappedDEKs {
		dek, err := aeadOpen(k.kek, oldWrapped, []byte("wrap:"+tenantID))
		if err != nil {
			return reWrapped, err
		}
		newWrapped, err := aeadSeal(newKEK, dek, []byte("wrap:"+tenantID))
		for i := range dek {
			dek[i] = 0
		}
		if err != nil {
			return reWrapped, err
		}
		k.wrappedDEKs[tenantID] = newWrapped
		reWrapped++
	}
	for i := range k.kek {
		k.kek[i] = 0
	}
	k.kek = newKEK
	return reWrapped, nil
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
