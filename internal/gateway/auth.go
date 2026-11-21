package gateway

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

func ConstantTimeEquals(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// ValidateTenantID checks that id is 1..63 chars of [A-Za-z0-9_-].
func ValidateTenantID(id string) bool {
	if len(id) == 0 || len(id) > 63 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		isAlnum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !isAlnum && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// ParseSNITenant extracts the leading subdomain label from sniHost, ignoring IPv4/IPv6 literals and single-label hosts.
func ParseSNITenant(sniHost string) string {
	trimmed := strings.TrimSpace(sniHost)
	if trimmed == "" {
		return ""
	}
	if net.ParseIP(trimmed) != nil {
		return ""
	}
	dot := strings.IndexByte(trimmed, '.')
	if dot <= 0 {
		return ""
	}
	return trimmed[:dot]
}

// ExtractTenant resolves the tenant from explicit database startup parameter (if non-"postgres") or SNI subdomain.
func ExtractTenant(sniHost string, params map[string]string) string {
	if db, ok := params["database"]; ok {
		db = strings.TrimSpace(db)
		if db != "" && db != "postgres" {
			return db
		}
	}
	return ParseSNITenant(sniHost)
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func sha256Bytes(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

// SCRAMSHA256ClientAuth performs client-side SCRAM-SHA-256 SASL exchange against a PostgreSQL backend stream.
func SCRAMSHA256ClientAuth(rw io.ReadWriter, user, password string) error {
	var randBytes [18]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		return fmt.Errorf("rand.Read failed: %w", err)
	}
	clientNonce := base64.StdEncoding.EncodeToString(randBytes[:])
	clientFirstBare := "n=" + user + ",r=" + clientNonce
	clientFirst := "n,," + clientFirstBare

	var initBody []byte
	initBody = appendCStr(initBody, "SCRAM-SHA-256")
	initBody = appendBE32(initBody, uint32(len(clientFirst)))
	initBody = append(initBody, clientFirst...)
	if _, err := rw.Write(BuildPGMessage('p', initBody)); err != nil {
		return fmt.Errorf("failed to write SASLInitialResponse: %w", err)
	}

	msgType, body, err := ReadPGPacket(rw)
	if err != nil || msgType != 'R' || len(body) < 4 {
		return fmt.Errorf("failed to read SASLContinue")
	}
	if binary.BigEndian.Uint32(body[:4]) != 11 {
		return fmt.Errorf("expected AuthenticationSASLContinue (11)")
	}
	serverFirst := string(body[4:])

	var serverNonce, saltB64 string
	var iterations int
	for _, tok := range strings.Split(serverFirst, ",") {
		switch {
		case strings.HasPrefix(tok, "r="):
			serverNonce = tok[2:]
		case strings.HasPrefix(tok, "s="):
			saltB64 = tok[2:]
		case strings.HasPrefix(tok, "i="):
			iterations, _ = strconv.Atoi(tok[2:])
		}
	}
	if !strings.HasPrefix(serverNonce, clientNonce) || saltB64 == "" || iterations <= 0 {
		return fmt.Errorf("invalid SCRAM server-first-message")
	}

	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return fmt.Errorf("invalid SCRAM salt: %w", err)
	}
	saltedPW, err := pbkdf2.Key(sha256.New, password, salt, iterations, 32)
	if err != nil {
		return fmt.Errorf("PBKDF2 failed: %w", err)
	}

	clientKey := hmacSHA256(saltedPW, "Client Key")
	storedKey := sha256Bytes(clientKey)
	clientFinalWithoutProof := "c=biws,r=" + serverNonce
	authMsg := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	clientSig := hmacSHA256(storedKey, authMsg)

	proof := make([]byte, 32)
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	clientFinal := clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)

	if _, err := rw.Write(BuildPGMessage('p', []byte(clientFinal))); err != nil {
		return fmt.Errorf("failed to write SASLResponse: %w", err)
	}

	msgType, body, err = ReadPGPacket(rw)
	if err != nil || msgType != 'R' || len(body) < 4 {
		return fmt.Errorf("failed to read SASLFinal")
	}
	if binary.BigEndian.Uint32(body[:4]) != 12 {
		return fmt.Errorf("SCRAM authentication rejected by backend")
	}
	return nil
}

// CancelTarget identifies the backend PostgreSQL process for CancelRequest forwarding.
type CancelTarget struct {
	Host          string
	SockDir       string
	Port          int
	BackendPID    uint32
	BackendSecret uint32
}

// CancelRegistry maps gateway-issued (pid, secret) pairs from BackendKeyData
// to the currently active backend PostgreSQL (pid, secret, endpoint).
type CancelRegistry struct {
	mu      sync.Mutex
	targets map[uint64]CancelTarget
}

func NewCancelRegistry() *CancelRegistry {
	return &CancelRegistry{
		targets: make(map[uint64]CancelTarget),
	}
}

func cancelKey(pid, secret uint32) uint64 {
	return (uint64(pid) << 32) | uint64(secret)
}

func (r *CancelRegistry) Register(gwPID, gwSecret uint32, target CancelTarget) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets[cancelKey(gwPID, gwSecret)] = target
}

func (r *CancelRegistry) Unregister(gwPID, gwSecret uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.targets, cancelKey(gwPID, gwSecret))
}

func (r *CancelRegistry) Lookup(gwPID, gwSecret uint32) (CancelTarget, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.targets[cancelKey(gwPID, gwSecret)]
	return t, ok
}
