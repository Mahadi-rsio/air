package pageserver

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/tenant"
)

const (
	// DefaultMultipartThreshold is the layer size above which S3/R2 multipart upload is used.
	DefaultMultipartThreshold int64 = 16 * 1024 * 1024 // 16 MiB
	// DefaultMultipartPartSize is the part size used for multipart uploads (S3/R2 minimum is 5 MiB).
	DefaultMultipartPartSize int64 = 8 * 1024 * 1024 // 8 MiB

	streamHashBufSize = 64 * 1024

	metaKeySHA256 = "air-sha256"
	metaKeySize   = "air-size"
)

var (
	ErrInvalidSHA256         = errors.New("pageserver: invalid sha256 hex digest")
	ErrLayerNotFinalized     = errors.New("pageserver: layer file is not finalized or invalid")
	ErrLayerSizeMismatch     = errors.New("pageserver: remote layer size mismatch")
	ErrLayerChecksumMismatch = errors.New("pageserver: remote layer sha256 checksum mismatch")
	ErrRemoteObjectMissing   = errors.New("pageserver: remote layer object missing after upload")
	ErrManifestCASConflict   = errors.New("pageserver: manifest publish CAS conflict")
)

// UploadMetrics captures timing and request counts for a single CAU upload operation.
type UploadMetrics struct {
	HashDuration       time.Duration
	DedupCheckDuration time.Duration
	UploadDuration     time.Duration
	VerifyDuration     time.Duration
	TotalDuration      time.Duration
	BytesTransferred   int64
	PutRequests        int
	HeadRequests       int
	MultipartParts     int
	Deduplicated       bool
	R2Key              string
	SHA256             string
	FileSize           int64
}

// TimelineLayerManifest is the durable per-timeline layer index published to R2
// after each layer object is uploaded and verified.
type TimelineLayerManifest struct {
	TenantID   string      `json:"tenant_id"`
	Generation string      `json:"generation,omitempty"`
	Epoch      int64       `json:"epoch"`
	Version    int         `json:"version"`
	MaxSeq     uint64      `json:"max_seq"`
	UpdatedAt  string      `json:"updated_at"`
	Layers     []LayerMeta `json:"layers"`
}

// ValidateSHA256Hex verifies that h is a 64-character hexadecimal SHA-256 digest
// and returns its canonical lowercase representation.
func ValidateSHA256Hex(h string) (string, error) {
	if len(h) != 64 {
		return "", fmt.Errorf("%w: length=%d want=64", ErrInvalidSHA256, len(h))
	}
	lower := strings.ToLower(h)
	for i := 0; i < len(lower); i++ {
		c := lower[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return "", fmt.Errorf("%w: invalid hex char at %d", ErrInvalidSHA256, i)
		}
	}
	return lower, nil
}

// TenantR2Prefix returns the R2 key prefix for a tenant, scoped by generation when non-empty.
func TenantR2Prefix(tenantID, generation string) string {
	if generation == "" {
		return fmt.Sprintf("tenants/%s", tenantID)
	}
	return fmt.Sprintf("tenants/%s/%s", tenantID, generation)
}

// ContentAddressedLayerKey returns the canonical tenant-scoped R2 object key for a layer:
//
//	tenants/{tenant_id}/layers/sha256/{first_two_hex}/{full_sha256}
//
// Key generation is isolated here so future deduplication policies can be evaluated independently.
func ContentAddressedLayerKey(tenantID, sha256Hex string) (string, error) {
	return ContentAddressedLayerKeyGen(tenantID, "", sha256Hex)
}

// ContentAddressedLayerKeyGen returns a generation-isolated CAU R2 key when generation is non-empty.
func ContentAddressedLayerKeyGen(tenantID, generation, sha256Hex string) (string, error) {
	if err := tenant.ValidateID(tenantID); err != nil {
		return "", err
	}
	if generation != "" {
		if err := tenant.ValidateID(generation); err != nil {
			return "", err
		}
	}
	norm, err := ValidateSHA256Hex(sha256Hex)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/layers/sha256/%s/%s", TenantR2Prefix(tenantID, generation), norm[:2], norm), nil
}

// IsContentAddressedLayerKey reports whether key matches the tenant-scoped CAU layer key format.
func IsContentAddressedLayerKey(key string) bool {
	parts := strings.Split(key, "/")
	if len(parts) == 7 {
		if parts[0] != "tenants" || parts[3] != "layers" || parts[4] != "sha256" {
			return false
		}
		if err := tenant.ValidateID(parts[1]); err != nil {
			return false
		}
		if err := tenant.ValidateID(parts[2]); err != nil {
			return false
		}
		norm, err := ValidateSHA256Hex(parts[6])
		if err != nil {
			return false
		}
		return len(parts[5]) == 2 && parts[5] == norm[:2]
	}
	if len(parts) != 6 {
		return false
	}
	if parts[0] != "tenants" || parts[2] != "layers" || parts[3] != "sha256" {
		return false
	}
	if err := tenant.ValidateID(parts[1]); err != nil {
		return false
	}
	norm, err := ValidateSHA256Hex(parts[5])
	if err != nil {
		return false
	}
	return len(parts[4]) == 2 && parts[4] == norm[:2]
}

// LayerManifestR2Key returns the R2 key for a tenant timeline's layer manifest.
func LayerManifestR2Key(tenantID string, epoch int64) string {
	return LayerManifestR2KeyGen(tenantID, "", epoch)
}

// LayerManifestR2KeyGen returns the generation-isolated R2 key for a tenant timeline's layer manifest.
func LayerManifestR2KeyGen(tenantID, generation string, epoch int64) string {
	return fmt.Sprintf("%s/timeline_%d/layer_manifest.json", TenantR2Prefix(tenantID, generation), epoch)
}

// StreamSHA256 computes the lowercase hex SHA-256 digest and byte count of r in a single streaming pass.
func StreamSHA256(r io.Reader) (sha256Hex string, size int64, err error) {
	h := sha256.New()
	buf := make([]byte, streamHashBufSize)
	n, err := io.CopyBuffer(h, r, buf)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// StreamSHA256File computes the lowercase hex SHA-256 digest and byte count of the file at path.
func StreamSHA256File(path string) (sha256Hex string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	return StreamSHA256(f)
}

func sha256HexToBase64(sha256Hex string) (string, error) {
	raw, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}
