package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	ErrStaleFencingEpoch = errors.New("fencing: local epoch is older than R2 manifest fencing_epoch (CAS rejected)")
	ErrCASPrecondition   = errors.New("fencing: R2 conditional If-Match/If-None-Match ETag precondition failed (HTTP 412)")
	ErrR2ClientRequired  = errors.New("fencing: S3/R2 client and bucket are required")
)

// BackupArtifact describes a verified, encrypted backup object in R2.
type BackupArtifact struct {
	BackupID     string                  `json:"backup_id"`
	ParentID     string                  `json:"parent_id"`
	Type         string                  `json:"type"` // "FULL" or "INCR"
	R2Key        string                  `json:"r2_key"`
	Epoch        int64                   `json:"epoch"`
	StartLSN     string                  `json:"start_lsn"`
	EndLSN       string                  `json:"end_lsn"`
	CipherBytes  int                     `json:"cipher_bytes"`
	PlainSHA256  string                  `json:"plain_sha256,omitempty"`
	CipherSHA256 string                  `json:"cipher_sha256,omitempty"`
	CreatedAt    time.Time               `json:"created_at"`
	Verified     bool                    `json:"verified"`
	DEKBundle    crypto.WrappedDEKBundle `json:"dek_bundle"`
	LocalBackupD string                  `json:"-"`
}

// TenantFenceManifest is stored in R2 at tenants/<id>/manifest.json
// and guarded by HTTP ETag conditional writes (If-None-Match / If-Match).
type TenantFenceManifest struct {
	TenantID     string                  `json:"tenant_id"`
	Generation   string                  `json:"generation,omitempty"`
	FencingEpoch int64                   `json:"fencing_epoch"`
	HolderCell   string                  `json:"holder_cell"`
	UpdatedAt    string                  `json:"updated_at"`
	DEKBundle    crypto.WrappedDEKBundle `json:"dek_bundle"`
	Backups      []BackupArtifact        `json:"backups"`
	WALSegments  []string                `json:"wal_segments"`
}

func ManifestR2Key(tenantID string) string {
	return ManifestR2KeyGen(tenantID, "")
}

func ManifestR2KeyGen(tenantID, generation string) string {
	if generation == "" {
		return fmt.Sprintf("tenants/%s/manifest.json", tenantID)
	}
	return fmt.Sprintf("tenants/%s/%s/manifest.json", tenantID, generation)
}

// IsNotFoundErr reports whether err represents a missing S3/R2 object (HTTP 404 / NoSuchKey).
func IsNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "404") || strings.Contains(msg, "NotFound")
}

// IsPreconditionErr reports whether err represents an HTTP 412 Precondition Failed response from S3/R2.
func IsPreconditionErr(err error) bool {
	return isPreconditionErr(err)
}

func isPreconditionErr(err error) bool {
	if err == nil {
		return false
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "412") || strings.Contains(msg, "PreconditionFailed")
}

// ReadManifestWithETag fetches the current TenantFenceManifest and its HTTP ETag from R2.
func ReadManifestWithETag(ctx context.Context, s3c *s3.Client, bucket, tenantID string) (*TenantFenceManifest, string, error) {
	return ReadManifestWithETagGen(ctx, s3c, bucket, tenantID, "")
}

// ReadManifestWithETagGen fetches the generation-scoped TenantFenceManifest and its HTTP ETag from R2.
func ReadManifestWithETagGen(ctx context.Context, s3c *s3.Client, bucket, tenantID, generation string) (*TenantFenceManifest, string, error) {
	if s3c == nil || strings.TrimSpace(bucket) == "" {
		return nil, "", ErrR2ClientRequired
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, "", err
	}
	key := ManifestR2KeyGen(tenantID, generation)
	out, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}
	defer out.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(out.Body, 16*1024*1024))
	if err != nil {
		return nil, "", err
	}
	var m TenantFenceManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, "", err
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return &m, etag, nil
}

// PutManifestCAS atomically creates (expectedETag == "") or updates (If-Match: expectedETag)
// the tenant's manifest in R2, rejecting any writer whose epoch < remote epoch or whose ETag is stale.
func PutManifestCAS(ctx context.Context, s3c *s3.Client, bucket string, m *TenantFenceManifest, expectedETag string) (string, error) {
	if s3c == nil || strings.TrimSpace(bucket) == "" {
		return "", ErrR2ClientRequired
	}
	if m == nil {
		return "", errors.New("fencing: manifest is nil")
	}
	if err := tenant.ValidateID(m.TenantID); err != nil {
		return "", err
	}

	if expectedETag != "" {
		remote, _, err := ReadManifestWithETagGen(ctx, s3c, bucket, m.TenantID, m.Generation)
		if err == nil && remote != nil && m.FencingEpoch < remote.FencingEpoch {
			return "", fmt.Errorf("%w: writer epoch=%d < remote epoch=%d", ErrStaleFencingEpoch, m.FencingEpoch, remote.FencingEpoch)
		}
	}

	m.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(ManifestR2KeyGen(m.TenantID, m.Generation)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	}
	if expectedETag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(expectedETag)
	}

	putOut, err := s3c.PutObject(ctx, input)
	if err != nil {
		if isPreconditionErr(err) {
			return "", fmt.Errorf("%w: %v", ErrCASPrecondition, err)
		}
		return "", err
	}
	newETag := ""
	if putOut.ETag != nil {
		newETag = *putOut.ETag
	}
	return newETag, nil
}

// StartLeaseWatchdog starts the Cell Agent's CLOCK_MONOTONIC lease watchdog.
func StartLeaseWatchdog(ctx context.Context, agent *cell.Agent, selfFenceDeadline time.Duration) {
	if agent != nil {
		agent.StartLeaseWatchdog(ctx, selfFenceDeadline)
	}
}
