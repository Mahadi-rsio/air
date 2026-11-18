package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	defaultBackupRetries = 4
	maxManifestBytes     = 16 * 1024 * 1024
	maxVerifyObjectBytes = 4096 * 1024 * 1024
)

// Store wraps an R2/S3 client with backup-specific integrity and immutability helpers.
type Store struct {
	Client *s3.Client
	Bucket string
	// Retries is the number of attempts for transient upload/download failures.
	Retries int
}

func NewStore(client *s3.Client, bucket string) *Store {
	return &Store{Client: client, Bucket: strings.TrimSpace(bucket), Retries: defaultBackupRetries}
}

func (s *Store) Ready() bool { return s != nil && s.Client != nil && s.Bucket != "" }

func (s *Store) ensureReady() error {
	if !s.Ready() {
		return ErrR2ClientRequired
	}
	return nil
}

func isS3NotFound(err error) bool {
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
	return strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "NotFound") || strings.Contains(msg, "404")
}

func isS3Precondition(err error) bool {
	if err == nil {
		return false
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "PreconditionFailed") || strings.Contains(msg, "412")
}

func (s *Store) withRetry(ctx context.Context, fn func() error) error {
	attempts := s.Retries
	if attempts <= 0 {
		attempts = defaultBackupRetries
	}
	var last error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = fn()
		if last == nil {
			return nil
		}
		if isS3NotFound(last) || isS3Precondition(last) {
			return last
		}
		if i == attempts-1 {
			break
		}
		backoff := time.Duration(100*(1<<uint(i))) * time.Millisecond
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return last
}

// ObjectInfo is returned by Head.
type ObjectInfo struct {
	Key    string
	Size   int64
	ETag   string
	SHA256 string
}

// Head returns object metadata and the provider-side SHA-256 checksum when available.
func (s *Store) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	var out *s3.HeadObjectOutput
	err := s.withRetry(ctx, func() error {
		var e error
		out, e = s.Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:       aws.String(s.Bucket),
			Key:          aws.String(key),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		return e
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrObjectMissing, key)
		}
		return nil, err
	}
	info := &ObjectInfo{Key: key}
	if out.ContentLength != nil {
		info.Size = *out.ContentLength
	}
	if out.ETag != nil {
		info.ETag = strings.Trim(*out.ETag, `"`)
	}
	if out.ChecksumSHA256 != nil {
		if raw, decErr := base64.StdEncoding.DecodeString(*out.ChecksumSHA256); decErr == nil {
			info.SHA256 = hex.EncodeToString(raw)
		}
	}
	if info.SHA256 == "" && out.Metadata != nil {
		info.SHA256 = strings.ToLower(strings.TrimSpace(out.Metadata["air-sha256"]))
	}
	return info, nil
}

// GetBytes downloads up to maxBytes and enforces the limit.
func (s *Store) GetBytes(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	var raw []byte
	err := s.withRetry(ctx, func() error {
		out, e := s.Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(key),
		})
		if e != nil {
			return e
		}
		defer out.Body.Close()
		raw, e = io.ReadAll(io.LimitReader(out.Body, maxBytes+1))
		if e != nil {
			return e
		}
		if int64(len(raw)) > maxBytes {
			return fmt.Errorf("%w: object %s exceeds %d bytes", ErrObjectSizeMismatch, key, maxBytes)
		}
		return nil
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrObjectMissing, key)
		}
		return nil, err
	}
	return raw, nil
}

// PutImmutable uploads an object only if it does not already exist.
func (s *Store) PutImmutable(ctx context.Context, key string, data []byte, contentType string) (*ObjectInfo, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	var out *s3.PutObjectOutput
	err := s.withRetry(ctx, func() error {
		input := &s3.PutObjectInput{
			Bucket:      aws.String(s.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			IfNoneMatch: aws.String("*"),
			Metadata:    map[string]string{"air-sha256": sha, "air-size": fmt.Sprintf("%d", len(data))},
		}
		if contentType != "" {
			input.ContentType = aws.String(contentType)
		}
		var e error
		out, e = s.Client.PutObject(ctx, input)
		return e
	})
	if err != nil {
		if isS3Precondition(err) {
			return nil, fmt.Errorf("%w: %s", ErrManifestExists, key)
		}
		return nil, err
	}
	info := &ObjectInfo{Key: key, Size: int64(len(data)), SHA256: sha}
	if out != nil && out.ETag != nil {
		info.ETag = strings.Trim(*out.ETag, `"`)
	}
	return info, nil
}

// PutBytesOverwrite uploads a control object that is not part of the immutable backup set.
func (s *Store) PutBytesOverwrite(ctx context.Context, key string, data []byte, contentType string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	return s.withRetry(ctx, func() error {
		_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(s.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
			Metadata:    map[string]string{"air-sha256": hex.EncodeToString(sum[:])},
		})
		return err
	})
}

// VerifyObject checks existence, size, and SHA-256. When streamHash is true the full object
// is downloaded and re-hashed; otherwise a provider checksum is accepted when present.
func (s *Store) VerifyObject(ctx context.Context, ref ObjectRef, streamHash bool) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if ref.Key == "" {
		return fmt.Errorf("%w: empty object key", ErrObjectMissing)
	}
	info, err := s.Head(ctx, ref.Key)
	if err != nil {
		return err
	}
	if ref.Size > 0 && info.Size != ref.Size {
		return fmt.Errorf("%w: %s size=%d want=%d", ErrObjectSizeMismatch, ref.Key, info.Size, ref.Size)
	}
	want, err := ValidateSHA256(ref.SHA256)
	if err != nil {
		return err
	}
	if !streamHash && info.SHA256 != "" {
		if !strings.EqualFold(info.SHA256, want) {
			return fmt.Errorf("%w: %s provider=%s want=%s", ErrObjectChecksum, ref.Key, info.SHA256, want)
		}
		return nil
	}
	// Stream and verify with a bounded reader.
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return fmt.Errorf("%w: %s", ErrObjectMissing, ref.Key)
		}
		return err
	}
	defer out.Body.Close()
	h := sha256.New()
	n, copyErr := io.Copy(h, io.LimitReader(out.Body, maxVerifyObjectBytes+1))
	if copyErr != nil {
		return copyErr
	}
	if n > maxVerifyObjectBytes {
		return fmt.Errorf("%w: %s exceeds verification limit", ErrObjectSizeMismatch, ref.Key)
	}
	if ref.Size > 0 && n != ref.Size {
		return fmt.Errorf("%w: %s streamed size=%d want=%d", ErrObjectSizeMismatch, ref.Key, n, ref.Size)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, want) {
		return fmt.Errorf("%w: %s streamed=%s want=%s", ErrObjectChecksum, ref.Key, got, want)
	}
	return nil
}

// PublishManifest writes an immutable, sealed backup manifest.
func (s *Store) PublishManifest(ctx context.Context, m *BackupManifest) error {
	if err := m.Validate(true); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	key := BackupManifestKey(m.TenantID, m.Generation, m.BackupID)
	if _, err := s.PutImmutable(ctx, key, raw, "application/json"); err != nil {
		if errors.Is(err, ErrManifestExists) {
			// Restart/replay safety: accept an identical existing manifest.
			existing, getErr := s.LoadManifest(ctx, m.TenantID, m.Generation, m.BackupID)
			if getErr == nil && existing.Checksum == m.Checksum {
				return nil
			}
		}
		return err
	}
	return nil
}

// LoadManifest loads and validates a backup manifest.
func (s *Store) LoadManifest(ctx context.Context, tenantID, generation, backupID string) (*BackupManifest, error) {
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(generation); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(backupID); err != nil {
		return nil, err
	}
	raw, err := s.GetBytes(ctx, BackupManifestKey(tenantID, generation, backupID), maxManifestBytes)
	if err != nil {
		return nil, err
	}
	var m BackupManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", ErrInvalidBackupManifest, err)
	}
	if err := m.Validate(true); err != nil {
		return nil, err
	}
	if m.TenantID != tenantID || m.Generation != generation || m.BackupID != backupID {
		return nil, fmt.Errorf("%w: manifest identity mismatch", ErrInvalidBackupManifest)
	}
	return &m, nil
}

// ListManifests lists and validates all published manifests for a tenant generation.
func (s *Store) ListManifests(ctx context.Context, tenantID, generation string) ([]*BackupManifest, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(generation); err != nil {
		return nil, err
	}
	prefix := BackupCatalogPrefix(tenantID, generation)
	var token *string
	var out []*BackupManifest
	for {
		resp, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range resp.Contents {
			if obj.Key == nil || !strings.HasSuffix(*obj.Key, "/manifest.json") {
				continue
			}
			rest := strings.TrimPrefix(*obj.Key, prefix)
			parts := strings.Split(rest, "/")
			if len(parts) != 2 || parts[1] != "manifest.json" {
				continue
			}
			m, err := s.LoadManifest(ctx, tenantID, generation, parts[0])
			if err != nil {
				continue
			}
			out = append(out, m)
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		token = resp.NextContinuationToken
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CompletedAt.Equal(out[j].CompletedAt) {
			return out[i].BackupID < out[j].BackupID
		}
		return out[i].CompletedAt.Before(out[j].CompletedAt)
	})
	return out, nil
}

// DeleteObject deletes one object (used by retention/GC and failed-create cleanup).
func (s *Store) DeleteObject(ctx context.Context, key string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	return s.withRetry(ctx, func() error {
		_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(key),
		})
		return err
	})
}
