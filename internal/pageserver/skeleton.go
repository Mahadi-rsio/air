package pageserver

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
)

const (
	skeletonAADKey       = "pageserver-metadata-skeleton"
	maxSkeletonTotalSize = 256 * 1024 * 1024 // 256 MiB safety cap
)

// SkeletonMeta describes a lightweight non-relation PGDATA metadata snapshot.
type SkeletonMeta struct {
	TenantID        string `json:"tenant_id"`
	Epoch           int64  `json:"epoch"`
	FileCount       int    `json:"file_count"`
	RawBytes        int64  `json:"raw_bytes"`
	CompressedBytes int64  `json:"compressed_bytes"`
	SkippedRelFiles int    `json:"skipped_rel_files"`
	SkippedRelBytes int64  `json:"skipped_rel_bytes"`
	LocalPath       string `json:"local_path"`
	R2Key           string `json:"r2_key,omitempty"`
}

func (p *Pageserver) skeletonPaths(tenantID string) (localPath, r2Key string, encrypted bool) {
	encrypted = p.cfg.KMS != nil
	name := "skeleton.tar.zst"
	if encrypted {
		name += ".enc"
	}
	if p.cfg.LayerDir != "" {
		localPath = filepath.Join(p.cfg.LayerDir, tenantID, name)
	}
	r2Key = fmt.Sprintf("tenants/%s/skeleton/%s", tenantID, name)
	return localPath, r2Key, encrypted
}

// isRelationForkRelPath reports whether relSlash (slash-separated path relative to PGDATA)
// is a PostgreSQL relation fork file under global/ or base/<dbOid>/ owned by the Pageserver.
func isRelationForkRelPath(relSlash string) bool {
	parts := strings.Split(relSlash, "/")
	switch len(parts) {
	case 2:
		if parts[0] != "global" {
			return false
		}
		_, _, _, ok := parsePGDataRelFile(parts[1])
		return ok
	case 3:
		if parts[0] != "base" {
			return false
		}
		if _, err := strconv.ParseUint(parts[1], 10, 32); err != nil {
			return false
		}
		_, _, _, ok := parsePGDataRelFile(parts[2])
		return ok
	default:
		return false
	}
}

func isEphemeralPGFile(relSlash string) bool {
	if relSlash == "postmaster.pid" || relSlash == "postmaster.opts" {
		return true
	}
	if strings.HasPrefix(relSlash, "pg_stat_tmp/") || strings.HasPrefix(relSlash, "pg_dynshmem/") {
		return true
	}
	return false
}

// SaveMetadataSkeleton captures only non-relation PGDATA compute state (pg_control,
// pg_filenode.map, pg_xact, pg_multixact, PG_VERSION, configs, and WAL tail) into a
// zstd-compressed (and KMS-encrypted when configured) snapshot, skipping all base/ and
// global/ relation fork files stored in the Pageserver.
func (p *Pageserver) SaveMetadataSkeleton(ctx context.Context, tenantID string, epoch int64, pgDataDir string) (*SkeletonMeta, error) {
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	if epoch <= 0 {
		epoch = 1
	}

	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, fmt.Errorf("skeleton zstd writer: %w", err)
	}
	tw := tar.NewWriter(zw)

	meta := &SkeletonMeta{
		TenantID: tenantID,
		Epoch:    epoch,
	}

	walkErr := filepath.WalkDir(pgDataDir, func(path string, entry os.DirEntry, wErr error) error {
		if wErr != nil {
			return wErr
		}
		rel, err := filepath.Rel(pgDataDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		relSlash := filepath.ToSlash(rel)

		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}

		if entry.IsDir() {
			return tw.WriteHeader(&tar.Header{
				Name:     relSlash + "/",
				Typeflag: tar.TypeDir,
				Mode:     0o700,
			})
		}

		if isEphemeralPGFile(relSlash) {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		if isRelationForkRelPath(relSlash) {
			meta.SkippedRelFiles++
			meta.SkippedRelBytes += info.Size()
			return nil
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		if err := tw.WriteHeader(&tar.Header{
			Name:     relSlash,
			Typeflag: tar.TypeReg,
			Size:     info.Size(),
			Mode:     0o600,
		}); err != nil {
			return err
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		n, copyErr := io.CopyN(tw, f, info.Size())
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		meta.FileCount++
		meta.RawBytes += n
		return nil
	})
	if walkErr != nil {
		_ = tw.Close()
		_ = zw.Close()
		return nil, fmt.Errorf("walk pgdata for skeleton: %w", walkErr)
	}
	if err := tw.Close(); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}

	payload := buf.Bytes()
	localPath, r2Key, encrypted := p.skeletonPaths(tenantID)
	if encrypted {
		enc, err := p.cfg.KMS.EncryptObject(tenantID, epoch, skeletonAADKey, payload)
		if err != nil {
			return nil, fmt.Errorf("encrypt skeleton: %w", err)
		}
		payload = enc
	}
	meta.CompressedBytes = int64(len(payload))

	if localPath != "" {
		if err := os.MkdirAll(filepath.Dir(localPath), 0o700); err != nil {
			return nil, err
		}
		tmp := localPath + ".tmp"
		if err := os.WriteFile(tmp, payload, 0o600); err != nil {
			return nil, err
		}
		if err := os.Rename(tmp, localPath); err != nil {
			_ = os.Remove(tmp)
			return nil, err
		}
		meta.LocalPath = localPath
	}

	if p.cfg.S3Client != nil && p.cfg.R2Bucket != "" {
		if _, err := p.cfg.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(p.cfg.R2Bucket),
			Key:    aws.String(r2Key),
			Body:   bytes.NewReader(payload),
		}); err != nil {
			return nil, fmt.Errorf("upload skeleton to R2: %w", err)
		}
		meta.R2Key = r2Key
	}

	return meta, nil
}

// HasMetadataSkeleton reports whether a metadata skeleton exists locally or in R2.
func (p *Pageserver) HasMetadataSkeleton(ctx context.Context, tenantID string) bool {
	if err := tenant.ValidateID(tenantID); err != nil {
		return false
	}
	localPath, r2Key, _ := p.skeletonPaths(tenantID)
	if localPath != "" {
		if _, err := os.Stat(localPath); err == nil {
			return true
		}
	}
	if p.cfg.S3Client != nil && p.cfg.R2Bucket != "" {
		_, err := p.cfg.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(p.cfg.R2Bucket),
			Key:    aws.String(r2Key),
		})
		return err == nil
	}
	return false
}

// RestoreMetadataSkeleton reconstructs the minimal PGDATA directory skeleton
// from the saved snapshot without downloading or restoring relation fork files.
func (p *Pageserver) RestoreMetadataSkeleton(ctx context.Context, tenantID string, epoch int64, pgDataDir string) (*SkeletonMeta, error) {
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	if epoch <= 0 {
		epoch = 1
	}

	localPath, r2Key, encrypted := p.skeletonPaths(tenantID)
	var payload []byte
	if localPath != "" {
		if b, err := os.ReadFile(localPath); err == nil {
			payload = b
		}
	}
	if len(payload) == 0 && p.cfg.S3Client != nil && p.cfg.R2Bucket != "" {
		out, err := p.cfg.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(p.cfg.R2Bucket),
			Key:    aws.String(r2Key),
		})
		if err != nil {
			return nil, fmt.Errorf("download skeleton from R2: %w", err)
		}
		b, err := io.ReadAll(io.LimitReader(out.Body, maxSkeletonTotalSize))
		_ = out.Body.Close()
		if err != nil {
			return nil, err
		}
		payload = b
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("%w: metadata skeleton for %s", ErrNotFound, tenantID)
	}

	compressedBytes := int64(len(payload))
	if encrypted {
		dec, err := p.cfg.KMS.DecryptObject(tenantID, epoch, skeletonAADKey, payload)
		if err != nil {
			return nil, fmt.Errorf("decrypt skeleton: %w", err)
		}
		payload = dec
	}

	zr, err := zstd.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("skeleton zstd reader: %w", err)
	}
	defer zr.Close()

	if err := os.MkdirAll(pgDataDir, 0o700); err != nil {
		return nil, err
	}

	tr := tar.NewReader(zr)
	meta := &SkeletonMeta{
		TenantID:        tenantID,
		Epoch:           epoch,
		CompressedBytes: compressedBytes,
		LocalPath:       localPath,
	}

	var totalExtracted int64
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read skeleton tar: %w", err)
		}

		clean := filepath.Clean(hdr.Name)
		if clean == "." || clean == "" || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) || strings.HasPrefix(hdr.Name, "/") {
			return nil, fmt.Errorf("unsafe skeleton tar path %q", hdr.Name)
		}
		target := filepath.Join(pgDataDir, clean)
		rel, err := filepath.Rel(pgDataDir, target)
		if err != nil || strings.HasPrefix(rel, "..") {
			return nil, fmt.Errorf("skeleton path escape %q", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return nil, err
			}
		case tar.TypeReg:
			if hdr.Size < 0 || totalExtracted+hdr.Size > maxSkeletonTotalSize {
				return nil, fmt.Errorf("skeleton extracted size exceeds %d bytes", maxSkeletonTotalSize)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return nil, err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if err != nil {
				return nil, err
			}
			n, copyErr := io.CopyN(f, tr, hdr.Size)
			closeErr := f.Close()
			if copyErr != nil {
				return nil, copyErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			totalExtracted += n
			meta.FileCount++
			meta.RawBytes += n
		default:
			return nil, fmt.Errorf("unsupported skeleton tar entry type %v for %q", hdr.Typeflag, hdr.Name)
		}
	}

	return meta, nil
}
