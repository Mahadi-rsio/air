package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/fencing"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	maxRestoreObjMiB = 2048
	maxWALObjBytes   = 128 * 1024 * 1024
)

var (
	ErrR2ClientRequired = errors.New("backup: R2 client and bucket are required (fail-closed)")
	ErrInvalidWALName   = errors.New("backup: invalid WAL segment or timeline history name")
	walSegmentRe        = regexp.MustCompile(`^[0-9A-F]{24}$`)
	walHistoryRe        = regexp.MustCompile(`^[0-9A-F]{8}\.history$`)
	walBackupLabelRe    = regexp.MustCompile(`^[0-9A-F]{24}\.[0-9A-F]{8}\.backup$`)
)

type WALSegmentArtifact struct {
	SegmentName string `json:"segment_name"`
	R2Key       string `json:"r2_key"`
	Epoch       int64  `json:"epoch"`
	CipherBytes int    `json:"cipher_bytes"`
	PlainSHA256 string `json:"plain_sha256,omitempty"`
	CipherETag  string `json:"cipher_etag,omitempty"`
	Status      string `json:"status,omitempty"` // PENDING, VERIFIED_IN_R2, LOCAL_DELETED
}

type WALUploadManifest struct {
	TenantID  string                         `json:"tenant_id"`
	UpdatedAt string                         `json:"updated_at"`
	Segments  map[string]*WALSegmentArtifact `json:"segments"`
}

func isValidWALFilename(name string) bool {
	return walSegmentRe.MatchString(name) || walHistoryRe.MatchString(name) || walBackupLabelRe.MatchString(name)
}

func loadWALManifest(path, tenantID string) (*WALUploadManifest, error) {
	m := &WALUploadManifest{
		TenantID: tenantID,
		Segments: make(map[string]*WALSegmentArtifact),
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("corrupt wal_upload_manifest.json: %w", err)
	}
	if m.Segments == nil {
		m.Segments = make(map[string]*WALSegmentArtifact)
	}
	return m, nil
}

func saveWALManifestAtomic(path string, m *WALUploadManifest) error {
	m.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// RunEncryptedBackupStep executes pg_basebackup (FULL or INCR), verifies pg_available_wal_summaries()
// prior to INCR, runs pg_verifybackup, compresses with tar+zstd, encrypts with AES-256-GCM-AAD,
// uploads to R2 with If-None-Match: "*", and atomically commits the BackupArtifact into
// TenantFenceManifest via PutManifestCAS (aborting and cleaning up on CAS/epoch conflict).
func r2TenantPrefix(tenantID, generation string) string {
	if generation == "" {
		return fmt.Sprintf("tenants/%s", tenantID)
	}
	return fmt.Sprintf("tenants/%s/%s", tenantID, generation)
}

func RunEncryptedBackupStep(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID, bkpID, parentID, bkpType, parentManifestPath string,
) (*fencing.BackupArtifact, string, error) {
	return RunEncryptedBackupStepGen(ctx, agent, s3c, bucket, tenantID, "", bkpID, parentID, bkpType, parentManifestPath)
}

func RunEncryptedBackupStepGen(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID, generation, bkpID, parentID, bkpType, parentManifestPath string,
) (*fencing.BackupArtifact, string, error) {
	if s3c == nil || strings.TrimSpace(bucket) == "" {
		return nil, "", ErrR2ClientRequired
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, "", err
	}
	if err := tenant.ValidateID(bkpID); err != nil {
		return nil, "", fmt.Errorf("backup: invalid bkpID: %w", err)
	}
	rt, ok := agent.GetTenant(tenantID)
	if !ok {
		return nil, "", fmt.Errorf("tenant %s not active", tenantID)
	}
	dekBundle, err := agent.KMS().ExportWrappedDEKBundle(tenantID)
	if err != nil {
		return nil, "", err
	}

	// Read current manifest + ETag early to fail fast on stale fencing epoch.
	manifest, etag, err := fencing.ReadManifestWithETagGen(ctx, s3c, bucket, tenantID, generation)
	if err != nil {
		if !fencing.IsNotFoundErr(err) {
			return nil, "", fmt.Errorf("read fence manifest: %w", err)
		}
		manifest = &fencing.TenantFenceManifest{
			TenantID:     tenantID,
			Generation:   generation,
			FencingEpoch: rt.FencingEpoch,
			HolderCell:   "cell-local",
			DEKBundle:    dekBundle,
		}
		etag = ""
	} else if rt.FencingEpoch < manifest.FencingEpoch {
		return nil, "", fmt.Errorf("%w: writer epoch=%d < remote epoch=%d", fencing.ErrStaleFencingEpoch, rt.FencingEpoch, manifest.FencingEpoch)
	}

	sockDir := rt.SockDir
	if sockDir == "" {
		activeBase, _ := tenant.SafeSubpath(agent.VolDir(), "tenants/active", tenantID)
		sockDir = filepath.Join(activeBase, "sock")
	}

	if bkpType == "INCR" {
		sumCount, err := agent.VerifyWALSummaryContinuity(ctx, tenantID)
		if err != nil || sumCount == 0 || parentManifestPath == "" {
			bkpType = "FULL"
			parentID = ""
			parentManifestPath = ""
		}
	} else {
		bkpType = "FULL"
		_, _ = agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "psql"), "-h", sockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-d", "postgres", "-c", "CHECKPOINT;")
	}

	bkpBase, err := tenant.SafeSubpath(agent.VolDir(), "backups", tenantID)
	if err != nil {
		return nil, "", err
	}
	bkpDir := filepath.Join(bkpBase, bkpID)
	_ = os.RemoveAll(bkpDir)
	if err := os.MkdirAll(filepath.Dir(bkpDir), 0o700); err != nil {
		return nil, "", err
	}

	if bkpType == "INCR" {
		time.Sleep(200 * time.Millisecond)
	}

	args := []string{
		filepath.Join(agent.PGBin(), "pg_basebackup"),
		"-h", sockDir,
		"-p", strconv.Itoa(rt.Port),
		"-U", "postgres",
		"-D", bkpDir,
		"-c", "fast",
	}
	if bkpType == "INCR" && parentManifestPath != "" {
		args = append(args, "--incremental="+parentManifestPath)
	}
	if out, err := agent.RunPostgresCmd(args[0], args[1:]...); err != nil {
		_ = os.RemoveAll(bkpDir)
		return nil, "", fmt.Errorf("pg_basebackup (%s) failed: %w (%s)", bkpType, err, out)
	}

	if out, err := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "pg_verifybackup"), bkpDir); err != nil {
		_ = os.RemoveAll(bkpDir)
		return nil, "", fmt.Errorf("pg_verifybackup failed: %w (%s)", err, out)
	}

	plainBytes, err := CompressDirToTarZst(bkpDir)
	if err != nil {
		_ = os.RemoveAll(bkpDir)
		return nil, "", fmt.Errorf("compress backup tar.zst: %w", err)
	}

	r2Key := fmt.Sprintf("%s/epoch_%06d/%s.tar.zst.enc", r2TenantPrefix(tenantID, generation), rt.FencingEpoch, bkpID)
	cipherBytes, err := agent.KMS().EncryptObject(tenantID, rt.FencingEpoch, r2Key, plainBytes)
	if err != nil {
		_ = os.RemoveAll(bkpDir)
		return nil, "", err
	}

	if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(r2Key),
		Body:        bytes.NewReader(cipherBytes),
		IfNoneMatch: aws.String("*"),
	}); err != nil {
		_ = os.RemoveAll(bkpDir)
		return nil, "", fmt.Errorf("R2 PutObject (%s) failed: %w", r2Key, err)
	}

	art := fencing.BackupArtifact{
		BackupID:     bkpID,
		ParentID:     parentID,
		Type:         bkpType,
		R2Key:        r2Key,
		Epoch:        rt.FencingEpoch,
		CipherBytes:  len(cipherBytes),
		CreatedAt:    time.Now().UTC(),
		Verified:     true,
		DEKBundle:    dekBundle,
		LocalBackupD: bkpDir,
	}

	// Atomically commit the verified backup artifact into TenantFenceManifest via CAS.
	manifest.DEKBundle = dekBundle
	manifest.FencingEpoch = rt.FencingEpoch
	manifest.Backups = append(manifest.Backups, art)
	if _, casErr := fencing.PutManifestCAS(ctx, s3c, bucket, manifest, etag); casErr != nil {
		// Clean up orphaned backup object and local staging directory on CAS failure.
		_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(r2Key),
		})
		_ = os.RemoveAll(bkpDir)
		return nil, "", casErr
	}

	return &art, filepath.Join(bkpDir, "backup_manifest"), nil
}

// DrainAndEncryptWALSpoolToR2 forces a WAL switch on the live cluster, compresses all spooled
// WAL files with zstd, encrypts them with AES-256-GCM-AAD, verifies remote upload via HeadObject,
// commits a durable fsynced manifest, and ONLY then deletes local WAL.
func DrainAndEncryptWALSpoolToR2(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID string,
) ([]WALSegmentArtifact, error) {
	return DrainAndEncryptWALSpoolToR2WithFault(ctx, agent, s3c, bucket, tenantID, nil)
}

// DrainAndEncryptWALSpoolToR2WithFault allows injecting deterministic faults at upload/manifest/delete boundaries.
func DrainAndEncryptWALSpoolToR2WithFault(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID string,
	faultHook func(stage, segName string) error,
) ([]WALSegmentArtifact, error) {
	if s3c == nil || strings.TrimSpace(bucket) == "" {
		return nil, ErrR2ClientRequired
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	rt, ok := agent.GetTenant(tenantID)
	if !ok {
		return nil, fmt.Errorf("tenant %s not active", tenantID)
	}
	sockDir := rt.SockDir
	if sockDir == "" {
		activeBase, _ := tenant.SafeSubpath(agent.VolDir(), "tenants/active", tenantID)
		sockDir = filepath.Join(activeBase, "sock")
	}
	walSpoolDir := rt.WALSpoolDir
	if walSpoolDir == "" {
		var err error
		walSpoolDir, err = tenant.SafeSubpath(agent.VolDir(), "spool/wal", tenantID)
		if err != nil {
			return nil, err
		}
	}

	_, _ = agent.RunPostgresCmd(
		filepath.Join(agent.PGBin(), "psql"),
		"-h", sockDir,
		"-p", strconv.Itoa(rt.Port),
		"-U", "postgres",
		"-d", "postgres",
		"-c", "SELECT pg_switch_wal(); CHECKPOINT;",
	)
	time.Sleep(300 * time.Millisecond)

	return drainWALSpoolDirWithFault(ctx, agent.KMS(), s3c, bucket, tenantID, rt.FencingEpoch, walSpoolDir, faultHook)
}

// DrainStoppedWALSpoolToR2 drains any remaining spooled WAL segments for an already-stopped
// tenant to R2 without requiring an active PostgreSQL backend.
func DrainStoppedWALSpoolToR2(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID string,
	epoch int64,
) ([]WALSegmentArtifact, error) {
	return DrainStoppedWALSpoolToR2Gen(ctx, agent, s3c, bucket, tenantID, "", epoch)
}

func DrainStoppedWALSpoolToR2Gen(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID, generation string,
	epoch int64,
) ([]WALSegmentArtifact, error) {
	if s3c == nil || strings.TrimSpace(bucket) == "" {
		return nil, ErrR2ClientRequired
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	walSpoolDir, err := tenant.SafeSubpath(agent.VolDir(), "spool/wal", tenantID)
	if err != nil {
		return nil, err
	}
	return drainWALSpoolDirWithFaultGen(ctx, agent.KMS(), s3c, bucket, tenantID, generation, epoch, walSpoolDir, nil)
}

func drainWALSpoolDirWithFault(
	ctx context.Context,
	kms *crypto.EnvelopeKMS,
	s3c *s3.Client,
	bucket, tenantID string,
	epoch int64,
	walSpoolDir string,
	faultHook func(stage, segName string) error,
) ([]WALSegmentArtifact, error) {
	return drainWALSpoolDirWithFaultGen(ctx, kms, s3c, bucket, tenantID, "", epoch, walSpoolDir, faultHook)
}

func drainWALSpoolDirWithFaultGen(
	ctx context.Context,
	kms *crypto.EnvelopeKMS,
	s3c *s3.Client,
	bucket, tenantID, generation string,
	epoch int64,
	walSpoolDir string,
	faultHook func(stage, segName string) error,
) ([]WALSegmentArtifact, error) {
	if s3c == nil || strings.TrimSpace(bucket) == "" {
		return nil, ErrR2ClientRequired
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(walSpoolDir)
	if err != nil {
		return nil, err
	}

	manifestPath := filepath.Join(walSpoolDir, "wal_upload_manifest.json")
	manifest, err := loadWALManifest(manifestPath, tenantID)
	if err != nil {
		return nil, err
	}

	var segNames []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name == "wal_upload_manifest.json" || name == "wal_upload_manifest.json.tmp" || strings.HasSuffix(name, ".zst") {
			continue
		}
		if !isValidWALFilename(name) {
			return nil, fmt.Errorf("%w: %q", ErrInvalidWALName, name)
		}
		segNames = append(segNames, name)
	}
	sort.Strings(segNames)

	var uploaded []WALSegmentArtifact
	for _, segName := range segNames {
		segPath := filepath.Join(walSpoolDir, segName)

		if prev, exists := manifest.Segments[segName]; exists && prev.Status == "VERIFIED_IN_R2" {
			headOut, hErr := s3c.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(prev.R2Key),
			})
			if hErr == nil && headOut.ContentLength != nil && *headOut.ContentLength == int64(prev.CipherBytes) {
				_ = os.Remove(segPath)
				prev.Status = "LOCAL_DELETED"
				if err := saveWALManifestAtomic(manifestPath, manifest); err != nil {
					return uploaded, err
				}
				uploaded = append(uploaded, *prev)
				continue
			}
		}

		rawWAL, err := os.ReadFile(segPath)
		if err != nil {
			return uploaded, err
		}
		plainSum := sha256.Sum256(rawWAL)
		plainSHA := hex.EncodeToString(plainSum[:])

		compressedBytes, err := CompressBytesZstd(rawWAL)
		if err != nil {
			return uploaded, fmt.Errorf("zstd compress wal %s: %w", segName, err)
		}

		r2Key := fmt.Sprintf("%s/epoch_%06d/wal/%s.zst.enc", r2TenantPrefix(tenantID, generation), epoch, segName)
		cipherBytes, err := kms.EncryptObject(tenantID, epoch, r2Key, compressedBytes)
		if err != nil {
			return uploaded, err
		}

		putOut, err := s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(r2Key),
			Body:   bytes.NewReader(cipherBytes),
		})
		if err != nil {
			return uploaded, fmt.Errorf("R2 PutObject WAL %s: %w", r2Key, err)
		}

		headOut, err := s3c.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(r2Key),
		})
		if err != nil || headOut.ContentLength == nil || *headOut.ContentLength != int64(len(cipherBytes)) {
			return uploaded, fmt.Errorf("R2 HeadObject verification failed for %s: err=%v", r2Key, err)
		}
		if putOut.ETag != nil && headOut.ETag != nil && *putOut.ETag != *headOut.ETag {
			return uploaded, fmt.Errorf("R2 HeadObject ETag mismatch for %s: put=%s head=%s", r2Key, *putOut.ETag, *headOut.ETag)
		}

		if faultHook != nil {
			if err := faultHook("after_upload_before_manifest", segName); err != nil {
				return uploaded, err
			}
		}

		etag := ""
		if putOut.ETag != nil {
			etag = *putOut.ETag
		}
		art := &WALSegmentArtifact{
			SegmentName: segName,
			R2Key:       r2Key,
			Epoch:       epoch,
			CipherBytes: len(cipherBytes),
			PlainSHA256: plainSHA,
			CipherETag:  etag,
			Status:      "VERIFIED_IN_R2",
		}
		manifest.Segments[segName] = art
		if err := saveWALManifestAtomic(manifestPath, manifest); err != nil {
			return uploaded, fmt.Errorf("commit durable WAL manifest: %w", err)
		}

		if faultHook != nil {
			if err := faultHook("after_manifest_before_delete", segName); err != nil {
				return uploaded, err
			}
		}

		if err := os.Remove(segPath); err != nil {
			return uploaded, err
		}
		art.Status = "LOCAL_DELETED"
		if err := saveWALManifestAtomic(manifestPath, manifest); err != nil {
			return uploaded, err
		}
		uploaded = append(uploaded, *art)
	}
	return uploaded, nil
}

// CombineAndReplayPITR downloads the multi-step backup chain (e.g. B0 -> I1 -> I2) and all encrypted
// WAL segments from R2, extracts them with streaming archive/tar validation & uncompressed byte caps,
// runs pg_combinebackup + pg_verifybackup, replays archived WAL up to targetLSN, promotes the cluster,
// executes verifyQuery, and runs pg_amcheck.
func CombineAndReplayPITR(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID string,
	chain []*fencing.BackupArtifact,
	walSegments []WALSegmentArtifact,
	targetLSN string,
	verifyQuery string,
) (queryOut string, stagingHeadroomOK bool, amcheckOK bool, err error) {
	return CombineAndReplayPITRWithLimits(ctx, agent, s3c, bucket, tenantID, chain, walSegments, targetLSN, verifyQuery, TarExtractLimits{})
}

// CombineAndReplayPITRWithLimits is like CombineAndReplayPITR with explicit tar extraction byte caps.
func CombineAndReplayPITRWithLimits(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID string,
	chain []*fencing.BackupArtifact,
	walSegments []WALSegmentArtifact,
	targetLSN string,
	verifyQuery string,
	limits TarExtractLimits,
) (queryOut string, stagingHeadroomOK bool, amcheckOK bool, err error) {
	return CombineAndReplayPITRWithOptions(ctx, agent, s3c, bucket, tenantID, "postgres", chain, walSegments, targetLSN, verifyQuery, limits)
}

// CombineAndReplayPITRWithOptions is like CombineAndReplayPITRWithLimits with an explicit target database name for verification.
func CombineAndReplayPITRWithOptions(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID, dbName string,
	chain []*fencing.BackupArtifact,
	walSegments []WALSegmentArtifact,
	targetLSN string,
	verifyQuery string,
	limits TarExtractLimits,
) (queryOut string, stagingHeadroomOK bool, amcheckOK bool, err error) {
	if s3c == nil || strings.TrimSpace(bucket) == "" {
		return "", false, false, ErrR2ClientRequired
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return "", false, false, err
	}
	var totalCipherBytes uint64
	for _, b := range chain {
		totalCipherBytes += uint64(b.CipherBytes)
	}
	estBytes := totalCipherBytes * 8
	if estBytes < 40*1024*1024 {
		estBytes = 40 * 1024 * 1024
	}
	_, _, headroomOK, hErr := agent.VerifyCombineStagingHeadroom(estBytes)
	if hErr != nil || !headroomOK {
		return "", false, false, fmt.Errorf("insufficient staging disk headroom for pg_combinebackup: %v", hErr)
	}
	stagingHeadroomOK = true

	bkpBase, err := tenant.SafeSubpath(agent.VolDir(), "backups", tenantID)
	if err != nil {
		return "", stagingHeadroomOK, false, err
	}
	restoreRoot := filepath.Join(bkpBase, fmt.Sprintf("pitr_restore_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(restoreRoot, 0o700); err != nil {
		return "", stagingHeadroomOK, false, err
	}
	defer func() { _ = os.RemoveAll(restoreRoot) }()

	if len(chain) == 0 || chain[0].Type != "FULL" {
		return "", stagingHeadroomOK, false, fmt.Errorf("invalid backup chain: must begin with FULL backup")
	}
	for i := 1; i < len(chain); i++ {
		if chain[i].Type != "INCR" {
			return "", stagingHeadroomOK, false, fmt.Errorf("invalid backup chain at %s: expected INCR, got %s", chain[i].BackupID, chain[i].Type)
		}
		if chain[i].Epoch != chain[0].Epoch {
			return "", stagingHeadroomOK, false, fmt.Errorf("backup chain epoch mismatch at %s: got=%d expected=%d", chain[i].BackupID, chain[i].Epoch, chain[0].Epoch)
		}
		if chain[i].ParentID != chain[i-1].BackupID {
			return "", stagingHeadroomOK, false, fmt.Errorf("broken backup chain at %s: parent=%s expected=%s", chain[i].BackupID, chain[i].ParentID, chain[i-1].BackupID)
		}
	}

	var unpackedDirs []string
	maxObjBytes := int64(maxRestoreObjMiB) * 1024 * 1024
	for i, art := range chain {
		obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(art.R2Key),
		})
		if err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("get backup %s: %w", art.R2Key, err)
		}
		cipherBytes, err := io.ReadAll(io.LimitReader(obj.Body, maxObjBytes+1))
		_ = obj.Body.Close()
		if err != nil {
			return "", stagingHeadroomOK, false, err
		}
		if int64(len(cipherBytes)) > maxObjBytes {
			return "", stagingHeadroomOK, false, fmt.Errorf("backup object %s exceeds %d MiB limit", art.R2Key, maxRestoreObjMiB)
		}
		plainBytes, err := agent.KMS().DecryptObject(tenantID, art.Epoch, art.R2Key, cipherBytes)
		if err != nil {
			return "", stagingHeadroomOK, false, err
		}
		partDir := filepath.Join(restoreRoot, fmt.Sprintf("part_%d", i))
		if err := ExtractTarZstSafe(bytes.NewReader(plainBytes), partDir, limits); err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("extract part_%d: %w", i, err)
		}
		unpackedDirs = append(unpackedDirs, partDir)
	}

	combinedDir := filepath.Join(restoreRoot, "combined_pgdata")
	if len(unpackedDirs) == 1 {
		if err := os.Rename(unpackedDirs[0], combinedDir); err != nil {
			return "", stagingHeadroomOK, false, err
		}
	} else {
		args := append([]string{filepath.Join(agent.PGBin(), "pg_combinebackup"), "-o", combinedDir}, unpackedDirs...)
		if out, err := agent.RunPostgresCmd(args[0], args[1:]...); err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("pg_combinebackup failed: %w (%s)", err, out)
		}
	}

	_ = os.Chmod(combinedDir, 0o700)
	if out, err := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "pg_verifybackup"), combinedDir); err != nil {
		return "", stagingHeadroomOK, false, fmt.Errorf("pg_verifybackup on combined dir failed: %w (%s)", err, out)
	}

	walRestoreDir := filepath.Join(restoreRoot, "wal_archive")
	if err := os.MkdirAll(walRestoreDir, 0o700); err != nil {
		return "", stagingHeadroomOK, false, err
	}
	for _, seg := range walSegments {
		if !isValidWALFilename(seg.SegmentName) {
			return "", stagingHeadroomOK, false, fmt.Errorf("%w: %q", ErrInvalidWALName, seg.SegmentName)
		}
		obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(seg.R2Key),
		})
		if err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("get wal %s: %w", seg.R2Key, err)
		}
		cipherBytes, err := io.ReadAll(io.LimitReader(obj.Body, maxWALObjBytes+1))
		_ = obj.Body.Close()
		if err != nil {
			return "", stagingHeadroomOK, false, err
		}
		if int64(len(cipherBytes)) > maxWALObjBytes {
			return "", stagingHeadroomOK, false, fmt.Errorf("wal object %s exceeds %d byte limit", seg.R2Key, maxWALObjBytes)
		}
		zstBytes, err := agent.KMS().DecryptObject(tenantID, seg.Epoch, seg.R2Key, cipherBytes)
		if err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("decrypt wal %s: %w", seg.SegmentName, err)
		}
		rawWAL, err := DecompressBytesZstd(zstBytes, maxWALObjBytes)
		if err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("decompress wal %s: %w", seg.SegmentName, err)
		}
		rawWalPath := filepath.Join(walRestoreDir, seg.SegmentName)
		if err := os.WriteFile(rawWalPath, rawWAL, 0o600); err != nil {
			return "", stagingHeadroomOK, false, err
		}
	}

	port, err := allocFreePort()
	if err != nil {
		return "", stagingHeadroomOK, false, err
	}
	sockDir := fmt.Sprintf("/tmp/air_pitr_%d", port)
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return "", stagingHeadroomOK, false, err
	}
	defer func() { _ = os.RemoveAll(sockDir) }()

	targetCfg := ""
	if targetLSN != "" {
		targetCfg = fmt.Sprintf("recovery_target_lsn = '%s'\nrecovery_target_inclusive = off\n", targetLSN)
	}
	pitrConf := fmt.Sprintf(`
port = %d
unix_socket_directories = '%s'
archive_mode = off
restore_command = 'cp %s/%%f %%p'
%srecovery_target_action = 'promote'
`, port, sockDir, walRestoreDir, targetCfg)

	f, err := os.OpenFile(filepath.Join(combinedDir, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return "", stagingHeadroomOK, false, err
	}
	_, _ = f.WriteString(pitrConf)
	_ = f.Close()

	if err := os.WriteFile(filepath.Join(combinedDir, "recovery.signal"), []byte(""), 0o600); err != nil {
		return "", stagingHeadroomOK, false, err
	}

	logFile := filepath.Join(restoreRoot, "pitr_restore.log")
	if out, err := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "pg_ctl"), "-D", combinedDir, "-l", logFile, "-w", "-t", "25", "start"); err != nil {
		logBytes, _ := os.ReadFile(logFile)
		return "", stagingHeadroomOK, false, fmt.Errorf("pitr pg_ctl start failed: %w (%s / log: %s)", err, out, string(logBytes))
	}
	defer func() {
		_, _ = agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "pg_ctl"), "-D", combinedDir, "-m", "immediate", "-w", "stop")
	}()

	for i := 0; i < 40; i++ {
		inRec, _ := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres", "-t", "-A", "-c", "SELECT pg_is_in_recovery();")
		if strings.TrimSpace(inRec) == "f" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	verifyDB := strings.TrimSpace(dbName)
	if verifyDB == "" {
		verifyDB = "postgres"
	}

	qOut := ""
	if strings.TrimSpace(verifyQuery) != "" {
		qOut, err = agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", verifyDB, "-t", "-A", "-c", verifyQuery)
		if err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("pitr verifyQuery failed: %w (%s)", err, qOut)
		}
	}

	_, _ = agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", verifyDB, "-c", "CREATE EXTENSION IF NOT EXISTS amcheck;")
	_, amErr := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "pg_amcheck"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", verifyDB)

	return strings.TrimSpace(qOut), stagingHeadroomOK, amErr == nil, nil
}

func allocFreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

type DAGGCResult struct {
	RetainedBackupIDs      []string `json:"retained_backup_ids"`
	RetainedAncestorIDs    []string `json:"retained_ancestor_ids"`
	SweptBackupIDs         []string `json:"swept_backup_ids"`
	MinRetainedBackupID    string   `json:"min_retained_backup_id"`
	MinRetainedWALStartLSN string   `json:"min_retained_wal_start_lsn"`
}

// GarbageCollectBackupDAG implements Mark-and-Sweep DAG retention over backup chains:
// 1. Mark every backup node with CreatedAt >= retentionCutoff as RETAINED.
// 2. Recursively mark every ParentID ancestor of any RETAINED node up to its root FULL backup as RETAINED_ANCESTOR.
// 3. Sweep only nodes that are neither RETAINED nor RETAINED_ANCESTOR.
func GarbageCollectBackupDAG(nodes []fencing.BackupArtifact, retentionCutoff time.Time) DAGGCResult {
	byID := make(map[string]fencing.BackupArtifact, len(nodes))
	for _, n := range nodes {
		byID[n.BackupID] = n
	}

	retainedDirect := make(map[string]bool)
	retainedAncestor := make(map[string]bool)

	for _, n := range nodes {
		if !n.CreatedAt.Before(retentionCutoff) {
			retainedDirect[n.BackupID] = true
			currParent := n.ParentID
			for currParent != "" {
				pNode, exists := byID[currParent]
				if !exists {
					break
				}
				if !retainedDirect[currParent] {
					retainedAncestor[currParent] = true
				}
				currParent = pNode.ParentID
			}
		}
	}

	var res DAGGCResult
	var earliestTime time.Time
	for _, n := range nodes {
		if retainedDirect[n.BackupID] || retainedAncestor[n.BackupID] {
			res.RetainedBackupIDs = append(res.RetainedBackupIDs, n.BackupID)
			if retainedAncestor[n.BackupID] {
				res.RetainedAncestorIDs = append(res.RetainedAncestorIDs, n.BackupID)
			}
			if earliestTime.IsZero() || n.CreatedAt.Before(earliestTime) {
				earliestTime = n.CreatedAt
				res.MinRetainedBackupID = n.BackupID
				res.MinRetainedWALStartLSN = n.StartLSN
			}
		} else {
			res.SweptBackupIDs = append(res.SweptBackupIDs, n.BackupID)
		}
	}
	sort.Strings(res.RetainedBackupIDs)
	sort.Strings(res.RetainedAncestorIDs)
	sort.Strings(res.SweptBackupIDs)
	return res
}

// HydrateTenantPGDataFromR2 downloads and decrypts the tenant's latest backup chain and WAL segments from R2,
// replays WAL to a consistent promoted state, and installs the resulting pgdata into tenants/active/<tenantID>/pgdata.
func HydrateTenantPGDataFromR2(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID string,
) error {
	return HydrateTenantPGDataFromR2Gen(ctx, agent, s3c, bucket, tenantID, "")
}

func HydrateTenantPGDataFromR2Gen(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID, generation string,
) error {
	if s3c == nil || strings.TrimSpace(bucket) == "" {
		return ErrR2ClientRequired
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return err
	}
	activeBase, err := tenant.SafeSubpath(agent.VolDir(), "tenants/active", tenantID)
	if err != nil {
		return err
	}
	targetPGData := filepath.Join(activeBase, "pgdata")
	if _, err := os.Stat(filepath.Join(targetPGData, "PG_VERSION")); err == nil {
		return nil
	}

	manifest, _, err := fencing.ReadManifestWithETagGen(ctx, s3c, bucket, tenantID, generation)
	if err != nil {
		return fmt.Errorf("read R2 manifest for %s: %w", tenantID, err)
	}
	if len(manifest.Backups) == 0 {
		return fmt.Errorf("no backups recorded in R2 manifest for %s", tenantID)
	}

	byID := make(map[string]*fencing.BackupArtifact, len(manifest.Backups))
	for i := range manifest.Backups {
		b := manifest.Backups[i]
		byID[b.BackupID] = &b
	}
	var revChain []*fencing.BackupArtifact
	curr := &manifest.Backups[len(manifest.Backups)-1]
	for curr != nil {
		revChain = append(revChain, curr)
		if curr.Type == "FULL" || curr.ParentID == "" {
			break
		}
		curr = byID[curr.ParentID]
	}
	chain := make([]*fencing.BackupArtifact, len(revChain))
	for i := range revChain {
		chain[i] = revChain[len(revChain)-1-i]
	}

	bkpBase, err := tenant.SafeSubpath(agent.VolDir(), "backups", tenantID)
	if err != nil {
		return err
	}
	hydrateRoot := filepath.Join(bkpBase, fmt.Sprintf("hydrate_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(hydrateRoot, 0o700); err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(hydrateRoot) }()

	var unpackedDirs []string
	maxObjBytes := int64(maxRestoreObjMiB) * 1024 * 1024
	for i, art := range chain {
		obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(art.R2Key),
		})
		if err != nil {
			return fmt.Errorf("get backup %s: %w", art.R2Key, err)
		}
		cipherBytes, err := io.ReadAll(io.LimitReader(obj.Body, maxObjBytes+1))
		_ = obj.Body.Close()
		if err != nil {
			return err
		}
		if int64(len(cipherBytes)) > maxObjBytes {
			return fmt.Errorf("backup object %s exceeds %d MiB limit", art.R2Key, maxRestoreObjMiB)
		}
		plainBytes, err := agent.KMS().DecryptObject(tenantID, art.Epoch, art.R2Key, cipherBytes)
		if err != nil {
			return err
		}
		partDir := filepath.Join(hydrateRoot, fmt.Sprintf("part_%d", i))
		if err := ExtractTarZstSafe(bytes.NewReader(plainBytes), partDir, TarExtractLimits{}); err != nil {
			return fmt.Errorf("extract part_%d: %w", i, err)
		}
		unpackedDirs = append(unpackedDirs, partDir)
	}

	combinedDir := filepath.Join(hydrateRoot, "combined_pgdata")
	if len(unpackedDirs) == 1 {
		if err := os.Rename(unpackedDirs[0], combinedDir); err != nil {
			return err
		}
	} else {
		args := append([]string{filepath.Join(agent.PGBin(), "pg_combinebackup"), "-o", combinedDir}, unpackedDirs...)
		if out, err := agent.RunPostgresCmd(args[0], args[1:]...); err != nil {
			return fmt.Errorf("pg_combinebackup failed: %w (%s)", err, out)
		}
	}
	_ = os.Chmod(combinedDir, 0o700)
	if out, err := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "pg_verifybackup"), combinedDir); err != nil {
		return fmt.Errorf("pg_verifybackup failed: %w (%s)", err, out)
	}

	walRestoreDir := filepath.Join(hydrateRoot, "wal_archive")
	if err := os.MkdirAll(walRestoreDir, 0o700); err != nil {
		return err
	}
	for _, r2Key := range manifest.WALSegments {
		segName := strings.TrimSuffix(filepath.Base(r2Key), ".zst.enc")
		if !isValidWALFilename(segName) {
			continue
		}
		epoch := manifest.FencingEpoch
		for _, part := range strings.Split(r2Key, "/") {
			if strings.HasPrefix(part, "epoch_") {
				if parsed, pErr := strconv.ParseInt(strings.TrimPrefix(part, "epoch_"), 10, 64); pErr == nil && parsed > 0 {
					epoch = parsed
				}
			}
		}
		obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(r2Key),
		})
		if err != nil {
			return fmt.Errorf("get wal %s: %w", r2Key, err)
		}
		cipherBytes, err := io.ReadAll(io.LimitReader(obj.Body, maxWALObjBytes+1))
		_ = obj.Body.Close()
		if err != nil {
			return err
		}
		zstBytes, err := agent.KMS().DecryptObject(tenantID, epoch, r2Key, cipherBytes)
		if err != nil {
			return fmt.Errorf("decrypt wal %s: %w", segName, err)
		}
		rawWAL, err := DecompressBytesZstd(zstBytes, maxWALObjBytes)
		if err != nil {
			return fmt.Errorf("decompress wal %s: %w", segName, err)
		}
		if err := os.WriteFile(filepath.Join(walRestoreDir, segName), rawWAL, 0o600); err != nil {
			return err
		}
	}

	port, err := allocFreePort()
	if err != nil {
		return err
	}
	sockDir := fmt.Sprintf("/tmp/air_hydrate_%d", port)
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(sockDir) }()

	pgConfPath := filepath.Join(combinedDir, "postgresql.conf")
	origPGConf, _ := os.ReadFile(pgConfPath)
	pitrConf := fmt.Sprintf("\nport = %d\nunix_socket_directories = '%s'\narchive_mode = off\nrestore_command = 'cp %s/%%f %%p'\nrecovery_target_action = 'promote'\n", port, sockDir, walRestoreDir)
	f, err := os.OpenFile(pgConfPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, _ = f.WriteString(pitrConf)
	_ = f.Close()

	if err := os.WriteFile(filepath.Join(combinedDir, "recovery.signal"), []byte(""), 0o600); err != nil {
		return err
	}
	logFile := filepath.Join(hydrateRoot, "hydrate_restore.log")
	if out, err := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "pg_ctl"), "-D", combinedDir, "-l", logFile, "-w", "-t", "25", "start"); err != nil {
		logBytes, _ := os.ReadFile(logFile)
		return fmt.Errorf("hydrate pg_ctl start failed: %w (%s / log: %s)", err, out, string(logBytes))
	}
	for i := 0; i < 40; i++ {
		inRec, _ := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres", "-t", "-A", "-c", "SELECT pg_is_in_recovery();")
		if strings.TrimSpace(inRec) == "f" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if out, err := agent.RunPostgresCmd(filepath.Join(agent.PGBin(), "pg_ctl"), "-D", combinedDir, "-m", "fast", "-w", "-t", "15", "stop"); err != nil {
		return fmt.Errorf("hydrate pg_ctl stop failed: %w (%s)", err, out)
	}

	if len(origPGConf) > 0 {
		_ = os.WriteFile(pgConfPath, origPGConf, 0o600)
	}

	_ = os.Remove(filepath.Join(combinedDir, "postmaster.pid"))
	_ = os.Remove(filepath.Join(combinedDir, "recovery.signal"))
	_ = os.Remove(filepath.Join(combinedDir, "standby.signal"))

	if err := os.MkdirAll(activeBase, 0o755); err != nil {
		return err
	}
	_ = os.RemoveAll(targetPGData)
	if err := os.Rename(combinedDir, targetPGData); err != nil {
		return err
	}
	return agent.WriteTenantConfigs(tenantID)
}
