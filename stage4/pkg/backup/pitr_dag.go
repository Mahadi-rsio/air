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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"air-stage4/pkg/cell"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

const (
	pgBin            = "/usr/lib/postgresql/17/bin"
	maxRestoreObjMiB = 2048
)

var (
	ErrR2ClientRequired = errors.New("backup: R2 client and bucket are required (fail-closed)")
	ErrInvalidWALName   = errors.New("backup: invalid WAL segment or timeline history name")
	walSegmentRe        = regexp.MustCompile(`^[0-9A-F]{24}$`)
	walHistoryRe        = regexp.MustCompile(`^[0-9A-F]{8}\.history$`)
)

type WALSegmentArtifact struct {
	SegmentName string `json:"segment_name"`
	R2Key       string `json:"r2_key"`
	Epoch       int64  `json:"epoch"`
	CipherBytes int    `json:"cipher_bytes"`
	PlainSHA256 string `json:"plain_sha256,omitempty"`
	CipherETag  string `json:"cipher_etag,omitempty"`
	Status      string `json:"status,omitempty"` // PENDING, UPLOADED_UNVERIFIED, VERIFIED_IN_R2, LOCAL_DELETED
}

type WALUploadManifest struct {
	TenantID  string                          `json:"tenant_id"`
	UpdatedAt string                          `json:"updated_at"`
	Segments  map[string]*WALSegmentArtifact `json:"segments"`
}

func isValidWALFilename(name string) bool {
	return walSegmentRe.MatchString(name) || walHistoryRe.MatchString(name)
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
// prior to INCR, runs pg_verifybackup, compresses with tar --zstd, encrypts with AES-256-GCM-AAD,
// and uploads to Cloudflare R2 with If-None-Match: "*".
func RunEncryptedBackupStep(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID, bkpID, parentID, bkpType, parentManifestPath string,
) (*cell.BackupArtifact, string, error) {
	rt, ok := agent.GetTenant(tenantID)
	if !ok {
		return nil, "", fmt.Errorf("tenant %s not active", tenantID)
	}
	dekBundle, err := agent.KMS().EnsureTenantDEK(tenantID)
	if err != nil {
		return nil, "", err
	}

	// Pre-flight check for INCR: verify WAL summaries are present (S4-RISK-02)
	if bkpType == "INCR" {
		sumCount, err := agent.VerifyWALSummaryContinuity(ctx, tenantID)
		if err != nil || sumCount == 0 {
			// Fallback to FULL backup if WAL summary continuity is missing
			bkpType = "FULL"
			parentID = ""
			parentManifestPath = ""
		}
	} else {
		_, _ = cell.RunPostgresCmd(filepath.Join(pgBin, "psql"), "-h", rt.SockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-d", "postgres", "-c", "CHECKPOINT;")
	}

	bkpDir := filepath.Join(agent.VolDir(), "backups", tenantID, bkpID)
	_ = os.RemoveAll(bkpDir)
	if err := os.MkdirAll(filepath.Dir(bkpDir), 0o755); err != nil {
		return nil, "", err
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", filepath.Dir(bkpDir)).Run()

	if bkpType == "INCR" {
		time.Sleep(250 * time.Millisecond)
	}

	args := []string{
		filepath.Join(pgBin, "pg_basebackup"),
		"-h", rt.SockDir,
		"-p", strconv.Itoa(rt.Port),
		"-U", "postgres",
		"-D", bkpDir,
		"-c", "fast",
	}
	if bkpType == "INCR" && parentManifestPath != "" {
		args = append(args, "--incremental="+parentManifestPath)
	}
	if out, err := cell.RunPostgresCmd(args[0], args[1:]...); err != nil {
		return nil, "", fmt.Errorf("pg_basebackup (%s) failed: %w (%s)", bkpType, err, out)
	}

	// Mandatory pre-publish pg_verifybackup check
	if out, err := cell.RunPostgresCmd(filepath.Join(pgBin, "pg_verifybackup"), bkpDir); err != nil {
		return nil, "", fmt.Errorf("pg_verifybackup failed: %w (%s)", err, out)
	}

	tarZst := bkpDir + ".tar.zst"
	if out, err := exec.Command("tar", "--zstd", "-cf", tarZst, "-C", bkpDir, ".").CombinedOutput(); err != nil {
		return nil, "", fmt.Errorf("tar --zstd failed: %w (%s)", err, string(out))
	}
	plainBytes, err := os.ReadFile(tarZst)
	_ = os.Remove(tarZst)
	if err != nil {
		return nil, "", err
	}

	r2Key := fmt.Sprintf("stage4/tenants/%s/epoch_%06d/%s.tar.zst.enc", tenantID, rt.Epoch, bkpID)
	cipherBytes, err := agent.KMS().EncryptObject(tenantID, rt.Epoch, r2Key, plainBytes)
	if err != nil {
		return nil, "", err
	}

	if s3c != nil {
		_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(r2Key),
			Body:        bytes.NewReader(cipherBytes),
			IfNoneMatch: aws.String("*"),
		})
		if err != nil {
			return nil, "", fmt.Errorf("R2 PutObject (%s) failed: %w", r2Key, err)
		}
	}

	return &cell.BackupArtifact{
		BackupID:     bkpID,
		ParentID:     parentID,
		Type:         bkpType,
		R2Key:        r2Key,
		Epoch:        rt.Epoch,
		CipherBytes:  len(cipherBytes),
		CreatedAt:    time.Now().UTC(),
		Verified:     true,
		DEKBundle:    dekBundle,
		LocalBackupD: bkpDir,
	}, filepath.Join(bkpDir, "backup_manifest"), nil
}

// DrainAndEncryptWALSpoolToR2 forces a WAL switch on the live cluster, compresses all spooled
// WAL files in /vol/spool/wal/<tenant> with zstd, encrypts them with AES-256-GCM-AAD,
// verifies remote upload to R2 via HeadObject, commits a durable manifest, and ONLY then deletes local WAL.
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
	if err := cell.ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	rt, ok := agent.GetTenant(tenantID)
	if !ok {
		return nil, fmt.Errorf("tenant %s not active", tenantID)
	}
	// Force PostgreSQL to switch and archive the current WAL segment
	_, _ = cell.RunPostgresCmd(
		filepath.Join(pgBin, "psql"),
		"-h", rt.SockDir,
		"-p", strconv.Itoa(rt.Port),
		"-U", "postgres",
		"-d", "postgres",
		"-c", "SELECT pg_switch_wal(); CHECKPOINT;",
	)
	time.Sleep(300 * time.Millisecond)

	entries, err := os.ReadDir(rt.WALSpoolDir)
	if err != nil {
		return nil, err
	}

	manifestPath := filepath.Join(rt.WALSpoolDir, "wal_upload_manifest.json")
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
	// Sort in deterministic timeline + LSN order
	sort.Strings(segNames)

	var uploaded []WALSegmentArtifact
	for _, segName := range segNames {
		segPath := filepath.Join(rt.WALSpoolDir, segName)

		// Resume check: if already VERIFIED_IN_R2 in durable manifest, verify remote HeadObject and finish local cleanup safely.
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

		zstPath := segPath + ".zst"
		if out, err := exec.Command("zstd", "-q", "-f", segPath, "-o", zstPath).CombinedOutput(); err != nil {
			return uploaded, fmt.Errorf("zstd compress wal %s: %w (%s)", segName, err, string(out))
		}
		compressedBytes, err := os.ReadFile(zstPath)
		_ = os.Remove(zstPath)
		if err != nil {
			return uploaded, err
		}
		r2Key := fmt.Sprintf("stage4/tenants/%s/epoch_%06d/wal/%s.zst.enc", tenantID, rt.Epoch, segName)
		cipherBytes, err := agent.KMS().EncryptObject(tenantID, rt.Epoch, r2Key, compressedBytes)
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

		// Verify remote object existence and exact byte length via HeadObject before committing manifest!
		headOut, err := s3c.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(r2Key),
		})
		if err != nil || headOut.ContentLength == nil || *headOut.ContentLength != int64(len(cipherBytes)) {
			return uploaded, fmt.Errorf("R2 HeadObject verification failed for %s: err=%v", r2Key, err)
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
			Epoch:       rt.Epoch,
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
// WAL segments from Cloudflare R2, verifies >= 2.2x staging disk headroom, runs pg_combinebackup +
// pg_verifybackup, replays archived WAL up to targetLSN (stopping right before a destructive DROP TABLE),
// promotes the cluster, executes verifyQuery, and runs pg_amcheck.
func CombineAndReplayPITR(
	ctx context.Context,
	agent *cell.Agent,
	s3c *s3.Client,
	bucket, tenantID string,
	chain []*cell.BackupArtifact,
	walSegments []WALSegmentArtifact,
	targetLSN string,
	verifyQuery string,
) (queryOut string, stagingHeadroomOK bool, amcheckOK bool, err error) {
	var totalCipherBytes uint64
	for _, b := range chain {
		totalCipherBytes += uint64(b.CipherBytes)
	}
	// Estimate uncompressed cluster bytes (~8x compressed cipher size, min 40MB)
	estBytes := totalCipherBytes * 8
	if estBytes < 40*1024*1024 {
		estBytes = 40 * 1024 * 1024
	}
	_, _, headroomOK, hErr := agent.VerifyCombineStagingHeadroom(estBytes)
	if hErr != nil || !headroomOK {
		return "", false, false, fmt.Errorf("insufficient staging disk headroom for pg_combinebackup: %v", hErr)
	}
	stagingHeadroomOK = true

	restoreRoot := filepath.Join(agent.VolDir(), "backups", tenantID, fmt.Sprintf("pitr_restore_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(restoreRoot, 0o755); err != nil {
		return "", stagingHeadroomOK, false, err
	}
	defer func() { _ = os.RemoveAll(restoreRoot) }()

	// 1. Validate backup chain parent-child linkage and download bounded objects
	if len(chain) == 0 || chain[0].Type != "FULL" {
		return "", stagingHeadroomOK, false, fmt.Errorf("invalid backup chain: must begin with FULL backup")
	}
	for i := 1; i < len(chain); i++ {
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
		tarZst := filepath.Join(restoreRoot, fmt.Sprintf("part_%d.tar.zst", i))
		partDir := filepath.Join(restoreRoot, fmt.Sprintf("part_%d", i))
		_ = os.MkdirAll(partDir, 0o700)
		if err := os.WriteFile(tarZst, plainBytes, 0o600); err != nil {
			return "", stagingHeadroomOK, false, err
		}
		if out, err := exec.Command("tar", "--zstd", "--no-same-owner", "--no-same-permissions", "-xf", tarZst, "-C", partDir).CombinedOutput(); err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("untar part_%d: %w (%s)", i, err, string(out))
		}
		_ = os.Remove(tarZst)
		unpackedDirs = append(unpackedDirs, partDir)
	}

	// 2. Run pg_combinebackup across all unpacked layers (B0 + I1 + I2)
	combinedDir := filepath.Join(restoreRoot, "combined_pgdata")
	if len(unpackedDirs) == 1 {
		if err := os.Rename(unpackedDirs[0], combinedDir); err != nil {
			return "", stagingHeadroomOK, false, err
		}
	} else {
		args := append([]string{filepath.Join(pgBin, "pg_combinebackup"), "-o", combinedDir}, unpackedDirs...)
		_ = exec.Command("chown", "-R", "postgres:postgres", restoreRoot).Run()
		if out, err := cell.RunPostgresCmd(args[0], args[1:]...); err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("pg_combinebackup failed: %w (%s)", err, out)
		}
	}

	// 3. Verify combined backup integrity via pg_verifybackup BEFORE replaying archived WAL
	_ = exec.Command("chown", "-R", "postgres:postgres", combinedDir).Run()
	_ = os.Chmod(combinedDir, 0o700)
	if out, err := cell.RunPostgresCmd(filepath.Join(pgBin, "pg_verifybackup"), combinedDir); err != nil {
		return "", stagingHeadroomOK, false, fmt.Errorf("pg_verifybackup on combined dir failed: %w (%s)", err, out)
	}

	// 4. Download, decrypt, and decompress all archived WAL segments from R2 into walRestoreDir
	walRestoreDir := filepath.Join(restoreRoot, "wal_archive")
	if err := os.MkdirAll(walRestoreDir, 0o755); err != nil {
		return "", stagingHeadroomOK, false, err
	}
	for _, seg := range walSegments {
		obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(seg.R2Key),
		})
		if err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("get wal %s: %w", seg.R2Key, err)
		}
		cipherBytes, err := io.ReadAll(obj.Body)
		_ = obj.Body.Close()
		if err != nil {
			return "", stagingHeadroomOK, false, err
		}
		zstBytes, err := agent.KMS().DecryptObject(tenantID, seg.Epoch, seg.R2Key, cipherBytes)
		if err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("decrypt wal %s: %w", seg.SegmentName, err)
		}
		zstPath := filepath.Join(walRestoreDir, seg.SegmentName+".zst")
		rawWalPath := filepath.Join(walRestoreDir, seg.SegmentName)
		if err := os.WriteFile(zstPath, zstBytes, 0o600); err != nil {
			return "", stagingHeadroomOK, false, err
		}
		if out, err := exec.Command("zstd", "-d", "-q", "-f", zstPath, "-o", rawWalPath).CombinedOutput(); err != nil {
			return "", stagingHeadroomOK, false, fmt.Errorf("zstd decompress wal %s: %w (%s)", seg.SegmentName, err, string(out))
		}
		_ = os.Remove(zstPath)
	}
	_ = exec.Command("chown", "-R", "postgres:postgres", walRestoreDir).Run()

	// 5. Configure PITR recovery to targetLSN in postgresql.conf + recovery.signal
	port := 18900 + int(time.Now().UnixNano()%500)
	sockDir := fmt.Sprintf("/tmp/air_pitr_%d", port)
	_ = os.MkdirAll(sockDir, 0o755)
	_ = exec.Command("chown", "-R", "postgres:postgres", sockDir).Run()
	defer func() { _ = os.RemoveAll(sockDir) }()

	targetCfg := ""
	if targetLSN != "" {
		targetCfg = fmt.Sprintf("recovery_target_lsn = '%s'\nrecovery_target_inclusive = on\n", targetLSN)
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
	_ = exec.Command("chown", "-R", "postgres:postgres", restoreRoot).Run()

	logFile := filepath.Join(restoreRoot, "pitr_restore.log")
	if out, err := cell.RunPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", combinedDir, "-l", logFile, "-w", "-t", "25", "start"); err != nil {
		logBytes, _ := os.ReadFile(logFile)
		return "", stagingHeadroomOK, false, fmt.Errorf("pitr pg_ctl start failed: %w (%s / log: %s)", err, out, string(logBytes))
	}
	defer func() {
		_, _ = cell.RunPostgresCmd(filepath.Join(pgBin, "pg_ctl"), "-D", combinedDir, "-m", "immediate", "-w", "stop")
	}()

	// Wait for recovery_target_action = 'promote' to finish recovery
	for i := 0; i < 40; i++ {
		inRec, _ := cell.RunPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres", "-t", "-A", "-c", "SELECT pg_is_in_recovery();")
		if strings.TrimSpace(inRec) == "f" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	qOut, err := cell.RunPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres", "-t", "-A", "-c", verifyQuery)
	if err != nil {
		return "", stagingHeadroomOK, false, fmt.Errorf("pitr verifyQuery failed: %w (%s)", err, qOut)
	}

	_, _ = cell.RunPostgresCmd(filepath.Join(pgBin, "psql"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "-d", "postgres", "-c", "CREATE EXTENSION IF NOT EXISTS amcheck;")
	_, amErr := cell.RunPostgresCmd(filepath.Join(pgBin, "pg_amcheck"), "-h", sockDir, "-p", strconv.Itoa(port), "-U", "postgres", "postgres")

	return strings.TrimSpace(qOut), stagingHeadroomOK, amErr == nil, nil
}

// =========================================================================
// WORKSTREAM 2 REMEDIATION: Mark-and-Sweep DAG Garbage Collector (S4-RISK-02)
// =========================================================================

type DAGGCResult struct {
	RetainedBackupIDs      []string `json:"retained_backup_ids"`
	RetainedAncestorIDs    []string `json:"retained_ancestor_ids"`
	SweptBackupIDs         []string `json:"swept_backup_ids"`
	MinRetainedBackupID    string   `json:"min_retained_backup_id"`
	MinRetainedWALStartLSN string   `json:"min_retained_wal_start_lsn"`
}

// GarbageCollectDAG implements Mark-and-Sweep DAG retention over backup chains:
// 1. Mark every backup node with CreatedAt >= retentionCutoff as RETAINED.
// 2. Recursively mark every ParentID ancestor of any RETAINED node up to its root FULL backup as RETAINED_ANCESTOR.
// 3. Sweep only nodes that are neither RETAINED nor RETAINED_ANCESTOR.
// This guarantees an active incremental backup NEVER loses its base or parent incremental ancestors.
func GarbageCollectDAG(nodes []cell.BackupArtifact, retentionCutoff time.Time) DAGGCResult {
	byID := make(map[string]cell.BackupArtifact, len(nodes))
	for _, n := range nodes {
		byID[n.BackupID] = n
	}

	retainedDirect := make(map[string]bool)
	retainedAncestor := make(map[string]bool)

	for _, n := range nodes {
		if !n.CreatedAt.Before(retentionCutoff) {
			retainedDirect[n.BackupID] = true
			// Walk parent chain up to root FULL backup
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

type Workstream2Report struct {
	Pass                             bool        `json:"pass"`
	WALSummaryPreFlightCount         int         `json:"wal_summary_preflight_count"`
	StagingHeadroom22xVerified       bool        `json:"staging_headroom_2_2x_verified"`
	FullB0CipherBytes                int         `json:"full_b0_cipher_bytes"`
	IncrI1CipherBytes                int         `json:"incr_i1_cipher_bytes"`
	IncrI2CipherBytes                int         `json:"incr_i2_cipher_bytes"`
	ArchivedWALSegmentsUploadedToR2  int         `json:"archived_wal_segments_uploaded_to_r2"`
	TargetLSNBeforeDropTable         string      `json:"target_lsn_before_drop_table"`
	RestoredRowCountAfterCombinePITR int         `json:"restored_row_count_after_combine_pitr"`
	ExpectedRowCountBeforeDropTable  int         `json:"expected_row_count_before_drop_table"`
	PgAmcheckPassedPostPITR          bool        `json:"pg_amcheck_passed_post_pitr"`
	DAGGCCaseAAncestorsPreserved     DAGGCResult `json:"dag_gc_case_a_ancestors_preserved"`
	DAGGCCaseBOldChainSweptCleanly   DAGGCResult `json:"dag_gc_case_b_old_chain_swept_cleanly"`
}

// RunWorkstream2BackupPITRAndGC executes EXP-S4-2A (B0 -> I1 -> I2 + Encrypted WAL Archiving + Exact-LSN PITR
// before DROP TABLE on live Cloudflare R2) and EXP-S4-2B (Mark-and-Sweep DAG Garbage Collection safety proof).
func RunWorkstream2BackupPITRAndGC(ctx context.Context, agent *cell.Agent, s3c *s3.Client, bucket string) (Workstream2Report, error) {
	var rep Workstream2Report
	rep.ExpectedRowCountBeforeDropTable = 2300

	tenantID := "t_ws2_pitr"
	if _, err := agent.ClaimCluster(tenantID); err != nil {
		return rep, err
	}
	rt, err := agent.ActivateCluster(tenantID, "pw_ws2", 1)
	if err != nil {
		return rep, err
	}
	defer func() { _ = agent.StopAndPurgeTenant(tenantID) }()

	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", rt.Port)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return rep, err
	}
	defer conn.Close(ctx)

	// Step 1: Create table and insert 1,000 rows -> Full Backup B0
	_, err = conn.Exec(ctx, `
		CREATE TABLE pitr_orders (
			id INT PRIMARY KEY,
			batch TEXT NOT NULL,
			payload TEXT NOT NULL
		);
		INSERT INTO pitr_orders SELECT g, 'B0_FULL', md5(g::text) FROM generate_series(1, 1000) g;
	`)
	if err != nil {
		return rep, err
	}

	b0Art, b0Manifest, err := RunEncryptedBackupStep(ctx, agent, s3c, bucket, tenantID, "B0_full", "", "FULL", "")
	if err != nil {
		return rep, fmt.Errorf("B0 full backup: %w", err)
	}
	rep.FullB0CipherBytes = b0Art.CipherBytes

	// Step 2: Insert 500 rows (1001..1500) -> Incremental Backup I1 (parent = B0)
	_, err = conn.Exec(ctx, `INSERT INTO pitr_orders SELECT g, 'I1_INCR', md5(g::text) FROM generate_series(1001, 1500) g;`)
	if err != nil {
		return rep, err
	}
	sumCount, _ := agent.VerifyWALSummaryContinuity(ctx, tenantID)
	rep.WALSummaryPreFlightCount = sumCount

	i1Art, i1Manifest, err := RunEncryptedBackupStep(ctx, agent, s3c, bucket, tenantID, "I1_incr", b0Art.BackupID, "INCR", b0Manifest)
	if err != nil {
		return rep, fmt.Errorf("I1 incr backup: %w", err)
	}
	rep.IncrI1CipherBytes = i1Art.CipherBytes

	// Step 3: Insert 500 rows (1501..2000) -> Incremental Backup I2 (parent = I1)
	_, err = conn.Exec(ctx, `INSERT INTO pitr_orders SELECT g, 'I2_INCR', md5(g::text) FROM generate_series(1501, 2000) g;`)
	if err != nil {
		return rep, err
	}
	i2Art, _, err := RunEncryptedBackupStep(ctx, agent, s3c, bucket, tenantID, "I2_incr", i1Art.BackupID, "INCR", i1Manifest)
	if err != nil {
		return rep, fmt.Errorf("I2 incr backup: %w", err)
	}
	rep.IncrI2CipherBytes = i2Art.CipherBytes

	// Step 4: Insert 300 rows (2001..2300) AFTER I2 completed, capture exact LSN, then execute destructive DROP TABLE!
	_, err = conn.Exec(ctx, `INSERT INTO pitr_orders SELECT g, 'POST_I2_WAL', md5(g::text) FROM generate_series(2001, 2300) g;`)
	if err != nil {
		return rep, err
	}
	var targetLSN string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text;").Scan(&targetLSN); err != nil {
		return rep, err
	}
	rep.TargetLSNBeforeDropTable = targetLSN

	// Execute accidental disaster: DROP TABLE pitr_orders!
	if _, err := conn.Exec(ctx, "DROP TABLE pitr_orders;"); err != nil {
		return rep, err
	}
	_ = conn.Close(ctx)

	// Archive all WAL segments (including the 300 post-I2 rows and the DROP TABLE) to live Cloudflare R2
	walSegs, err := DrainAndEncryptWALSpoolToR2(ctx, agent, s3c, bucket, tenantID)
	if err != nil {
		return rep, fmt.Errorf("drain WAL spool to R2: %w", err)
	}
	rep.ArchivedWALSegmentsUploadedToR2 = len(walSegs)

	// Step 5: Combine B0 + I1 + I2 and replay archived WAL from R2 stopping at targetLSN (right before DROP TABLE!)
	qOut, headroomOK, amcheckOK, err := CombineAndReplayPITR(
		ctx, agent, s3c, bucket, tenantID,
		[]*cell.BackupArtifact{b0Art, i1Art, i2Art},
		walSegs,
		targetLSN,
		"SELECT count(*) FROM pitr_orders;",
	)
	if err != nil {
		return rep, fmt.Errorf("CombineAndReplayPITR failed: %w", err)
	}
	rep.StagingHeadroom22xVerified = headroomOK
	rep.PgAmcheckPassedPostPITR = amcheckOK
	rep.RestoredRowCountAfterCombinePITR, _ = strconv.Atoi(strings.TrimSpace(qOut))

	// Cleanup R2 objects created in EXP-S4-2A
	for _, k := range []string{b0Art.R2Key, i1Art.R2Key, i2Art.R2Key} {
		_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)})
	}
	for _, w := range walSegs {
		_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(w.R2Key)})
	}

	// Step 6: EXP-S4-2B — Verify Mark-and-Sweep DAG Garbage Collector preserves required ancestors
	baseTime := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	dagNodes := []cell.BackupArtifact{
		{BackupID: "B0", ParentID: "", Type: "FULL", StartLSN: "0/1000000", CreatedAt: baseTime.Add(24 * time.Hour)},      // Day 1
		{BackupID: "I1", ParentID: "B0", Type: "INCR", StartLSN: "0/2000000", CreatedAt: baseTime.Add(2 * 24 * time.Hour)}, // Day 2
		{BackupID: "I2", ParentID: "I1", Type: "INCR", StartLSN: "0/3000000", CreatedAt: baseTime.Add(5 * 24 * time.Hour)}, // Day 5
		{BackupID: "B1", ParentID: "", Type: "FULL", StartLSN: "0/5000000", CreatedAt: baseTime.Add(6 * 24 * time.Hour)},   // Day 6
		{BackupID: "I3", ParentID: "B1", Type: "INCR", StartLSN: "0/6000000", CreatedAt: baseTime.Add(7 * 24 * time.Hour)}, // Day 7
	}

	// Case A: Cutoff = Day 4 (B0 & I1 are older than Day 4, but I2 on Day 5 depends on I1 -> B0!)
	rep.DAGGCCaseAAncestorsPreserved = GarbageCollectDAG(dagNodes, baseTime.Add(4*24*time.Hour))
	// Case B: Cutoff = Day 6 (B0, I1, I2 are all older than Day 6 and have no retained descendants -> swept!)
	rep.DAGGCCaseBOldChainSweptCleanly = GarbageCollectDAG(dagNodes, baseTime.Add(6*24*time.Hour))

	caseAPass := len(rep.DAGGCCaseAAncestorsPreserved.SweptBackupIDs) == 0 &&
		len(rep.DAGGCCaseAAncestorsPreserved.RetainedAncestorIDs) == 2 &&
		rep.DAGGCCaseAAncestorsPreserved.MinRetainedWALStartLSN == "0/1000000"

	caseBPass := len(rep.DAGGCCaseBOldChainSweptCleanly.SweptBackupIDs) == 3 &&
		len(rep.DAGGCCaseBOldChainSweptCleanly.RetainedBackupIDs) == 2 &&
		rep.DAGGCCaseBOldChainSweptCleanly.MinRetainedWALStartLSN == "0/5000000"

	rep.Pass = rep.WALSummaryPreFlightCount > 0 &&
		rep.StagingHeadroom22xVerified &&
		rep.ArchivedWALSegmentsUploadedToR2 > 0 &&
		rep.RestoredRowCountAfterCombinePITR == 2300 &&
		rep.PgAmcheckPassedPostPITR &&
		caseAPass &&
		caseBPass

	return rep, nil
}
