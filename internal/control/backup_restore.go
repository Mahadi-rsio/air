package control

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/backup"
	"github.com/Mahadi-rsio/air/internal/fencing"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

// BackupResponse describes a completed backup or WAL archival operation.
type BackupResponse struct {
	TenantID            string `json:"tenant_id"`
	Generation          string `json:"generation,omitempty"`
	BackupID            string `json:"backup_id"`
	ParentID            string `json:"parent_id,omitempty"`
	Type                string `json:"type"`
	R2Key               string `json:"r2_key"`
	ManifestKey         string `json:"manifest_key,omitempty"`
	Epoch               int64  `json:"epoch"`
	CipherBytes         int    `json:"cipher_bytes"`
	WALSegmentsArchived int    `json:"wal_segments_archived"`
	WALStartLSN         string `json:"wal_start_lsn,omitempty"`
	WALEndLSN           string `json:"wal_end_lsn,omitempty"`
	ConsistencyLSN      string `json:"consistency_lsn,omitempty"`
}

// RestoreOptions controls a PITR validation restore or restore into a new isolated tenant.
type RestoreOptions struct {
	BackupID    string `json:"backup_id,omitempty"`
	TargetLSN   string `json:"target_lsn,omitempty"`
	TargetTime  string `json:"target_time,omitempty"`
	NewTenantID string `json:"new_tenant_id,omitempty"`
	VerifyQuery string `json:"verify_query,omitempty"`
}

// RestoreResponse describes the outcome of a database restore operation.
type RestoreResponse struct {
	TenantID            string `json:"tenant_id"`
	RestoredTenantID    string `json:"restored_tenant_id,omitempty"`
	RestoredGeneration  string `json:"restored_generation,omitempty"`
	DirectPort          int    `json:"direct_port,omitempty"`
	BackupID            string `json:"backup_id,omitempty"`
	TargetLSN           string `json:"target_lsn,omitempty"`
	TargetTime          string `json:"target_time,omitempty"`
	RestoredBackups     int    `json:"restored_backups"`
	WALSegmentsReplayed int    `json:"wal_segments_replayed"`
	StagingHeadroomOK   bool   `json:"staging_headroom_ok"`
	AmcheckOK           bool   `json:"amcheck_ok"`
	VerifyQueryOutput   string `json:"verify_query_output,omitempty"`
}

// TriggerBackup executes an encrypted FULL, INCR, or WAL backup for an active tenant.
func (cp *ControlPlane) TriggerBackup(ctx context.Context, accountID, tenantID, backupType string) (*BackupResponse, error) {
	if accountID != "" {
		if err := tenant.ValidateAccountID(accountID); err != nil {
			return nil, err
		}
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	var owner, state string
	err := cp.db.QueryRow(ctx, `SELECT account_id, state FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&owner, &state)
	if errors.Is(err, pgx.ErrNoRows) || state == "PURGED" {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if accountID != "" && owner != accountID {
		return nil, ErrForbidden
	}
	if state != "ACTIVE" {
		return nil, fmt.Errorf("tenant %s is not ACTIVE (state=%s)", tenantID, state)
	}
	if cp.agent == nil {
		return nil, errors.New("cell agent is not configured")
	}
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return nil, ErrBackupNotConfigured
	}

	bkpType := strings.ToUpper(strings.TrimSpace(backupType))
	if bkpType == "" {
		bkpType = "FULL"
	}
	if bkpType != "FULL" && bkpType != "INCR" && bkpType != "WAL" {
		return nil, fmt.Errorf("unsupported backup type %q (must be FULL, INCR, or WAL)", backupType)
	}
	return cp.backupTenantToR2(ctx, tenantID, bkpType)
}

func readTenantFenceManifestWithFallback(ctx context.Context, s3c *s3.Client, bucket, tenantID, generation string) (*fencing.TenantFenceManifest, string, error) {
	if generation != "" {
		m, etag, err := fencing.ReadManifestWithETagGen(ctx, s3c, bucket, tenantID, generation)
		if err == nil {
			return m, etag, nil
		}
		if !fencing.IsNotFoundErr(err) {
			return nil, "", err
		}
	}
	return fencing.ReadManifestWithETag(ctx, s3c, bucket, tenantID)
}

func (cp *ControlPlane) backupTenantToR2(ctx context.Context, tenantID, bkpType string) (*BackupResponse, error) {
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return nil, ErrBackupNotConfigured
	}
	gen := cp.getTenantGeneration(ctx, tenantID)
	startedAt := time.Now().UTC()

	var art *fencing.BackupArtifact
	var bkpID string
	if bkpType != "WAL" {
		var parentID, parentManifestPath string
		if bkpType == "INCR" {
			manifest, _, mErr := readTenantFenceManifestWithFallback(ctx, s3c, bucket, tenantID, gen)
			if mErr == nil && manifest != nil && len(manifest.Backups) > 0 {
				last := manifest.Backups[len(manifest.Backups)-1]
				parentID = last.BackupID
				if bkpBase, sErr := tenant.SafeSubpath(cp.agent.VolDir(), "backups", tenantID); sErr == nil {
					candidate := filepath.Join(bkpBase, last.BackupID, "backup_manifest")
					if _, stErr := os.Stat(candidate); stErr == nil {
						parentManifestPath = candidate
					}
				}
			}
			if parentID == "" || parentManifestPath == "" {
				bkpType = "FULL"
				parentID = ""
				parentManifestPath = ""
			}
		}

		bkpID = fmt.Sprintf("bkp_%d", time.Now().UnixNano())
		cp.recordBackupOpStart(ctx, bkpID, tenantID, gen, parentID, bkpType, startedAt)

		var err error
		art, _, err = backup.RunEncryptedBackupStepGen(ctx, cp.agent, s3c, bucket, tenantID, gen, bkpID, parentID, bkpType, parentManifestPath)
		if err != nil {
			cp.recordBackupOpFailed(ctx, bkpID, err)
			return nil, err
		}
	}

	walCount, err := cp.drainWALSpoolAndUpdateManifestGen(ctx, tenantID, gen, 0, false)
	if err != nil {
		if bkpID != "" {
			cp.recordBackupOpFailed(ctx, bkpID, err)
		}
		return nil, err
	}

	resp := &BackupResponse{
		TenantID:            tenantID,
		Generation:          gen,
		Type:                bkpType,
		WALSegmentsArchived: walCount,
	}
	if art != nil {
		resp.BackupID = art.BackupID
		resp.ParentID = art.ParentID
		resp.Type = art.Type
		resp.R2Key = art.R2Key
		resp.Epoch = art.Epoch
		resp.CipherBytes = art.CipherBytes

		// Collect all verified WAL segments for this tenant generation and publish the immutable Phase 13 BackupManifest.
		walSegs := cp.collectTenantWALArtifacts(ctx, s3c, bucket, tenantID, gen, art.Epoch)
		if len(walSegs) > 0 {
			m, pubErr := backup.PublishCompletedBackupManifest(ctx, s3c, bucket, tenantID, gen, startedAt, art, walSegs, nil)
			if pubErr != nil {
				cp.recordBackupOpFailed(ctx, bkpID, pubErr)
				return nil, fmt.Errorf("publish immutable backup manifest: %w", pubErr)
			}
			resp.ManifestKey = backup.BackupManifestKey(tenantID, m.Generation, m.BackupID)
			resp.WALStartLSN = m.WALStartLSN
			resp.WALEndLSN = m.WALEndLSN
			resp.ConsistencyLSN = m.ConsistencyLSN
			cp.recordBackupOpSucceeded(ctx, m, art.CipherBytes, len(m.WALSegments))
		} else {
			cp.recordBackupOpFailed(ctx, bkpID, backup.ErrMissingWALSegment)
			return nil, fmt.Errorf("%w: backup %s completed without archived WAL segments", backup.ErrMissingWALSegment, bkpID)
		}
	}
	return resp, nil
}

func (cp *ControlPlane) collectTenantWALArtifacts(ctx context.Context, s3c *s3.Client, bucket, tenantID, generation string, defaultEpoch int64) []backup.WALSegmentArtifact {
	var out []backup.WALSegmentArtifact
	seen := make(map[string]bool)

	// 1. Read local wal_upload_manifest.json if available (contains full PlainSHA256 & CipherSHA256).
	if cp.agent != nil {
		if walSpoolDir, err := tenant.SafeSubpath(cp.agent.VolDir(), "spool/wal", tenantID); err == nil {
			if raw, rErr := os.ReadFile(filepath.Join(walSpoolDir, "wal_upload_manifest.json")); rErr == nil {
				var wm backup.WALUploadManifest
				if json.Unmarshal(raw, &wm) == nil && wm.Segments != nil {
					for _, seg := range wm.Segments {
						if seg == nil || seg.R2Key == "" {
							continue
						}
						// Match generation prefix if tenant is generation-scoped.
						if generation != "" && !strings.Contains(seg.R2Key, "/"+generation+"/") {
							continue
						}
						out = append(out, *seg)
						seen[seg.SegmentName] = true
					}
				}
			}
		}
	}

	// 2. Supplement with any WAL segment keys recorded in TenantFenceManifest.
	if fm, _, err := readTenantFenceManifestWithFallback(ctx, s3c, bucket, tenantID, generation); err == nil && fm != nil {
		st := backup.NewStore(s3c, bucket)
		for _, r2Key := range fm.WALSegments {
			segName := strings.TrimSuffix(filepath.Base(r2Key), ".zst.enc")
			if seen[segName] {
				continue
			}
			epoch := defaultEpoch
			if epoch <= 0 {
				epoch = fm.FencingEpoch
			}
			var size int
			var csha, psha string
			if info, hErr := st.Head(ctx, r2Key); hErr == nil && info != nil {
				size = int(info.Size)
				csha = info.SHA256
				psha = info.PlainSHA256
			}
			if psha == "" {
				psha = csha
			}
			out = append(out, backup.WALSegmentArtifact{
				SegmentName:  segName,
				R2Key:        r2Key,
				Epoch:        epoch,
				CipherBytes:  size,
				CipherSHA256: csha,
				PlainSHA256:  psha,
			})
			seen[segName] = true
		}
	}
	return out
}

func (cp *ControlPlane) recordBackupOpStart(ctx context.Context, bkpID, tenantID, generation, parentID, bkpType string, startedAt time.Time) {
	if cp.db == nil || bkpID == "" {
		return
	}
	_, _ = cp.db.Exec(ctx,
		`INSERT INTO backup_ops (backup_id, tenant_id, generation, parent_id, backup_type, state, started_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		 ON CONFLICT (backup_id) DO NOTHING`,
		bkpID, tenantID, generation, parentID, bkpType, backup.BackupStateRunning, startedAt,
	)
}

func (cp *ControlPlane) recordBackupOpFailed(ctx context.Context, bkpID string, cause error) {
	if cp.db == nil || bkpID == "" {
		return
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	_, _ = cp.db.Exec(ctx,
		`UPDATE backup_ops SET state = $2, error_details = $3, completed_at = now(), updated_at = now() WHERE backup_id = $1`,
		bkpID, backup.BackupStateFailed, msg,
	)
}

func (cp *ControlPlane) recordBackupOpSucceeded(ctx context.Context, m *backup.BackupManifest, cipherBytes, walCount int) {
	if cp.db == nil || m == nil {
		return
	}
	baseKey := ""
	if b, err := m.BaseObject(); err == nil {
		baseKey = b.Key
	}
	manifestKey := backup.BackupManifestKey(m.TenantID, m.Generation, m.BackupID)
	_, _ = cp.db.Exec(ctx,
		`UPDATE backup_ops SET
			state = $2,
			manifest_key = $3,
			base_object_key = $4,
			wal_start_lsn = $5,
			wal_end_lsn = $6,
			consistency_lsn = $7,
			cipher_bytes = $8,
			wal_segments_count = $9,
			verified = TRUE,
			error_details = '',
			completed_at = $10,
			updated_at = now()
		 WHERE backup_id = $1`,
		m.BackupID, backup.BackupStateSucceeded, manifestKey, baseKey,
		m.WALStartLSN, m.WALEndLSN, m.ConsistencyLSN,
		int64(cipherBytes), walCount, m.CompletedAt,
	)
}

func (cp *ControlPlane) drainWALSpoolAndUpdateManifest(ctx context.Context, tenantID string, epoch int64, stopped bool) (int, error) {
	return cp.drainWALSpoolAndUpdateManifestGen(ctx, tenantID, cp.getTenantGeneration(ctx, tenantID), epoch, stopped)
}

func (cp *ControlPlane) drainWALSpoolAndUpdateManifestGen(ctx context.Context, tenantID, generation string, epoch int64, stopped bool) (int, error) {
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return 0, nil
	}
	var walSegs []backup.WALSegmentArtifact
	var err error
	if stopped {
		walSegs, err = backup.DrainStoppedWALSpoolToR2Gen(ctx, cp.agent, s3c, bucket, tenantID, generation, epoch)
	} else {
		walSegs, err = backup.DrainAndEncryptWALSpoolToR2Gen(ctx, cp.agent, s3c, bucket, tenantID, generation)
	}
	if err != nil {
		return 0, err
	}
	if len(walSegs) > 0 {
		manifest, etag, mErr := readTenantFenceManifestWithFallback(ctx, s3c, bucket, tenantID, generation)
		if mErr == nil && manifest != nil {
			seen := make(map[string]bool, len(manifest.WALSegments))
			for _, k := range manifest.WALSegments {
				seen[k] = true
			}
			for _, w := range walSegs {
				if !seen[w.R2Key] {
					manifest.WALSegments = append(manifest.WALSegments, w.R2Key)
					seen[w.R2Key] = true
				}
			}
			if _, casErr := fencing.PutManifestCAS(ctx, s3c, bucket, manifest, etag); casErr != nil {
				return 0, casErr
			}
		}
	}
	return len(walSegs), nil
}

// ListBackups returns all published immutable BackupManifests for a tenant's active generation.
func (cp *ControlPlane) ListBackups(ctx context.Context, accountID, tenantID string) ([]*backup.BackupManifest, error) {
	if accountID != "" {
		if err := tenant.ValidateAccountID(accountID); err != nil {
			return nil, err
		}
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	var owner, state, gen string
	err := cp.db.QueryRow(ctx, `SELECT account_id, state, COALESCE(generation, '') FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&owner, &state, &gen)
	if errors.Is(err, pgx.ErrNoRows) || state == "PURGED" {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if accountID != "" && owner != accountID {
		return nil, ErrForbidden
	}
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return nil, ErrBackupNotConfigured
	}
	effGen := gen
	if effGen == "" {
		effGen = "legacy"
	}
	st := backup.NewStore(s3c, bucket)
	return st.ListManifests(ctx, tenantID, effGen)
}

// VerifyDatabaseBackup runs Phase 13 structural, checksum, WAL continuity, and optional deep restore verification on a backup.
func (cp *ControlPlane) VerifyDatabaseBackup(ctx context.Context, accountID, tenantID, backupID string, streamHash, deepRestore bool) (*backup.VerificationReport, error) {
	if accountID != "" {
		if err := tenant.ValidateAccountID(accountID); err != nil {
			return nil, err
		}
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	if err := tenant.ValidateID(backupID); err != nil {
		return nil, err
	}
	var owner, state, gen string
	err := cp.db.QueryRow(ctx, `SELECT account_id, state, COALESCE(generation, '') FROM clusters WHERE tenant_id = $1`, tenantID).Scan(&owner, &state, &gen)
	if errors.Is(err, pgx.ErrNoRows) || state == "PURGED" {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if accountID != "" && owner != accountID {
		return nil, ErrForbidden
	}
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return nil, ErrBackupNotConfigured
	}
	effGen := gen
	if effGen == "" {
		effGen = "legacy"
	}
	st := backup.NewStore(s3c, bucket)
	m, err := st.LoadManifest(ctx, tenantID, effGen, backupID)
	if err != nil {
		if errors.Is(err, backup.ErrObjectMissing) {
			return nil, fmt.Errorf("%w: backup %s", ErrNotFound, backupID)
		}
		return nil, err
	}
	opts := backup.VerifyOptions{
		ExpectedTenantID:   tenantID,
		ExpectedGeneration: effGen,
		StreamHash:         streamHash,
		VerifyLayerObjects: len(m.LayerIDs) > 0,
		DeepTestRestore:    deepRestore,
	}
	if deepRestore {
		opts.DeepTestRestoreFunc = func(rCtx context.Context, bm *backup.BackupManifest) error {
			_, rErr := cp.RestoreDatabaseWithOptions(rCtx, accountID, tenantID, RestoreOptions{
				BackupID:    bm.BackupID,
				VerifyQuery: "SELECT 1;",
			})
			return rErr
		}
	}
	return backup.VerifyBackup(ctx, st, m, opts)
}

// RestoreDatabase executes a PITR validation restore up to targetLSN (or latest WAL).
func (cp *ControlPlane) RestoreDatabase(ctx context.Context, accountID, tenantID, targetLSN, verifyQuery string) (*RestoreResponse, error) {
	return cp.RestoreDatabaseWithOptions(ctx, accountID, tenantID, RestoreOptions{
		TargetLSN:   targetLSN,
		VerifyQuery: verifyQuery,
	})
}

// RestoreDatabaseWithOptions performs a verified PITR restore from R2.
// When opts.NewTenantID is empty, it runs an isolated staging validation restore and cleans up the staging directory.
// When opts.NewTenantID is provided, it restores into a newly provisioned, generation-isolated tenant and activates it
// only after PostgreSQL promotion, the built-in connectivity check, and pg_amcheck succeed.
func (cp *ControlPlane) RestoreDatabaseWithOptions(ctx context.Context, accountID, tenantID string, opts RestoreOptions) (*RestoreResponse, error) {
	if err := backup.ValidateRestoreVerificationQuery(opts.VerifyQuery); err != nil {
		return nil, err
	}
	if accountID != "" {
		if err := tenant.ValidateAccountID(accountID); err != nil {
			return nil, err
		}
	}
	if err := tenant.ValidateID(tenantID); err != nil {
		return nil, err
	}
	newTenantID := strings.TrimSpace(opts.NewTenantID)
	if newTenantID != "" {
		if err := tenant.ValidateID(newTenantID); err != nil {
			return nil, fmt.Errorf("invalid new_tenant_id: %w", err)
		}
		if newTenantID == tenantID {
			return nil, fmt.Errorf("%w: new_tenant_id %q must differ from source tenant_id", backup.ErrInvalidRecoveryTarget, newTenantID)
		}
	}

	targetLSN := strings.TrimSpace(opts.TargetLSN)
	targetTime := strings.TrimSpace(opts.TargetTime)
	latest := targetLSN == "" && targetTime == ""
	target, err := backup.ParseRecoveryTarget(latest, targetLSN, targetTime)
	if err != nil {
		return nil, err
	}

	var owner, state, srcGen, srcWrappedDEK string
	err = cp.db.QueryRow(ctx,
		`SELECT account_id, state, COALESCE(generation, ''), COALESCE(wrapped_dek, '') FROM clusters WHERE tenant_id = $1`,
		tenantID,
	).Scan(&owner, &state, &srcGen, &srcWrappedDEK)
	if errors.Is(err, pgx.ErrNoRows) || state == "PURGED" {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if accountID != "" && owner != accountID {
		return nil, ErrForbidden
	}
	if newTenantID != "" {
		var existingState string
		qErr := cp.db.QueryRow(ctx, `SELECT state FROM clusters WHERE tenant_id = $1`, newTenantID).Scan(&existingState)
		if qErr == nil && existingState != "PURGED" {
			return nil, fmt.Errorf("target tenant %s already exists in state %s", newTenantID, existingState)
		}
	}
	if cp.agent == nil {
		return nil, errors.New("cell agent is not configured")
	}
	s3c, bucket := cp.backupStore()
	if s3c == nil || bucket == "" {
		return nil, ErrBackupNotConfigured
	}
	if cp.kms != nil && srcWrappedDEK != "" {
		_ = cp.kms.LoadWrappedDEK(tenantID, srcWrappedDEK)
	}

	restoreID := fmt.Sprintf("restore_%d", time.Now().UnixNano())
	cp.recordRestoreOpStart(ctx, restoreID, tenantID, srcGen, newTenantID, opts.BackupID, target.String())

	manifest, _, err := readTenantFenceManifestWithFallback(ctx, s3c, bucket, tenantID, srcGen)
	if err != nil {
		cp.recordRestoreOpFailed(ctx, restoreID, err)
		if fencing.IsNotFoundErr(err) {
			return nil, fmt.Errorf("%w: no backup manifest for tenant %s", ErrNotFound, tenantID)
		}
		return nil, err
	}
	if len(manifest.Backups) == 0 {
		err := fmt.Errorf("%w: manifest has no backups for tenant %s", ErrNotFound, tenantID)
		cp.recordRestoreOpFailed(ctx, restoreID, err)
		return nil, err
	}

	chain, selectedBkpID, err := selectBackupChain(manifest.Backups, strings.TrimSpace(opts.BackupID))
	if err != nil {
		cp.recordRestoreOpFailed(ctx, restoreID, err)
		return nil, err
	}

	effGen := srcGen
	if effGen == "" {
		effGen = "legacy"
	}
	st := backup.NewStore(s3c, bucket)
	var bm *backup.BackupManifest
	if loaded, lErr := st.LoadManifest(ctx, tenantID, effGen, selectedBkpID); lErr == nil {
		bm = loaded
		// Enforce generation match against source tenant.
		if bm.Generation != effGen {
			gErr := fmt.Errorf("%w: manifest generation=%s expected=%s", backup.ErrBackupGeneration, bm.Generation, effGen)
			cp.recordRestoreOpFailed(ctx, restoreID, gErr)
			return nil, gErr
		}
	}

	walSegs := cp.buildRestoreWALArtifacts(ctx, s3c, bucket, tenantID, srcGen, bm, manifest, chain[0].Epoch)
	if len(walSegs) == 0 {
		wErr := fmt.Errorf("%w: no archived WAL segments found for tenant %s", backup.ErrMissingWALSegment, tenantID)
		cp.recordRestoreOpFailed(ctx, restoreID, wErr)
		return nil, wErr
	}
	walRefs := backup.WALRefsFromArtifacts(walSegs)
	if len(walRefs) == 0 {
		wErr := fmt.Errorf("%w: no 24-hex WAL segments for tenant %s", backup.ErrMissingWALSegment, tenantID)
		cp.recordRestoreOpFailed(ctx, restoreID, wErr)
		return nil, wErr
	}

	// Construct the effective recovery manifest spanning the base backup through all contiguous archived WAL.
	pitrManifest := &backup.BackupManifest{
		ManifestVersion: backup.ManifestVersion,
		BackupID:        selectedBkpID,
		TenantID:        tenantID,
		Generation:      effGen,
		TimelineID:      walRefs[0].TimelineID,
		Type:            chain[len(chain)-1].Type,
		State:           backup.BackupStateSucceeded,
		WALStartLSN:     walRefs[0].StartLSN,
		WALEndLSN:       walRefs[len(walRefs)-1].EndLSN,
		ConsistencyLSN:  walRefs[0].StartLSN,
		TimeIndex:       backup.AggregateTimeIndex(walRefs),
		WALSegments:     walRefs,
	}
	if bm != nil {
		pitrManifest.ConsistencyTime = bm.ConsistencyTime
		if target.Kind != backup.RecoveryTargetLSN {
			pitrManifest.ConsistencyLSN = bm.ConsistencyLSN
		}
	}
	if vErr := target.ValidateAgainstManifest(pitrManifest); vErr != nil {
		cp.recordRestoreOpFailed(ctx, restoreID, vErr)
		return nil, vErr
	}

	stageRes, err := backup.StagePITRRestore(ctx, cp.agent, s3c, bucket, pitrManifest, chain, walSegs, target, backup.StagePITROptions{
		DBName:             tenantID,
		KeepStaging:        newTenantID != "",
		RenameDatabaseFrom: tenantID,
		RenameDatabaseTo:   newTenantID,
	})
	if err != nil {
		cp.recordRestoreOpFailed(ctx, restoreID, err)
		return nil, err
	}
	if stageRes.Cleanup != nil {
		defer stageRes.Cleanup()
	}

	resp := &RestoreResponse{
		TenantID:            tenantID,
		BackupID:            selectedBkpID,
		TargetLSN:           target.LSN,
		TargetTime:          target.Time,
		RestoredBackups:     len(chain),
		WALSegmentsReplayed: len(walSegs),
		StagingHeadroomOK:   stageRes.StagingHeadroomOK,
		AmcheckOK:           stageRes.AmcheckOK,
		VerifyQueryOutput:   stageRes.QueryOutput,
	}

	if newTenantID == "" {
		cp.recordRestoreOpSucceeded(ctx, restoreID, "", "")
		return resp, nil
	}

	// Activate restored cluster as an isolated new tenant generation.
	newGen, newPort, err := cp.activateRestoredTenant(ctx, owner, newTenantID, stageRes.PGDataDir)
	if err != nil {
		cp.recordRestoreOpFailed(ctx, restoreID, err)
		return nil, err
	}
	cp.recordRestoreOpSucceeded(ctx, restoreID, newTenantID, newGen)
	resp.RestoredTenantID = newTenantID
	resp.RestoredGeneration = newGen
	resp.DirectPort = newPort
	return resp, nil
}

func (cp *ControlPlane) activateRestoredTenant(ctx context.Context, accountID, newTenantID, stagedPGDataDir string) (string, int, error) {
	unlock := cp.lockTenantLifecycle(newTenantID)
	defer unlock()

	newGen := newGenerationID()
	appPW := randomPassword(24)
	wrappedDEK, err := cp.kms.EnsureTenantDEK(newTenantID)
	if err != nil {
		return "", 0, err
	}
	encBytes, err := cp.kms.EncryptObject(newTenantID, 1, "app_password", []byte(appPW))
	if err != nil {
		return "", 0, err
	}
	encB64 := base64.StdEncoding.EncodeToString(encBytes)

	if cp.faultHook != nil {
		if fErr := cp.faultHook("restore_before_activate"); fErr != nil {
			_ = cp.agent.StopAndPurgeTenant(newTenantID)
			return "", 0, fErr
		}
	}

	activeBase, err := tenant.SafeSubpath(cp.agent.VolDir(), "tenants/active", newTenantID)
	if err != nil {
		return "", 0, err
	}
	_ = os.RemoveAll(activeBase)
	if err := os.MkdirAll(activeBase, 0o755); err != nil {
		return "", 0, err
	}
	targetPGData := filepath.Join(activeBase, "pgdata")
	if err := os.Rename(stagedPGDataDir, targetPGData); err != nil {
		_ = os.RemoveAll(activeBase)
		return "", 0, fmt.Errorf("install restored pgdata for %s: %w", newTenantID, err)
	}
	if err := cp.agent.WriteTenantConfigs(newTenantID); err != nil {
		_ = cp.agent.StopAndPurgeTenant(newTenantID)
		return "", 0, fmt.Errorf("write restored tenant configs for %s: %w", newTenantID, err)
	}

	if ps := cp.getPageserver(); ps != nil {
		ps.SetTenantGeneration(ctx, newTenantID, newGen)
		initLSN := cp.readPGControlLSN(targetPGData)
		if initLSN == 0 {
			initLSN = 1
		}
		if _, bErr := ps.BootstrapTenantFromPGData(ctx, newTenantID, 1, targetPGData, initLSN); bErr != nil {
			_ = cp.agent.StopAndPurgeTenant(newTenantID)
			return "", 0, fmt.Errorf("bootstrap pageserver for restored tenant %s: %w", newTenantID, bErr)
		}
	}

	rt, err := cp.agent.StartOrAdoptTenant(newTenantID, 1)
	if err != nil {
		_ = cp.agent.StopAndPurgeTenant(newTenantID)
		return "", 0, fmt.Errorf("start restored tenant %s: %w", newTenantID, err)
	}

	// Ensure tenant role (<newTenantID>) password and database ownership are configured on the promoted instance.
	escapedRole := strings.ReplaceAll(newTenantID, "'", "''")
	escapedPW := strings.ReplaceAll(appPW, "'", "''")
	quotedRole := pgx.Identifier{newTenantID}.Sanitize()
	_, _ = cp.agent.RunPostgresCmd(
		filepath.Join(cp.agent.PGBin(), "psql"),
		"-h", rt.SockDir, "-p", strconv.Itoa(rt.Port), "-U", "postgres", "-d", "postgres",
		"-c", fmt.Sprintf("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '%s') THEN CREATE ROLE %s LOGIN PASSWORD '%s'; ELSE ALTER ROLE %s WITH LOGIN PASSWORD '%s'; END IF; END $$;", escapedRole, quotedRole, escapedPW, quotedRole, escapedPW),
	)

	if cp.faultHook != nil {
		if fErr := cp.faultHook("restore_after_start_before_commit"); fErr != nil {
			_ = cp.agent.StopAndPurgeTenant(newTenantID)
			return "", 0, fErr
		}
	}

	if _, err := cp.db.Exec(ctx, `
		INSERT INTO clusters (tenant_id, generation, lifecycle_seq, account_id, state, direct_port, system_identifier, fencing_epoch, app_password_enc, wrapped_dek)
		VALUES ($1, $2, 1, $3, 'ACTIVE', $4, '', 1, $5, $6)
		ON CONFLICT (tenant_id) DO UPDATE SET
			generation = EXCLUDED.generation,
			lifecycle_seq = clusters.lifecycle_seq + 1,
			account_id = EXCLUDED.account_id,
			state = 'ACTIVE',
			direct_port = EXCLUDED.direct_port,
			fencing_epoch = 1,
			app_password_enc = EXCLUDED.app_password_enc,
			wrapped_dek = EXCLUDED.wrapped_dek,
			updated_at = now()
	`, newTenantID, newGen, accountID, rt.Port, encB64, wrappedDEK); err != nil {
		_ = cp.agent.StopAndPurgeTenant(newTenantID)
		return "", 0, err
	}

	cp.emitTenantEvent(ctx, newTenantID, tenantEventUpsert, "ACTIVE", rt.Port, 1)
	if err := cp.ensurePageserverBootstrapAndWAL(ctx, newTenantID, rt); err != nil {
		log.Printf("[pageserver] walreceiver for restored tenant %s failed: %v", newTenantID, err)
	}
	cp.TouchTenantActivity(newTenantID)
	return newGen, rt.Port, nil
}

func selectBackupChain(backups []fencing.BackupArtifact, targetBackupID string) ([]*fencing.BackupArtifact, string, error) {
	if len(backups) == 0 {
		return nil, "", ErrNotFound
	}
	byID := make(map[string]*fencing.BackupArtifact, len(backups))
	for i := range backups {
		b := backups[i]
		byID[b.BackupID] = &b
	}
	if targetBackupID != "" {
		curr, ok := byID[targetBackupID]
		if !ok {
			return nil, "", fmt.Errorf("%w: backup %s not found", ErrNotFound, targetBackupID)
		}
		var rev []*fencing.BackupArtifact
		for curr != nil {
			rev = append(rev, curr)
			if curr.Type == "FULL" || curr.ParentID == "" {
				break
			}
			parent, pOK := byID[curr.ParentID]
			if !pOK {
				return nil, "", fmt.Errorf("%w: missing parent backup %s for %s", backup.ErrInvalidBackupManifest, curr.ParentID, curr.BackupID)
			}
			curr = parent
		}
		if len(rev) == 0 || rev[len(rev)-1].Type != "FULL" {
			return nil, "", fmt.Errorf("%w: chain for %s does not root at a FULL backup", backup.ErrInvalidBackupManifest, targetBackupID)
		}
		chain := make([]*fencing.BackupArtifact, len(rev))
		for i := range rev {
			chain[i] = rev[len(rev)-1-i]
		}
		return chain, targetBackupID, nil
	}

	// Default: select the latest FULL -> INCR chain.
	lastFullIdx := -1
	for i := len(backups) - 1; i >= 0; i-- {
		if backups[i].Type == "FULL" {
			lastFullIdx = i
			break
		}
	}
	if lastFullIdx < 0 {
		return nil, "", fmt.Errorf("no FULL backup found in manifest")
	}
	var chain []*fencing.BackupArtifact
	first := backups[lastFullIdx]
	chain = append(chain, &first)
	for i := lastFullIdx + 1; i < len(backups); i++ {
		cand := backups[i]
		if cand.Type == "INCR" && cand.ParentID == chain[len(chain)-1].BackupID {
			c := cand
			chain = append(chain, &c)
		}
	}
	return chain, chain[len(chain)-1].BackupID, nil
}

func (cp *ControlPlane) buildRestoreWALArtifacts(
	ctx context.Context,
	s3c *s3.Client,
	bucket, tenantID, generation string,
	bm *backup.BackupManifest,
	fm *fencing.TenantFenceManifest,
	defaultEpoch int64,
) []backup.WALSegmentArtifact {
	var out []backup.WALSegmentArtifact
	seen := make(map[string]bool)
	if bm != nil && len(bm.WALSegments) > 0 {
		for _, w := range bm.WALSegments {
			epoch := w.Epoch
			if epoch <= 0 {
				epoch = defaultEpoch
			}
			out = append(out, backup.WALSegmentArtifact{
				SegmentName:  w.SegmentName,
				TimelineID:   w.TimelineID,
				R2Key:        w.R2Key,
				Epoch:        epoch,
				CipherBytes:  int(w.Size),
				CipherSHA256: w.CipherSHA256,
				PlainSHA256:  w.SHA256,
			})
			seen[w.SegmentName] = true
		}
	}
	for _, seg := range cp.collectTenantWALArtifacts(ctx, s3c, bucket, tenantID, generation, defaultEpoch) {
		if !seen[seg.SegmentName] {
			out = append(out, seg)
			seen[seg.SegmentName] = true
		}
	}
	if fm != nil {
		for _, r2Key := range fm.WALSegments {
			segName := strings.TrimSuffix(filepath.Base(r2Key), ".zst.enc")
			if seen[segName] {
				continue
			}
			out = append(out, backup.WALSegmentArtifact{
				SegmentName: segName,
				R2Key:       r2Key,
				Epoch:       defaultEpoch,
			})
			seen[segName] = true
		}
	}
	return out
}

func (cp *ControlPlane) recordRestoreOpStart(ctx context.Context, restoreID, srcTenantID, srcGen, targetTenantID, backupID, recoveryTarget string) {
	if cp.db == nil || restoreID == "" {
		return
	}
	_, _ = cp.db.Exec(ctx,
		`INSERT INTO restore_ops (restore_id, source_tenant_id, source_generation, target_tenant_id, backup_id, recovery_target, state, started_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, 'running', now(), now())`,
		restoreID, srcTenantID, srcGen, targetTenantID, backupID, recoveryTarget,
	)
}

func (cp *ControlPlane) recordRestoreOpFailed(ctx context.Context, restoreID string, cause error) {
	if cp.db == nil || restoreID == "" {
		return
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	_, _ = cp.db.Exec(ctx,
		`UPDATE restore_ops SET state = 'failed', error_details = $2, completed_at = now(), updated_at = now() WHERE restore_id = $1`,
		restoreID, msg,
	)
}

func (cp *ControlPlane) recordRestoreOpSucceeded(ctx context.Context, restoreID, targetTenantID, targetGen string) {
	if cp.db == nil || restoreID == "" {
		return
	}
	_, _ = cp.db.Exec(ctx,
		`UPDATE restore_ops SET state = 'succeeded', target_tenant_id = $2, target_generation = $3, error_details = '', completed_at = now(), updated_at = now() WHERE restore_id = $1`,
		restoreID, targetTenantID, targetGen,
	)
}
