package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/Mahadi-rsio/air/internal/tenant"
)

// ManifestVersion is the Phase 13 immutable backup manifest schema version.
const ManifestVersion = 2

// Backup states. A manifest is published only in the terminal "succeeded" state;
// the other states are used by durable control-plane operation records.
const (
	BackupStatePending   = "pending"
	BackupStateRunning   = "running"
	BackupStateVerifying = "verifying"
	BackupStateSucceeded = "succeeded"
	BackupStateFailed    = "failed"
	BackupStateCancelled = "cancelled"
)

const (
	BackupTypeFull = "FULL"
	BackupTypeIncr = "INCR"
)

var (
	ErrInvalidBackupManifest = errors.New("backup: invalid backup manifest")
	ErrBackupNotRestorable   = errors.New("backup: backup is not in a restorable/verified state")
	ErrBackupGeneration      = errors.New("backup: tenant generation mismatch")
	ErrObjectMissing         = errors.New("backup: referenced object is missing")
	ErrObjectSizeMismatch    = errors.New("backup: referenced object size mismatch")
	ErrObjectChecksum        = errors.New("backup: referenced object checksum mismatch")
	ErrManifestChecksum      = errors.New("backup: manifest checksum mismatch")
	ErrManifestExists        = errors.New("backup: immutable manifest already exists")
	ErrWALDiscontinuity      = errors.New("backup: WAL segment continuity violated")
	ErrMissingWALSegment     = errors.New("backup: required WAL segment is missing")
	ErrCorruptWALSegment     = errors.New("backup: WAL segment failed integrity verification")
	ErrInvalidRecoveryTarget = errors.New("backup: invalid recovery target")
	ErrTargetOutOfRange      = errors.New("backup: recovery target is outside the proven recoverable range")
	ErrActiveRestore         = errors.New("backup: object is referenced by an active restore")
)

// ObjectRef identifies one immutable backup object and its integrity metadata.
type ObjectRef struct {
	Kind   string `json:"kind"` // base, wal, layer
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	ETag   string `json:"etag,omitempty"`
}

// WALRef identifies one archived WAL segment used for PITR.
type WALRef struct {
	SegmentName  string `json:"segment_name"`
	TimelineID   string `json:"timeline_id,omitempty"`
	R2Key        string `json:"r2_key"`
	Epoch        int64  `json:"epoch"`
	Size         int64  `json:"size"`
	CipherSHA256 string `json:"cipher_sha256"`
	SHA256       string `json:"sha256"`
	StartLSN     string `json:"start_lsn,omitempty"`
	EndLSN       string `json:"end_lsn,omitempty"`
}

// BackupManifest is the immutable, self-describing record for one recoverable backup.
type BackupManifest struct {
	ManifestVersion   int                     `json:"manifest_version"`
	BackupID          string                  `json:"backup_id"`
	TenantID          string                  `json:"tenant_id"`
	Generation        string                  `json:"generation"`
	TimelineID        string                  `json:"timeline_id"`
	ParentID          string                  `json:"parent_id,omitempty"`
	Type              string                  `json:"type"`
	PostgreSQLVersion string                  `json:"postgresql_version"`
	SystemIdentifier  string                  `json:"system_identifier,omitempty"`
	DatabaseName      string                  `json:"database_name,omitempty"`
	Epoch             int64                   `json:"epoch"`
	LayerIDs          []string                `json:"layer_ids,omitempty"`
	StartedAt         time.Time               `json:"started_at"`
	CompletedAt       time.Time               `json:"completed_at"`
	ConsistencyLSN    string                  `json:"consistency_lsn"`
	CheckpointLSN     string                  `json:"checkpoint_lsn,omitempty"`
	WALStartLSN       string                  `json:"wal_start_lsn"`
	WALEndLSN         string                  `json:"wal_end_lsn"`
	ConsistencyTime   time.Time               `json:"consistency_time"`
	Objects           []ObjectRef             `json:"objects"`
	WALSegments       []WALRef                `json:"wal_segments"`
	State             string                  `json:"state"`
	DEKBundle         crypto.WrappedDEKBundle `json:"dek_bundle"`
	Checksum          string                  `json:"checksum"`
}

// r2TenantPrefix mirrors pageserver.TenantR2Prefix without importing the storage package.
func r2TenantPrefix(tenantID, generation string) string {
	if generation == "" {
		return fmt.Sprintf("tenants/%s", tenantID)
	}
	return fmt.Sprintf("tenants/%s/%s", tenantID, generation)
}

// BackupPrefix returns the generation-isolated, backup-aware prefix for one backup.
func BackupPrefix(tenantID, generation, backupID string) string {
	return fmt.Sprintf("%s/backups/%s", r2TenantPrefix(tenantID, generation), backupID)
}

// BackupManifestKey returns the immutable manifest key for a backup.
func BackupManifestKey(tenantID, generation, backupID string) string {
	return BackupPrefix(tenantID, generation, backupID) + "/manifest.json"
}

// BackupBaseObjectKey returns the backup-aware base archive key for a backup.
func BackupBaseObjectKey(tenantID, generation, backupID string) string {
	return BackupPrefix(tenantID, generation, backupID) + "/base.tar.zst.enc"
}

// BackupCatalogPrefix returns the prefix used to list backup manifests for a generation.
func BackupCatalogPrefix(tenantID, generation string) string {
	return r2TenantPrefix(tenantID, generation) + "/backups/"
}

// Seal computes and stores the manifest checksum. It must be called after all fields are set.
func (m *BackupManifest) Seal() error {
	m.Checksum = ""
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	m.Checksum = hex.EncodeToString(sum[:])
	return nil
}

// VerifyChecksum verifies the stored manifest checksum.
func (m *BackupManifest) VerifyChecksum() error {
	if m == nil || m.Checksum == "" {
		return fmt.Errorf("%w: missing checksum", ErrManifestChecksum)
	}
	want := m.Checksum
	cp := *m
	cp.Checksum = ""
	raw, err := json.Marshal(&cp)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if !strings.EqualFold(want, hex.EncodeToString(sum[:])) {
		return fmt.Errorf("%w: stored=%s computed=%s", ErrManifestChecksum, want, hex.EncodeToString(sum[:]))
	}
	return nil
}

// Validate structurally validates a manifest. If complete is true the manifest must describe
// a published, recoverable backup with all required fields and integrity references.
func (m *BackupManifest) Validate(complete bool) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidBackupManifest)
	}
	if m.ManifestVersion != ManifestVersion {
		return fmt.Errorf("%w: unsupported manifest_version=%d (want %d)", ErrInvalidBackupManifest, m.ManifestVersion, ManifestVersion)
	}
	if err := tenant.ValidateID(m.TenantID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBackupManifest, err)
	}
	if m.Generation == "" {
		return fmt.Errorf("%w: missing tenant generation", ErrInvalidBackupManifest)
	}
	if err := tenant.ValidateID(m.Generation); err != nil {
		return fmt.Errorf("%w: invalid generation: %v", ErrInvalidBackupManifest, err)
	}
	if err := tenant.ValidateID(m.BackupID); err != nil {
		return fmt.Errorf("%w: invalid backup_id: %v", ErrInvalidBackupManifest, err)
	}
	if m.Type != BackupTypeFull && m.Type != BackupTypeIncr {
		return fmt.Errorf("%w: invalid type %q", ErrInvalidBackupManifest, m.Type)
	}
	if m.Type == BackupTypeIncr && m.ParentID == "" {
		return fmt.Errorf("%w: INCR backup missing parent_id", ErrInvalidBackupManifest)
	}
	if m.State != BackupStateSucceeded {
		return fmt.Errorf("%w: manifest state=%s", ErrBackupNotRestorable, m.State)
	}
	if !m.VerifyChecksumErr() {
		return fmt.Errorf("%w", ErrManifestChecksum)
	}
	if _, err := ParseLSN(m.ConsistencyLSN); err != nil {
		return fmt.Errorf("%w: consistency_lsn: %v", ErrInvalidBackupManifest, err)
	}
	if _, err := ParseLSN(m.WALStartLSN); err != nil {
		return fmt.Errorf("%w: wal_start_lsn: %v", ErrInvalidBackupManifest, err)
	}
	if _, err := ParseLSN(m.WALEndLSN); err != nil {
		return fmt.Errorf("%w: wal_end_lsn: %v", ErrInvalidBackupManifest, err)
	}
	start, _ := ParseLSN(m.WALStartLSN)
	end, _ := ParseLSN(m.WALEndLSN)
	if end < start {
		return fmt.Errorf("%w: wal_end_lsn precedes wal_start_lsn", ErrInvalidBackupManifest)
	}
	if complete {
		if m.PostgreSQLVersion == "" {
			return fmt.Errorf("%w: missing postgresql_version", ErrInvalidBackupManifest)
		}
		if m.TimelineID == "" {
			return fmt.Errorf("%w: missing timeline_id", ErrInvalidBackupManifest)
		}
		if m.StartedAt.IsZero() || m.CompletedAt.IsZero() || m.CompletedAt.Before(m.StartedAt) {
			return fmt.Errorf("%w: invalid start/completion timestamps", ErrInvalidBackupManifest)
		}
		if len(m.Objects) == 0 {
			return fmt.Errorf("%w: no backup objects", ErrInvalidBackupManifest)
		}
		seen := make(map[string]struct{})
		for _, o := range m.Objects {
			if o.Key == "" || o.Size < 0 {
				return fmt.Errorf("%w: malformed object ref", ErrInvalidBackupManifest)
			}
			if !strings.HasPrefix(o.Key, BackupPrefix(m.TenantID, m.Generation, m.BackupID)+"/") {
				return fmt.Errorf("%w: object %s escapes backup prefix", ErrInvalidBackupManifest, o.Key)
			}
			if _, err := ValidateSHA256(o.SHA256); err != nil {
				return fmt.Errorf("%w: object %s: %v", ErrInvalidBackupManifest, o.Key, err)
			}
			if _, dup := seen[o.Key]; dup {
				return fmt.Errorf("%w: duplicate object %s", ErrInvalidBackupManifest, o.Key)
			}
			seen[o.Key] = struct{}{}
		}
		if len(m.WALSegments) == 0 {
			return fmt.Errorf("%w: no WAL segments", ErrInvalidBackupManifest)
		}
		for _, w := range m.WALSegments {
			if !isValidWALFilename(w.SegmentName) {
				return fmt.Errorf("%w: invalid WAL segment %q", ErrInvalidBackupManifest, w.SegmentName)
			}
			if w.R2Key == "" || w.Size < 0 {
				return fmt.Errorf("%w: malformed WAL ref %q", ErrInvalidBackupManifest, w.SegmentName)
			}
			if _, err := ValidateSHA256(w.SHA256); err != nil {
				return fmt.Errorf("%w: WAL %s plain sha: %v", ErrInvalidBackupManifest, w.SegmentName, err)
			}
			if _, err := ValidateSHA256(w.CipherSHA256); err != nil {
				return fmt.Errorf("%w: WAL %s cipher sha: %v", ErrInvalidBackupManifest, w.SegmentName, err)
			}
		}
		if err := ValidateWALContinuity(m.WALSegments, m.WALStartLSN, m.WALEndLSN); err != nil {
			return err
		}
	}
	return nil
}

// VerifyChecksumErr reports checksum validity without returning an error (helper for Validate).
func (m *BackupManifest) VerifyChecksumErr() bool {
	return m.VerifyChecksum() == nil
}

// ValidateSHA256 validates and canonicalizes a lowercase SHA-256 hex digest.
func ValidateSHA256(h string) (string, error) {
	if len(h) != 64 {
		return "", fmt.Errorf("invalid sha256 length %d", len(h))
	}
	lower := strings.ToLower(h)
	for i := 0; i < len(lower); i++ {
		c := lower[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return "", fmt.Errorf("invalid sha256 character at %d", i)
		}
	}
	return lower, nil
}

// SortWALRefs sorts WAL refs by segment start LSN then name.
func SortWALRefs(refs []WALRef) {
	sort.Slice(refs, func(i, j int) bool {
		si, _ := WALSegmentStartLSN(refs[i].SegmentName)
		sj, _ := WALSegmentStartLSN(refs[j].SegmentName)
		if si != sj {
			return si < sj
		}
		return refs[i].SegmentName < refs[j].SegmentName
	})
}

// WALRefsFromArtifacts converts 24-hex WAL upload artifacts into manifest WAL refs.
// Auxiliary timeline history (.history) and backup label (.backup) files are skipped.
func WALRefsFromArtifacts(segs []WALSegmentArtifact) []WALRef {
	out := make([]WALRef, 0, len(segs))
	for _, s := range segs {
		if !IsWALSegmentName(s.SegmentName) {
			continue
		}
		start, err := WALSegmentStartLSN(s.SegmentName)
		if err != nil {
			continue
		}
		e := WALRef{
			SegmentName:  s.SegmentName,
			TimelineID:   s.TimelineID,
			R2Key:        s.R2Key,
			Epoch:        s.Epoch,
			Size:         int64(s.CipherBytes),
			CipherSHA256: s.CipherSHA256,
			SHA256:       s.PlainSHA256,
			StartLSN:     FormatLSN(start),
			EndLSN:       FormatLSN(start + WALSegmentSize),
		}
		if e.TimelineID == "" {
			e.TimelineID = timelineIDFromSegment(s.SegmentName)
		}
		out = append(out, e)
	}
	SortWALRefs(out)
	return out
}

func timelineIDFromSegment(name string) string {
	if len(name) >= 8 {
		return name[:8]
	}
	return ""
}
