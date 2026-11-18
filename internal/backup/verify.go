package backup

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// VerificationReport is the durable record returned by backup verification.
type VerificationReport struct {
	BackupID           string    `json:"backup_id"`
	TenantID           string    `json:"tenant_id"`
	Generation         string    `json:"generation"`
	Valid              bool      `json:"valid"`
	ManifestOK         bool      `json:"manifest_ok"`
	ObjectsOK          bool      `json:"objects_ok"`
	WALContinuityOK    bool      `json:"wal_continuity_ok"`
	LayersOK           bool      `json:"layers_ok"`
	DeepRestoreOK      bool      `json:"deep_restore_ok"`
	CheckedObjects     int       `json:"checked_objects"`
	CheckedWALSegments int       `json:"checked_wal_segments"`
	CheckedLayers      int       `json:"checked_layers"`
	Errors             []string  `json:"errors,omitempty"`
	StartedAt          time.Time `json:"started_at"`
	CompletedAt        time.Time `json:"completed_at"`

	verifiedAt time.Time
}

// VerifyOptions controls backup integrity verification.
type VerifyOptions struct {
	ExpectedTenantID    string
	ExpectedGeneration  string
	StreamHash          bool
	VerifyLayerObjects  bool
	DeepTestRestore     bool
	DeepTestRestoreFunc func(ctx context.Context, m *BackupManifest) error
}

// VerifyBackup verifies the manifest, all referenced objects, WAL continuity, optional layer
// availability, and (when requested) performs a real PostgreSQL test restore.
func VerifyBackup(ctx context.Context, st *Store, m *BackupManifest, opts VerifyOptions) (*VerificationReport, error) {
	rep := &VerificationReport{
		StartedAt: time.Now().UTC(),
	}
	if m != nil {
		rep.BackupID = m.BackupID
		rep.TenantID = m.TenantID
		rep.Generation = m.Generation
	}
	addErr := func(err error) {
		rep.Errors = append(rep.Errors, err.Error())
	}

	if err := m.Validate(true); err != nil {
		addErr(err)
	} else {
		rep.ManifestOK = true
	}
	if opts.ExpectedTenantID != "" && m != nil && m.TenantID != opts.ExpectedTenantID {
		addErr(fmt.Errorf("%w: manifest tenant=%s expected=%s", ErrBackupGeneration, m.TenantID, opts.ExpectedTenantID))
	}
	if opts.ExpectedGeneration != "" && m != nil && m.Generation != opts.ExpectedGeneration {
		addErr(fmt.Errorf("%w: manifest generation=%s expected=%s", ErrBackupGeneration, m.Generation, opts.ExpectedGeneration))
	}

	if m != nil {
		if err := ensureSingleBaseObject(m); err != nil {
			addErr(err)
		}
	}

	objectsOK := true
	if m != nil {
		for _, obj := range m.Objects {
			rep.CheckedObjects++
			if err := st.VerifyObject(ctx, obj, opts.StreamHash); err != nil {
				objectsOK = false
				addErr(fmt.Errorf("object %s: %w", obj.Key, err))
			}
		}
	}
	rep.ObjectsOK = objectsOK

	walOK := true
	if m != nil {
		if err := ValidateWALContinuity(m.WALSegments, m.WALStartLSN, m.WALEndLSN); err != nil {
			walOK = false
			addErr(err)
		}
		for _, w := range m.WALSegments {
			rep.CheckedWALSegments++
			ref := ObjectRef{Kind: "wal", Key: w.R2Key, Size: w.Size, SHA256: w.CipherSHA256}
			if err := st.VerifyObject(ctx, ref, opts.StreamHash); err != nil {
				walOK = false
				addErr(fmt.Errorf("wal %s: %w", w.SegmentName, err))
			}
		}
	}
	rep.WALContinuityOK = walOK

	if m != nil && opts.VerifyLayerObjects && len(m.LayerIDs) > 0 {
		layersOK := true
		for _, id := range m.LayerIDs {
			rep.CheckedLayers++
			if _, err := st.Head(ctx, id); err != nil {
				layersOK = false
				addErr(fmt.Errorf("layer %s: %w", id, err))
			}
		}
		rep.LayersOK = layersOK
	} else {
		rep.LayersOK = true
	}

	rep.Valid = len(rep.Errors) == 0
	if rep.Valid && opts.DeepTestRestore && opts.DeepTestRestoreFunc != nil {
		if err := opts.DeepTestRestoreFunc(ctx, m); err != nil {
			rep.DeepRestoreOK = false
			rep.Valid = false
			addErr(fmt.Errorf("deep test restore: %w", err))
		} else {
			rep.DeepRestoreOK = true
		}
	}

	rep.CompletedAt = time.Now().UTC()
	rep.verifiedAt = rep.CompletedAt
	return rep, nil
}

func ensureSingleBaseObject(m *BackupManifest) error {
	count := 0
	for _, o := range m.Objects {
		if o.Kind == "base" {
			count++
		}
	}
	if m.Type == BackupTypeFull && count != 1 {
		return fmt.Errorf("%w: FULL backup has %d base objects (want 1)", ErrInvalidBackupManifest, count)
	}
	if m.Type == BackupTypeIncr && count != 1 {
		return fmt.Errorf("%w: INCR backup has %d base objects (want 1)", ErrInvalidBackupManifest, count)
	}
	return nil
}

// BaseObject returns the single base archive ref, or an error.
func (m *BackupManifest) BaseObject() (ObjectRef, error) {
	var found ObjectRef
	n := 0
	for _, o := range m.Objects {
		if o.Kind == "base" || strings.HasSuffix(o.Key, "/base.tar.zst.enc") {
			found = o
			n++
		}
	}
	if n != 1 {
		return ObjectRef{}, fmt.Errorf("%w: expected one base object, found %d", ErrInvalidBackupManifest, n)
	}
	return found, nil
}
