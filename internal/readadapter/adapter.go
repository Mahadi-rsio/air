package readadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/Mahadi-rsio/air/internal/tenant"
)

var (
	ErrInvalidTenant   = errors.New("readadapter: invalid tenant identifier")
	ErrStaleGeneration = errors.New("readadapter: stale or mismatched tenant generation")
	ErrInvalidSnapshot = errors.New("readadapter: invalid or released snapshot")
	ErrRelationClosed  = errors.New("readadapter: relation handle is closed")
	ErrTenantMismatch  = errors.New("readadapter: snapshot/relation tenant or generation mismatch")
	ErrReadOnly        = errors.New("readadapter: persistent writes are prohibited")
	ErrNotFound        = pageserver.ErrNotFound
)

// StorageBackend is the authoritative AIR read-only page source (implemented by *pageserver.Pageserver).
type StorageBackend interface {
	GetTenantGeneration(tenantID string) string
	GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error)
	GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error)
	GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error)
	GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error)
}

// SnapshotInfo describes a committed MVCC snapshot from AIR's authoritative Write Engine.
type SnapshotInfo struct {
	LSN        uint64
	Xmin       uint32
	Xmax       uint32
	ActiveXIPs []uint32
}

// SnapshotProvider supplies consistent MVCC snapshots for a tenant generation.
type SnapshotProvider interface {
	CurrentSnapshot(ctx context.Context, tenantID, generation string) (SnapshotInfo, error)
}

// RelationSpec identifies a PostgreSQL relation fork within a tenant database.
type RelationSpec struct {
	TenantID   string
	Generation string
	SpcOID     uint32
	DbOID      uint32
	RelNumber  uint32
	ForkNum    uint8
}

// RelationHandle is an open, read-only handle to a tenant relation fork.
type RelationHandle struct {
	ID         uint64
	Spec       RelationSpec
	closed     atomic.Bool
	openedAt   time.Time
}

// ReadSnapshot pins a consistent (LSN, MVCC) point-in-time view for a tenant generation.
type ReadSnapshot struct {
	ID         uint64
	TenantID   string
	Generation string
	LSN        uint64
	Xmin       uint32
	Xmax       uint32
	ActiveXIPs []uint32
	AcquiredAt time.Time
	released   atomic.Bool
}

// PageVersion identifies the exact storage version of a returned 8 KiB page.
type PageVersion struct {
	TenantID   string
	Generation string
	SnapshotID uint64
	PageLSN    uint64
	BlockNum   uint32
}

// PageData holds an immutable 8192-byte PostgreSQL page and its version metadata.
type PageData struct {
	Data    []byte
	Version PageVersion
}

// RelationMetadata describes relation existence and block count at a snapshot.
type RelationMetadata struct {
	Spec    RelationSpec
	Exists  bool
	NBlocks uint32
	LSN     uint64
}

// Adapter is the controlled, read-only AIR Storage Adapter for the native PG17 read-only engine.
// It enforces tenant/generation isolation, snapshot pinning, and zero persistent writes.
type Adapter struct {
	backend  StorageBackend
	snapProv SnapshotProvider

	mu        sync.RWMutex
	nextID    atomic.Uint64
	relations map[uint64]*RelationHandle
	snapshots map[uint64]*ReadSnapshot
}

// NewAdapter creates a read-only Storage Adapter backed by AIR's Pageserver and optional SnapshotProvider.
func NewAdapter(backend StorageBackend, snapProv SnapshotProvider) *Adapter {
	return &Adapter{
		backend:   backend,
		snapProv:  snapProv,
		relations: make(map[uint64]*RelationHandle),
		snapshots: make(map[uint64]*ReadSnapshot),
	}
}

func (a *Adapter) validateGeneration(tenantID, generation string) error {
	if err := tenant.ValidateID(tenantID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTenant, err)
	}
	if generation == "" {
		return fmt.Errorf("%w: empty generation", ErrStaleGeneration)
	}
	activeGen := a.backend.GetTenantGeneration(tenantID)
	if activeGen != "" && activeGen != generation {
		return fmt.Errorf("%w: active=%s requested=%s", ErrStaleGeneration, activeGen, generation)
	}
	return nil
}

// GetSnapshot acquires and pins a consistent read snapshot for (tenantID, generation).
func (a *Adapter) GetSnapshot(ctx context.Context, tenantID, generation string) (*ReadSnapshot, error) {
	if err := a.validateGeneration(tenantID, generation); err != nil {
		return nil, err
	}
	var info SnapshotInfo
	if a.snapProv != nil {
		var err error
		info, err = a.snapProv.CurrentSnapshot(ctx, tenantID, generation)
		if err != nil {
			return nil, err
		}
	}
	var xips []uint32
	if len(info.ActiveXIPs) > 0 {
		xips = append([]uint32(nil), info.ActiveXIPs...)
	}
	snap := &ReadSnapshot{
		ID:         a.nextID.Add(1),
		TenantID:   tenantID,
		Generation: generation,
		LSN:        info.LSN,
		Xmin:       info.Xmin,
		Xmax:       info.Xmax,
		ActiveXIPs: xips,
		AcquiredAt: time.Now(),
	}
	a.mu.Lock()
	a.snapshots[snap.ID] = snap
	a.mu.Unlock()
	return snap, nil
}

// ReleaseSnapshot unpins a previously acquired ReadSnapshot.
func (a *Adapter) ReleaseSnapshot(snap *ReadSnapshot) error {
	if snap == nil {
		return ErrInvalidSnapshot
	}
	if !snap.released.CompareAndSwap(false, true) {
		return ErrInvalidSnapshot
	}
	a.mu.Lock()
	delete(a.snapshots, snap.ID)
	a.mu.Unlock()
	return nil
}

// PinnedMinLSN returns the lowest LSN among all active pinned snapshots for tenantID (0 if none).
func (a *Adapter) PinnedMinLSN(tenantID string) uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var minLSN uint64
	for _, s := range a.snapshots {
		if s.TenantID != tenantID || s.released.Load() {
			continue
		}
		if s.LSN != 0 && (minLSN == 0 || s.LSN < minLSN) {
			minLSN = s.LSN
		}
	}
	return minLSN
}

// ActiveSnapshotCount returns the number of currently pinned snapshots.
func (a *Adapter) ActiveSnapshotCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.snapshots)
}

// OpenRelation opens a read-only relation handle scoped to (tenantID, generation).
func (a *Adapter) OpenRelation(_ context.Context, spec RelationSpec) (*RelationHandle, error) {
	if err := a.validateGeneration(spec.TenantID, spec.Generation); err != nil {
		return nil, err
	}
	h := &RelationHandle{
		ID:       a.nextID.Add(1),
		Spec:     spec,
		openedAt: time.Now(),
	}
	a.mu.Lock()
	a.relations[h.ID] = h
	a.mu.Unlock()
	return h, nil
}

// CloseRelation closes an open relation handle.
func (a *Adapter) CloseRelation(h *RelationHandle) error {
	if h == nil {
		return ErrRelationClosed
	}
	if !h.closed.CompareAndSwap(false, true) {
		return ErrRelationClosed
	}
	a.mu.Lock()
	delete(a.relations, h.ID)
	a.mu.Unlock()
	return nil
}

func (a *Adapter) validateAccess(h *RelationHandle, snap *ReadSnapshot) error {
	if h == nil || h.closed.Load() {
		return ErrRelationClosed
	}
	if snap == nil || snap.released.Load() {
		return ErrInvalidSnapshot
	}
	if h.Spec.TenantID != snap.TenantID || h.Spec.Generation != snap.Generation {
		return ErrTenantMismatch
	}
	return a.validateGeneration(h.Spec.TenantID, h.Spec.Generation)
}

// GetRelationMetadata returns relation existence and block count as of snap.
func (a *Adapter) GetRelationMetadata(ctx context.Context, h *RelationHandle, snap *ReadSnapshot) (*RelationMetadata, error) {
	if err := a.validateAccess(h, snap); err != nil {
		return nil, err
	}
	exists, err := a.backend.GetRelExists(ctx, h.Spec.TenantID, h.Spec.SpcOID, h.Spec.DbOID, h.Spec.RelNumber, h.Spec.ForkNum, snap.LSN)
	if err != nil {
		return nil, err
	}
	var nblocks uint32
	if exists {
		nblocks, err = a.backend.GetRelSize(ctx, h.Spec.TenantID, h.Spec.SpcOID, h.Spec.DbOID, h.Spec.RelNumber, h.Spec.ForkNum, snap.LSN)
		if err != nil && !errors.Is(err, pageserver.ErrNotFound) {
			return nil, err
		}
	}
	return &RelationMetadata{
		Spec:    h.Spec,
		Exists:  exists,
		NBlocks: nblocks,
		LSN:     snap.LSN,
	}, nil
}

// ReadPage reads a single 8192-byte page at blockNum as of snap.
// ReadPageInto reads an 8192-byte page directly into dst (len >= PGPageSize) with zero per-page allocations on L1 hit.
func (a *Adapter) ReadPageInto(ctx context.Context, h *RelationHandle, snap *ReadSnapshot, blockNum uint32, dst []byte) error {
	if len(dst) < pageserver.PageSize {
		return fmt.Errorf("readadapter: dst buffer too small (%d < %d)", len(dst), pageserver.PageSize)
	}
	if err := a.validateAccess(h, snap); err != nil {
		return err
	}
	key := pageserver.RelBlockKey(h.Spec.SpcOID, h.Spec.DbOID, h.Spec.RelNumber, h.Spec.ForkNum, blockNum)
	if intoBackend, ok := a.backend.(interface {
		GetPageAtLSNInto(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64, dst []byte) error
	}); ok {
		return intoBackend.GetPageAtLSNInto(ctx, h.Spec.TenantID, key, snap.LSN, dst)
	}
	raw, err := a.backend.GetPageAtLSN(ctx, h.Spec.TenantID, key, snap.LSN)
	if err != nil {
		return err
	}
	if len(raw) != pageserver.PageSize {
		return fmt.Errorf("readadapter: invalid page size %d", len(raw))
	}
	copy(dst[:pageserver.PageSize], raw)
	return nil
}

func (a *Adapter) ReadPage(ctx context.Context, h *RelationHandle, snap *ReadSnapshot, blockNum uint32) (*PageData, error) {
	if err := a.validateAccess(h, snap); err != nil {
		return nil, err
	}
	key := pageserver.RelBlockKey(h.Spec.SpcOID, h.Spec.DbOID, h.Spec.RelNumber, h.Spec.ForkNum, blockNum)
	raw, err := a.backend.GetPageAtLSN(ctx, h.Spec.TenantID, key, snap.LSN)
	if err != nil {
		return nil, err
	}
	if len(raw) != pageserver.PageSize {
		return nil, fmt.Errorf("readadapter: invalid page size %d", len(raw))
	}
	pageCopy := make([]byte, pageserver.PageSize)
	copy(pageCopy, raw)
	hdr, err := ParsePageHeader(pageCopy)
	var pageLSN uint64
	if err == nil {
		pageLSN = hdr.LSN
	}
	return &PageData{
		Data: pageCopy,
		Version: PageVersion{
			TenantID:   h.Spec.TenantID,
			Generation: h.Spec.Generation,
			SnapshotID: snap.ID,
			PageLSN:    pageLSN,
			BlockNum:   blockNum,
		},
	}, nil
}

// ReadPages reads up to nblocks contiguous pages starting at startBlock as of snap.
func (a *Adapter) ReadPages(ctx context.Context, h *RelationHandle, snap *ReadSnapshot, startBlock, nblocks uint32) ([]*PageData, error) {
	if err := a.validateAccess(h, snap); err != nil {
		return nil, err
	}
	if nblocks == 0 {
		return nil, nil
	}
	buf, got, err := a.backend.GetPageBatch(ctx, h.Spec.TenantID, h.Spec.SpcOID, h.Spec.DbOID, h.Spec.RelNumber, h.Spec.ForkNum, startBlock, nblocks, snap.LSN)
	if err != nil {
		return nil, err
	}
	out := make([]*PageData, 0, got)
	for i := uint32(0); i < got; i++ {
		off := int(i) * pageserver.PageSize
		if off+pageserver.PageSize > len(buf) {
			break
		}
		p := buf[off : off+pageserver.PageSize]
		var pageLSN uint64
		if hdr, herr := ParsePageHeader(p); herr == nil {
			pageLSN = hdr.LSN
		}
		out = append(out, &PageData{
			Data: p,
			Version: PageVersion{
				TenantID:   h.Spec.TenantID,
				Generation: h.Spec.Generation,
				SnapshotID: snap.ID,
				PageLSN:    pageLSN,
				BlockNum:   startBlock + i,
			},
		})
	}
	return out, nil
}

// GetPageVersion returns the PageVersion (including page LSN) for blockNum at snap.
func (a *Adapter) GetPageVersion(ctx context.Context, h *RelationHandle, snap *ReadSnapshot, blockNum uint32) (PageVersion, error) {
	pg, err := a.ReadPage(ctx, h, snap, blockNum)
	if err != nil {
		return PageVersion{}, err
	}
	return pg.Version, nil
}
