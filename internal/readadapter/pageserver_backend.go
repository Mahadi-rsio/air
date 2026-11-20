package readadapter

import (
	"context"
	"fmt"
	"sync"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

// PageserverBackend adapts a live *pageserver.Pageserver to readadapter.StorageBackend
// so the read-only execution layer can fetch 8 KiB pages, relation sizes, and
// relation existence at pinned snapshot LSNs without touching write paths.
type PageserverBackend struct {
	ps *pageserver.Pageserver
}

// NewPageserverBackend wraps ps as a read-only StorageBackend.
func NewPageserverBackend(ps *pageserver.Pageserver) (*PageserverBackend, error) {
	if ps == nil {
		return nil, fmt.Errorf("pageserver must not be nil")
	}
	return &PageserverBackend{ps: ps}, nil
}

func (b *PageserverBackend) GetTenantGeneration(tenantID string) string {
	return b.ps.GetTenantGeneration(tenantID)
}

func (b *PageserverBackend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	return b.ps.GetPageAtLSN(ctx, tenantID, key, lsn)
}

func (b *PageserverBackend) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	return b.ps.GetPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, lsn)
}

func (b *PageserverBackend) GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	return b.ps.GetRelSize(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, lsn)
}

func (b *PageserverBackend) GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error) {
	return b.ps.GetRelExists(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, lsn)
}

type publishedSnapshot struct {
	generation string
	info       SnapshotInfo
}

// StaticSnapshotProvider supplies deterministic SnapshotInfo values for a
// tenant. It is updated by the Write Engine commit publisher (Phase 6) or tests.
type StaticSnapshotProvider struct {
	mu        sync.RWMutex
	snapshots map[string]publishedSnapshot
}

// NewStaticSnapshotProvider creates an empty StaticSnapshotProvider.
func NewStaticSnapshotProvider() *StaticSnapshotProvider {
	return &StaticSnapshotProvider{
		snapshots: make(map[string]publishedSnapshot),
	}
}

// SetSnapshot records the latest committed snapshot for tenantID and generation.
func (p *StaticSnapshotProvider) SetSnapshot(tenantID, generation string, info SnapshotInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshots[tenantID] = publishedSnapshot{
		generation: generation,
		info:       info,
	}
}

// CurrentSnapshot returns the latest snapshot for tenantID matching generation.
func (p *StaticSnapshotProvider) CurrentSnapshot(_ context.Context, tenantID, generation string) (SnapshotInfo, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	pub, ok := p.snapshots[tenantID]
	if !ok {
		return SnapshotInfo{}, fmt.Errorf("no snapshot published for tenant %q", tenantID)
	}
	if generation != "" && pub.generation != "" && pub.generation != generation {
		return SnapshotInfo{}, fmt.Errorf("snapshot generation mismatch for tenant %q: got %q, want %q", tenantID, pub.generation, generation)
	}
	return pub.info, nil
}
