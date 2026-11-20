package readadapter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// BlockCoord identifies a single 8 KiB relation block modified by a Write Engine commit.
type BlockCoord struct {
	SpcOID    uint32
	DbOID     uint32
	RelNumber uint32
	ForkNum   uint8
	BlockNum  uint32
}

// RelCoord identifies a relation fork invalidated by DDL/TRUNCATE/DROP.
type RelCoord struct {
	SpcOID    uint32
	DbOID     uint32
	RelNumber uint32
	ForkNum   uint8
}

// CommitEvent is emitted by AIR's authoritative Write Engine upon transaction commit
// (or WAL replay catch-up) to advance Read Engine snapshots and invalidate stale cache entries.
type CommitEvent struct {
	TenantID        string
	Generation      string
	CommitLSN       uint64
	CommitXID       uint32
	Xmin            uint32
	Xmax            uint32
	ActiveXIPs      []uint32
	ChangedBlocks   []BlockCoord
	InvalidatedRels []RelCoord
	CommittedAt     time.Time
}

// PublisherStats reports commit publication and invalidation counters.
type PublisherStats struct {
	CommitsPublished  uint64
	BlocksInvalidated uint64
	RelsInvalidated   uint64
	FencesProcessed   uint64
	CatchUpSyncs      uint64
}

// CommitPublisher bridges AIR's Write Engine commits to the Read Engine's
// SnapshotCoordinator and TieredCacheBackend, and tracks per-session write watermarks
// for read-your-own-write causal consistency.
type CommitPublisher struct {
	coords *SnapshotCoordinator
	cache  *TieredCacheBackend

	mu                sync.Mutex
	latestLSN         map[string]uint64
	sessionWatermarks map[string]uint64

	commitsPublished  atomic.Uint64
	blocksInvalidated atomic.Uint64
	relsInvalidated   atomic.Uint64
	fencesProcessed   atomic.Uint64
	catchUpSyncs      atomic.Uint64
}

// NewCommitPublisher creates a CommitPublisher wired to coords and optional cache.
func NewCommitPublisher(coords *SnapshotCoordinator, cache *TieredCacheBackend) (*CommitPublisher, error) {
	if coords == nil {
		return nil, fmt.Errorf("readadapter: SnapshotCoordinator must not be nil")
	}
	return &CommitPublisher{
		coords:            coords,
		cache:             cache,
		latestLSN:         make(map[string]uint64),
		sessionWatermarks: make(map[string]uint64),
	}, nil
}

// PublishCommit records an authoritative Write Engine commit, invalidates any
// older cached blocks/relations in L1+L2, and wakes causal read waiters.
func (p *CommitPublisher) PublishCommit(ev CommitEvent) error {
	if ev.TenantID == "" {
		return ErrInvalidTenant
	}
	if ev.CommitLSN == 0 {
		return fmt.Errorf("readadapter: CommitLSN must be non-zero")
	}

	if p.cache != nil {
		for _, blk := range ev.ChangedBlocks {
			p.cache.InvalidateBlock(
				ev.TenantID,
				ev.Generation,
				blk.SpcOID,
				blk.DbOID,
				blk.RelNumber,
				blk.ForkNum,
				blk.BlockNum,
				ev.CommitLSN,
			)
			p.blocksInvalidated.Add(1)
		}
		for _, rel := range ev.InvalidatedRels {
			p.cache.InvalidateRelation(
				ev.TenantID,
				ev.Generation,
				rel.SpcOID,
				rel.DbOID,
				rel.RelNumber,
				rel.ForkNum,
			)
			p.relsInvalidated.Add(1)
		}
	}

	p.mu.Lock()
	if ev.CommitLSN > p.latestLSN[ev.TenantID] {
		p.latestLSN[ev.TenantID] = ev.CommitLSN
	}
	p.mu.Unlock()

	p.coords.Publish(ev.TenantID, ev.Generation, SnapshotInfo{
		LSN:        ev.CommitLSN,
		Xmin:       ev.Xmin,
		Xmax:       ev.Xmax,
		ActiveXIPs: append([]uint32(nil), ev.ActiveXIPs...),
	})
	p.commitsPublished.Add(1)
	return nil
}

// OnGenerationFence handles primary failover/fencing by invalidating all
// cached pages for the fenced oldGeneration and publishing the new generation's baseline.
func (p *CommitPublisher) OnGenerationFence(tenantID, oldGeneration, newGeneration string, baseline SnapshotInfo) {
	if p.cache != nil && oldGeneration != "" {
		p.cache.InvalidateGeneration(tenantID, oldGeneration)
	}
	p.fencesProcessed.Add(1)
	if baseline.LSN > 0 {
		p.coords.Publish(tenantID, newGeneration, baseline)
	}
}

// RecordSessionWrite records the write LSN produced by sessionID so subsequent
// reads on that session can enforce read-your-own-write causal visibility.
func (p *CommitPublisher) RecordSessionWrite(sessionID string, writeLSN uint64) {
	if sessionID == "" || writeLSN == 0 {
		return
	}
	p.mu.Lock()
	if writeLSN > p.sessionWatermarks[sessionID] {
		p.sessionWatermarks[sessionID] = writeLSN
	}
	p.mu.Unlock()
}

// SessionWatermark returns the minimum LSN required for sessionID's reads (0 if none).
func (p *CommitPublisher) SessionWatermark(sessionID string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessionWatermarks[sessionID]
}

// ClearSession removes session watermark state when a client disconnects.
func (p *CommitPublisher) ClearSession(sessionID string) {
	p.mu.Lock()
	delete(p.sessionWatermarks, sessionID)
	p.mu.Unlock()
}

// AwaitSessionRead waits until the Read Engine snapshot for (tenantID, generation)
// has caught up to sessionID's last write LSN (if any), then returns the SnapshotInfo.
func (p *CommitPublisher) AwaitSessionRead(ctx context.Context, tenantID, generation, sessionID string) (SnapshotInfo, error) {
	minLSN := p.SessionWatermark(sessionID)
	if minLSN == 0 {
		return p.coords.CurrentSnapshot(ctx, tenantID, generation)
	}
	return p.coords.AwaitLSN(ctx, tenantID, generation, minLSN)
}

// CatchUpLSN advances the published snapshot if external WAL/Pageserver state
// has progressed beyond the last published LSN.
func (p *CommitPublisher) CatchUpLSN(tenantID, generation string, info SnapshotInfo) {
	p.mu.Lock()
	cur := p.latestLSN[tenantID]
	if info.LSN > cur {
		p.latestLSN[tenantID] = info.LSN
	}
	p.mu.Unlock()

	if info.LSN > cur {
		p.coords.Publish(tenantID, generation, info)
		p.catchUpSyncs.Add(1)
	}
}

// Stats returns a snapshot of CommitPublisher metrics.
func (p *CommitPublisher) Stats() PublisherStats {
	return PublisherStats{
		CommitsPublished:  p.commitsPublished.Load(),
		BlocksInvalidated: p.blocksInvalidated.Load(),
		RelsInvalidated:   p.relsInvalidated.Load(),
		FencesProcessed:   p.fencesProcessed.Load(),
		CatchUpSyncs:      p.catchUpSyncs.Load(),
	}
}
