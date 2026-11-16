package pageserver

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// Timeline manages the 2D (Key, LSN) storage state for a single tenant.
type Timeline struct {
	mu          sync.RWMutex
	tenantID    string
	epoch       int64
	lastLSN     uint64
	inMem       *InMemoryLayer
	layers      []LayerMeta // ordered oldest -> newest
	store       *LayerStore
	redoer      WALRedoer
	maxInMemBytes int64
	relSizes    map[Key]uint32
	layerSeq    uint64
}

// NewTimeline initializes a tenant Timeline.
func NewTimeline(tenantID string, epoch int64, store *LayerStore, redoer WALRedoer) *Timeline {
	if redoer == nil {
		redoer = NewDefaultWALRedoer("")
	}
	return &Timeline{
		tenantID:      tenantID,
		epoch:         epoch,
		inMem:         NewInMemoryLayer(0),
		store:         store,
		redoer:        redoer,
		maxInMemBytes: 64 * 1024 * 1024,
		relSizes:      make(map[Key]uint32),
	}
}

// LastLSN returns the highest WAL LSN ingested into this Timeline.
func (t *Timeline) LastLSN() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastLSN
}

// LayerCount returns the number of immutable layers registered in this Timeline.
func (t *Timeline) LayerCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.layers)
}

// AddLayer registers an existing or newly written LayerMeta in the Timeline.
func (t *Timeline) AddLayer(meta LayerMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.layers = append(t.layers, meta)
	if meta.EndLSN > t.lastLSN {
		t.lastLSN = meta.EndLSN
	}
}

// PutPage inserts a full 8192-byte page snapshot into the active InMemoryLayer.
func (t *Timeline) PutPage(key Key, lsn uint64, page []byte) {
	p := make([]byte, PageSize)
	copy(p, page)

	t.mu.Lock()
	t.inMem.Put(key, WALDelta{
		LSN:       lsn,
		Flags:     EntryFlagPageImage,
		PageImage: p,
	})
	sizeKey := RelSizeKey(key.SpcOID, key.DbOID, key.RelNumber, key.ForkNum)
	if cur := t.relSizes[sizeKey]; key.BlockNum+1 > cur {
		t.relSizes[sizeKey] = key.BlockNum + 1
		t.inMem.Put(sizeKey, WALDelta{
			LSN:     lsn,
			Flags:   EntryFlagRelSize,
			RelSize: key.BlockNum + 1,
		})
	}
	if lsn > t.lastLSN {
		t.lastLSN = lsn
	}
	t.mu.Unlock()
}

// ApplyWALRecord ingests a decoded PG17 WAL record into the Timeline's InMemoryLayer.
func (t *Timeline) ApplyWALRecord(rec *DecodedWALRecord) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, blk := range rec.Blocks {
		key := RelBlockKey(blk.Rel.SpcOID, blk.Rel.DbOID, blk.Rel.RelNumber, blk.ForkNum, blk.BlockNum)
		delta := WALDelta{
			LSN:       rec.EndLSN,
			Flags:     EntryFlagWALRecord,
			WALRecord: rec.RawRecord,
		}
		if blk.WillInit {
			delta.Flags |= EntryFlagWillInit
		}
		if blk.HasImage {
			if page, err := RestoreBkpImage(blk); err == nil {
				delta.Flags |= EntryFlagPageImage
				delta.PageImage = page
			}
		}
		t.inMem.Put(key, delta)

		sizeKey := RelSizeKey(blk.Rel.SpcOID, blk.Rel.DbOID, blk.Rel.RelNumber, blk.ForkNum)
		if cur := t.relSizes[sizeKey]; blk.BlockNum+1 > cur {
			t.relSizes[sizeKey] = blk.BlockNum + 1
			t.inMem.Put(sizeKey, WALDelta{
				LSN:     rec.EndLSN,
				Flags:   EntryFlagRelSize,
				RelSize: blk.BlockNum + 1,
			})
		}
	}

	// Track SMGR create/truncate relation sizes from MainData.
	if rec.Rmid == RmSmgrID {
		op := rec.Info & XLogInfoOpMask
		if op == XLogSmgrCreate && len(rec.MainData) >= 13 {
			rel := RelFileLocator{
				SpcOID:    binary.LittleEndian.Uint32(rec.MainData[0:4]),
				DbOID:     binary.LittleEndian.Uint32(rec.MainData[4:8]),
				RelNumber: binary.LittleEndian.Uint32(rec.MainData[8:12]),
			}
			fork := rec.MainData[12]
			sizeKey := RelSizeKey(rel.SpcOID, rel.DbOID, rel.RelNumber, fork)
			if _, ok := t.relSizes[sizeKey]; !ok {
				t.relSizes[sizeKey] = 0
			}
			t.inMem.Put(sizeKey, WALDelta{
				LSN:     rec.EndLSN,
				Flags:   EntryFlagRelSize,
				RelSize: t.relSizes[sizeKey],
			})
		} else if op == XLogSmgrTruncate && len(rec.MainData) >= 20 {
			blkno := binary.LittleEndian.Uint32(rec.MainData[0:4])
			rel := RelFileLocator{
				SpcOID:    binary.LittleEndian.Uint32(rec.MainData[4:8]),
				DbOID:     binary.LittleEndian.Uint32(rec.MainData[8:12]),
				RelNumber: binary.LittleEndian.Uint32(rec.MainData[12:16]),
			}
			flags := binary.LittleEndian.Uint32(rec.MainData[16:20])
			for fork := uint8(0); fork <= 3; fork++ {
				if (flags & (1 << fork)) != 0 {
					sizeKey := RelSizeKey(rel.SpcOID, rel.DbOID, rel.RelNumber, fork)
					t.relSizes[sizeKey] = blkno
					t.inMem.Put(sizeKey, WALDelta{
						LSN:     rec.EndLSN,
						Flags:   EntryFlagRelSize,
						RelSize: blkno,
					})
				}
			}
		}
	}

	if rec.EndLSN > t.lastLSN {
		t.lastLSN = rec.EndLSN
	}
}

// FlushInMemory freezes the current InMemoryLayer and writes it as an immutable L0 layer.
func (t *Timeline) FlushInMemory(ctx context.Context) (*LayerMeta, error) {
	t.mu.Lock()
	recs, startLSN, endLSN := t.inMem.SnapshotAll()
	if len(recs) == 0 {
		t.mu.Unlock()
		return nil, nil
	}
	oldMem := t.inMem
	t.inMem = NewInMemoryLayer(endLSN)
	t.layerSeq++
	seq := t.layerSeq
	epoch := t.epoch
	tenantID := t.tenantID
	t.mu.Unlock()

	fileName := fmt.Sprintf("l0_%06d_%016x_%016x.layer", seq, startLSN, endLSN)
	r2Key := fmt.Sprintf("tenants/%s/timeline_%d/layers/%s", tenantID, epoch, fileName)
	meta := LayerMeta{
		TenantID: tenantID,
		Epoch:    epoch,
		Kind:     LayerKindL0,
		FileName: fileName,
		R2Key:    r2Key,
		StartLSN: startLSN,
		EndLSN:   endLSN,
	}
	written, err := t.store.WriteLayer(ctx, meta, recs)
	if err != nil {
		// Restore in-memory entries on failure.
		t.mu.Lock()
		for _, r := range recs {
			t.inMem.Put(r.Key, r.Delta)
		}
		_ = oldMem
		t.mu.Unlock()
		return nil, err
	}

	t.mu.Lock()
	t.layers = append(t.layers, written)
	t.mu.Unlock()
	return &written, nil
}

// GetPageAtLSN reconstructs the 8192-byte page for key as of lsn (0 = latest).
func (t *Timeline) GetPageAtLSN(ctx context.Context, key Key, lsn uint64) ([]byte, error) {
	t.mu.RLock()
	inMemDeltas := t.inMem.Lookup(key, lsn)
	layersCopy := append([]LayerMeta(nil), t.layers...)
	t.mu.RUnlock()

	// Collect deltas from InMemoryLayer + relevant immutable layers.
	var collected []WALDelta
	collected = append(collected, inMemDeltas...)

	// Walk layers from newest to oldest; stop once we hit an Image layer or FPI.
	var basePage []byte
	foundBase := false
	for i := len(layersCopy) - 1; i >= 0; i-- {
		lm := layersCopy[i]
		if lsn != 0 && lm.StartLSN > lsn {
			continue
		}
		if key.Compare(lm.StartKey) < 0 || key.Compare(lm.EndKey) > 0 {
			continue
		}
		deltas, err := t.store.LookupKeyInLayer(ctx, lm, key, lsn)
		if err != nil {
			return nil, err
		}
		if len(deltas) == 0 {
			continue
		}
		collected = append(collected, deltas...)
		// Check if the newest delta in this layer is a full page image.
		last := deltas[len(deltas)-1]
		if (last.Flags&EntryFlagPageImage) != 0 && len(last.PageImage) == PageSize {
			basePage = last.PageImage
			foundBase = true
			break
		}
		if lm.Kind == LayerKindImage {
			break
		}
	}

	if len(collected) == 0 && !foundBase {
		return nil, ErrNotFound
	}

	// Sort all collected deltas by LSN ascending and keep only from last FPI onward.
	sort.Slice(collected, func(i, j int) bool {
		return collected[i].LSN < collected[j].LSN
	})
	startIdx := 0
	for i := len(collected) - 1; i >= 0; i-- {
		if (collected[i].Flags&EntryFlagPageImage) != 0 && len(collected[i].PageImage) == PageSize {
			basePage = collected[i].PageImage
			startIdx = i + 1
			break
		}
	}
	if startIdx >= len(collected) && len(basePage) == PageSize {
		return append([]byte(nil), basePage...), nil
	}
	return t.redoer.Redo(ctx, key, basePage, collected[startIdx:])
}

// GetPageBatch fetches up to nblocks consecutive blocks starting at startBlock.
func (t *Timeline) GetPageBatch(ctx context.Context, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	out := make([]byte, 0, int(nblocks)*PageSize)
	var count uint32
	for i := uint32(0); i < nblocks; i++ {
		key := RelBlockKey(spcOID, dbOID, relNumber, forkNum, startBlock+i)
		page, err := t.GetPageAtLSN(ctx, key, lsn)
		if err == ErrNotFound {
			if i == 0 {
				return nil, 0, ErrNotFound
			}
			break
		}
		if err != nil {
			return nil, 0, err
		}
		out = append(out, page...)
		count++
	}
	return out, count, nil
}

// GetRelSize returns the number of blocks in a relation fork as of lsn.
func (t *Timeline) GetRelSize(ctx context.Context, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	sizeKey := RelSizeKey(spcOID, dbOID, relNumber, forkNum)

	t.mu.RLock()
	if lsn == 0 || lsn >= t.lastLSN {
		if nb, ok := t.relSizes[sizeKey]; ok {
			t.mu.RUnlock()
			return nb, nil
		}
	}
	if lsn == 0 {
		lsn = t.lastLSN
	}
	inMemDeltas := t.inMem.Lookup(sizeKey, lsn)
	layersCopy := append([]LayerMeta(nil), t.layers...)
	t.mu.RUnlock()

	if len(inMemDeltas) > 0 {
		return inMemDeltas[len(inMemDeltas)-1].RelSize, nil
	}
	for i := len(layersCopy) - 1; i >= 0; i-- {
		lm := layersCopy[i]
		if lsn != 0 && lm.StartLSN > lsn {
			continue
		}
		deltas, err := t.store.LookupKeyInLayer(ctx, lm, sizeKey, lsn)
		if err != nil {
			return 0, err
		}
		if len(deltas) > 0 {
			return deltas[len(deltas)-1].RelSize, nil
		}
	}
	return 0, ErrNotFound
}

// GetRelExists reports whether the relation fork exists in the Timeline.
func (t *Timeline) GetRelExists(ctx context.Context, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error) {
	_, err := t.GetRelSize(ctx, spcOID, dbOID, relNumber, forkNum, lsn)
	if err == ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetDBSize returns the approximate byte size of a database across its tracked relations.
func (t *Timeline) GetDBSize(_ context.Context, spcOID, dbOID uint32, _ uint64) (uint64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var total uint64
	for k, nblocks := range t.relSizes {
		if k.SpcOID == spcOID && k.DbOID == dbOID {
			total += uint64(nblocks) * uint64(PageSize)
		}
	}
	return total, nil
}
