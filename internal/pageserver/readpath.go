package pageserver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
)

const defaultMaxInMemBytes int64 = 16 * 1024 * 1024

// Timeline manages the 2D (Key, LSN) storage state for a single tenant.
type Timeline struct {
	mu            sync.RWMutex
	manifestMu    sync.Mutex
	compactMu     sync.Mutex
	flushMu       sync.Mutex
	bgFlushMu     sync.Mutex
	bgFlushing    bool
	flushDoneCh   chan struct{}
	lastFlushErr  error
	tenantID      string
	generation    string
	epoch         int64
	lastLSN       uint64
	inMem         *InMemoryLayer
	frozenMems    []*InMemoryLayer // ordered oldest -> newest, currently flushing
	layers        []LayerMeta      // ordered oldest -> newest
	store         *LayerStore
	redoer        WALRedoer
	maxInMemBytes int64
	relSizes          map[Key]uint32
	relSizeKnown      map[Key]bool
	allRelSizesSeeded bool
	slruPages         map[Key][]byte
	layerSeq          uint64
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
		maxInMemBytes: defaultMaxInMemBytes,
		relSizes:      make(map[Key]uint32),
		relSizeKnown:  make(map[Key]bool),
		slruPages:     make(map[Key][]byte),
	}
}

// SeedSLRUPage seeds a baseline 8 KiB SLRU page (e.g. pg_xact) into the Timeline
// so subsequent WAL XACT updates preserve all previously committed XIDs.
func (t *Timeline) SeedSLRUPage(key Key, page []byte) {
	if len(page) != PageSize {
		return
	}
	cp := make([]byte, PageSize)
	copy(cp, page)
	t.mu.Lock()
	t.slruPages[key] = cp
	t.inMem.Put(key, WALDelta{
		LSN:       1,
		Flags:     EntryFlagPageImage,
		PageImage: cp,
	})
	t.mu.Unlock()
}

// SetGeneration sets the immutable generation identifier for this Timeline.
func (t *Timeline) SetGeneration(gen string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.generation = gen
}

// Generation returns the immutable generation identifier for this Timeline.
func (t *Timeline) Generation() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.generation
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

// LoadLayersFromR2 populates t.layers from R2 if the timeline currently has no layers loaded.
func (t *Timeline) LoadLayersFromR2(ctx context.Context) error {
	t.mu.RLock()
	hasLayers := len(t.layers) > 0
	gen := t.generation
	t.mu.RUnlock()
	if hasLayers || t.store == nil {
		return nil
	}
	layers, maxSeq, err := t.store.ListLayersFromR2Gen(ctx, t.tenantID, gen, t.epoch)
	if err != nil {
		return err
	}
	if len(layers) == 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.layers) > 0 {
		return nil
	}
	t.layers = layers
	if maxSeq > t.layerSeq {
		t.layerSeq = maxSeq
	}
	for _, lm := range layers {
		if lm.EndLSN > t.lastLSN {
			t.lastLSN = lm.EndLSN
		}
	}
	return nil
}

// EnsureRelSizeSeeded ensures t.relSizes[sizeKey] is seeded from immutable layers
// if not already known in memory, preventing post-restart writes to lower block
// numbers from regressing the tracked relation size.
func (t *Timeline) EnsureRelSizeSeeded(ctx context.Context, spcOID, dbOID, relNumber uint32, forkNum uint8) {
	sizeKey := RelSizeKey(spcOID, dbOID, relNumber, forkNum)
	t.mu.RLock()
	known := t.relSizeKnown[sizeKey] || len(t.layers) == 0
	t.mu.RUnlock()
	if known {
		return
	}
	_, _ = t.GetRelSize(ctx, spcOID, dbOID, relNumber, forkNum, 0)
}

// PutPage inserts a full 8192-byte page snapshot into the active InMemoryLayer.
func (t *Timeline) PutPage(key Key, lsn uint64, page []byte) {
	t.EnsureRelSizeSeeded(context.Background(), key.SpcOID, key.DbOID, key.RelNumber, key.ForkNum)
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
	approx := t.inMem.ApproxBytes
	t.mu.Unlock()
	if approx >= t.maxInMemBytes {
		t.MaybeFlushAsync()
	}
}

// InMemBytes returns the approximate byte size of the active and flushing InMemoryLayers.
func (t *Timeline) InMemBytes() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	total := t.inMem.ApproxBytes
	for _, fm := range t.frozenMems {
		total += fm.ApproxBytes
	}
	return total
}

func (t *Timeline) lookupInMemLayersLocked(key Key, lsn uint64) ([]WALDelta, bool) {
	deltas := t.inMem.Lookup(key, lsn)
	if len(deltas) > 0 {
		last := deltas[len(deltas)-1]
		if (last.Flags&EntryFlagPageImage) != 0 && len(last.PageImage) == PageSize {
			return deltas, true
		}
	}
	var out []WALDelta
	if len(deltas) > 0 {
		out = append(out, deltas...)
	}
	for i := len(t.frozenMems) - 1; i >= 0; i-- {
		fd := t.frozenMems[i].Lookup(key, lsn)
		if len(fd) == 0 {
			continue
		}
		out = append(out, fd...)
		last := fd[len(fd)-1]
		if (last.Flags&EntryFlagPageImage) != 0 && len(last.PageImage) == PageSize {
			return out, true
		}
	}
	return out, false
}

func (t *Timeline) removeFrozenMemLocked(target *InMemoryLayer) {
	for i, fm := range t.frozenMems {
		if fm == target {
			t.frozenMems = append(t.frozenMems[:i], t.frozenMems[i+1:]...)
			return
		}
	}
}

func (t *Timeline) hardMaxInMemBytes() int64 {
	if t.maxInMemBytes <= 0 {
		return 0
	}
	return t.maxInMemBytes * 2
}

// MaybeFlushAsync triggers a non-blocking background FlushInMemory if one is not already running.
func (t *Timeline) MaybeFlushAsync() {
	if t.store == nil {
		return
	}
	t.bgFlushMu.Lock()
	if t.bgFlushing {
		t.bgFlushMu.Unlock()
		return
	}
	t.bgFlushing = true
	done := make(chan struct{})
	t.flushDoneCh = done
	t.bgFlushMu.Unlock()

	go func() {
		_, err := t.FlushInMemory(context.Background())
		t.bgFlushMu.Lock()
		t.lastFlushErr = err
		t.bgFlushing = false
		close(done)
		t.bgFlushMu.Unlock()
	}()
}

// EnforceInMemBudget triggers an async flush at the soft limit and applies backpressure
// at the hard limit (2x maxInMemBytes) until the flush completes.
func (t *Timeline) EnforceInMemBudget(ctx context.Context) error {
	if t.store == nil || t.maxInMemBytes <= 0 {
		return nil
	}
	cur := t.InMemBytes()
	if cur < t.maxInMemBytes {
		return nil
	}
	t.MaybeFlushAsync()
	hard := t.hardMaxInMemBytes()
	for cur >= hard {
		t.bgFlushMu.Lock()
		flushing := t.bgFlushing
		done := t.flushDoneCh
		lastErr := t.lastFlushErr
		t.bgFlushMu.Unlock()

		if !flushing {
			if lastErr != nil {
				return lastErr
			}
			t.MaybeFlushAsync()
			t.bgFlushMu.Lock()
			done = t.flushDoneCh
			t.bgFlushMu.Unlock()
			if done == nil {
				return nil
			}
		}
		select {
		case <-done:
			t.bgFlushMu.Lock()
			lastErr = t.lastFlushErr
			t.bgFlushMu.Unlock()
			if lastErr != nil {
				return lastErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		cur = t.InMemBytes()
	}
	return nil
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

	// Track XACT commit/abort status in pg_xact SLRU (KeyKindSLRU, SpcOID=0)
	// so native MVCC readers (CLOGResolver) can resolve uncommitted hint bits.
	if rec.Rmid == RmXactID && rec.XID >= 3 {
		op := rec.Info & XLogXactOpMask
		var status byte
		switch op {
		case XLogXactCommit, XLogXactCommitPrepared:
			status = 0x01 // XactStatusCommitted
		case XLogXactAbort, XLogXactAbortPrepared:
			status = 0x02 // XactStatusAborted
		}
		if status != 0 {
			markCLOG := func(xid uint32) {
				if xid < 3 {
					return
				}
				const clogXactsPerPage = uint32(PageSize) * 4
				pageNum := xid / clogXactsPerPage
				slruKey := Key{Kind: KeyKindSLRU, SpcOID: 0, BlockNum: pageNum}
				page := make([]byte, PageSize)
				if cur, ok := t.slruPages[slruKey]; ok && len(cur) == PageSize {
					copy(page, cur)
				} else if deltas := t.inMem.Lookup(slruKey, ^uint64(0)); len(deltas) > 0 {
					for i := len(deltas) - 1; i >= 0; i-- {
						if (deltas[i].Flags&EntryFlagPageImage) != 0 && len(deltas[i].PageImage) == PageSize {
							copy(page, deltas[i].PageImage)
							break
						}
					}
				}
				pageIdx := xid % clogXactsPerPage
				byteIdx := pageIdx / 4
				bitShift := (pageIdx % 4) * 2
				mask := byte(0x03 << bitShift)
				page[byteIdx] = (page[byteIdx] &^ mask) | ((status & 0x03) << bitShift)
				t.slruPages[slruKey] = page
				t.inMem.Put(slruKey, WALDelta{
					LSN:       rec.EndLSN,
					Flags:     EntryFlagPageImage,
					PageImage: page,
				})
			}

			markCLOG(rec.XID)

			// Parse subtransactions if XLOG_XACT_HAS_INFO (0x80) and XACT_XINFO_HAS_SUBXACTS (0x10) are set.
			const (
				xlogXactHasInfo      = 0x80
				xactXinfoHasDBInfo   = 0x0001
				xactXinfoHasSubxacts = 0x0010
			)
			if (rec.Info&xlogXactHasInfo) != 0 && len(rec.MainData) >= 12 {
				xinfo := binary.LittleEndian.Uint32(rec.MainData[8:12])
				off := 12
				if (xinfo & xactXinfoHasDBInfo) != 0 {
					off += 8
				}
				if (xinfo&xactXinfoHasSubxacts) != 0 && off+4 <= len(rec.MainData) {
					nsub := int(binary.LittleEndian.Uint32(rec.MainData[off : off+4]))
					off += 4
					for i := 0; i < nsub && off+4 <= len(rec.MainData); i++ {
						subXID := binary.LittleEndian.Uint32(rec.MainData[off : off+4])
						off += 4
						markCLOG(subXID)
					}
				}
			}
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
			t.relSizeKnown[sizeKey] = true
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
					t.relSizeKnown[sizeKey] = true
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

// PublishNewLayer publishes meta to the durable timeline layer manifest before making it visible in t.layers.
func (t *Timeline) PublishNewLayer(ctx context.Context, meta LayerMeta) error {
	return t.PublishNewLayers(ctx, []LayerMeta{meta})
}

// PublishNewLayers publishes a batch of already-uploaded layers in a single atomic manifest update,
// preserving manifest durability ordering while amortizing manifest PutObject overhead.
func (t *Timeline) PublishNewLayers(ctx context.Context, metas []LayerMeta) error {
	if len(metas) == 0 {
		return nil
	}
	t.manifestMu.Lock()
	defer t.manifestMu.Unlock()

	t.mu.RLock()
	next := make([]LayerMeta, len(t.layers)+len(metas))
	copy(next, t.layers)
	copy(next[len(t.layers):], metas)
	seq := t.layerSeq
	gen := t.generation
	t.mu.RUnlock()

	if t.store != nil {
		if err := t.store.PublishLayerManifestGen(ctx, t.tenantID, gen, t.epoch, next, seq); err != nil {
			return err
		}
	}
	for _, meta := range metas {
		t.AddLayer(meta)
	}
	return nil
}

// FlushInMemory freezes the current InMemoryLayer and writes it as an immutable L0 layer.
func (t *Timeline) FlushInMemory(ctx context.Context) (*LayerMeta, error) {
	if t.store == nil {
		return nil, nil
	}
	t.flushMu.Lock()
	defer t.flushMu.Unlock()

	t.mu.Lock()
	recs, startLSN, endLSN := t.inMem.SnapshotAll()
	if len(recs) == 0 {
		t.mu.Unlock()
		return nil, nil
	}
	oldMem := t.inMem
	t.frozenMems = append(t.frozenMems, oldMem)
	t.inMem = NewInMemoryLayer(endLSN)
	t.layerSeq++
	seq := t.layerSeq
	epoch := t.epoch
	tenantID := t.tenantID
	gen := t.generation
	t.mu.Unlock()

	fileName := fmt.Sprintf("l0_%06d_%016x_%016x.layer", seq, startLSN, endLSN)
	logicalKey := fmt.Sprintf("%s/timeline_%d/layers/%s", TenantR2Prefix(tenantID, gen), epoch, fileName)
	meta := LayerMeta{
		TenantID:   tenantID,
		Generation: gen,
		Epoch:      epoch,
		Kind:       LayerKindL0,
		Seq:        seq,
		FileName:   fileName,
		R2Key:      logicalKey,
		AADKey:     logicalKey,
		StartLSN:   startLSN,
		EndLSN:     endLSN,
	}
	written, err := t.store.WriteLayer(ctx, meta, recs)
	if err != nil {
		// Keep oldMem in frozenMems so concurrent reads never lose visibility,
		// and also re-index entries into inMem if needed on retry.
		t.mu.Lock()
		t.removeFrozenMemLocked(oldMem)
		if !errors.Is(err, ErrStaleGeneration) {
			for _, r := range recs {
				t.inMem.Put(r.Key, r.Delta)
			}
		}
		t.mu.Unlock()
		return nil, err
	}

	t.manifestMu.Lock()
	t.mu.RLock()
	next := make([]LayerMeta, len(t.layers)+1)
	copy(next, t.layers)
	next[len(t.layers)] = written
	seq = t.layerSeq
	t.mu.RUnlock()

	if err = t.store.PublishLayerManifestGen(ctx, t.tenantID, gen, t.epoch, next, seq); err != nil {
		t.manifestMu.Unlock()
		t.mu.Lock()
		t.removeFrozenMemLocked(oldMem)
		if !errors.Is(err, ErrStaleGeneration) {
			for _, r := range recs {
				t.inMem.Put(r.Key, r.Delta)
			}
		}
		t.mu.Unlock()
		return nil, err
	}
	t.mu.Lock()
	t.layers = append(t.layers, written)
	if written.EndLSN > t.lastLSN {
		t.lastLSN = written.EndLSN
	}
	t.removeFrozenMemLocked(oldMem)
	t.mu.Unlock()
	t.manifestMu.Unlock()
	return &written, nil
}

// GetPageAtLSN reconstructs the 8192-byte page for key as of lsn (0 = latest).
func (t *Timeline) GetPageAtLSN(ctx context.Context, key Key, lsn uint64) ([]byte, error) {
	if t.store != nil && t.store.readLimiter != nil {
		release, err := t.store.readLimiter.Acquire(ctx, t.tenantID)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	t.mu.RLock()
	inMemDeltas, foundBase := t.lookupInMemLayersLocked(key, lsn)
	var layersCopy []LayerMeta
	if !foundBase {
		layersCopy = append([]LayerMeta(nil), t.layers...)
	}
	t.mu.RUnlock()

	// Collect deltas from InMemoryLayer + relevant immutable layers.
	var collected []WALDelta
	collected = append(collected, inMemDeltas...)

	// Walk layers from newest to oldest; stop once we hit an Image layer or FPI.
	for i := len(layersCopy) - 1; i >= 0; i-- {
		lm := layersCopy[i]
		if lsn != 0 && lm.StartLSN > lsn {
			continue
		}
		if lm.EndKey != (Key{}) && (key.Compare(lm.StartKey) < 0 || key.Compare(lm.EndKey) > 0) {
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
	return reconstructPage(ctx, t.redoer, key, collected)
}

func prepareCollectedForRedo(collected []WALDelta) ([]byte, []WALDelta) {
	// Sort all collected deltas by LSN ascending and keep only from last FPI onward.
	sort.SliceStable(collected, func(i, j int) bool {
		if collected[i].LSN != collected[j].LSN {
			return collected[i].LSN < collected[j].LSN
		}
		// If a flushed page image and a WAL delta share the same LSN, order the image after the delta.
		iImg := (collected[i].Flags & EntryFlagPageImage) != 0
		jImg := (collected[j].Flags & EntryFlagPageImage) != 0
		return !iImg && jImg
	})
	var basePage []byte
	startIdx := 0
	for i := len(collected) - 1; i >= 0; i-- {
		if (collected[i].Flags&EntryFlagPageImage) != 0 && len(collected[i].PageImage) == PageSize {
			basePage = collected[i].PageImage
			startIdx = i + 1
			break
		}
	}
	return basePage, collected[startIdx:]
}

func reconstructPage(ctx context.Context, redoer WALRedoer, key Key, collected []WALDelta) ([]byte, error) {
	basePage, tail := prepareCollectedForRedo(collected)
	if len(tail) == 0 && len(basePage) == PageSize {
		return append([]byte(nil), basePage...), nil
	}
	return redoer.Redo(ctx, key, basePage, tail)
}

// GetPageBatch fetches up to nblocks consecutive blocks starting at startBlock
// using batched layer lookups and coalesced range reads.
func (t *Timeline) GetPageBatch(ctx context.Context, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	if nblocks == 0 {
		return nil, 0, nil
	}
	if t.store != nil && t.store.readLimiter != nil {
		release, err := t.store.readLimiter.Acquire(ctx, t.tenantID)
		if err != nil {
			return nil, 0, err
		}
		defer release()
	}
	n := int(nblocks)
	keys := make([]Key, n)
	collected := make([][]WALDelta, n)
	resolved := make([]bool, n)
	pendingCount := n

	t.mu.RLock()
	for i := 0; i < n; i++ {
		keys[i] = RelBlockKey(spcOID, dbOID, relNumber, forkNum, startBlock+uint32(i))
		deltas, isBase := t.lookupInMemLayersLocked(keys[i], lsn)
		if len(deltas) > 0 {
			collected[i] = deltas
			if isBase {
				resolved[i] = true
				pendingCount--
			}
		}
	}
	layersCopy := append([]LayerMeta(nil), t.layers...)
	t.mu.RUnlock()

	if pendingCount > 0 && t.store != nil {
		queryKeys := make([]Key, 0, pendingCount)
		queryIdx := make([]int, 0, pendingCount)

		for li := len(layersCopy) - 1; li >= 0 && pendingCount > 0; li-- {
			lm := layersCopy[li]
			if lsn != 0 && lm.StartLSN > lsn {
				continue
			}
			hasKeyRange := lm.EndKey != (Key{})
			queryKeys = queryKeys[:0]
			queryIdx = queryIdx[:0]
			for i := 0; i < n; i++ {
				if resolved[i] {
					continue
				}
				if hasKeyRange && (keys[i].Compare(lm.StartKey) < 0 || keys[i].Compare(lm.EndKey) > 0) {
					continue
				}
				queryKeys = append(queryKeys, keys[i])
				queryIdx = append(queryIdx, i)
			}
			if len(queryKeys) == 0 {
				continue
			}
			batchDeltas, err := t.store.LookupKeysInLayer(ctx, lm, queryKeys, lsn)
			if err != nil {
				return nil, 0, err
			}
			for qi, deltas := range batchDeltas {
				if len(deltas) == 0 {
					continue
				}
				bi := queryIdx[qi]
				collected[bi] = append(collected[bi], deltas...)
				last := deltas[len(deltas)-1]
				if ((last.Flags&EntryFlagPageImage) != 0 && len(last.PageImage) == PageSize) || lm.Kind == LayerKindImage {
					resolved[bi] = true
					pendingCount--
				}
			}
		}
	}

	out := make([]byte, 0, n*PageSize)
	var count uint32

	// Determine the contiguous prefix of blocks that have at least one delta.
	validN := 0
	for validN < n && len(collected[validN]) > 0 {
		validN++
	}
	if validN == 0 {
		return nil, 0, ErrNotFound
	}

	if batchRedoer, ok := t.redoer.(BatchWALRedoer); ok && validN > 1 {
		basePages := make([][]byte, validN)
		var redoReqs []WALRedoRequest
		var redoIdx []int
		for i := 0; i < validN; i++ {
			base, deltas := prepareCollectedForRedo(collected[i])
			if len(deltas) == 0 && len(base) == PageSize {
				basePages[i] = base
			} else {
				redoIdx = append(redoIdx, i)
				redoReqs = append(redoReqs, WALRedoRequest{
					Key:      keys[i],
					BasePage: base,
					Deltas:   deltas,
				})
			}
		}
		if len(redoReqs) > 0 {
			pages, errs := batchRedoer.RedoBatch(ctx, redoReqs)
			for ri, origIdx := range redoIdx {
				if errs[ri] != nil {
					if errs[ri] == ErrNotFound {
						// Truncate at first ErrNotFound; handled below.
						basePages[origIdx] = nil
						continue
					}
					return nil, 0, errs[ri]
				}
				basePages[origIdx] = pages[ri]
			}
		}
		for i := 0; i < validN; i++ {
			if len(basePages[i]) != PageSize {
				if i == 0 {
					return nil, 0, ErrNotFound
				}
				break
			}
			out = append(out, basePages[i]...)
			count++
		}
		return out, count, nil
	}

	for i := 0; i < validN; i++ {
		page, err := reconstructPage(ctx, t.redoer, keys[i], collected[i])
		if err != nil {
			if err == ErrNotFound {
				if i == 0 {
					return nil, 0, ErrNotFound
				}
				break
			}
			return nil, 0, err
		}
		out = append(out, page...)
		count++
	}
	return out, count, nil
}

// PutPageBatch ingests materialized 8192-byte pages flushed by air_smgr into the InMemoryLayer.
func (t *Timeline) PutPageBatch(ctx context.Context, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64, pages []byte) error {
	if len(pages) != int(nblocks)*PageSize {
		return fmt.Errorf("pageserver: PutPageBatch size mismatch: got %d want %d", len(pages), int(nblocks)*PageSize)
	}
	if err := t.EnforceInMemBudget(ctx); err != nil {
		return err
	}
	t.EnsureRelSizeSeeded(ctx, spcOID, dbOID, relNumber, forkNum)
	t.mu.Lock()

	useLSN := lsn
	if useLSN == 0 {
		useLSN = t.lastLSN
		if useLSN == 0 {
			useLSN = 1
		}
	}
	for i := uint32(0); i < nblocks; i++ {
		blk := startBlock + i
		off := int(i) * PageSize
		cp := make([]byte, PageSize)
		copy(cp, pages[off:off+PageSize])
		key := RelBlockKey(spcOID, dbOID, relNumber, forkNum, blk)
		t.inMem.Put(key, WALDelta{
			LSN:       useLSN,
			Flags:     EntryFlagPageImage,
			PageImage: cp,
		})
	}
	sizeKey := RelSizeKey(spcOID, dbOID, relNumber, forkNum)
	endBlk := startBlock + nblocks
	if cur := t.relSizes[sizeKey]; endBlk > cur {
		t.relSizes[sizeKey] = endBlk
		t.inMem.Put(sizeKey, WALDelta{
			LSN:     useLSN,
			Flags:   EntryFlagRelSize,
			RelSize: endBlk,
		})
	}
	approx := t.inMem.ApproxBytes
	t.mu.Unlock()
	if approx >= t.maxInMemBytes {
		t.MaybeFlushAsync()
	}
	return nil
}

// GetRelSize returns the number of blocks in a relation fork as of lsn.
func (t *Timeline) GetRelSize(ctx context.Context, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	sizeKey := RelSizeKey(spcOID, dbOID, relNumber, forkNum)
	isLatest := lsn == 0

	t.mu.RLock()
	if (isLatest || lsn >= t.lastLSN) && t.relSizeKnown[sizeKey] {
		if nb, ok := t.relSizes[sizeKey]; ok {
			t.mu.RUnlock()
			return nb, nil
		}
		t.mu.RUnlock()
		return 0, ErrNotFound
	}
	if lsn == 0 {
		lsn = t.lastLSN
	}
	inMemDeltas := t.inMem.Lookup(sizeKey, lsn)
	var frozenDelta *WALDelta
	if len(inMemDeltas) == 0 {
		for i := len(t.frozenMems) - 1; i >= 0; i-- {
			if fd := t.frozenMems[i].Lookup(sizeKey, lsn); len(fd) > 0 {
				d := fd[len(fd)-1]
				frozenDelta = &d
				break
			}
		}
	}
	layersCopy := append([]LayerMeta(nil), t.layers...)
	t.mu.RUnlock()

	if len(inMemDeltas) > 0 && (isLatest || !t.layerLookupNeededForSize(layersCopy, sizeKey, lsn)) {
		lastD := inMemDeltas[len(inMemDeltas)-1]
		if isLatest {
			// Check layers too if relSizeKnown is false, so a lower-block write after restart
			// cannot mask a larger size in immutable layers.
			t.mu.RLock()
			known := t.relSizeKnown[sizeKey]
			t.mu.RUnlock()
			if known {
				if (lastD.Flags & EntryFlagRelUnlinked) != 0 {
					return 0, ErrNotFound
				}
				return lastD.RelSize, nil
			}
		} else {
			if (lastD.Flags & EntryFlagRelUnlinked) != 0 {
				return 0, ErrNotFound
			}
			return lastD.RelSize, nil
		}
	}
	if frozenDelta != nil && !isLatest {
		if (frozenDelta.Flags & EntryFlagRelUnlinked) != 0 {
			return 0, ErrNotFound
		}
		return frozenDelta.RelSize, nil
	}

	var layerDelta *WALDelta
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
			d := deltas[len(deltas)-1]
			layerDelta = &d
			break
		}
	}

	if isLatest {
		t.mu.Lock()
		if len(inMemDeltas) > 0 {
			lastD := inMemDeltas[len(inMemDeltas)-1]
			if (lastD.Flags & EntryFlagRelUnlinked) != 0 {
				delete(t.relSizes, sizeKey)
				t.relSizeKnown[sizeKey] = true
				t.mu.Unlock()
				return 0, ErrNotFound
			}
			if layerDelta != nil && (layerDelta.Flags&EntryFlagRelUnlinked) == 0 && layerDelta.RelSize > lastD.RelSize {
				t.relSizes[sizeKey] = layerDelta.RelSize
			} else {
				t.relSizes[sizeKey] = lastD.RelSize
			}
			t.relSizeKnown[sizeKey] = true
			nb := t.relSizes[sizeKey]
			t.mu.Unlock()
			return nb, nil
		}
		if frozenDelta != nil {
			if (frozenDelta.Flags & EntryFlagRelUnlinked) != 0 {
				delete(t.relSizes, sizeKey)
				t.relSizeKnown[sizeKey] = true
				t.mu.Unlock()
				return 0, ErrNotFound
			}
			t.relSizes[sizeKey] = frozenDelta.RelSize
			t.relSizeKnown[sizeKey] = true
			t.mu.Unlock()
			return frozenDelta.RelSize, nil
		}
		if layerDelta != nil {
			if (layerDelta.Flags & EntryFlagRelUnlinked) != 0 {
				delete(t.relSizes, sizeKey)
				t.relSizeKnown[sizeKey] = true
				t.mu.Unlock()
				return 0, ErrNotFound
			}
			if cur, ok := t.relSizes[sizeKey]; !ok || layerDelta.RelSize > cur {
				t.relSizes[sizeKey] = layerDelta.RelSize
			}
			t.relSizeKnown[sizeKey] = true
			nb := t.relSizes[sizeKey]
			t.mu.Unlock()
			return nb, nil
		}
		t.mu.Unlock()
		return 0, ErrNotFound
	}

	if len(inMemDeltas) > 0 {
		lastD := inMemDeltas[len(inMemDeltas)-1]
		if (lastD.Flags & EntryFlagRelUnlinked) != 0 {
			return 0, ErrNotFound
		}
		return lastD.RelSize, nil
	}
	if frozenDelta != nil {
		if (frozenDelta.Flags & EntryFlagRelUnlinked) != 0 {
			return 0, ErrNotFound
		}
		return frozenDelta.RelSize, nil
	}
	if layerDelta != nil {
		if (layerDelta.Flags & EntryFlagRelUnlinked) != 0 {
			return 0, ErrNotFound
		}
		return layerDelta.RelSize, nil
	}
	return 0, ErrNotFound
}

// UnlinkRel records that a relation fork was dropped/unlinked so future size and existence
// queries return 0 / ErrNotFound and GetDBSize excludes it.
func (t *Timeline) UnlinkRel(spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) {
	sizeKey := RelSizeKey(spcOID, dbOID, relNumber, forkNum)
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.relSizes, sizeKey)
	t.relSizeKnown[sizeKey] = true
	if lsn == 0 {
		lsn = t.lastLSN + 1
	}
	if lsn > t.lastLSN {
		t.lastLSN = lsn
	}
	t.inMem.Put(sizeKey, WALDelta{
		LSN:     lsn,
		Flags:   EntryFlagRelSize | EntryFlagRelUnlinked,
		RelSize: 0,
	})
}

func (t *Timeline) seedAllRelSizes(ctx context.Context) {
	t.mu.RLock()
	if t.allRelSizesSeeded || t.store == nil {
		t.mu.RUnlock()
		return
	}
	layersCopy := append([]LayerMeta(nil), t.layers...)
	t.mu.RUnlock()

	seen := make(map[Key]bool)
	for i := len(layersCopy) - 1; i >= 0; i-- {
		keys, err := t.store.ListRelSizeKeysInLayer(ctx, layersCopy[i])
		if err != nil {
			continue
		}
		for _, k := range keys {
			if seen[k] {
				continue
			}
			seen[k] = true
			t.mu.RLock()
			known := t.relSizeKnown[k]
			t.mu.RUnlock()
			if !known {
				_, _ = t.GetRelSize(ctx, k.SpcOID, k.DbOID, k.RelNumber, k.ForkNum, 0)
			}
		}
	}
	t.mu.Lock()
	t.allRelSizesSeeded = true
	t.mu.Unlock()
}

func (t *Timeline) layerLookupNeededForSize(_ []LayerMeta, _ Key, _ uint64) bool {
	return false
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

// GetDBSize returns the byte size of a database across all its live tracked relation forks.
func (t *Timeline) GetDBSize(ctx context.Context, spcOID, dbOID uint32, _ uint64) (uint64, error) {
	t.seedAllRelSizes(ctx)
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
