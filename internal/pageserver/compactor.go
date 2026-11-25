package pageserver

import (
	"container/heap"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	DefaultL0CompactionThreshold = 4
	compactV1BatchEntries        = 64
	maxDeltaChainBeforeFPI       = 256
)

// CompactIfNeeded compacts L0 delta layers into an L1 (or Image) layer using a bounded-memory
// streaming K-way merge and streaming V2 output encoding.
func (t *Timeline) CompactIfNeeded(ctx context.Context, threshold int, materializeImage bool) (*LayerMeta, error) {
	return t.compactL0s(ctx, threshold, 0, materializeImage)
}

// CompactL0Batch compacts at most maxBatch oldest L0 layers into an L1 layer to bound
// memory usage during background compaction.
func (t *Timeline) CompactL0Batch(ctx context.Context, threshold, maxBatch int) (*LayerMeta, error) {
	return t.compactL0s(ctx, threshold, maxBatch, false)
}

func (t *Timeline) compactL0s(ctx context.Context, threshold, maxBatch int, materializeImage bool) (*LayerMeta, error) {
	t.compactMu.Lock()
	defer t.compactMu.Unlock()

	if threshold <= 0 {
		threshold = DefaultL0CompactionThreshold
	}
	t.mu.RLock()
	var l0s []LayerMeta
	for _, lm := range t.layers {
		if lm.Kind == LayerKindL0 {
			l0s = append(l0s, lm)
		}
	}
	t.mu.RUnlock()

	if len(l0s) < threshold {
		return nil, nil
	}
	sort.Slice(l0s, func(i, j int) bool {
		if l0s[i].StartLSN != l0s[j].StartLSN {
			return l0s[i].StartLSN < l0s[j].StartLSN
		}
		return l0s[i].Seq < l0s[j].Seq
	})
	if !materializeImage && maxBatch > 0 && len(l0s) > maxBatch {
		l0s = l0s[:maxBatch]
	}
	start := time.Now()
	debugf("compactor", "CompactIfNeeded start tenant=%s l0_count=%d threshold=%d materialize_image=%v", t.tenantID, len(l0s), threshold, materializeImage)

	var minLSN, maxLSN uint64
	iters := make([]*layerRecordIterator, 0, len(l0s))
	for _, lm := range l0s {
		if minLSN == 0 || lm.StartLSN < minLSN {
			minLSN = lm.StartLSN
		}
		if lm.EndLSN > maxLSN {
			maxLSN = lm.EndLSN
		}
		it, err := newLayerRecordIterator(ctx, t.store, lm)
		if err != nil {
			return nil, err
		}
		iters = append(iters, it)
	}

	merger, err := newKWayLayerMerger(ctx, iters)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.layerSeq++
	seq := t.layerSeq
	epoch := t.epoch
	tenantID := t.tenantID
	gen := t.generation
	t.mu.Unlock()

	kind := LayerKindL1
	prefix := "l1"
	var atLSN uint64
	if materializeImage {
		kind = LayerKindImage
		prefix = "img"
		atLSN = maxLSN
		if atLSN > minLSN {
			atLSN = maxLSN - 1
		}
	}

	fileName := fmt.Sprintf("%s_%06d_%016x_%016x.layer", prefix, seq, minLSN, maxLSN)
	logicalKey := fmt.Sprintf("%s/timeline_%d/layers/%s", TenantR2Prefix(tenantID, gen), epoch, fileName)
	meta := LayerMeta{
		TenantID:   tenantID,
		Generation: gen,
		Epoch:      epoch,
		Kind:       kind,
		Seq:        seq,
		FileName:   fileName,
		R2Key:      logicalKey,
		AADKey:     logicalKey,
		StartLSN:   minLSN,
		EndLSN:     maxLSN,
		Encrypted:  t.store != nil && t.store.kms != nil,
	}

	var kms = t.store.kms
	written, err := t.store.writeLayerFromStream(ctx, meta, func(sw *v2StreamWriter) error {
		var group []LayerRecord
		flushGroup := func() error {
			if len(group) == 0 {
				return nil
			}
			outRecs := t.compactKeyGroup(ctx, group, atLSN, materializeImage)
			for _, r := range outRecs {
				if err := sw.Add(r); err != nil {
					return err
				}
			}
			group = group[:0]
			return nil
		}

		for {
			rec, ok, err := merger.Next(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			if len(group) > 0 && group[0].Key.Compare(rec.Key) != 0 {
				if err := flushGroup(); err != nil {
					return err
				}
			}
			group = append(group, rec)
		}
		return flushGroup()
	})
	_ = kms
	if err != nil {
		return nil, err
	}
	if t.store != nil && t.store.cawal != nil {
		for _, h := range written.CAWALBlobHashes {
			defer t.store.cawal.Unpin(h)
		}
	}

	t.manifestMu.Lock()
	defer t.manifestMu.Unlock()

	t.mu.RLock()
	l0Set := make(map[string]struct{}, len(l0s))
	for _, lm := range l0s {
		l0Set[lm.R2Key] = struct{}{}
	}
	var updated []LayerMeta
	for _, lm := range t.layers {
		if _, compacted := l0Set[lm.R2Key]; !compacted {
			updated = append(updated, lm)
		}
	}
	updated = append(updated, written)
	sort.Slice(updated, func(i, j int) bool {
		if updated[i].EndLSN != updated[j].EndLSN {
			return updated[i].EndLSN < updated[j].EndLSN
		}
		if updated[i].StartLSN != updated[j].StartLSN {
			return updated[i].StartLSN < updated[j].StartLSN
		}
		return updated[i].Seq < updated[j].Seq
	})
	curSeq := t.layerSeq
	t.mu.RUnlock()

	if t.store != nil {
		if err := t.store.PublishLayerManifestGen(ctx, tenantID, gen, epoch, updated, curSeq); err != nil {
			return nil, err
		}
	}

	t.mu.Lock()
	t.layers = updated
	t.mu.Unlock()

	if t.store != nil && t.store.idxCache != nil {
		for _, lm := range l0s {
			t.store.idxCache.evict(lm.R2Key)
		}
	}

	debugf("compactor", "CompactIfNeeded done tenant=%s compacted_l0=%d out_file=%s kind=%s size=%d lsn=%s..%s total_layers=%d dur=%s",
		tenantID, len(l0s), written.FileName, written.Kind, written.FileSize, formatLSNHex(minLSN), formatLSNHex(maxLSN), len(updated), time.Since(start))
	return &written, nil
}

// writeLayerFromStream streams records produced by feed(sw) directly into a V2 layer file
// with bounded memory, incremental SHA-256, optional CAU upload, and optional DiskCache population.
func (s *LayerStore) writeLayerFromStream(ctx context.Context, meta LayerMeta, feed func(sw *v2StreamWriter) error) (LayerMeta, error) {
	if err := s.checkGeneration(meta.TenantID, meta.Generation); err != nil {
		return meta, err
	}
	meta.Encrypted = s.kms != nil
	if meta.Encrypted && meta.AADKey == "" {
		if meta.R2Key != "" {
			meta.AADKey = meta.R2Key
		} else {
			meta.AADKey = fmt.Sprintf("%s/timeline_%d/layers/%s", TenantR2Prefix(meta.TenantID, meta.Generation), meta.Epoch, meta.FileName)
		}
	}

	var localPath string
	var tmpPath string
	var cleanupTemp bool
	var outFile *os.File
	var err error

	if s.localDir != "" {
		localPath = s.localLayerPath(meta.TenantID, meta.Generation, meta.FileName)
		if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
			return meta, err
		}
		tmpPath = localPath + ".tmp"
		outFile, err = os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return meta, err
		}
	} else {
		outFile, err = os.CreateTemp("", "air-stream-layer-*.layer")
		if err != nil {
			return meta, err
		}
		localPath = outFile.Name()
		cleanupTemp = true
	}

	cleanupOnErr := func() {
		if outFile != nil {
			_ = outFile.Close()
		}
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		} else if localPath != "" {
			_ = os.Remove(localPath)
		}
	}
	if cleanupTemp {
		defer os.Remove(localPath)
	}

	sw := newV2StreamWriter(outFile, meta.TenantID, meta.Epoch, meta.aadKey(), meta.Kind, meta.EndLSN, s.kms, v2EncodeOptions{
		CAWAL: s.activeCAWALStore(),
		Ctx:   ctx,
	})
	if err := feed(sw); err != nil {
		cleanupOnErr()
		return meta, err
	}
	res, err := sw.Finish()
	if err != nil {
		cleanupOnErr()
		return meta, err
	}
	if err := outFile.Sync(); err != nil {
		cleanupOnErr()
		return meta, err
	}
	if err := outFile.Close(); err != nil {
		cleanupOnErr()
		return meta, err
	}
	outFile = nil

	meta.FileSize = res.FileSize
	meta.SHA256 = res.SHA256Hex
	meta.StartKey = res.StartKey
	meta.EndKey = res.EndKey
	meta.CAWALBlobHashes = res.CAWALBlobHashes

	uploadPath := localPath
	if tmpPath != "" {
		uploadPath = tmpPath
	}
	if s.s3c != nil && s.bucket != "" {
		meta, err = s.uploadLayerFilePrehashed(ctx, meta, uploadPath, meta.SHA256, meta.FileSize)
		if err != nil {
			if tmpPath != "" {
				_ = os.Remove(tmpPath)
			}
			return meta, err
		}
	} else if cauKey, kErr := ContentAddressedLayerKey(meta.TenantID, meta.SHA256); kErr == nil {
		meta.R2Key = cauKey
	}

	if tmpPath != "" {
		if err := os.Rename(tmpPath, localPath); err != nil {
			_ = os.Remove(tmpPath)
			return meta, err
		}
	}

	if s.cache != nil && s.localDir == "" {
		if raw, rErr := os.ReadFile(localPath); rErr == nil {
			_ = s.cache.Put(meta.R2Key, raw)
		}
	}
	return meta, nil
}

func (t *Timeline) compactKeyGroup(ctx context.Context, group []LayerRecord, atLSN uint64, materializeImage bool) []LayerRecord {
	if len(group) == 0 {
		return nil
	}
	k := group[0].Key
	if k.Kind == KeyKindRelSize {
		if !materializeImage {
			return group
		}
		last := group[len(group)-1]
		last.Delta.LSN = atLSN
		return []LayerRecord{last}
	}
	if k.Kind != KeyKindRelBlock {
		return group
	}

	if !materializeImage {
		// Count trailing non-FPI deltas in this key group; materialize a checkpoint FPI
		// when the chain exceeds maxDeltaChainBeforeFPI so read-path WAL redo stays bounded
		// while preserving all historical deltas for MVCC snapshots.
		pending := 0
		for i := len(group) - 1; i >= 0; i-- {
			d := group[i].Delta
			if (d.Flags&EntryFlagPageImage) != 0 && len(d.PageImage) == PageSize {
				break
			}
			pending++
		}
		if pending < maxDeltaChainBeforeFPI {
			return group
		}
		page, ok := t.tryMaterializeGroupPage(ctx, k, group)
		if !ok {
			return group
		}
		lastLSN := group[len(group)-1].Delta.LSN
		out := make([]LayerRecord, len(group)+1)
		copy(out, group)
		out[len(group)] = LayerRecord{
			Key: k,
			Delta: WALDelta{
				LSN:       lastLSN,
				Flags:     EntryFlagPageImage,
				PageImage: page,
			},
		}
		return out
	}

	if page, ok := t.tryMaterializeGroupPage(ctx, k, group); ok {
		return []LayerRecord{{
			Key: k,
			Delta: WALDelta{
				LSN:       atLSN,
				Flags:     EntryFlagPageImage,
				PageImage: page,
			},
		}}
	}
	// Keep raw deltas if base page is unavailable.
	return group
}

func (t *Timeline) tryMaterializeGroupPage(ctx context.Context, k Key, group []LayerRecord) ([]byte, bool) {
	deltas := make([]WALDelta, len(group))
	for idx, r := range group {
		deltas[idx] = r.Delta
	}
	basePage, tail := prepareCollectedForRedo(deltas)
	if len(tail) == 0 && len(basePage) == PageSize {
		return append([]byte(nil), basePage...), true
	}
	if len(basePage) != PageSize && len(tail) > 0 && (tail[0].Flags&EntryFlagWillInit) == 0 {
		firstLSN := tail[0].LSN
		if firstLSN > 1 {
			if prev, err := t.GetPageAtLSN(ctx, k, firstLSN-1); err == nil && len(prev) == PageSize {
				basePage = prev
			}
		}
	}
	page, err := t.redoer.Redo(ctx, k, basePage, tail)
	if err != nil || len(page) != PageSize {
		return nil, false
	}
	return page, true
}

// layerRecordIterator streams sorted records from a single V1 or V2 layer with bounded memory.
type layerRecordIterator struct {
	store   *LayerStore
	meta    LayerMeta
	idxData *layerIndexData
	pos     int
	buf     []LayerRecord
	bufIdx  int

	// V2 state
	walLoaded bool
	walTable  [][]byte
}

func newLayerRecordIterator(ctx context.Context, store *LayerStore, meta LayerMeta) (*layerRecordIterator, error) {
	if store == nil {
		return nil, fmt.Errorf("pageserver: nil LayerStore in iterator")
	}
	idxData, err := store.loadLayerIndex(ctx, meta)
	if err != nil {
		return nil, err
	}
	return &layerRecordIterator{
		store:   store,
		meta:    meta,
		idxData: idxData,
	}, nil
}

func (it *layerRecordIterator) Next(ctx context.Context) (LayerRecord, bool, error) {
	for it.bufIdx >= len(it.buf) {
		if it.pos >= len(it.idxData.entries) {
			return LayerRecord{}, false, nil
		}
		if err := it.fillNextBatch(ctx); err != nil {
			return LayerRecord{}, false, err
		}
	}
	rec := it.buf[it.bufIdx]
	it.bufIdx++
	return rec, true, nil
}

func (it *layerRecordIterator) fillNextBatch(ctx context.Context) error {
	it.buf = it.buf[:0]
	it.bufIdx = 0

	if it.idxData.version == LayerVersionV2 {
		// Read one V2 chunk at a time (bounded to ~64 KiB).
		firstEntry := it.idxData.entries[it.pos]
		cIdx := uint32(firstEntry.Offset)
		if int(cIdx) >= len(it.idxData.chunks) {
			return fmt.Errorf("pageserver: v2 chunk %d out of bounds", cIdx)
		}
		cm := it.idxData.chunks[cIdx]
		cipherBuf, err := it.store.ReadRange(ctx, it.meta, int64(cm.Offset), int64(cm.CipherLen))
		if err != nil {
			return err
		}
		chunkRaw, err := decodeV2ChunkRaw(cipherBuf, it.meta, int(cIdx), cm, it.store.kms)
		if err != nil {
			return err
		}

		for it.pos < len(it.idxData.entries) && uint32(it.idxData.entries[it.pos].Offset) == cIdx {
			e := it.idxData.entries[it.pos]
			delta, walIDPlusOne, err := extractV2RecordFromChunk(chunkRaw, e.Length, e.LSN, uint64(e.Flags))
			if err != nil {
				return err
			}
			if walIDPlusOne > 0 {
				if !it.walLoaded {
					wt, wErr := it.store.loadV2WALTable(ctx, it.meta, it.idxData)
					if wErr != nil {
						return wErr
					}
					it.walTable = wt
					it.walLoaded = true
				}
				wIdx := int(walIDPlusOne - 1)
				if wIdx >= len(it.walTable) {
					return fmt.Errorf("pageserver: v2 WAL index %d out of bounds", wIdx)
				}
				delta.WALRecord = it.walTable[wIdx]
			}
			it.buf = append(it.buf, LayerRecord{Key: e.Key, Delta: delta})
			it.pos++
		}
		return nil
	}

	// V1 bounded batch read.
	startIdx := it.pos
	endIdx := startIdx + 1
	spanStart := int64(it.idxData.entries[startIdx].Offset)
	spanEnd := spanStart + int64(it.idxData.entries[startIdx].Length)
	for endIdx < len(it.idxData.entries) && endIdx-startIdx < compactV1BatchEntries {
		e := it.idxData.entries[endIdx]
		nextStart := int64(e.Offset)
		nextEnd := nextStart + int64(e.Length)
		if nextStart < spanStart || nextEnd-spanStart > maxCoalescedSpan || nextStart-spanEnd > coalesceGapBytes {
			break
		}
		if nextEnd > spanEnd {
			spanEnd = nextEnd
		}
		endIdx++
	}

	spanBuf, err := it.store.ReadRange(ctx, it.meta, spanStart, spanEnd-spanStart)
	if err != nil {
		return err
	}
	for i := startIdx; i < endIdx; i++ {
		e := it.idxData.entries[i]
		relOff := int64(e.Offset) - spanStart
		raw := spanBuf[relOff : relOff+int64(e.Length)]
		if it.meta.Encrypted {
			raw, err = it.store.kms.DecryptObject(it.meta.TenantID, it.meta.Epoch, fmt.Sprintf("%s#e%d", it.meta.aadKey(), i), raw)
			if err != nil {
				return err
			}
		}
		d, err := decodeDeltaPayload(raw, e.LSN, e.Flags)
		if err != nil {
			return err
		}
		it.buf = append(it.buf, LayerRecord{Key: e.Key, Delta: d})
	}
	it.pos = endIdx
	return nil
}

type mergeHeapItem struct {
	rec     LayerRecord
	iterIdx int
}

type mergeHeap []mergeHeapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := h[i].rec.Key.Compare(h[j].rec.Key); c != 0 {
		return c < 0
	}
	if h[i].rec.Delta.LSN != h[j].rec.Delta.LSN {
		return h[i].rec.Delta.LSN < h[j].rec.Delta.LSN
	}
	return h[i].iterIdx < h[j].iterIdx
}
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)         { *h = append(*h, x.(mergeHeapItem)) }
func (h *mergeHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

type kwayLayerMerger struct {
	iters []*layerRecordIterator
	h     mergeHeap
}

func newKWayLayerMerger(ctx context.Context, iters []*layerRecordIterator) (*kwayLayerMerger, error) {
	m := &kwayLayerMerger{iters: iters}
	for idx, it := range iters {
		rec, ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			m.h = append(m.h, mergeHeapItem{rec: rec, iterIdx: idx})
		}
	}
	heap.Init(&m.h)
	return m, nil
}

func (m *kwayLayerMerger) Next(ctx context.Context) (LayerRecord, bool, error) {
	if len(m.h) == 0 {
		return LayerRecord{}, false, nil
	}
	top := heap.Pop(&m.h).(mergeHeapItem)
	nextRec, ok, err := m.iters[top.iterIdx].Next(ctx)
	if err != nil {
		return LayerRecord{}, false, err
	}
	if ok {
		heap.Push(&m.h, mergeHeapItem{rec: nextRec, iterIdx: top.iterIdx})
	}
	return top.rec, true, nil
}
