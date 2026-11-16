package pageserver

import (
	"context"
	"fmt"
	"sort"
)

const (
	DefaultL0CompactionThreshold = 4
)

// CompactIfNeeded compacts L0 delta layers into an L1 layer when >= threshold L0s exist,
// and optionally materializes a base Image layer at the latest LSN.
func (t *Timeline) CompactIfNeeded(ctx context.Context, threshold int, materializeImage bool) (*LayerMeta, error) {
	if threshold <= 0 {
		threshold = DefaultL0CompactionThreshold
	}
	t.mu.RLock()
	var l0s []LayerMeta
	var keep []LayerMeta
	for _, lm := range t.layers {
		if lm.Kind == LayerKindL0 {
			l0s = append(l0s, lm)
		} else {
			keep = append(keep, lm)
		}
	}
	t.mu.RUnlock()

	if len(l0s) < threshold {
		return nil, nil
	}

	var merged []LayerRecord
	var minLSN, maxLSN uint64
	for _, lm := range l0s {
		if minLSN == 0 || lm.StartLSN < minLSN {
			minLSN = lm.StartLSN
		}
		if lm.EndLSN > maxLSN {
			maxLSN = lm.EndLSN
		}
		recs, err := t.readAllRecords(ctx, lm)
		if err != nil {
			return nil, err
		}
		merged = append(merged, recs...)
	}

	sort.Slice(merged, func(i, j int) bool {
		if c := merged[i].Key.Compare(merged[j].Key); c != 0 {
			return c < 0
		}
		return merged[i].Delta.LSN < merged[j].Delta.LSN
	})

	t.mu.Lock()
	t.layerSeq++
	seq := t.layerSeq
	epoch := t.epoch
	tenantID := t.tenantID
	t.mu.Unlock()

	kind := LayerKindL1
	prefix := "l1"
	outRecs := merged
	if materializeImage {
		kind = LayerKindImage
		prefix = "img"
		atLSN := maxLSN
		if atLSN > minLSN {
			atLSN = maxLSN - 1
		}
		outRecs = t.materializeLatest(ctx, merged, atLSN)
	}

	fileName := fmt.Sprintf("%s_%06d_%016x_%016x.layer", prefix, seq, minLSN, maxLSN)
	logicalKey := fmt.Sprintf("tenants/%s/timeline_%d/layers/%s", tenantID, epoch, fileName)
	meta := LayerMeta{
		TenantID: tenantID,
		Epoch:    epoch,
		Kind:     kind,
		Seq:      seq,
		FileName: fileName,
		R2Key:    logicalKey,
		AADKey:   logicalKey,
		StartLSN: minLSN,
		EndLSN:   maxLSN,
	}
	written, err := t.store.WriteLayer(ctx, meta, outRecs)
	if err != nil {
		return nil, err
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
	curSeq := t.layerSeq
	t.mu.RUnlock()

	if t.store != nil {
		if err := t.store.PublishLayerManifest(ctx, tenantID, epoch, updated, curSeq); err != nil {
			return nil, err
		}
	}

	t.mu.Lock()
	t.layers = updated
	t.mu.Unlock()

	return &written, nil
}

func (t *Timeline) readAllRecords(ctx context.Context, meta LayerMeta) ([]LayerRecord, error) {
	footerRaw, err := t.store.ReadRange(ctx, meta, meta.FileSize-int64(LayerFooterSize), int64(LayerFooterSize))
	if err != nil {
		return nil, err
	}
	footer, err := DecodeLayerFooter(footerRaw)
	if err != nil {
		return nil, err
	}
	if footer.EntryCount == 0 {
		return nil, nil
	}
	idxRaw, err := t.store.ReadRange(ctx, meta, int64(footer.IndexOffset), int64(footer.IndexLen))
	if err != nil {
		return nil, err
	}
	entries, err := DecodeLayerIndex(idxRaw, meta, footer.EntryCount, t.store.kms)
	if err != nil {
		return nil, err
	}
	out := make([]LayerRecord, 0, len(entries))
	for i, e := range entries {
		raw, err := t.store.ReadRange(ctx, meta, int64(e.Offset), int64(e.Length))
		if err != nil {
			return nil, err
		}
		if meta.Encrypted {
			raw, err = t.store.kms.DecryptObject(meta.TenantID, meta.Epoch, fmt.Sprintf("%s#e%d", meta.aadKey(), i), raw)
			if err != nil {
				return nil, err
			}
		}
		d, err := decodeDeltaPayload(raw, e.LSN, e.Flags)
		if err != nil {
			return nil, err
		}
		out = append(out, LayerRecord{Key: e.Key, Delta: d})
	}
	return out, nil
}

func (t *Timeline) materializeLatest(ctx context.Context, sorted []LayerRecord, atLSN uint64) []LayerRecord {
	var out []LayerRecord
	for i := 0; i < len(sorted); {
		k := sorted[i].Key
		j := i + 1
		for j < len(sorted) && sorted[j].Key.Compare(k) == 0 {
			j++
		}
		group := sorted[i:j]
		if k.Kind == KeyKindRelSize {
			last := group[len(group)-1]
			last.Delta.LSN = atLSN
			out = append(out, last)
		} else if k.Kind == KeyKindRelBlock {
			deltas := make([]WALDelta, len(group))
			for idx, r := range group {
				deltas[idx] = r.Delta
			}
			if page, err := t.redoer.Redo(ctx, k, nil, deltas); err == nil && len(page) == PageSize {
				out = append(out, LayerRecord{
					Key: k,
					Delta: WALDelta{
						LSN:       atLSN,
						Flags:     EntryFlagPageImage,
						PageImage: page,
					},
				})
			} else {
				// Keep raw deltas if base page is in an older layer.
				out = append(out, group...)
			}
		} else {
			out = append(out, group...)
		}
		i = j
	}
	return out
}
