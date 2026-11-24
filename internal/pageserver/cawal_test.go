package pageserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestCAWAL_DeduplicationAndIndependentRefs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cas := NewCAWALStore(dir)

	chunk := bytes.Repeat([]byte("PG17_WAL_RECORD_PAYLOAD_"), 512) // 12 KiB
	res1, err := cas.PutBlobIfAbsent(ctx, chunk)
	if err != nil {
		t.Fatalf("PutBlobIfAbsent 1: %v", err)
	}
	if res1.Deduplicated {
		t.Fatalf("expected first put not to be deduplicated")
	}

	res2, err := cas.PutBlobIfAbsent(ctx, chunk)
	if err != nil {
		t.Fatalf("PutBlobIfAbsent 2: %v", err)
	}
	if !res2.Deduplicated || res2.Hash != res1.Hash {
		t.Fatalf("expected second put to deduplicate with same hash: got dedup=%v hash=%s want=%s", res2.Deduplicated, res2.Hash, res1.Hash)
	}

	// Publish two independent manifests (different timelines & LSNs) pointing to the same blob.
	m1 := &CAWALManifest{
		TenantID:   "tenant-a",
		TimelineID: "tl-1",
		Epoch:      1,
		Refs: []CAWALRef{{
			TenantID:    "tenant-a",
			TimelineID:  "tl-1",
			Epoch:       1,
			Seq:         1,
			StartLSN:    0x1000,
			EndLSN:      0x2000,
			BlobHash:    res1.Hash,
			RawLen:      res1.RawLen,
			RecordCount: 10,
		}},
	}
	m2 := &CAWALManifest{
		TenantID:   "tenant-b",
		TimelineID: "tl-2",
		Epoch:      2,
		Refs: []CAWALRef{{
			TenantID:    "tenant-b",
			TimelineID:  "tl-2",
			Epoch:       2,
			Seq:         5,
			StartLSN:    0x9000,
			EndLSN:      0xA000,
			BlobHash:    res2.Hash,
			RawLen:      res2.RawLen,
			RecordCount: 10,
		}},
	}
	if err := cas.PublishManifest(ctx, m1); err != nil {
		t.Fatalf("PublishManifest m1: %v", err)
	}
	if err := cas.PublishManifest(ctx, m2); err != nil {
		t.Fatalf("PublishManifest m2: %v", err)
	}

	loaded1, err := cas.LoadManifest("tenant-a", "", 1)
	if err != nil || loaded1 == nil || len(loaded1.Refs) != 1 || loaded1.Refs[0].StartLSN != 0x1000 {
		t.Fatalf("LoadManifest m1 mismatch: %+v err=%v", loaded1, err)
	}
	loaded2, err := cas.LoadManifest("tenant-b", "", 2)
	if err != nil || loaded2 == nil || len(loaded2.Refs) != 1 || loaded2.Refs[0].StartLSN != 0x9000 {
		t.Fatalf("LoadManifest m2 mismatch: %+v err=%v", loaded2, err)
	}

	got, err := cas.GetBlobVerified(ctx, res1.Hash)
	if err != nil {
		t.Fatalf("GetBlobVerified: %v", err)
	}
	if !bytes.Equal(got, chunk) {
		t.Fatalf("verified payload mismatch")
	}

	metrics := cas.Metrics()
	if metrics.BlobWrites != 1 || metrics.DedupHits != 1 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}

func TestCAWAL_ConcurrentDuplicateWrites(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cas := NewCAWALStore(dir)

	chunk := bytes.Repeat([]byte("CONCURRENT_WAL_CHUNK_"), 1024)
	sum := sha256.Sum256(chunk)
	wantHash := hex.EncodeToString(sum[:])

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	hashes := make(chan string, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := cas.PutBlobIfAbsent(ctx, chunk)
			if err != nil {
				errs <- err
				return
			}
			hashes <- res.Hash
		}()
	}
	wg.Wait()
	close(errs)
	close(hashes)

	for err := range errs {
		t.Fatalf("concurrent PutBlobIfAbsent failed: %v", err)
	}
	for h := range hashes {
		if h != wantHash {
			t.Fatalf("hash mismatch: got %s want %s", h, wantHash)
		}
	}

	got, err := cas.GetBlobVerified(ctx, wantHash)
	if err != nil || !bytes.Equal(got, chunk) {
		t.Fatalf("GetBlobVerified after concurrent writes failed: err=%v", err)
	}
	m := cas.Metrics()
	if m.BlobWrites != 1 || m.DedupHits != workers-1 {
		t.Fatalf("expected 1 write and %d dedup hits, got %+v", workers-1, m)
	}
}

func TestCAWAL_FaultInjection_CrashScenarios(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cas := NewCAWALStore(dir)

	chunk1 := []byte("WAL_RECORD_1_DURABLE_COMMITTED")
	chunk2 := []byte("WAL_RECORD_2_CRASH_TEST")

	// 1. Process crash during blob write: partial temp file must not become visible.
	cas.faultHook = func(stage string) error {
		if stage == "during_blob_write" {
			return ErrCAWALFaultInjected
		}
		return nil
	}
	_, err := cas.PutBlobIfAbsent(ctx, chunk2)
	if !errors.Is(err, ErrCAWALFaultInjected) {
		t.Fatalf("expected fault during_blob_write, got %v", err)
	}
	sum2 := sha256.Sum256(chunk2)
	hash2 := hex.EncodeToString(sum2[:])
	if cas.HasBlob(hash2) {
		t.Fatalf("crashed blob write must not be visible")
	}

	// 2. Crash after blob persistence but before manifest commit.
	cas.faultHook = nil
	res1, err := cas.PutBlobIfAbsent(ctx, chunk1)
	if err != nil {
		t.Fatalf("PutBlobIfAbsent chunk1: %v", err)
	}
	cas.faultHook = func(stage string) error {
		if stage == "before_manifest_commit" {
			return ErrCAWALFaultInjected
		}
		return nil
	}
	m := &CAWALManifest{
		TenantID:   "t1",
		TimelineID: "tl1",
		Epoch:      1,
		Refs: []CAWALRef{{
			TenantID: "t1", TimelineID: "tl1", Epoch: 1, StartLSN: 100, EndLSN: 200,
			BlobHash: res1.Hash, RawLen: res1.RawLen,
		}},
	}
	if err := cas.PublishManifest(ctx, m); !errors.Is(err, ErrCAWALFaultInjected) {
		t.Fatalf("expected fault before_manifest_commit, got %v", err)
	}
	loaded, err := cas.LoadManifest("t1", "", 1)
	if err != nil || loaded != nil {
		t.Fatalf("manifest should not exist after pre-commit crash: loaded=%v err=%v", loaded, err)
	}
	// Orphan blob from uncommitted manifest should be cleaned by GC after grace period.
	gcRes, err := cas.GC(ctx, 0, nil)
	if err != nil || gcRes.DeletedBlobs != 1 {
		t.Fatalf("expected GC to reclaim 1 uncommitted blob, got %+v err=%v", gcRes, err)
	}

	// 3. Crash during manifest update preserves prior valid manifest.
	cas.faultHook = nil
	res1, err = cas.PutBlobIfAbsent(ctx, chunk1)
	if err != nil {
		t.Fatalf("PutBlobIfAbsent chunk1 retry: %v", err)
	}
	if err := cas.PublishManifest(ctx, m); err != nil {
		t.Fatalf("PublishManifest initial: %v", err)
	}
	res2, err := cas.PutBlobIfAbsent(ctx, chunk2)
	if err != nil {
		t.Fatalf("PutBlobIfAbsent chunk2: %v", err)
	}
	mUpdated := &CAWALManifest{
		TenantID:   "t1",
		TimelineID: "tl1",
		Epoch:      1,
		Refs: append(append([]CAWALRef(nil), m.Refs...), CAWALRef{
			TenantID: "t1", TimelineID: "tl1", Epoch: 1, StartLSN: 200, EndLSN: 300,
			BlobHash: res2.Hash, RawLen: res2.RawLen,
		}),
	}
	cas.faultHook = func(stage string) error {
		if stage == "during_manifest_update" {
			return ErrCAWALFaultInjected
		}
		return nil
	}
	if err := cas.PublishManifest(ctx, mUpdated); !errors.Is(err, ErrCAWALFaultInjected) {
		t.Fatalf("expected fault during_manifest_update, got %v", err)
	}
	loadedAfterCrash, err := cas.LoadManifest("t1", "", 1)
	if err != nil || loadedAfterCrash == nil || len(loadedAfterCrash.Refs) != 1 {
		t.Fatalf("expected original 1-ref manifest to remain intact, got %+v err=%v", loadedAfterCrash, err)
	}

	// 4. Missing blob referenced by metadata must fail PublishManifest.
	cas.faultHook = nil
	missingManifest := &CAWALManifest{
		TenantID:   "t1",
		TimelineID: "tl1",
		Epoch:      2,
		Refs: []CAWALRef{{
			TenantID: "t1", TimelineID: "tl1", Epoch: 2, StartLSN: 100, EndLSN: 200,
			BlobHash: "0000000000000000000000000000000000000000000000000000000000000000",
			RawLen:   10,
		}},
	}
	if err := cas.PublishManifest(ctx, missingManifest); !errors.Is(err, ErrCAWALBlobMissing) {
		t.Fatalf("expected ErrCAWALBlobMissing, got %v", err)
	}

	// 5. Corrupted blob payload must be rejected by GetBlobVerified.
	blobPath := cas.blobPath(res1.Hash)
	rawFile, err := os.ReadFile(blobPath)
	if err != nil {
		t.Fatalf("ReadFile blob: %v", err)
	}
	rawFile[len(rawFile)-1] ^= 0xFF
	if err := os.WriteFile(blobPath, rawFile, 0o600); err != nil {
		t.Fatalf("WriteFile corrupt blob: %v", err)
	}
	if _, err := cas.GetBlobVerified(ctx, res1.Hash); !errors.Is(err, ErrCAWALIntegrityMismatch) {
		t.Fatalf("expected ErrCAWALIntegrityMismatch on corrupt blob, got %v", err)
	}
}

func TestCAWAL_GC_PinProtectionInterruptAndMemoryPressure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cas := NewCAWALStore(dir)

	pinnedChunk := []byte("PINNED_INFLIGHT_WAL_CHUNK")
	orphanChunk1 := []byte("ORPHAN_WAL_CHUNK_1")
	orphanChunk2 := []byte("ORPHAN_WAL_CHUNK_2")

	rPin, err := cas.PutBlobIfAbsent(ctx, pinnedChunk)
	if err != nil {
		t.Fatalf("put pinned: %v", err)
	}
	cas.Pin(rPin.Hash)

	rOrph1, err := cas.PutBlobIfAbsent(ctx, orphanChunk1)
	if err != nil {
		t.Fatalf("put orphan1: %v", err)
	}
	rOrph2, err := cas.PutBlobIfAbsent(ctx, orphanChunk2)
	if err != nil {
		t.Fatalf("put orphan2: %v", err)
	}

	// Memory pressure abort prevents GC from running aggressively.
	gcAbort, err := cas.GC(ctx, 0, func() bool { return true })
	if err != nil || !gcAbort.Aborted || gcAbort.DeletedBlobs != 0 {
		t.Fatalf("expected aborted GC under memory pressure, got %+v err=%v", gcAbort, err)
	}

	// Interrupted GC mid-sweep deletes first orphan, then second run is idempotent and finishes.
	sweeps := 0
	cas.faultHook = func(stage string) error {
		if stage == "during_gc_sweep" {
			sweeps++
			if sweeps == 2 {
				return ErrCAWALFaultInjected
			}
		}
		return nil
	}
	gcPart, err := cas.GC(ctx, 0, nil)
	if !errors.Is(err, ErrCAWALFaultInjected) || !gcPart.Aborted {
		t.Fatalf("expected interrupted GC, got %+v err=%v", gcPart, err)
	}
	if !cas.HasBlob(rPin.Hash) {
		t.Fatalf("pinned blob must never be deleted by GC")
	}

	cas.faultHook = nil
	gcDone, err := cas.GC(ctx, 0, nil)
	if err != nil {
		t.Fatalf("final GC failed: %v", err)
	}
	if !cas.HasBlob(rPin.Hash) {
		t.Fatalf("pinned blob must still exist after GC")
	}
	if cas.HasBlob(rOrph1.Hash) || cas.HasBlob(rOrph2.Hash) {
		t.Fatalf("orphan blobs should have been deleted, gcDone=%+v", gcDone)
	}

	// Unpin and re-run GC: now pinnedChunk becomes unreachable and is cleaned.
	cas.Unpin(rPin.Hash)
	gcFinal, err := cas.GC(ctx, 0, nil)
	if err != nil || gcFinal.DeletedBlobs != 1 {
		t.Fatalf("expected 1 deleted blob after unpin, got %+v err=%v", gcFinal, err)
	}
}

func TestCAWAL_PageserverIntegrationAndBackwardCompat(t *testing.T) {
	ctx := context.Background()
	layerDir := filepath.Join(t.TempDir(), "layers")

	// 1. Start Pageserver with EnableCAWAL=false (legacy V2 Zstd WAL table).
	ps1, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		EnableCAWAL:       false,
	})
	if err != nil {
		t.Fatalf("New ps1: %v", err)
	}

	tl1 := ps1.EnsureTenant("tenant-1", 1)
	key1 := RelBlockKey(1663, 5, 16385, 0, 0)
	basePage := make([]byte, PageSize)
	copy(basePage, "BASE_PAGE_V1")
	tl1.PutPage(key1, 100, basePage)
	if _, err := tl1.FlushInMemory(ctx); err != nil {
		t.Fatalf("FlushInMemory legacy: %v", err)
	}

	// 2. Enable CA-WAL and flush non-FPI WAL records.
	ps1.Store().SetEnableCAWAL(true)
	walPayload := bytes.Repeat([]byte("XLOG_HEAP_INSERT_RECORD_"), 64)
	tl1.inMem.Put(key1, WALDelta{
		LSN:       200,
		Flags:     EntryFlagWALRecord,
		WALRecord: walPayload,
	})
	meta2, err := tl1.FlushInMemory(ctx)
	if err != nil {
		t.Fatalf("FlushInMemory CA-WAL: %v", err)
	}
	if len(meta2.CAWALBlobHashes) != 1 {
		t.Fatalf("expected 1 CAWALBlobHash on flushed layer, got %v", meta2.CAWALBlobHashes)
	}
	if err := ps1.Close(); err != nil {
		t.Fatalf("ps1.Close: %v", err)
	}

	// 3. Restart Pageserver with EnableCAWAL=false (rollback mode) and verify both layers are readable!
	ps2, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		EnableCAWAL:       false,
	})
	if err != nil {
		t.Fatalf("New ps2 (rollback): %v", err)
	}
	defer ps2.Close()

	tl2, err := ps2.getTimeline("tenant-1")
	if err != nil || tl2.LastLSN() < 200 {
		t.Fatalf("recovered timeline invalid: tl=%v err=%v", tl2, err)
	}
	deltas, err := ps2.Store().LookupKeyInLayer(ctx, *meta2, key1, 200)
	if err != nil {
		t.Fatalf("LookupKeyInLayer CA-WAL after rollback: %v", err)
	}
	if len(deltas) != 1 || !bytes.Equal(deltas[0].WALRecord, walPayload) {
		t.Fatalf("WALRecord mismatch after recovery+rollback: got %d deltas", len(deltas))
	}

	// Verify GC preserves the CA-WAL blob referenced by timeline_1_layer_manifest.json.
	gcRes, err := ps2.Store().CAWALStore().GC(ctx, 0, nil)
	if err != nil {
		t.Fatalf("GC after recovery: %v", err)
	}
	if gcRes.DeletedBlobs != 0 || !ps2.Store().CAWALStore().HasBlob(meta2.CAWALBlobHashes[0]) {
		t.Fatalf("GC deleted reachable layer CA-WAL blob! gcRes=%+v", gcRes)
	}
	if _, err := fmt.Fprintf(os.Stdout, ""); err != nil {
		t.Fatal(err)
	}
}
