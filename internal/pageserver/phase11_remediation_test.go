package pageserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestPhase11LargeWALTableV2Layer(t *testing.T) {
	dir := t.TempDir()
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(dir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("t_large_wal"); err != nil {
		t.Fatal(err)
	}
	store := NewLayerStore(dir, nil, "", kms, nil)

	// Construct a V2 layer whose uncompressed WAL table exceeds maxV2DecompressedSize (256 MiB)
	// by using 270 distinct 1 MiB WAL records plus FPI page images.
	const numRecords = 270
	recs := make([]LayerRecord, 0, numRecords)
	for i := 0; i < numRecords; i++ {
		page := make([]byte, PageSize)
		page[0] = byte(i)
		page[PageSize-1] = byte(i >> 8)
		wal := make([]byte, 1024*1024)
		wal[0] = byte(i)
		wal[1] = byte(i >> 8)
		wal[len(wal)-1] = byte(i + 1)
		recs = append(recs, LayerRecord{
			Key: RelBlockKey(1663, 16385, 1000, 0, uint32(i)),
			Delta: WALDelta{
				LSN:       uint64(1000 + i),
				Flags:     EntryFlagPageImage | EntryFlagWALRecord,
				PageImage: page,
				WALRecord: wal,
			},
		})
	}

	meta := LayerMeta{
		TenantID: "t_large_wal",
		Epoch:    1,
		Kind:     LayerKindL0,
		Seq:      1,
		FileName: "l0_large_wal.layer",
		StartLSN: 1000,
		EndLSN:   1000 + numRecords,
	}
	ctx := context.Background()
	written, err := store.WriteLayer(ctx, meta, recs)
	if err != nil {
		t.Fatalf("WriteLayer: %v", err)
	}
	if written.SHA256 == "" {
		t.Fatal("expected non-empty SHA256")
	}

	// Verify file SHA-256 matches written.SHA256
	rawFile, err := os.ReadFile(filepath.Join(dir, meta.TenantID, meta.FileName))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(rawFile)
	if hex.EncodeToString(sum[:]) != written.SHA256 {
		t.Fatalf("SHA256 mismatch: got %s want %s", hex.EncodeToString(sum[:]), written.SHA256)
	}

	// Load index & WAL table and read pages at boundary indices.
	idxData, err := store.loadLayerIndex(ctx, written)
	if err != nil {
		t.Fatalf("loadLayerIndex: %v", err)
	}
	if idxData.walMeta.RawLen <= maxV2DecompressedSize {
		t.Fatalf("expected walRawLen > %d, got %d", maxV2DecompressedSize, idxData.walMeta.RawLen)
	}
	wt, err := store.loadV2WALTable(ctx, written, idxData)
	if err != nil {
		t.Fatalf("loadV2WALTable: %v", err)
	}
	if len(wt) != numRecords {
		t.Fatalf("wal table count: got %d want %d", len(wt), numRecords)
	}

	tl := NewTimeline("t_large_wal", 1, store, nil)
	tl.AddLayer(written)
	for _, blk := range []uint32{0, 1, 135, 268, 269} {
		got, cnt, err := tl.GetPageBatch(ctx, 1663, 16385, 1000, 0, blk, 1, 0)
		if err != nil {
			t.Fatalf("GetPageBatch(blk=%d): %v", blk, err)
		}
		if cnt != 1 || len(got) != PageSize {
			t.Fatalf("GetPageBatch(blk=%d): cnt=%d len=%d", blk, cnt, len(got))
		}
		if got[0] != byte(blk) || got[PageSize-1] != byte(blk>>8) {
			t.Fatalf("GetPageBatch(blk=%d): data mismatch", blk)
		}
	}
}

func TestPhase11BoundedInMemAndBackpressure(t *testing.T) {
	dir := t.TempDir()
	store := NewLayerStore(dir, nil, "", nil, nil)
	tl := NewTimeline("t_bounded", 1, store, nil)
	tl.maxInMemBytes = 256 * 1024 // 256 KiB soft, 512 KiB hard

	ctx := context.Background()
	const workers = 8
	const batchesPerWorker = 16
	const blocksPerBatch = 8 // 64 KiB per batch -> 8 MiB total

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for b := 0; b < batchesPerWorker; b++ {
				pages := make([]byte, blocksPerBatch*PageSize)
				for i := 0; i < blocksPerBatch; i++ {
					pages[i*PageSize] = byte(wid)
					pages[i*PageSize+1] = byte(b)
				}
				startBlk := uint32((wid*batchesPerWorker + b) * blocksPerBatch)
				lsn := uint64(1000 + wid*100 + b)
				if err := tl.PutPageBatch(ctx, 1663, 16385, 2000, 0, startBlk, blocksPerBatch, lsn, pages); err != nil {
					errs <- err
					return
				}
				// Soft/hard watermark + single batch slack must stay well below 2 MiB
				if cur := tl.InMemBytes(); cur > 2*1024*1024 {
					errs <- os.ErrInvalid
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("worker error: %v", err)
	}

	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatalf("final FlushInMemory: %v", err)
	}
	if tl.LayerCount() < 4 {
		t.Fatalf("expected multiple flushed L0 layers, got %d", tl.LayerCount())
	}

	// Verify all written pages are readable
	for w := 0; w < workers; w++ {
		for b := 0; b < batchesPerWorker; b++ {
			startBlk := uint32((widBatch(w, b)) * blocksPerBatch)
			got, cnt, err := tl.GetPageBatch(ctx, 1663, 16385, 2000, 0, startBlk, blocksPerBatch, 0)
			if err != nil || cnt != blocksPerBatch {
				t.Fatalf("GetPageBatch(w=%d,b=%d): cnt=%d err=%v", w, b, cnt, err)
			}
			for i := 0; i < blocksPerBatch; i++ {
				if got[i*PageSize] != byte(w) || got[i*PageSize+1] != byte(b) {
					t.Fatalf("page mismatch w=%d b=%d i=%d", w, b, i)
				}
			}
		}
	}
}

func widBatch(w, b int) int {
	return w*16 + b
}

func TestPhase11FlushAllAndLocalManifestRecovery(t *testing.T) {
	dir := t.TempDir()
	layerDir := filepath.Join(dir, "ps_layers")
	cacheDir := filepath.Join(dir, "ps_cache")
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(dir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("t_dur"); err != nil {
		t.Fatal(err)
	}

	ps1, err := New(Config{
		SocketPath: filepath.Join(dir, "ps1.sock"),
		LayerDir:   layerDir,
		CacheDir:   cacheDir,
		KMS:        kms,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tl1 := ps1.EnsureTenant("t_dur", 1)
	wantPage := bytes.Repeat([]byte{0x5A}, PageSize)
	if err := tl1.PutPageBatch(ctx, 1663, 16385, 3000, 0, 0, 1, 5000, wantPage); err != nil {
		t.Fatal(err)
	}
	if err := ps1.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	_ = ps1.Close()

	// Reopen a fresh Pageserver on the same LayerDir (no R2) and verify recovery from local manifest.
	ps2, err := New(Config{
		SocketPath: filepath.Join(dir, "ps2.sock"),
		LayerDir:   layerDir,
		CacheDir:   cacheDir,
		KMS:        kms,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()

	tl2 := ps2.EnsureTenant("t_dur", 1)
	if err := tl2.LoadLayersFromR2(ctx); err != nil {
		t.Fatalf("LoadLayersFromR2 local fallback: %v", err)
	}
	if tl2.LayerCount() == 0 {
		t.Fatal("expected recovered layers > 0")
	}
	got, err := ps2.GetPageAtLSN(ctx, "t_dur", RelBlockKey(1663, 16385, 3000, 0, 0), 0)
	if err != nil {
		t.Fatalf("GetPageAtLSN after restart: %v", err)
	}
	if !bytes.Equal(got, wantPage) {
		t.Fatal("recovered page mismatch")
	}
}

func TestPhase11FailedR2UploadSurfacesErrorAndSkipsManifest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`<Error><Code>InternalError</Code></Error>`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(dir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("t_r2fail"); err != nil {
		t.Fatal(err)
	}

	s3c := s3.New(s3.Options{
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Region:       "auto",
		Credentials:  credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		RetryMaxAttempts: 1,
	})
	store := NewLayerStore(dir, s3c, "test-bucket", kms, nil)
	tl := NewTimeline("t_r2fail", 1, store, nil)

	ctx := context.Background()
	page := bytes.Repeat([]byte{0x42}, PageSize)
	if err := tl.PutPageBatch(ctx, 1663, 16385, 4000, 0, 0, 1, 1000, page); err != nil {
		t.Fatal(err)
	}
	if _, err := tl.FlushInMemory(ctx); err == nil {
		t.Fatal("expected FlushInMemory to surface R2 upload error, got nil")
	}
	manifestPath := filepath.Join(dir, "t_r2fail", "layers.manifest.json")
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("manifest must not be published after failed R2 upload (stat err=%v)", err)
	}
}

func TestPhase11RelSizeDoesNotRegressAfterRestart(t *testing.T) {
	dir := t.TempDir()
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(dir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("t_relsz"); err != nil {
		t.Fatal(err)
	}
	store := NewLayerStore(dir, nil, "", kms, nil)
	ctx := context.Background()

	// Write 4 blocks (0..3) and flush to L0.
	tl1 := NewTimeline("t_relsz", 1, store, nil)
	fourPages := bytes.Repeat([]byte{0x77}, 4*PageSize)
	if err := tl1.PutPageBatch(ctx, 1663, 16385, 2662, 0, 0, 4, 1000, fourPages); err != nil {
		t.Fatal(err)
	}
	if _, err := tl1.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}

	// Simulate restart: new Timeline loads manifest from disk (empty relSizes map).
	tl2 := NewTimeline("t_relsz", 1, store, nil)
	if err := tl2.LoadLayersFromR2(ctx); err != nil {
		t.Fatal(err)
	}
	// Write ONLY block 1 (lower than 4) and flush again.
	onePage := bytes.Repeat([]byte{0x88}, PageSize)
	if err := tl2.PutPageBatch(ctx, 1663, 16385, 2662, 0, 1, 1, 2000, onePage); err != nil {
		t.Fatal(err)
	}
	sz, err := tl2.GetRelSize(ctx, 1663, 16385, 2662, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sz != 4 {
		t.Fatalf("GetRelSize after lower-block write = %d, want 4", sz)
	}
	if _, err := tl2.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}

	// Simulate second restart: size must still be 4, not 2.
	tl3 := NewTimeline("t_relsz", 1, store, nil)
	if err := tl3.LoadLayersFromR2(ctx); err != nil {
		t.Fatal(err)
	}
	sz3, err := tl3.GetRelSize(ctx, 1663, 16385, 2662, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sz3 != 4 {
		t.Fatalf("GetRelSize after second restart = %d, want 4", sz3)
	}
}
