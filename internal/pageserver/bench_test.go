package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/klauspost/compress/zstd"
)

// TestMain allows this test binary to also act as a stateless WAL-redo worker
// subprocess when AIR_BENCH_WALREDO_HELPER=1 is set, enabling reproducible
// benchmarks of real OS process spawning vs persistent worker pools.
func TestMain(m *testing.M) {
	if os.Getenv("AIR_BENCH_WALREDO_HELPER") == "1" {
		runBenchWALRedoHelper()
		return
	}
	os.Exit(m.Run())
}

func runBenchWALRedoHelper() {
	persistent := os.Getenv("AIR_BENCH_WALREDO_PERSISTENT") == "1"
	failOnceFile := os.Getenv("AIR_BENCH_WALREDO_FAIL_ONCE_FILE")
	sleepMs := os.Getenv("AIR_BENCH_WALREDO_SLEEP_MS")
	for {
		var keyBuf [24]byte
		if _, err := io.ReadFull(os.Stdin, keyBuf[:]); err != nil {
			return
		}
		var hasBase [1]byte
		if _, err := io.ReadFull(os.Stdin, hasBase[:]); err != nil {
			return
		}
		page := make([]byte, PageSize)
		if hasBase[0] == 1 {
			if _, err := io.ReadFull(os.Stdin, page); err != nil {
				return
			}
		}
		var cntBuf [4]byte
		if _, err := io.ReadFull(os.Stdin, cntBuf[:]); err != nil {
			return
		}
		cnt := binary.LittleEndian.Uint32(cntBuf[:])
		for i := uint32(0); i < cnt; i++ {
			if _, err := io.ReadFull(os.Stdin, cntBuf[:]); err != nil {
				return
			}
			rlen := binary.LittleEndian.Uint32(cntBuf[:])
			rec := make([]byte, rlen)
			if _, err := io.ReadFull(os.Stdin, rec); err != nil {
				return
			}
			if len(rec) > 0 {
				page[int(i)%PageSize] ^= rec[0]
			}
		}
		if failOnceFile != "" {
			if _, err := os.Stat(failOnceFile); err == nil {
				_ = os.Remove(failOnceFile)
				os.Exit(1)
			}
		}
		if sleepMs == "500" {
			time.Sleep(500 * time.Millisecond)
		}
		if _, err := os.Stdout.Write(page); err != nil {
			return
		}
		if !persistent {
			return
		}
	}
}

func reportLatencyPercentiles(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p := func(pct float64) float64 {
		idx := int(float64(len(samples)-1) * pct)
		return float64(samples[idx].Nanoseconds()) / 1e3 // microseconds
	}
	b.ReportMetric(p(0.50), "p50_us")
	b.ReportMetric(p(0.95), "p95_us")
	b.ReportMetric(p(0.99), "p99_us")
}

// setupBenchTimeline creates a Timeline with numLayers L0 layers in mock R2,
// each containing pagesPerLayer 8KB pages (sequentially numbered across layers
// or overlapping), with optional encryption and DiskCache.
func setupBenchTimeline(b *testing.B, numLayers, pagesPerLayer int, warmCache bool, encrypted bool) (*Timeline, *mockR2Server, uint32) {
	b.Helper()
	ctx := context.Background()
	_, s3c, mock := newMockR2Bench(b)

	var kms *crypto.EnvelopeKMS
	if encrypted {
		var err error
		kms, err = crypto.NewEnvelopeKMS()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := kms.EnsureTenantDEK("t_bench"); err != nil {
			b.Fatal(err)
		}
	}

	tmpDir := b.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")
	cache, err := NewDiskCache(cacheDir, 512*1024*1024)
	if err != nil {
		b.Fatal(err)
	}

	// Store layers only in R2 (localDir == "") so cold/warm cache behavior is exercised.
	store := NewLayerStore("", s3c, "bench-bucket", kms, cache)
	tl := NewTimeline("t_bench", 1, store, nil)

	totalPages := uint32(numLayers * pagesPerLayer)
	var lsn uint64 = 100
	for l := 0; l < numLayers; l++ {
		for p := 0; p < pagesPerLayer; p++ {
			blk := uint32(l*pagesPerLayer + p)
			page := make([]byte, PageSize)
			// Realistic semi-compressible PG-like page: header + tuples + zero hole
			binary.LittleEndian.PutUint64(page[0:8], lsn)
			for off := 24; off < 2048; off += 32 {
				binary.LittleEndian.PutUint32(page[off:off+4], blk)
			}
			for off := 6144; off < PageSize; off += 16 {
				page[off] = byte((blk + uint32(off)) & 0xFF)
			}
			tl.PutPage(RelBlockKey(1663, 5, 16384, 0, blk), lsn, page)
			lsn += 10
		}
		if _, err := tl.FlushInMemory(ctx); err != nil {
			b.Fatal(err)
		}
	}

	if !warmCache {
		// Clear disk cache and index cache to measure true cold reads from R2.
		store.ClearIndexCache()
		freshCache, err := NewDiskCache(filepath.Join(tmpDir, "cache_cold"), 512*1024*1024)
		if err != nil {
			b.Fatal(err)
		}
		store.cache = freshCache
	}

	// Reset mock counters after setup.
	mock.getCount.Store(0)
	mock.headCount.Store(0)
	mock.putCount.Store(0)
	mock.bytesDownloaded.Store(0)
	mock.bytesUploaded.Store(0)

	return tl, mock, totalPages
}

func BenchmarkReadRandom8KB(b *testing.B) {
	ctx := context.Background()
	for _, numLayers := range []int{1, 4, 16} {
		for _, warm := range []bool{false, true} {
			name := fmt.Sprintf("layers=%d/warm=%v", numLayers, warm)
			b.Run(name, func(b *testing.B) {
				tl, mock, totalPages := setupBenchTimeline(b, numLayers, 64, warm, false)
				rng := rand.New(rand.NewSource(42))
				samples := make([]time.Duration, 0, b.N)

				b.SetBytes(int64(PageSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !warm {
						// Force cold R2 read per iteration by clearing caches.
						tl.store.ClearIndexCache()
						c, _ := NewDiskCache(filepath.Join(b.TempDir(), "c"), 512*1024*1024)
						tl.store.cache = c
					}
					blk := uint32(rng.Intn(int(totalPages)))
					t0 := time.Now()
					page, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 16384, 0, blk), 0)
					samples = append(samples, time.Since(t0))
					if err != nil || len(page) != PageSize {
						b.Fatalf("GetPageAtLSN(%d): err=%v len=%d", blk, err, len(page))
					}
				}
				b.StopTimer()
				reportLatencyPercentiles(b, samples)
				b.ReportMetric(float64(mock.getCount.Load())/float64(b.N), "r2_gets/op")
				b.ReportMetric(float64(mock.bytesDownloaded.Load())/float64(b.N), "net_bytes/op")
			})
		}
	}
}

func BenchmarkReadSequential(b *testing.B) {
	ctx := context.Background()
	for _, numLayers := range []int{1, 4, 16} {
		for _, warm := range []bool{false, true} {
			name := fmt.Sprintf("layers=%d/warm=%v", numLayers, warm)
			b.Run(name, func(b *testing.B) {
				tl, mock, totalPages := setupBenchTimeline(b, numLayers, 64, warm, false)
				samples := make([]time.Duration, 0, b.N)

				b.SetBytes(int64(PageSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					blk := uint32(i) % totalPages
					t0 := time.Now()
					page, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 16384, 0, blk), 0)
					samples = append(samples, time.Since(t0))
					if err != nil || len(page) != PageSize {
						b.Fatalf("GetPageAtLSN(%d): err=%v len=%d", blk, err, len(page))
					}
				}
				b.StopTimer()
				reportLatencyPercentiles(b, samples)
				b.ReportMetric(float64(mock.getCount.Load())/float64(b.N), "r2_gets/op")
				b.ReportMetric(float64(mock.bytesDownloaded.Load())/float64(b.N), "net_bytes/op")
			})
		}
	}
}

func BenchmarkReadBatch(b *testing.B) {
	ctx := context.Background()
	for _, batchSize := range []uint32{16, 128, 512} {
		for _, numLayers := range []int{1, 4, 16} {
			for _, warm := range []bool{false, true} {
				name := fmt.Sprintf("batch=%d/layers=%d/warm=%v", batchSize, numLayers, warm)
				b.Run(name, func(b *testing.B) {
					// Put 512 blocks across numLayers so every batch size is valid.
					perLayer := 512 / numLayers
					if perLayer < 1 {
						perLayer = 1
					}
					tl, mock, totalPages := setupBenchTimeline(b, numLayers, perLayer, warm, false)
					if totalPages < batchSize {
						b.Fatalf("totalPages %d < batchSize %d", totalPages, batchSize)
					}
					samples := make([]time.Duration, 0, b.N)

					b.SetBytes(int64(batchSize) * int64(PageSize))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if !warm {
							tl.store.ClearIndexCache()
							c, _ := NewDiskCache(filepath.Join(b.TempDir(), "c"), 512*1024*1024)
							tl.store.cache = c
						}
						t0 := time.Now()
						pages, got, err := tl.GetPageBatch(ctx, 1663, 5, 16384, 0, 0, batchSize, 0)
						samples = append(samples, time.Since(t0))
						if err != nil || got != batchSize || len(pages) != int(batchSize)*PageSize {
							b.Fatalf("GetPageBatch(%d): err=%v got=%d len=%d", batchSize, err, got, len(pages))
						}
					}
					b.StopTimer()
					reportLatencyPercentiles(b, samples)
					b.ReportMetric(float64(mock.getCount.Load())/float64(b.N), "r2_gets/op")
					b.ReportMetric(float64(mock.bytesDownloaded.Load())/float64(b.N), "net_bytes/op")
				})
			}
		}
	}
}

func BenchmarkStorageMetrics(b *testing.B) {
	b.Run("V1Encode_vs_ZstdPotential", func(b *testing.B) {
		recs := make([]LayerRecord, 256)
		for i := range recs {
			page := make([]byte, PageSize)
			binary.LittleEndian.PutUint64(page[0:8], uint64(100+i))
			for off := 24; off < 2048; off += 32 {
				binary.LittleEndian.PutUint32(page[off:off+4], uint32(i))
			}
			recs[i] = LayerRecord{
				Key:   RelBlockKey(1663, 5, 16384, 0, uint32(i)),
				Delta: WALDelta{LSN: uint64(100 + i), Flags: EntryFlagPageImage, PageImage: page},
			}
		}
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			b.Fatal(err)
		}
		defer enc.Close()

		var v1Size, zstdSize int64
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			raw, err := EncodeLayerBytes("t_bench", 1, "k", LayerKindL0, 500, recs, nil)
			if err != nil {
				b.Fatal(err)
			}
			v1Size = int64(len(raw))
			comp := enc.EncodeAll(raw, nil)
			zstdSize = int64(len(comp))
		}
		b.StopTimer()
		b.ReportMetric(float64(v1Size), "v1_bytes")
		b.ReportMetric(float64(zstdSize), "zstd_bytes")
		b.ReportMetric(float64(v1Size)/float64(zstdSize), "comp_ratio")
		b.ReportMetric(float64(len(recs)*int(unsafe.Sizeof(LayerIndexEntry{}))), "index_mem_bytes")
	})

	b.Run("WALDuplicationRatio", func(b *testing.B) {
		// Simulate a 4-block WAL record (e.g. B-tree split / heap multi-insert)
		rawWAL := bytes.Repeat([]byte{0xAB}, 512)
		rec := &DecodedWALRecord{
			LSN:       100,
			EndLSN:    612,
			RawRecord: rawWAL,
			Blocks: []WALBlockRef{
				{Rel: RelFileLocator{1663, 5, 16384}, ForkNum: 0, BlockNum: 1},
				{Rel: RelFileLocator{1663, 5, 16384}, ForkNum: 0, BlockNum: 2},
				{Rel: RelFileLocator{1663, 5, 16384}, ForkNum: 0, BlockNum: 3},
				{Rel: RelFileLocator{1663, 5, 16384}, ForkNum: 0, BlockNum: 4},
			},
		}
		tl := NewTimeline("t_bench", 1, nil, nil)
		tl.ApplyWALRecord(rec)
		recs, _, _ := tl.inMem.SnapshotAll()
		var storedWALBytes int
		for _, r := range recs {
			storedWALBytes += len(r.Delta.WALRecord)
		}
		b.ReportMetric(float64(len(rawWAL)), "unique_wal_bytes")
		b.ReportMetric(float64(storedWALBytes), "stored_wal_bytes")
		b.ReportMetric(float64(storedWALBytes)/float64(len(rawWAL)), "wal_dup_ratio")
	})

	b.Run("CompactionInputOutput", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			store := NewLayerStore(b.TempDir(), nil, "", nil, nil)
			tl := NewTimeline("t_bench", 1, store, nil)
			var inBytes int64
			for l := 0; l < 4; l++ {
				for p := 0; p < 16; p++ {
					// Overwrite same 16 blocks across 4 L0 layers
					tl.PutPage(RelBlockKey(1663, 5, 16384, 0, uint32(p)), uint64(100+l*20+p), bytes.Repeat([]byte{byte(l + p)}, PageSize))
				}
				lm, err := tl.FlushInMemory(ctx)
				if err != nil {
					b.Fatal(err)
				}
				inBytes += lm.FileSize
			}
			outMeta, err := tl.CompactIfNeeded(ctx, 4, true)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(inBytes), "compact_in_bytes")
			b.ReportMetric(float64(outMeta.FileSize), "compact_out_bytes")
		}
	})
}

func BenchmarkUploadCAU(b *testing.B) {
	ctx := context.Background()

	b.Run("HashingThroughput_8MiB", func(b *testing.B) {
		data := bytes.Repeat([]byte{0x5A}, 8*1024*1024)
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := StreamSHA256(bytes.NewReader(data)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("EncryptionThroughput_8KiB", func(b *testing.B) {
		kms, err := crypto.NewEnvelopeKMS()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := kms.EnsureTenantDEK("t_bench"); err != nil {
			b.Fatal(err)
		}
		pt := make([]byte, PageSize)
		b.SetBytes(int64(len(pt)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := kms.EncryptObject("t_bench", 1, "obj#e0", pt); err != nil {
				b.Fatal(err)
			}
		}
	})

	for _, tc := range []struct {
		name      string
		numPages  int
		threshold int64
		partSize  int64
	}{
		{"SinglePart_1MiB", 128, 16 * 1024 * 1024, 8 * 1024 * 1024},
		{"Multipart_12MiB", 1536, 4 * 1024 * 1024, 4 * 1024 * 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			_, s3c, mock := newMockR2Bench(b)
			store := NewLayerStore(b.TempDir(), s3c, "bench-bucket", nil, nil)
			store.SetMultipartConfig(tc.threshold, tc.partSize)

			recs := make([]LayerRecord, tc.numPages)
			for i := range recs {
				page := make([]byte, PageSize)
				binary.LittleEndian.PutUint32(page[0:4], uint32(i))
				recs[i] = LayerRecord{
					Key:   RelBlockKey(1663, 5, 16384, 0, uint32(i)),
					Delta: WALDelta{LSN: uint64(100 + i), Flags: EntryFlagPageImage, PageImage: page},
				}
			}
			raw, err := EncodeLayerBytes("t_bench", 1, "k", LayerKindL0, 9999, recs, nil)
			if err != nil {
				b.Fatal(err)
			}
			fPath := filepath.Join(b.TempDir(), "layer.bin")
			if err := os.WriteFile(fPath, raw, 0o600); err != nil {
				b.Fatal(err)
			}

			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Clear mock objects so each iteration performs a full upload
				mock.mu.Lock()
				mock.objects = make(map[string]*mockS3Object)
				mock.mu.Unlock()
				meta := LayerMeta{TenantID: "t_bench", Epoch: 1, Kind: LayerKindL0, FileName: "l0.layer"}
				if _, err := store.UploadLayerFile(ctx, meta, fPath); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(mock.putCount.Load())/float64(b.N), "r2_puts/op")
			b.ReportMetric(float64(mock.headCount.Load())/float64(b.N), "r2_heads/op")
			b.ReportMetric(float64(mock.bytesUploaded.Load())/float64(b.N), "net_bytes/op")
		})
	}
}

func BenchmarkWALRedo(b *testing.B) {
	ctx := context.Background()
	exe, err := os.Executable()
	if err != nil {
		b.Fatal(err)
	}
	b.Setenv("AIR_BENCH_WALREDO_HELPER", "1")
	b.Setenv("AIR_BENCH_WALREDO_PERSISTENT", "1")
	redoer := NewWALRedoPool(WALRedoPoolConfig{
		WorkerBin: exe,
		Workers:   4,
		QueueSize: 64,
	})
	b.Cleanup(func() { _ = redoer.Close() })

	key := RelBlockKey(1663, 5, 16384, 0, 1)
	base := make([]byte, PageSize)
	deltas := []WALDelta{
		{LSN: 100, Flags: EntryFlagWALRecord, WALRecord: []byte{1, 2, 3, 4}},
		{LSN: 110, Flags: EntryFlagWALRecord, WALRecord: []byte{5, 6, 7, 8}},
	}

	b.Run("SinglePage", func(b *testing.B) {
		samples := make([]time.Duration, 0, b.N)
		b.SetBytes(int64(PageSize))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t0 := time.Now()
			out, err := redoer.Redo(ctx, key, base, deltas)
			samples = append(samples, time.Since(t0))
			if err != nil || len(out) != PageSize {
				b.Fatalf("Redo: err=%v len=%d", err, len(out))
			}
		}
		b.StopTimer()
		reportLatencyPercentiles(b, samples)
	})

	b.Run("Batch16Pages", func(b *testing.B) {
		reqs := make([]WALRedoRequest, 16)
		for i := range reqs {
			reqs[i] = WALRedoRequest{
				Key:      RelBlockKey(1663, 5, 16384, 0, uint32(i)),
				BasePage: base,
				Deltas:   deltas,
			}
		}
		samples := make([]time.Duration, 0, b.N)
		b.SetBytes(int64(16 * PageSize))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t0 := time.Now()
			pages, errs := redoer.RedoBatch(ctx, reqs)
			samples = append(samples, time.Since(t0))
			for j := range reqs {
				if errs[j] != nil || len(pages[j]) != PageSize {
					b.Fatalf("RedoBatch[%d]: err=%v len=%d", j, errs[j], len(pages[j]))
				}
			}
		}
		b.StopTimer()
		reportLatencyPercentiles(b, samples)
	})
}
