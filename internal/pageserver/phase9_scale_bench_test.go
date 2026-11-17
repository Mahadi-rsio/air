package pageserver

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

func percentileDuration(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func TestPhase9_Part5_MultiTenantScaleMatrix(t *testing.T) {
	ctx := context.Background()

	type scaleCase struct {
		name      string
		timelines int
		readers   int
		l0Layers  int
		batchSize uint32
	}
	cases := []scaleCase{
		{name: "1tl_1r_1l0_b1", timelines: 1, readers: 1, l0Layers: 1, batchSize: 1},
		{name: "10tl_8r_16l0_b16", timelines: 10, readers: 8, l0Layers: 16, batchSize: 16},
		{name: "100tl_32r_1l0_b128", timelines: 100, readers: 32, l0Layers: 1, batchSize: 128},
		{name: "4tl_128r_64l0_b512", timelines: 4, readers: 128, l0Layers: 64, batchSize: 512},
	}

	for _, sc := range cases {
		t.Run(sc.name, func(t *testing.T) {
			_, s3c, mock := newMockR2Server(t)
			kms, err := crypto.NewEnvelopeKMS()
			if err != nil {
				t.Fatal(err)
			}
			cache, err := NewDiskCache(t.TempDir(), 64*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			store := NewLayerStore("", s3c, "air-bucket", kms, cache)

			timelines := make([]*Timeline, sc.timelines)
			for ti := 0; ti < sc.timelines; ti++ {
				tid := fmt.Sprintf("tenant-%03d", ti)
				if _, err := kms.EnsureTenantDEK(tid); err != nil {
					t.Fatal(err)
				}
				tl := NewTimeline(tid, 1, store, NewDefaultWALRedoer(""))
				timelines[ti] = tl

				// Populate layers with tenant-tagged pages (for strict isolation check).
				for l := 0; l < sc.l0Layers; l++ {
					// Put enough blocks to cover sc.batchSize on first layer, update block 0 on subsequent layers.
					nBlocks := uint32(1)
					if l == 0 {
						nBlocks = sc.batchSize
					}
					for b := uint32(0); b < nBlocks; b++ {
						pg := makePhase9CompressiblePage(uint64(ti*100000 + int(b)))
						tl.PutPage(RelBlockKey(1663, 1, 300, 0, b), uint64(l+1), pg)
					}
					if _, err := tl.FlushInMemory(ctx); err != nil {
						t.Fatalf("FlushInMemory tl=%d l=%d: %v", ti, l, err)
					}
				}
			}

			mock.headCount.Store(0)
			mock.getCount.Store(0)
			mock.bytesDownloaded.Store(0)

			var mBefore, mAfter runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&mBefore)

			opsPerReader := 8
			if sc.readers >= 128 {
				opsPerReader = 1
			}
			latencies := make([]time.Duration, sc.readers*opsPerReader)
			var errCount atomic.Int64
			var pagesRead atomic.Int64

			start := time.Now()
			var wg sync.WaitGroup
			for r := 0; r < sc.readers; r++ {
				wg.Add(1)
				go func(rid int) {
					defer wg.Done()
					tl := timelines[rid%sc.timelines]
					tidNum := rid % sc.timelines
					for op := 0; op < opsPerReader; op++ {
						t0 := time.Now()
						pages, cnt, err := tl.GetPageBatch(ctx, 1663, 1, 300, 0, 0, sc.batchSize, 0)
						lat := time.Since(t0)
						latencies[rid*opsPerReader+op] = lat
						if err != nil || cnt != sc.batchSize || len(pages) != int(sc.batchSize)*PageSize {
							errCount.Add(1)
							continue
						}
						// Verify tenant isolation on block 0.
						wantPrefix := makePhase9CompressiblePage(uint64(tidNum * 100000))
						if !bytes.Equal(pages[:8], wantPrefix[:8]) {
							errCount.Add(1)
							continue
						}
						pagesRead.Add(int64(cnt))
					}
				}(r)
			}
			wg.Wait()
			elapsed := time.Since(start)
			runtime.ReadMemStats(&mAfter)

			if errCount.Load() > 0 {
				t.Fatalf("scale matrix %s had %d errors", sc.name, errCount.Load())
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			p50 := percentileDuration(latencies, 0.50)
			p95 := percentileDuration(latencies, 0.95)
			p99 := percentileDuration(latencies, 0.99)
			tps := float64(pagesRead.Load()) / elapsed.Seconds()

			t.Logf("scale=%s pages=%d elapsed=%v throughput=%.1f pages/s p50=%v p95=%v p99=%v r2_gets=%d r2_bytes=%d heapAlloc=%d KiB",
				sc.name, pagesRead.Load(), elapsed, tps, p50, p95, p99,
				mock.getCount.Load(), mock.bytesDownloaded.Load(), mAfter.HeapAlloc/1024)
		})
	}
}

// --- Part 7: Separated Benchmark Suites ---

func BenchmarkPhase9_UnitMicro(b *testing.B) {
	b.Run("EnvelopeKMS_Encrypt_8KiB", func(b *testing.B) {
		kms, err := crypto.NewEnvelopeKMS()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := kms.EnsureTenantDEK("bench-t"); err != nil {
			b.Fatal(err)
		}
		pt := makePhase9IncompressiblePage(42)
		b.SetBytes(int64(PageSize))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := kms.EncryptObject("bench-t", 1, "obj", pt); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("V2_Compress_64KiB_Compressible", func(b *testing.B) {
		raw := bytes.Repeat(makePhase9CompressiblePage(7), 8)
		b.SetBytes(int64(len(raw)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := compressV2Payload(raw, false); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPhase9_InProcessIntegration(b *testing.B) {
	ctx := context.Background()
	for _, warm := range []bool{false, true} {
		name := "ColdCache"
		if warm {
			name = "WarmCache"
		}
		b.Run(name+"_Batch16_16L0", func(b *testing.B) {
			_, s3c, _ := newMockR2Bench(b)
			kms, err := crypto.NewEnvelopeKMS()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := kms.EnsureTenantDEK("bench-int"); err != nil {
				b.Fatal(err)
			}
			cache, err := NewDiskCache(b.TempDir(), 64*1024*1024)
			if err != nil {
				b.Fatal(err)
			}
			store := NewLayerStore("", s3c, "air-bucket", kms, cache)
			tl := NewTimeline("bench-int", 1, store, NewDefaultWALRedoer(""))
			for l := 0; l < 16; l++ {
				for blk := uint32(0); blk < 16; blk++ {
					tl.PutPage(RelBlockKey(1663, 1, 400, 0, blk), uint64(l+1), makePhase9CompressiblePage(uint64(blk)))
				}
				if _, err := tl.FlushInMemory(ctx); err != nil {
					b.Fatal(err)
				}
			}
			if warm {
				if _, _, err := tl.GetPageBatch(ctx, 1663, 1, 400, 0, 0, 16, 0); err != nil {
					b.Fatal(err)
				}
			}

			b.SetBytes(int64(16 * PageSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !warm {
					store.ClearIndexCache()
				}
				if _, _, err := tl.GetPageBatch(ctx, 1663, 1, 400, 0, 0, 16, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
