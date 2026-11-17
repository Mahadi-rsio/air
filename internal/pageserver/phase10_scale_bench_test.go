package pageserver

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

func readLinuxVmHWMKiB() int64 {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					return v
				}
			}
		}
	}
	return 0
}

type p10ScaleScenario struct {
	name      string
	timelines int
	readers   int
	l0Layers  int
	batchSize uint32
	warm      bool
	random    bool
	bounded   bool // true = Phase 10 ReadLimiter + DecodedChunkCache; false = Phase 9 unbounded baseline
}

func runPhase10ScaleScenario(t *testing.T, sc p10ScaleScenario) {
	t.Helper()
	ctx := context.Background()

	_, s3c, mock := newMockR2Server(t)
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewDiskCache(t.TempDir(), 256*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	store := NewLayerStore("", s3c, "air-bucket", kms, cache)
	if !sc.bounded {
		store.SetReadLimiter(nil)
		store.SetDecodedChunkCache(nil)
	} else {
		store.SetReadLimiter(NewReadLimiter(ReadLimiterConfig{
			MaxActive:  16,
			MaxPending: 512,
		}))
		store.SetDecodedChunkCache(NewDecodedChunkCache(DecodedChunkCacheConfig{
			MaxBytes: 64 * 1024 * 1024,
		}))
	}

	timelines := make([]*Timeline, sc.timelines)
	for ti := 0; ti < sc.timelines; ti++ {
		tid := fmt.Sprintf("tenant-%03d", ti)
		if _, err := kms.EnsureTenantDEK(tid); err != nil {
			t.Fatal(err)
		}
		tl := NewTimeline(tid, 1, store, NewDefaultWALRedoer(""))
		timelines[ti] = tl

		for l := 0; l < sc.l0Layers; l++ {
			nBlocks := uint32(1)
			if l == 0 {
				nBlocks = sc.batchSize
				if sc.random && nBlocks < 512 {
					nBlocks = 512
				}
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

	if !sc.warm {
		coldCache, err := NewDiskCache(t.TempDir(), 256*1024*1024)
		if err != nil {
			t.Fatal(err)
		}
		store.cache = coldCache
		if sc.bounded {
			store.SetDecodedChunkCache(NewDecodedChunkCache(DecodedChunkCacheConfig{MaxBytes: 64 * 1024 * 1024}))
		}
	} else {
		for ti := 0; ti < sc.timelines; ti++ {
			if _, _, err := timelines[ti].GetPageBatch(ctx, 1663, 1, 300, 0, 0, sc.batchSize, 0); err != nil {
				t.Fatalf("warmup tl=%d: %v", ti, err)
			}
		}
	}

	mock.headCount.Store(0)
	mock.getCount.Store(0)
	mock.bytesDownloaded.Store(0)

	runtime.GC()
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	opsPerReader := 4
	if sc.readers >= 64 {
		opsPerReader = 2
	}
	if sc.readers >= 128 && sc.batchSize >= 512 {
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
			rng := rand.New(rand.NewSource(int64(rid + 1)))
			tl := timelines[rid%sc.timelines]
			tidNum := rid % sc.timelines
			for op := 0; op < opsPerReader; op++ {
				startBlk := uint32(0)
				if sc.random && sc.batchSize < 512 {
					startBlk = uint32(rng.Intn(int(512 - sc.batchSize)))
				}
				t0 := time.Now()
				pages, cnt, err := tl.GetPageBatch(ctx, 1663, 1, 300, 0, startBlk, sc.batchSize, 0)
				lat := time.Since(t0)
				latencies[rid*opsPerReader+op] = lat
				if err != nil || cnt != sc.batchSize || len(pages) != int(sc.batchSize)*PageSize {
					errCount.Add(1)
					continue
				}
				wantPrefix := makePhase9CompressiblePage(uint64(tidNum*100000) + uint64(startBlk))
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

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := percentileDuration(latencies, 0.50)
	p95 := percentileDuration(latencies, 0.95)
	p99 := percentileDuration(latencies, 0.99)
	tps := float64(pagesRead.Load()) / elapsed.Seconds()

	allocDeltaMiB := float64(mAfter.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024)
	allocRateMBs := allocDeltaMiB / elapsed.Seconds()
	gcPauseMs := float64(mAfter.PauseTotalNs-mBefore.PauseTotalNs) / 1e6

	hitRatio := 0.0
	if dc := store.DecodedChunkCache(); dc != nil {
		st := dc.Stats()
		if st.Hits+st.Misses > 0 {
			hitRatio = float64(st.Hits) / float64(st.Hits+st.Misses)
		}
	}

	t.Logf("p10_scale name=%s bounded=%v warm=%v random=%v | pages=%d throughput=%.1f p/s | p50=%v p95=%v p99=%v | "+
		"heapAlloc=%d MiB heapInuse=%d MiB vmHWM=%d MiB allocRate=%.1f MiB/s gcPause=%.2f ms | "+
		"chunkHit=%.2f r2Gets=%d r2Bytes=%d err=%d",
		sc.name, sc.bounded, sc.warm, sc.random,
		pagesRead.Load(), tps, p50, p95, p99,
		mAfter.HeapAlloc/(1024*1024), mAfter.HeapInuse/(1024*1024), readLinuxVmHWMKiB()/1024,
		allocRateMBs, gcPauseMs,
		hitRatio, mock.getCount.Load(), mock.bytesDownloaded.Load(), errCount.Load(),
	)

	if errCount.Load() > 0 {
		t.Fatalf("scenario %s had %d errors", sc.name, errCount.Load())
	}
}

func TestPhase10_Part5_ScaleAndRegressionMatrix(t *testing.T) {
	scenarios := []p10ScaleScenario{
		// Baseline vs Phase 10 at target 128 readers x 512 batch (Cold & Warm)
		{name: "base_cold_128r_b512_64l0", timelines: 4, readers: 128, l0Layers: 64, batchSize: 512, warm: false, bounded: false},
		{name: "p10_cold_128r_b512_64l0", timelines: 4, readers: 128, l0Layers: 64, batchSize: 512, warm: false, bounded: true},
		{name: "base_warm_128r_b512_64l0", timelines: 4, readers: 128, l0Layers: 64, batchSize: 512, warm: true, bounded: false},
		{name: "p10_warm_128r_b512_64l0", timelines: 4, readers: 128, l0Layers: 64, batchSize: 512, warm: true, bounded: true},

		// Reader scaling: 1, 8, 32, 64, 128
		{name: "p10_1r_b128_16l0", timelines: 4, readers: 1, l0Layers: 16, batchSize: 128, warm: true, bounded: true},
		{name: "p10_8r_b128_16l0", timelines: 4, readers: 8, l0Layers: 16, batchSize: 128, warm: true, bounded: true},
		{name: "p10_32r_b128_16l0", timelines: 4, readers: 32, l0Layers: 16, batchSize: 128, warm: true, bounded: true},
		{name: "p10_64r_b128_16l0", timelines: 4, readers: 64, l0Layers: 16, batchSize: 128, warm: true, bounded: true},

		// Batch sizes 16, 128, 512 + Random vs Hot + 1, 16, 64, 256 L0 layers
		{name: "p10_random_32r_b16_1l0", timelines: 4, readers: 32, l0Layers: 1, batchSize: 16, warm: true, random: true, bounded: true},
		{name: "p10_hot_32r_b16_256l0", timelines: 1, readers: 32, l0Layers: 256, batchSize: 16, warm: true, random: false, bounded: true},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			runPhase10ScaleScenario(t, sc)
		})
	}
}

func BenchmarkPhase10_Part5_RandomVsHotChunk(b *testing.B) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		useCache  bool
		randomBlk bool
	}{
		{name: "HotChunk_NoDecodedCache", useCache: false, randomBlk: false},
		{name: "HotChunk_WithDecodedCache", useCache: true, randomBlk: false},
		{name: "Random_NoDecodedCache", useCache: false, randomBlk: true},
		{name: "Random_WithDecodedCache", useCache: true, randomBlk: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			kms, _ := crypto.NewEnvelopeKMS()
			_, _ = kms.EnsureTenantDEK("bench-t")
			cache, _ := NewDiskCache(b.TempDir(), 64*1024*1024)
			store := NewLayerStore(b.TempDir(), nil, "", kms, cache)
			if !tc.useCache {
				store.SetDecodedChunkCache(nil)
			}
			tl := NewTimeline("bench-t", 1, store, NewDefaultWALRedoer(""))
			for blk := uint32(0); blk < 512; blk++ {
				tl.PutPage(RelBlockKey(1663, 1, 10, 0, blk), 1, makePhase9CompressiblePage(uint64(blk)))
			}
			if _, err := tl.FlushInMemory(ctx); err != nil {
				b.Fatal(err)
			}
			// Warm DiskCache once.
			if _, _, err := tl.GetPageBatch(ctx, 1663, 1, 10, 0, 0, 128, 0); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(42))
				for pb.Next() {
					blk := uint32(0)
					if tc.randomBlk {
						blk = uint32(rng.Intn(512 - 16))
					}
					if _, _, err := tl.GetPageBatch(ctx, 1663, 1, 10, 0, blk, 16, 0); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
