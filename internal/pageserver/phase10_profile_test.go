package pageserver

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

func runPhase10BaselineProfile(t *testing.T, warmCache bool) {
	t.Helper()
	ctx := context.Background()

	const (
		numTimelines = 4
		numReaders   = 128
		numL0Layers  = 64
		batchSize    = uint32(512)
	)

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

	timelines := make([]*Timeline, numTimelines)
	for ti := 0; ti < numTimelines; ti++ {
		tid := fmt.Sprintf("tenant-%03d", ti)
		if _, err := kms.EnsureTenantDEK(tid); err != nil {
			t.Fatal(err)
		}
		tl := NewTimeline(tid, 1, store, NewDefaultWALRedoer(""))
		timelines[ti] = tl

		for l := 0; l < numL0Layers; l++ {
			nBlocks := uint32(1)
			if l == 0 {
				nBlocks = batchSize
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

	if !warmCache {
		// Force cold disk cache by creating a fresh empty DiskCache for the read phase.
		coldCache, err := NewDiskCache(t.TempDir(), 256*1024*1024)
		if err != nil {
			t.Fatal(err)
		}
		store.cache = coldCache
	} else {
		// Warm cache with 1 sequential pass per timeline.
		for ti := 0; ti < numTimelines; ti++ {
			if _, _, err := timelines[ti].GetPageBatch(ctx, 1663, 1, 300, 0, 0, batchSize, 0); err != nil {
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

	var errCount atomic.Int64
	var pagesRead atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			tl := timelines[rid%numTimelines]
			tidNum := rid % numTimelines
			pages, cnt, err := tl.GetPageBatch(ctx, 1663, 1, 300, 0, 0, batchSize, 0)
			if err != nil || cnt != batchSize || len(pages) != int(batchSize)*PageSize {
				errCount.Add(1)
				return
			}
			wantPrefix := makePhase9CompressiblePage(uint64(tidNum * 100000))
			if !bytes.Equal(pages[:8], wantPrefix[:8]) {
				errCount.Add(1)
				return
			}
			pagesRead.Add(int64(cnt))
		}(r)
	}
	wg.Wait()
	elapsed := time.Since(start)
	runtime.ReadMemStats(&mAfter)

	if errCount.Load() > 0 {
		t.Fatalf("errors=%d", errCount.Load())
	}

	deltaTotalAlloc := mAfter.TotalAlloc - mBefore.TotalAlloc
	deltaMallocs := mAfter.Mallocs - mBefore.Mallocs
	deltaNumGC := mAfter.NumGC - mBefore.NumGC
	deltaPauseNs := mAfter.PauseTotalNs - mBefore.PauseTotalNs

	t.Logf("warm=%v elapsed=%v pages=%d throughput=%.1f pages/s | "+
		"HeapAlloc=%d MiB HeapInuse=%d MiB HeapObjects=%d Sys=%d MiB | "+
		"TotalAllocDelta=%d MiB PerReqAlloc=%.2f MiB MallocsDelta=%d NumGC=%d GCPause=%.2fms | "+
		"R2Gets=%d R2Bytes=%d MiB",
		warmCache, elapsed, pagesRead.Load(), float64(pagesRead.Load())/elapsed.Seconds(),
		mAfter.HeapAlloc/(1024*1024), mAfter.HeapInuse/(1024*1024), mAfter.HeapObjects, mAfter.Sys/(1024*1024),
		deltaTotalAlloc/(1024*1024), float64(deltaTotalAlloc)/float64(numReaders)/(1024*1024), deltaMallocs, deltaNumGC, float64(deltaPauseNs)/1e6,
		mock.getCount.Load(), mock.bytesDownloaded.Load()/(1024*1024),
	)
}

func TestPhase10_Part1_BaselineCold(t *testing.T) {
	runPhase10BaselineProfile(t, false)
}

func TestPhase10_Part1_BaselineWarm(t *testing.T) {
	runPhase10BaselineProfile(t, true)
}
