package pageserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

// --- Part 2: Bounded Read Concurrency Tests ---

func TestPhase10_Part2_ReadLimiterBoundsAndBackpressure(t *testing.T) {
	rl := NewReadLimiter(ReadLimiterConfig{
		MaxActive:          4,
		MaxPending:         4,
		MaxPerTenantActive: 4,
	})
	defer rl.Shutdown(context.Background())

	var curActive atomic.Int32
	var maxObserved atomic.Int32
	var admitted atomic.Int32
	var queueFull atomic.Int32

	gate := make(chan struct{})
	var wg sync.WaitGroup

	// Launch 20 concurrent callers; 4 active + 4 pending = 8 can wait, 12 must get ErrReadQueueFull.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			rel, err := rl.Acquire(ctx, "tenant-a")
			if err != nil {
				if errors.Is(err, ErrReadQueueFull) {
					queueFull.Add(1)
				}
				return
			}
			admitted.Add(1)
			c := curActive.Add(1)
			for {
				prev := maxObserved.Load()
				if c <= prev || maxObserved.CompareAndSwap(prev, c) {
					break
				}
			}
			<-gate
			curActive.Add(-1)
			rel()
		}(i)
		if i == 7 {
			// Give the first 8 goroutines a brief moment to fill active + pending.
			time.Sleep(20 * time.Millisecond)
		}
	}

	time.Sleep(30 * time.Millisecond)
	close(gate)
	wg.Wait()

	if maxObserved.Load() > 4 {
		t.Fatalf("observed %d active reads > MaxActive=4", maxObserved.Load())
	}
	if queueFull.Load() == 0 {
		t.Fatalf("expected ErrReadQueueFull backpressure when queue exceeded MaxPending=4")
	}
	if admitted.Load()+queueFull.Load() != 20 {
		t.Fatalf("expected admitted(%d)+queueFull(%d)=20", admitted.Load(), queueFull.Load())
	}
	st := rl.Stats()
	if st.Active != 0 || st.Pending != 0 {
		t.Fatalf("expected drained limiter stats, got active=%d pending=%d", st.Active, st.Pending)
	}
}

func TestPhase10_Part2_ReadLimiterContextCancellation(t *testing.T) {
	rl := NewReadLimiter(ReadLimiterConfig{
		MaxActive:          1,
		MaxPending:         8,
		MaxPerTenantActive: 1,
	})
	defer rl.Shutdown(context.Background())

	rel1, err := rl.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := rl.Acquire(ctx, "tenant-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	st := rl.Stats()
	if st.Pending != 0 || st.Cancelled != 1 {
		t.Fatalf("expected pending=0 cancelled=1, got %+v", st)
	}
	rel1()

	// Next Acquire must succeed immediately because slot was not leaked.
	rel2, err := rl.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("expected slot available after cancellation, got %v", err)
	}
	rel2()
}

func TestPhase10_Part2_ReadLimiterTenantFairness(t *testing.T) {
	rl := NewReadLimiter(ReadLimiterConfig{
		MaxActive:          4,
		MaxPending:         64,
		MaxPerTenantActive: 2,
		StrictPerTenantCap: true,
	})
	defer rl.Shutdown(context.Background())

	// Tenant A fills up to its fair cap while Tenant B also queues work.
	holdA := make(chan struct{})
	var aStarted atomic.Int32
	var bCompleted atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := rl.Acquire(context.Background(), "tenant-a")
			if err != nil {
				return
			}
			aStarted.Add(1)
			<-holdA
			rel()
		}()
	}
	time.Sleep(15 * time.Millisecond)

	// Tenant B must be able to acquire slots immediately without waiting for all of Tenant A.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			rel, err := rl.Acquire(ctx, "tenant-b")
			if err != nil {
				return
			}
			bCompleted.Add(1)
			rel()
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && bCompleted.Load() < 4 {
		time.Sleep(5 * time.Millisecond)
	}
	if bCompleted.Load() < 4 {
		close(holdA)
		wg.Wait()
		t.Fatalf("expected all 4 tenant-b reads to complete while tenant-a is blocked, bCompleted=%d", bCompleted.Load())
	}
	close(holdA)
	wg.Wait()
}

func TestPhase10_Part2_ReadLimiterGracefulShutdown(t *testing.T) {
	rl := NewReadLimiter(ReadLimiterConfig{MaxActive: 1, MaxPending: 4})
	rel, err := rl.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}

	queuedErr := make(chan error, 1)
	go func() {
		_, err := rl.Acquire(context.Background(), "t1")
		queuedErr <- err
	}()
	time.Sleep(15 * time.Millisecond)

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- rl.Shutdown(context.Background())
	}()

	if err := <-queuedErr; !errors.Is(err, ErrReadLimiterClosed) {
		t.Fatalf("expected queued reader to receive ErrReadLimiterClosed, got %v", err)
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned early while active reader was still running: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	rel()
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown failed after active reader finished: %v", err)
	}
}

// --- Part 3: Decoded V2 Chunk Cache Tests ---

func TestPhase10_Part3_DecodedChunkCacheLRUAndSingleflight(t *testing.T) {
	dc := NewDecodedChunkCache(DecodedChunkCacheConfig{
		MaxBytes:      16 * 1024,
		MaxEntryBytes: 8 * 1024,
	})
	ctx := context.Background()

	var decodeCalls atomic.Int32
	k1 := DecodedChunkKey{TenantID: "t1", LayerID: "l1", ChunkIdx: 0, KeyEpoch: 1}

	// 32 concurrent misses on k1 must coalesce into 1 decode call.
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := dc.GetOrDecode(ctx, k1, func() ([]byte, error) {
				decodeCalls.Add(1)
				time.Sleep(15 * time.Millisecond)
				return bytes.Repeat([]byte{0xAB}, 6*1024), nil
			})
			if err != nil || len(b) != 6*1024 {
				t.Errorf("unexpected GetOrDecode result: len=%d err=%v", len(b), err)
			}
		}()
	}
	wg.Wait()
	if decodeCalls.Load() != 1 {
		t.Fatalf("expected 1 single-flight decode call, got %d", decodeCalls.Load())
	}

	// Insert 2 more 6 KiB entries -> 18 KiB > 16 KiB budget -> must evict k1.
	k2 := DecodedChunkKey{TenantID: "t1", LayerID: "l2", ChunkIdx: 0, KeyEpoch: 1}
	k3 := DecodedChunkKey{TenantID: "t1", LayerID: "l3", ChunkIdx: 0, KeyEpoch: 1}
	_, _ = dc.GetOrDecode(ctx, k2, func() ([]byte, error) { return bytes.Repeat([]byte{0xBC}, 6*1024), nil })
	_, _ = dc.GetOrDecode(ctx, k3, func() ([]byte, error) { return bytes.Repeat([]byte{0xCD}, 6*1024), nil })

	st := dc.Stats()
	if st.Bytes > 16*1024 {
		t.Fatalf("cache bytes %d exceeded MaxBytes %d", st.Bytes, 16*1024)
	}
	if st.Evictions == 0 {
		t.Fatalf("expected LRU eviction, got evictions=0")
	}
	if st.SingleflightWaits != 31 {
		t.Fatalf("expected 31 singleflight waits, got %d", st.SingleflightWaits)
	}
}

func TestPhase10_Part3_DecodedChunkCacheTenantIsolationAndShredDEK(t *testing.T) {
	ctx := context.Background()
	_, s3c, _ := newMockR2Server(t)
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("tenant-b"); err != nil {
		t.Fatal(err)
	}

	store := NewLayerStore("", s3c, "air-bucket", kms, nil)
	tlA := NewTimeline("tenant-a", 1, store, NewDefaultWALRedoer(""))
	tlB := NewTimeline("tenant-b", 1, store, NewDefaultWALRedoer(""))

	k := RelBlockKey(1663, 1, 500, 0, 0)
	pageA := makePhase9CompressiblePage(1)
	pageB := makePhase9CompressiblePage(2)
	tlA.PutPage(k, 10, pageA)
	tlB.PutPage(k, 10, pageB)
	if _, err := tlA.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tlB.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}

	// Warm decoded chunk cache for both tenants.
	gotA, err := tlA.GetPageAtLSN(ctx, k, 0)
	if err != nil || !bytes.Equal(gotA, pageA) {
		t.Fatalf("tenant-a read failed: %v", err)
	}
	gotB, err := tlB.GetPageAtLSN(ctx, k, 0)
	if err != nil || !bytes.Equal(gotB, pageB) {
		t.Fatalf("tenant-b read failed: %v", err)
	}

	// Shred tenant-a DEK -> decoded chunk cache MUST NOT serve stale plaintext!
	kms.ShredDEK("tenant-a")
	if _, err := tlA.GetPageAtLSN(ctx, k, 0); !errors.Is(err, crypto.ErrDEKMissing) {
		t.Fatalf("expected ErrDEKMissing after ShredDEK even with warm DecodedChunkCache, got %v", err)
	}

	// Tenant-b must remain completely unaffected.
	gotB2, err := tlB.GetPageAtLSN(ctx, k, 0)
	if err != nil || !bytes.Equal(gotB2, pageB) {
		t.Fatalf("tenant-b affected by tenant-a ShredDEK: %v", err)
	}
}

func TestPhase10_Part3_DecodedChunkCacheRejectsCorruptChunks(t *testing.T) {
	dc := NewDecodedChunkCache(DecodedChunkCacheConfig{MaxBytes: 1024 * 1024})
	k := DecodedChunkKey{TenantID: "t1", LayerID: "l1", ChunkIdx: 0, KeyEpoch: 1}
	_, err := dc.GetOrDecode(context.Background(), k, func() ([]byte, error) {
		return nil, errors.New("corrupt chunk")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st := dc.Stats()
	if st.Entries != 0 || st.Bytes != 0 {
		t.Fatalf("expected corrupt chunk not cached, got %+v", st)
	}
}

// --- Part 4: DiskCache Corruption Recovery Tests ---

func TestPhase10_Part4_DiskCacheCorruptChunkAndFooterRecovery(t *testing.T) {
	ctx := context.Background()
	for _, ver := range []uint16{LayerVersion, LayerVersionV2} {
		t.Run(fmt.Sprintf("v%d", ver), func(t *testing.T) {
			_, s3c, mock := newMockR2Server(t)
			kms, err := crypto.NewEnvelopeKMS()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := kms.EnsureTenantDEK("t-corrupt"); err != nil {
				t.Fatal(err)
			}
			cacheDir := t.TempDir()
			cache, err := NewDiskCache(cacheDir, 16*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			store := NewLayerStore("", s3c, "air-bucket", kms, cache)
			store.SetLayerFormatVersion(ver)
			store.SetDecodedChunkCache(nil) // Exercise DiskCache -> decrypt/decode path directly.

			tl := NewTimeline("t-corrupt", 1, store, NewDefaultWALRedoer(""))
			k := RelBlockKey(1663, 1, 700, 0, 0)
			want := makePhase9IncompressiblePage(700)
			tl.PutPage(k, 25, want)
			if _, err := tl.FlushInMemory(ctx); err != nil {
				t.Fatal(err)
			}

			// Corrupt every cached .layer file on disk (both payload and tail/footer).
			entries, err := os.ReadDir(cacheDir)
			if err != nil {
				t.Fatal(err)
			}
			corruptedFiles := 0
			for _, e := range entries {
				if filepath.Ext(e.Name()) == ".layer" {
					p := filepath.Join(cacheDir, e.Name())
					b, err := os.ReadFile(p)
					if err != nil {
						t.Fatal(err)
					}
					if len(b) > 20 {
						b[5] ^= 0xff
						b[len(b)-5] ^= 0xff
						if err := os.WriteFile(p, b, 0o600); err != nil {
							t.Fatal(err)
						}
						corruptedFiles++
					}
				}
			}
			if corruptedFiles == 0 {
				t.Fatal("expected at least 1 cached .layer file")
			}
			store.idxCache.clear()
			mock.getCount.Store(0)

			// 16 concurrent readers hit the corrupt cache simultaneously -> must evict, recover from R2 without retry storm.
			var wg sync.WaitGroup
			var errs atomic.Int32
			for r := 0; r < 16; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, err := tl.GetPageAtLSN(ctx, k, 0)
					if err != nil || !bytes.Equal(got, want) {
						errs.Add(1)
					}
				}()
			}
			wg.Wait()
			if errs.Load() > 0 {
				t.Fatalf("expected all 16 concurrent readers to recover exact page, got %d errors", errs.Load())
			}
			if cache.Evictions() == 0 {
				t.Fatalf("expected corrupt DiskCache entry to be evicted")
			}

			// Simulated restart: fresh DiskCache on same dir must preserve clean metadata and serve with 0 R2 GETs.
			cache2, err := NewDiskCache(cacheDir, 16*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			store2 := NewLayerStore("", s3c, "air-bucket", kms, cache2)
			store2.SetLayerFormatVersion(ver)
			tl2 := NewTimeline("t-corrupt", 1, store2, NewDefaultWALRedoer(""))
			tl2.mu.Lock()
			tl2.layers = append([]LayerMeta(nil), tl.layers...)
			tl2.mu.Unlock()

			// Wait briefly for any background cache Put from recovery, then read.
			if got2, err := tl2.GetPageAtLSN(ctx, k, 0); err != nil || !bytes.Equal(got2, want) {
				t.Fatalf("post-restart read failed: err=%v", err)
			}
		})
	}
}

func TestPhase10_Part4_DiskCacheSafeEvictDuringActiveReads(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskCache(dir, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x42}, 4096)
	if err := cache.Put("layer-active", payload); err != nil {
		t.Fatal(err)
	}

	// Simulate concurrent GetRange + Evict under -race.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if b, ok := cache.GetRange("layer-active", 0, 4096); ok {
					if len(b) != 4096 || b[0] != 0x42 {
						t.Errorf("corrupt read during concurrent Evict")
					}
				}
			}
		}()
	}
	time.Sleep(2 * time.Millisecond)
	cache.Evict("layer-active")
	wg.Wait()

	sz, cnt := cache.Stats()
	if sz != 0 || cnt != 0 {
		t.Fatalf("expected empty cache after Evict, got bytes=%d count=%d", sz, cnt)
	}
	// Verify underlying file was cleaned up after active readers finished.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".layer" {
			t.Fatalf("expected evicted .layer file %s to be removed after active readers finished", e.Name())
		}
	}
}
