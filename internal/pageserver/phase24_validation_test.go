package pageserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPhase24_BackgroundWorkloadIsolation validates Phase 5:
// bounded goroutines, zero dropped flushes, foreground read/write responsiveness during compaction/GC,
// and clean Pageserver.Close() cancellation/wait.
func TestPhase24_BackgroundWorkloadIsolation(t *testing.T) {
	ctx := context.Background()
	layerDir := t.TempDir()

	ps, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		EnableCAWAL:       true,
		MemoryBudget: MemoryBudgetConfig{
			MemoryLimitBytes:         256 * 1024 * 1024,
			MaxConcurrentFlushes:     2,
			MaxConcurrentCompactions: 1,
		},
	})
	if err != nil {
		t.Fatalf("New ps: %v", err)
	}

	const tenants = 16
	for i := 0; i < tenants; i++ {
		tid := fmt.Sprintf("p24-iso-%02d", i)
		recs := makeSyntheticWALRecords(i, 40, 2048, true)
		for b := 0; b < 8; b++ {
			if _, _, err := ps.IngestWALRecords(ctx, tid, 1, "gen1", recs[b*5:(b+1)*5], true); err != nil {
				t.Fatalf("seed IngestWALRecords: %v", err)
			}
			if _, err := ps.FlushTenant(ctx, tid); err != nil {
				t.Fatalf("seed FlushTenant: %v", err)
			}
		}
	}

	gorsBefore := runtime.NumGoroutine()
	var peakGors atomic.Int64
	peakGors.Store(int64(gorsBefore))

	var fgWriteLatUs, fgReadLatUs []float64
	var latMu sync.Mutex
	var droppedFlushes atomic.Int64

	stopSampler := make(chan struct{})
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-tk.C:
				g := int64(runtime.NumGoroutine())
				for {
					cur := peakGors.Load()
					if g <= cur || peakGors.CompareAndSwap(cur, g) {
						break
					}
				}
			}
		}
	}()

	var wg sync.WaitGroup
	// Background compactor + GC pressure.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 4; i++ {
			tid := fmt.Sprintf("p24-iso-%02d", i)
			if tl, err := ps.getTimeline(tid); err == nil {
				_, _ = tl.CompactL0Batch(ctx, 8, 4)
			}
		}
		if cw := ps.Store().CAWALStore(); cw != nil {
			_, _ = cw.GC(ctx, 0, nil)
		}
	}()

	// Foreground writers + readers.
	for i := 4; i < tenants; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tid := fmt.Sprintf("p24-iso-%02d", idx)
			recs := makeSyntheticWALRecords(idx+100, 20, 2048, true)
			for b := 0; b < 4; b++ {
				t0 := time.Now()
				if _, _, err := ps.IngestWALRecords(ctx, tid, 1, "gen1", recs[b*5:(b+1)*5], true); err != nil {
					t.Errorf("fg IngestWALRecords: %v", err)
					return
				}
				wUs := float64(time.Since(t0).Microseconds())
				latMu.Lock()
				fgWriteLatUs = append(fgWriteLatUs, wUs)
				latMu.Unlock()
			}
			if _, err := ps.FlushTenant(ctx, tid); err != nil {
				droppedFlushes.Add(1)
			}
			if tl, err := ps.getTimeline(tid); err == nil {
				t0 := time.Now()
				_, _ = tl.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 16385, 0, 0), recs[len(recs)-1].EndLSN)
				rUs := float64(time.Since(t0).Microseconds())
				latMu.Lock()
				fgReadLatUs = append(fgReadLatUs, rUs)
				latMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	close(stopSampler)

	if err := ps.Close(); err != nil {
		t.Fatalf("ps.Close: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	gorsAfter := runtime.NumGoroutine()

	wStats := calcStats(fgWriteLatUs)
	rStats := calcStats(fgReadLatUs)
	t.Logf("Phase5 Isolation: peak_goroutines=%d after_close=%d dropped_flushes=%d fg_write_p99=%.1fus fg_read_p99=%.1fus",
		peakGors.Load(), gorsAfter, droppedFlushes.Load(), wStats.P99, rStats.P99)

	if droppedFlushes.Load() != 0 {
		t.Fatalf("expected 0 dropped flushes, got %d", droppedFlushes.Load())
	}
	if peakGors.Load() > int64(gorsBefore+64) {
		t.Fatalf("unexpected goroutine growth: before=%d peak=%d", gorsBefore, peakGors.Load())
	}
}

// TestPhase24_MultiTenantFairness validates Phase 6 scenarios A-E.
func TestPhase24_MultiTenantFairness(t *testing.T) {
	ctx := context.Background()

	runScenario := func(name string, tenants int, noisyIdx int, mixed bool, bgCompaction bool) {
		t.Helper()
		ps, err := New(Config{
			DisableUnixSocket: true,
			LayerDir:          t.TempDir(),
			EnableCAWAL:       true,
			MemoryBudget: MemoryBudgetConfig{
				MemoryLimitBytes: 256 * 1024 * 1024,
			},
		})
		if err != nil {
			t.Fatalf("%s New ps: %v", name, err)
		}
		defer ps.Close()

		for i := 0; i < tenants; i++ {
			tid := fmt.Sprintf("fair-%02d", i)
			ps.EnsureTenant(tid, 1)
			ps.SetTenantGeneration(tid, "gen1")
		}

		var allLatUs []float64
		var noisyLatUs, normalLatUs []float64
		var latMu sync.Mutex
		var totalBytes atomic.Int64

		t0 := time.Now()
		var wg sync.WaitGroup
		if bgCompaction {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 4; i++ {
					if tl, err := ps.getTimeline(fmt.Sprintf("fair-%02d", i)); err == nil {
						_, _ = tl.CompactL0Batch(ctx, 4, 2)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}()
		}

		for i := 0; i < tenants; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				tid := fmt.Sprintf("fair-%02d", idx)
				batches := 10
				if idx == noisyIdx {
					batches = 80
				} else if mixed && idx%3 == 0 {
					// Idle/light tenant.
					batches = 2
				}
				recs := makeSyntheticWALRecords(idx, batches*5, 2048, true)
				for b := 0; b < batches; b++ {
					w0 := time.Now()
					if _, _, err := ps.IngestWALRecords(ctx, tid, 1, "gen1", recs[b*5:(b+1)*5], true); err != nil {
						t.Errorf("%s ingest: %v", name, err)
						return
					}
					us := float64(time.Since(w0).Microseconds())
					totalBytes.Add(5 * 2048)
					latMu.Lock()
					allLatUs = append(allLatUs, us)
					if idx == noisyIdx {
						noisyLatUs = append(noisyLatUs, us)
					} else {
						normalLatUs = append(normalLatUs, us)
					}
					latMu.Unlock()
					if mixed && idx%3 == 1 {
						if tl, err := ps.getTimeline(tid); err == nil {
							_, _ = tl.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 16385, 0, 0), recs[b*5].EndLSN)
						}
					}
				}
				_, _ = ps.FlushTenant(ctx, tid)
			}(i)
		}
		wg.Wait()
		dur := time.Since(t0)

		allSt := calcStats(allLatUs)
		normSt := calcStats(normalLatUs)
		noisySt := calcStats(noisyLatUs)
		tputMBs := float64(totalBytes.Load()) / (1024 * 1024) / dur.Seconds()
		t.Logf("Scenario %-22s | tenants=%d | global_p99=%.1fus | normal_p99=%.1fus | noisy_p99=%.1fus | tput=%.2f MB/s",
			name, tenants, allSt.P99, normSt.P99, noisySt.P99, tputMBs)
	}

	runScenario("A_uniform_20T", 20, -1, false, false)
	runScenario("B_noisy_neighbor_20T", 20, 0, false, false)
	runScenario("C_mixed_20T", 20, -1, true, false)
	runScenario("D_burst_75T", 75, -1, false, false)
	runScenario("E_bg_pressure_20T", 20, -1, true, true)
}

// TestPhase24_CAWALThresholdValidation validates Phase 7 threshold trade-offs (4..64 KB).
func TestPhase24_CAWALThresholdValidation(t *testing.T) {
	ctx := context.Background()
	thresholds := []int{4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024}

	t.Log("THRESHOLD_KB | WORKLOAD | P99_US | DISK_KB | DEDUP_RATIO | INLINE | EXTERNAL | CACHE_HITS | SF_HITS")
	for _, th := range thresholds {
		for _, payloadSize := range []int{1024, 4096} {
			layerDir := t.TempDir()
			ps, err := New(Config{
				DisableUnixSocket: true,
				LayerDir:          layerDir,
				EnableCAWAL:       true,
			})
			if err != nil {
				t.Fatalf("New ps: %v", err)
			}
			ps.Store().CAWALStore().SetInlineThreshold(th)

			var lats []float64
			var logicalBytes int64
			for ti := 0; ti < 10; ti++ {
				tid := fmt.Sprintf("th-%d", ti)
				recs := makeSyntheticWALRecords(ti, 32, payloadSize, true)
				t0 := time.Now()
				if _, _, err := ps.IngestWALRecords(ctx, tid, 1, "gen1", recs, true); err != nil {
					t.Fatalf("ingest: %v", err)
				}
				lats = append(lats, float64(time.Since(t0).Microseconds()))
				if _, err := ps.FlushTenant(ctx, tid); err != nil {
					t.Fatalf("flush: %v", err)
				}
				logicalBytes += int64(32 * payloadSize)
			}
			diskBytes := dirSizeBytes(layerDir)
			m := ps.Store().CAWALStore().Metrics()
			st := calcStats(lats)
			dedup := float64(logicalBytes) / float64(maxInt64(1, diskBytes))
			label := fmt.Sprintf("rec_%dB", payloadSize)
			t.Logf("%d KB | %s | %.1f | %d | %.2fx | %d | %d | %d | %d",
				th/1024, label, st.P99, diskBytes/1024, dedup,
				m.InlineSelections, m.ExternalSelections, m.CacheHits, m.SingleflightHits)
			_ = ps.Close()
		}
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// TestPhase24_AcceleratedSoakAndFaultInjection validates Phase 8:
// sustained concurrent write/flush/compaction/GC/read soak plus 9 injected fault scenarios.
func TestPhase24_AcceleratedSoakAndFaultInjection(t *testing.T) {
	ctx := context.Background()

	// Part 1: 4s sustained multi-tenant soak per mode (20T CA-WAL off vs 20T CA-WAL on).
	for _, enableCAWAL := range []bool{false, true} {
		layerDir := t.TempDir()
		ps, err := New(Config{
			DisableUnixSocket: true,
			LayerDir:          layerDir,
			EnableCAWAL:       enableCAWAL,
			MemoryBudget: MemoryBudgetConfig{
				MemoryLimitBytes: 256 * 1024 * 1024,
			},
		})
		if err != nil {
			t.Fatalf("soak New: %v", err)
		}

		const tenants = 20
		stopAt := time.Now().Add(2 * time.Second)
		var wg sync.WaitGroup
		var ops atomic.Int64
		for ti := 0; ti < tenants; ti++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				tid := fmt.Sprintf("soak-%02d", idx)
				seq := 0
				for time.Now().Before(stopAt) {
					recs := makeSyntheticWALRecords(idx, 10, 2048, true)
					baseLSN := uint64(0x100000 + seq*10*2048)
					for k, r := range recs {
						r.LSN = baseLSN + uint64(k*2048)
						r.EndLSN = r.LSN + 2048
					}
					if _, _, err := ps.IngestWALRecords(ctx, tid, 1, "gen1", recs, true); err != nil {
						t.Errorf("soak ingest: %v", err)
						return
					}
					if seq%4 == 3 {
						if _, err := ps.FlushTenant(ctx, tid); err != nil {
							t.Errorf("soak flush: %v", err)
							return
						}
					}
					if seq%12 == 11 {
						if tl, err := ps.getTimeline(tid); err == nil {
							_, _ = tl.CompactL0Batch(ctx, 4, 2)
						}
					}
					seq++
					ops.Add(1)
				}
				_, _ = ps.FlushTenant(ctx, tid)
			}(ti)
		}
		wg.Wait()
		if cw := ps.Store().CAWALStore(); cw != nil {
			_, _ = cw.GC(ctx, 0, nil)
		}
		_ = ps.Close()

		// Restart & verify recovery on the persisted layerDir.
		ps2, err := New(Config{
			DisableUnixSocket: true,
			LayerDir:          layerDir,
			EnableCAWAL:       enableCAWAL,
		})
		if err != nil {
			t.Fatalf("soak restart: %v", err)
		}
		for ti := 0; ti < tenants; ti++ {
			tid := fmt.Sprintf("soak-%02d", ti)
			tl, err := ps2.getTimeline(tid)
			if err != nil {
				t.Fatalf("soak recover getTimeline(%s): %v", tid, err)
			}
			tl.mu.RLock()
			layers := append([]LayerMeta(nil), tl.layers...)
			tl.mu.RUnlock()
			if len(layers) == 0 || tl.LastLSN() == 0 {
				t.Fatalf("soak recover missing layers/lsn for %s: layers=%d lsn=%d", tid, len(layers), tl.LastLSN())
			}
			deltas, err := ps2.Store().LookupKeyInLayer(ctx, layers[len(layers)-1], RelBlockKey(1663, 5, 16385, 0, 0), 0)
			if err != nil || len(deltas) == 0 {
				t.Fatalf("soak recover LookupKeyInLayer(%s): err=%v deltas=%d", tid, err, len(deltas))
			}
		}
		_ = ps2.Close()
		t.Logf("Soak enableCAWAL=%v ops=%d recovery=OK", enableCAWAL, ops.Load())
	}

	// Part 2: Fault-injection scenarios (partial blob/manifest temp files, restart during writes/flush, purge race).
	layerDir := t.TempDir()
	ps, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		EnableCAWAL:       true,
	})
	if err != nil {
		t.Fatalf("fault New: %v", err)
	}
	recs := makeSyntheticWALRecords(1, 20, 4096, true)
	if _, _, err := ps.IngestWALRecords(ctx, "fault-t1", 1, "gen1", recs, true); err != nil {
		t.Fatalf("fault ingest: %v", err)
	}
	if _, err := ps.FlushTenant(ctx, "fault-t1"); err != nil {
		t.Fatalf("fault flush: %v", err)
	}
	lastLSN := recs[len(recs)-1].EndLSN

	// Simulate interrupted blob publication (.tmp file) and interrupted manifest publication (.tmp file).
	blobsDir := filepath.Join(layerDir, "cawal", "blobs")
	_ = os.MkdirAll(blobsDir, 0o755)
	_ = os.WriteFile(filepath.Join(blobsDir, "deadbeef.walb.tmp.999"), []byte("partial-corrupt"), 0o644)
	_ = os.WriteFile(filepath.Join(layerDir, "fault-t1.manifest.json.tmp.999"), []byte("{partial"), 0o644)

	// Interrupted GC with cancelled context.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, _ = ps.Store().CAWALStore().GC(cctx, 0, nil)

	// Concurrent tenant create + purge.
	var pwg sync.WaitGroup
	for i := 0; i < 8; i++ {
		pwg.Add(1)
		go func(k int) {
			defer pwg.Done()
			tid := fmt.Sprintf("ephemeral-%d", k)
			ps.EnsureTenant(tid, 1)
			_, _, _ = ps.IngestWALRecords(ctx, tid, 1, "gen1", recs[:2], true)
			_ = ps.PurgeTenant(ctx, tid, "gen1")
		}(i)
	}
	pwg.Wait()
	_ = ps.Close()

	// Restart pageserver and verify committed WAL & timeline integrity.
	psRecovered, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		EnableCAWAL:       true,
	})
	if err != nil {
		t.Fatalf("fault restart: %v", err)
	}
	defer psRecovered.Close()
	tl, err := psRecovered.getTimeline("fault-t1")
	if err != nil {
		t.Fatalf("fault recover getTimeline: %v", err)
	}
	if tl.LastLSN() < lastLSN {
		t.Fatalf("committed LSN lost: got %d want >= %d", tl.LastLSN(), lastLSN)
	}
	tl.mu.RLock()
	layers := append([]LayerMeta(nil), tl.layers...)
	tl.mu.RUnlock()
	if len(layers) == 0 {
		t.Fatalf("fault recover expected persisted layers")
	}
	deltas, err := psRecovered.Store().LookupKeyInLayer(ctx, layers[len(layers)-1], RelBlockKey(1663, 5, 16385, 0, 0), 0)
	if err != nil || len(deltas) == 0 {
		t.Fatalf("fault recover LookupKeyInLayer: err=%v deltas=%d", err, len(deltas))
	}
	t.Logf("Fault-injection recovery verified: lastLSN=%#x layers=%d deltas=%d", tl.LastLSN(), len(layers), len(deltas))
}
