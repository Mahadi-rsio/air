package pageserver

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type phase24RunMetrics struct {
	RunIdx        int
	WallMs        float64
	ThroughputMBs float64
	SampleCount   int
	MeanUs        float64
	MedianUs      float64
	StdDevUs      float64
	MinUs         float64
	MaxUs         float64
	P50Us         float64
	P95Us         float64
	P99Us         float64
	P999Us        float64
	CVPct         float64
	AllocsMB      float64
	NumGC         uint32
	GCPauseMs     float64
	Goroutines    int
	SamplesUs     []float64
}

type phase24SummaryStats struct {
	N      int
	Mean   float64
	Median float64
	StdDev float64
	Min    float64
	Max    float64
	P50    float64
	P95    float64
	P99    float64
	CVPct  float64
}

func calcStats(vals []float64) phase24SummaryStats {
	if len(vals) == 0 {
		return phase24SummaryStats{}
	}
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	n := len(cp)
	sum := 0.0
	for _, v := range cp {
		sum += v
	}
	mean := sum / float64(n)
	var sq float64
	for _, v := range cp {
		d := v - mean
		sq += d * d
	}
	sd := math.Sqrt(sq / float64(n))
	cv := 0.0
	if mean > 0 {
		cv = (sd / mean) * 100.0
	}
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return cp[idx]
	}
	var median float64
	if n%2 == 1 {
		median = cp[n/2]
	} else {
		median = 0.5 * (cp[n/2-1] + cp[n/2])
	}
	return phase24SummaryStats{
		N:      n,
		Mean:   mean,
		Median: median,
		StdDev: sd,
		Min:    cp[0],
		Max:    cp[n-1],
		P50:    pct(0.50),
		P95:    pct(0.95),
		P99:    pct(0.99),
		CVPct:  cv,
	}
}

func readProcVmHWMB() float64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmHWM:") || strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && strings.HasPrefix(line, "VmHWM:") {
				kb, err := strconv.ParseFloat(fields[1], 64)
				if err == nil {
					return kb / 1024.0
				}
			}
		}
	}
	return 0
}

func readProcVmRSSMB() float64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseFloat(fields[1], 64)
				if err == nil {
					return kb / 1024.0
				}
			}
		}
	}
	return 0
}

func runPhase24SingleRun(t *testing.T, mode string, memMB, tenants, batchesPerTenant, recsPerBatch, payloadSize int, shared bool, runIdx int) phase24RunMetrics {
	t.Helper()
	ctx := context.Background()
	layerDir := t.TempDir()

	enableCAWAL := mode != "A_disabled"
	ps, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		EnableCAWAL:       enableCAWAL,
		MemoryBudget: MemoryBudgetConfig{
			MemoryLimitBytes:    int64(memMB) * 1024 * 1024,
			ApplyGoRuntimeLimit: false,
		},
	})
	if err != nil {
		t.Fatalf("New ps: %v", err)
	}
	if enableCAWAL && ps.Store().CAWALStore() != nil {
		if mode == "B_unadaptive_cawal" {
			ps.Store().CAWALStore().SetInlineThreshold(0)
		} else {
			ps.Store().CAWALStore().SetInlineThreshold(DefaultCAWALInlineThresholdBytes)
		}
	}

	tenantIDs := make([]string, tenants)
	tenantRecs := make([][]*DecodedWALRecord, tenants)
	for i := 0; i < tenants; i++ {
		tenantIDs[i] = fmt.Sprintf("p24-t-%03d", i)
		ps.EnsureTenant(tenantIDs[i], 1)
		tenantRecs[i] = makeSyntheticWALRecords(i, batchesPerTenant*recsPerBatch, payloadSize, shared)
	}

	runtime.GC()
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	samplesUs := make([]float64, tenants*batchesPerTenant)
	var totalBytes atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for ti := 0; ti < tenants; ti++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tid := tenantIDs[idx]
			recs := tenantRecs[idx]
			base := idx * batchesPerTenant
			for b := 0; b < batchesPerTenant; b++ {
				batch := recs[b*recsPerBatch : (b+1)*recsPerBatch]
				t0 := time.Now()
				if _, _, err := ps.IngestWALRecords(ctx, tid, 1, "gen1", batch, true); err != nil {
					t.Errorf("IngestWALRecords(%s): %v", tid, err)
					return
				}
				samplesUs[base+b] = float64(time.Since(t0).Microseconds())
				for _, r := range batch {
					totalBytes.Add(int64(len(r.RawRecord)))
				}
			}
			if _, err := ps.FlushTenant(ctx, tid); err != nil {
				t.Errorf("FlushTenant(%s): %v", tid, err)
			}
		}(ti)
	}
	wg.Wait()
	elapsed := time.Since(start)
	runtime.ReadMemStats(&mAfter)
	goroutines := runtime.NumGoroutine()
	_ = ps.Close()

	st := calcStats(samplesUs)
	p999 := st.Max
	if len(samplesUs) >= 1000 {
		cp := append([]float64(nil), samplesUs...)
		sort.Float64s(cp)
		idx := int(math.Ceil(0.999*float64(len(cp)))) - 1
		if idx >= len(cp) {
			idx = len(cp) - 1
		}
		p999 = cp[idx]
	}

	mb := float64(totalBytes.Load()) / (1024 * 1024)
	return phase24RunMetrics{
		RunIdx:        runIdx,
		WallMs:        float64(elapsed.Milliseconds()),
		ThroughputMBs: mb / elapsed.Seconds(),
		SampleCount:   len(samplesUs),
		MeanUs:        st.Mean,
		MedianUs:      st.Median,
		StdDevUs:      st.StdDev,
		MinUs:         st.Min,
		MaxUs:         st.Max,
		P50Us:         st.P50,
		P95Us:         st.P95,
		P99Us:         st.P99,
		P999Us:        p999,
		CVPct:         st.CVPct,
		AllocsMB:      float64(mAfter.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024),
		NumGC:         mAfter.NumGC - mBefore.NumGC,
		GCPauseMs:     float64(mAfter.PauseTotalNs-mBefore.PauseTotalNs) / 1e6,
		Goroutines:    goroutines,
		SamplesUs:     samplesUs,
	}
}

func TestPhase24_BenchmarkMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Phase 24 matrix in -short mode")
	}
	// Workloads: 20 batches x 5 records x 2048 B = 200 KB logical WAL per tenant per run.
	// At 50-100 tenants this generates 10-20 MB logical WAL per run across 10-20 repeats.
	const batchesPerTenant = 20
	const recsPerBatch = 5
	const payloadSize = 2048
	const memMB = 512

	tenantCounts := []int{1, 8, 20, 30, 50, 75, 100}
	modes := []string{"A_disabled", "B_unadaptive_cawal", "C_adaptive_cawal"}

	t.Log("CONFIG | TENANTS | RUNS | P99_MEAN_US | P99_MEDIAN_US | P99_STDDEV_US | P99_MIN_US | P99_MAX_US | P99_CV_% | POOLED_P50_US | POOLED_P95_US | POOLED_P99_US | POOLED_P999_US | TPUT_MEAN_MBPS | GC_MEAN")
	for _, tc := range tenantCounts {
		runs := 10
		if tc == 50 {
			runs = 20
		}
		for _, mode := range modes {
			var p99s, tputs, gcs []float64
			var pooled []float64
			for r := 0; r < runs; r++ {
				res := runPhase24SingleRun(t, mode, memMB, tc, batchesPerTenant, recsPerBatch, payloadSize, true, r)
				p99s = append(p99s, res.P99Us)
				tputs = append(tputs, res.ThroughputMBs)
				gcs = append(gcs, float64(res.NumGC))
				pooled = append(pooled, res.SamplesUs...)
			}
			p99Stats := calcStats(p99s)
			tputStats := calcStats(tputs)
			gcStats := calcStats(gcs)
			pooledStats := calcStats(pooled)
			pooledP999 := pooledStats.Max
			if len(pooled) >= 1000 {
				cp := append([]float64(nil), pooled...)
				sort.Float64s(cp)
				idx := int(math.Ceil(0.999*float64(len(cp)))) - 1
				if idx >= len(cp) {
					idx = len(cp) - 1
				}
				pooledP999 = cp[idx]
			}
			t.Logf("%s | %d | %d | %.1f | %.1f | %.1f | %.1f | %.1f | %.2f | %.1f | %.1f | %.1f | %.1f | %.2f | %.1f",
				mode, tc, runs,
				p99Stats.Mean, p99Stats.Median, p99Stats.StdDev, p99Stats.Min, p99Stats.Max, p99Stats.CVPct,
				pooledStats.P50, pooledStats.P95, pooledStats.P99, pooledP999,
				tputStats.Mean, gcStats.Mean)
		}
	}
}

func readSchedLatencyP99Ms() (p50Ms, p99Ms, gcCPUSec, userCPUSec float64) {
	samples := []metrics.Sample{
		{Name: "/sched/latencies:seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/user:cpu-seconds"},
	}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindFloat64Histogram {
		h := samples[0].Value.Float64Histogram()
		var total uint64
		for _, c := range h.Counts {
			total += c
		}
		if total > 0 {
			var cum uint64
			for i, c := range h.Counts {
				cum += c
				frac := float64(cum) / float64(total)
				b := h.Buckets[i+1]
				if math.IsInf(b, 1) {
					b = h.Buckets[i]
				}
				if p50Ms == 0 && frac >= 0.50 {
					p50Ms = b * 1000.0
				}
				if p99Ms == 0 && frac >= 0.99 {
					p99Ms = b * 1000.0
				}
			}
		}
	}
	if samples[1].Value.Kind() == metrics.KindFloat64 {
		gcCPUSec = samples[1].Value.Float64()
	}
	if samples[2].Value.Kind() == metrics.KindFloat64 {
		userCPUSec = samples[2].Value.Float64()
	}
	return p50Ms, p99Ms, gcCPUSec, userCPUSec
}

func TestPhase24_SchedulerStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Phase 24 scheduler stability in -short mode")
	}
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)

	procsList := []int{1, 2, 4}
	modes := []string{"A_disabled", "C_adaptive_cawal"}
	const tenants = 50
	const runs = 10

	t.Log("GOMAXPROCS | MODE | P99_MEAN_US | P99_MEDIAN_US | P99_STDDEV_US | P99_MAX_US | P99_CV_% | SCHED_P50_MS | SCHED_P99_MS | DELTA_GC_CPU_S | DELTA_USER_CPU_S | TPUT_MBPS")
	for _, gp := range procsList {
		runtime.GOMAXPROCS(gp)
		for _, mode := range modes {
			_, _, gc0, usr0 := readSchedLatencyP99Ms()
			var p99s, tputs []float64
			for r := 0; r < runs; r++ {
				res := runPhase24SingleRun(t, mode, 512, tenants, 20, 5, 2048, true, r)
				p99s = append(p99s, res.P99Us)
				tputs = append(tputs, res.ThroughputMBs)
			}
			sp50, sp99, gc1, usr1 := readSchedLatencyP99Ms()
			st := calcStats(p99s)
			tp := calcStats(tputs)
			t.Logf("%d | %s | %.1f | %.1f | %.1f | %.1f | %.2f | %.3f | %.3f | %.4f | %.4f | %.2f",
				gp, mode, st.Mean, st.Median, st.StdDev, st.Max, st.CVPct,
				sp50, sp99, gc1-gc0, usr1-usr0, tp.Mean)
		}
	}
}

func TestPhase24_MemoryAndGCStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Phase 24 memory stability in -short mode")
	}
	profiles := []int{256, 512}
	tenantsList := []int{1, 20, 50, 100}
	modes := []bool{false, true}

	t.Log("MEM_MB | TENANTS | CAWAL | PEAK_RSS_MB | HEAP_ALLOC_MB | HEAP_INUSE_MB | HEAP_IDLE_MB | HEAP_RELEASED_MB | TOTAL_ALLOC_MB | ALLOC_RATE_MBPS | NUM_GC | GC_PAUSE_MS | GOROUTINES | RETAINED_AFTER_GC_MB")
	for _, memMB := range profiles {
		for _, tc := range tenantsList {
			for _, enableCAWAL := range modes {
				ctx := context.Background()
				layerDir := t.TempDir()
				ps, err := New(Config{
					DisableUnixSocket: true,
					LayerDir:          layerDir,
					EnableCAWAL:       enableCAWAL,
					MemoryBudget: MemoryBudgetConfig{
						MemoryLimitBytes:    int64(memMB) * 1024 * 1024,
						ApplyGoRuntimeLimit: false,
					},
				})
				if err != nil {
					t.Fatalf("New ps: %v", err)
				}

				tenantIDs := make([]string, tc)
				for i := 0; i < tc; i++ {
					tenantIDs[i] = fmt.Sprintf("p24-mem-%03d", i)
					ps.EnsureTenant(tenantIDs[i], 1)
				}

				runtime.GC()
				var mBefore, mPeak, mRetained runtime.MemStats
				runtime.ReadMemStats(&mBefore)
				start := time.Now()

				var wg sync.WaitGroup
				for i := 0; i < tc; i++ {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						tid := tenantIDs[idx]
						recs := makeSyntheticWALRecords(idx, 100, 2048, true)
						for b := 0; b < 20; b++ {
							_, _, _ = ps.IngestWALRecords(ctx, tid, 1, "gen1", recs[b*5:(b+1)*5], true)
							if b%5 == 4 {
								_, _ = ps.FlushTenant(ctx, tid)
							}
						}
						_, _ = ps.FlushTenant(ctx, tid)
						if tl, err := ps.getTimeline(tid); err == nil {
							_, _ = tl.CompactL0Batch(ctx, 8, 4)
							_, _ = tl.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 16385, 0, 0), recs[len(recs)-1].EndLSN)
						}
					}(i)
				}
				wg.Wait()
				dur := time.Since(start)
				runtime.ReadMemStats(&mPeak)
				peakRSS := readProcVmHWMB()
				gors := runtime.NumGoroutine()

				_ = ps.Close()
				runtime.GC()
				runtime.ReadMemStats(&mRetained)

				totalAllocMB := float64(mPeak.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024)
				t.Logf("%d | %d | %v | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %d | %.2f | %d | %.2f",
					memMB, tc, enableCAWAL,
					peakRSS,
					float64(mPeak.HeapAlloc)/(1024*1024),
					float64(mPeak.HeapInuse)/(1024*1024),
					float64(mPeak.HeapIdle)/(1024*1024),
					float64(mPeak.HeapReleased)/(1024*1024),
					totalAllocMB,
					totalAllocMB/dur.Seconds(),
					mPeak.NumGC-mBefore.NumGC,
					float64(mPeak.PauseTotalNs-mBefore.PauseTotalNs)/1e6,
					gors,
					float64(mRetained.HeapAlloc)/(1024*1024),
				)
			}
		}
	}
}
