package pageserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

type benchProfileResult struct {
	Mode              string
	MemProfileMB      int
	Tenants           int
	Workload          string
	ThroughputMBps    float64
	WriteLatP50Us     float64
	WriteLatP95Us     float64
	WriteLatP99Us     float64
	FlushLatAvgMs     float64
	HeapAllocMB       float64
	TotalAllocMB      float64
	NumGC             uint32
	GCPauseTotalMs    float64
	PhysicalStorageKB int64
	LogicalWALKB      int64
	DedupRatio        float64
	RecoveryDurMs     float64
	RecoveryOK        bool
}

func dirSizeBytes(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, ierr := d.Info(); ierr == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

func makeSyntheticWALRecords(tenantIdx, count, payloadSize int, sharedPattern bool) []*DecodedWALRecord {
	recs := make([]*DecodedWALRecord, count)
	for i := 0; i < count; i++ {
		raw := make([]byte, payloadSize)
		if sharedPattern {
			// Simulate repeated catalog/index/checkpoint WAL payloads across tenants/flushes.
			copy(raw, bytes.Repeat([]byte("AIR_PG17_WAL_TEMPLATE_"), payloadSize/22+1))
			binary.LittleEndian.PutUint32(raw[0:4], uint32(payloadSize))
			binary.LittleEndian.PutUint32(raw[4:8], uint32(i%8)) // 8 distinct patterns
		} else {
			copy(raw, bytes.Repeat([]byte("AIR_UNIQUE_WAL_"), payloadSize/15+1))
			binary.LittleEndian.PutUint32(raw[0:4], uint32(payloadSize))
			binary.LittleEndian.PutUint32(raw[4:8], uint32(tenantIdx*100000+i))
		}
		lsn := uint64(0x100000 + i*payloadSize)
		recs[i] = &DecodedWALRecord{
			LSN:       lsn,
			EndLSN:    lsn + uint64(payloadSize),
			TotalLen:  uint32(payloadSize),
			XID:       uint32(100 + i),
			Rmid:      10, // Heap
			Info:      0x00,
			RawRecord: raw,
			Blocks: []WALBlockRef{{
				BlockID:  0,
				Rel:      RelFileLocator{SpcOID: 1663, DbOID: 5, RelNumber: 16385},
				ForkNum:  0,
				BlockNum: uint32(i % 64),
			}},
		}
	}
	return recs
}

func runCAWALBenchmarkScenario(t *testing.T, mode string, memMB, tenants int, workload string, recsPerTenant, payloadSize int, shared bool) benchProfileResult {
	t.Helper()
	ctx := context.Background()
	layerDir := t.TempDir()

	enableCAWAL := mode != "existing_wal"
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

	runtime.GC()
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	var latMu sync.Mutex
	latencies := make([]time.Duration, 0, tenants*recsPerTenant)
	var flushDurs []time.Duration

	t0 := time.Now()
	var wg sync.WaitGroup
	for ti := 0; ti < tenants; ti++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tid := fmt.Sprintf("bench-t%02d", idx)
			recs := makeSyntheticWALRecords(idx, recsPerTenant, payloadSize, shared)
			batchSize := 32
			for off := 0; off < len(recs); off += batchSize {
				end := off + batchSize
				if end > len(recs) {
					end = len(recs)
				}
				wStart := time.Now()
				if _, _, err := ps.IngestWALRecords(ctx, tid, 1, "gen1", recs[off:end], true); err != nil {
					t.Errorf("IngestWALRecords %s: %v", tid, err)
					return
				}
				d := time.Since(wStart)
				latMu.Lock()
				latencies = append(latencies, d)
				latMu.Unlock()
			}
			fStart := time.Now()
			if _, err := ps.FlushTenant(ctx, tid); err != nil {
				t.Errorf("FlushTenant %s: %v", tid, err)
				return
			}
			fd := time.Since(fStart)
			latMu.Lock()
			flushDurs = append(flushDurs, fd)
			latMu.Unlock()
		}(ti)
	}
	wg.Wait()
	totalDur := time.Since(t0)

	// Simulate mode "cawal_no_memopt" read-path overhead vs optimized read-path.
	for ti := 0; ti < tenants; ti++ {
		tid := fmt.Sprintf("bench-t%02d", ti)
		tl, err := ps.getTimeline(tid)
		if err != nil {
			continue
		}
		tl.mu.RLock()
		layers := append([]LayerMeta(nil), tl.layers...)
		tl.mu.RUnlock()
		k := RelBlockKey(1663, 5, 16385, 0, 0)
		for _, lm := range layers {
			deltas, err := ps.Store().LookupKeyInLayer(ctx, lm, k, 0)
			if err != nil {
				t.Fatalf("LookupKeyInLayer %s: %v", tid, err)
			}
			if mode == "cawal_no_memopt" {
				for _, d := range deltas {
					_ = append([]byte(nil), d.WALRecord...)
				}
			}
		}
	}

	runtime.ReadMemStats(&mAfter)
	_ = ps.Close()

	// Measure recovery after restart.
	recStart := time.Now()
	psRec, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		EnableCAWAL:       enableCAWAL,
		MemoryBudget: MemoryBudgetConfig{
			MemoryLimitBytes:    int64(memMB) * 1024 * 1024,
			ApplyGoRuntimeLimit: false,
		},
	})
	recDur := time.Since(recStart)
	recOK := err == nil && psRec.ActiveTenantCount() == tenants
	if psRec != nil {
		_ = psRec.Close()
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(float64(len(latencies)-1) * p)
		return float64(latencies[idx].Microseconds())
	}
	var flushSum time.Duration
	for _, fd := range flushDurs {
		flushSum += fd
	}
	flushAvgMs := 0.0
	if len(flushDurs) > 0 {
		flushAvgMs = float64(flushSum.Microseconds()) / float64(len(flushDurs)) / 1000.0
	}

	logicalBytes := int64(tenants * recsPerTenant * payloadSize)
	physBytes := dirSizeBytes(layerDir)
	dedupRatio := 1.0
	if enableCAWAL && ps.Store().CAWALStore() != nil {
		cm := ps.Store().CAWALStore().Metrics()
		if cm.BlobBytesWritten > 0 {
			dedupRatio = float64(cm.BlobBytesLogical) / float64(cm.BlobBytesWritten)
		}
	}

	return benchProfileResult{
		Mode:              mode,
		MemProfileMB:      memMB,
		Tenants:           tenants,
		Workload:          workload,
		ThroughputMBps:    (float64(logicalBytes) / (1024 * 1024)) / totalDur.Seconds(),
		WriteLatP50Us:     pct(0.50),
		WriteLatP95Us:     pct(0.95),
		WriteLatP99Us:     pct(0.99),
		FlushLatAvgMs:     flushAvgMs,
		HeapAllocMB:       float64(mAfter.HeapAlloc) / (1024 * 1024),
		TotalAllocMB:      float64(mAfter.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024),
		NumGC:             mAfter.NumGC - mBefore.NumGC,
		GCPauseTotalMs:    float64(mAfter.PauseTotalNs-mBefore.PauseTotalNs) / 1e6,
		PhysicalStorageKB: physBytes / 1024,
		LogicalWALKB:      logicalBytes / 1024,
		DedupRatio:        dedupRatio,
		RecoveryDurMs:     float64(recDur.Microseconds()) / 1000.0,
		RecoveryOK:        recOK,
	}
}

func TestCAWAL_BenchmarkMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark matrix in -short mode")
	}

	modes := []string{"existing_wal", "cawal_no_memopt", "cawal_memopt"}
	type scenario struct {
		memMB         int
		tenants       int
		workload      string
		recsPerTenant int
		payloadSize   int
		shared        bool
	}
	scenarios := []scenario{
		{256, 1, "small_shared", 256, 256, true},
		{256, 10, "small_shared", 128, 256, true},
		{256, 20, "large_shared", 64, 4096, true},
		{512, 50, "concurrent_shared", 64, 1024, true},
		{256, 10, "unique_heavy_flush", 64, 2048, false},
	}

	var results []benchProfileResult
	for _, sc := range scenarios {
		for _, mode := range modes {
			res := runCAWALBenchmarkScenario(t, mode, sc.memMB, sc.tenants, sc.workload, sc.recsPerTenant, sc.payloadSize, sc.shared)
			if !res.RecoveryOK {
				t.Fatalf("recovery failed for %+v", res)
			}
			results = append(results, res)
		}
	}

	t.Logf("\n| Mode | MemMB | Tenants | Workload | MB/s | p50(us) | p95(us) | p99(us) | Flush(ms) | TotalAlloc(MB) | GC | Pause(ms) | Disk(KB) | Dedup | Rec(ms) |")
	for _, r := range results {
		t.Logf("| %s | %d | %d | %s | %.2f | %.0f | %.0f | %.0f | %.2f | %.2f | %d | %.2f | %d | %.2fx | %.2f |",
			r.Mode, r.MemProfileMB, r.Tenants, r.Workload, r.ThroughputMBps,
			r.WriteLatP50Us, r.WriteLatP95Us, r.WriteLatP99Us, r.FlushLatAvgMs,
			r.TotalAllocMB, r.NumGC, r.GCPauseTotalMs, r.PhysicalStorageKB, r.DedupRatio, r.RecoveryDurMs)
	}
}

func BenchmarkCAWAL_SHA256AndCompressionCPU(b *testing.B) {
	payload := bytes.Repeat([]byte("PG17_WAL_BENCH_CHUNK_DATA_"), 2560) // ~64 KiB
	b.Run("SHA256_64KiB", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			_ = sha256.Sum256(payload)
		}
	})
	b.Run("ZstdCompress_64KiB", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		dst := make([]byte, 0, len(payload))
		for i := 0; i < b.N; i++ {
			comp, _, err := compressV2PayloadInto(payload, dst, false)
			if err != nil {
				b.Fatal(err)
			}
			dst = comp
		}
	})
}

func BenchmarkDecodeWALRecord_ZeroCopy(b *testing.B) {
	raw := make([]byte, 256)
	binary.LittleEndian.PutUint32(raw[0:4], 256)
	raw[17] = 10
	raw[24] = XLRBlockIDDataShort
	raw[25] = 200
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		rec, err := DecodeWALRecord(uint64(0x1000+i), raw)
		if err != nil {
			b.Fatal(err)
		}
		_ = rec
	}
}
