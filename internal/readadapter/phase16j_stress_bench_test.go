package readadapter

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func BenchmarkPhase16J_MultiTenantScale_PlanAndScheduler(b *testing.B) {
	for _, numTenants := range []int{1, 8, 32, 100, 500} {
		b.Run(fmt.Sprintf("Tenants_%d", numTenants), func(b *testing.B) {
			pc := NewQueryPlanCache(4096)
			mem := NewMemoryManager(MemoryManagerConfig{})
			sched := NewTenantScheduler(SchedulerConfig{
				MaxGlobalConcurrency:     16,
				DefaultTenantConcurrency: 4,
				MaxQueueDepth:            256,
				MemoryManager:            mem,
			})

			// Warm 4 queries per tenant
			for t := 0; t < numTenants; t++ {
				tid := fmt.Sprintf("t_%04d", t)
				for q := 0; q < 4; q++ {
					sql := fmt.Sprintf("SELECT id, val FROM items WHERE cat = $1 AND q = %d", q)
					pc.GetOrTranslateScoped(PlanScope{
						TenantID: tid, Generation: "g1", SchemaVersion: 1, ParamOIDs: []uint32{PGOIDInt4},
					}, sql, func(s string) (string, int, []uint32, []uint32) {
						return s, 1, []uint32{1001}, nil
					})
				}
			}

			var mBefore, mAfter runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&mBefore)

			var latMu sync.Mutex
			lats := make([]int64, 0, min(b.N, 4096))
			var counter atomic.Uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.Background()
				for pb.Next() {
					id := counter.Add(1)
					tid := fmt.Sprintf("t_%04d", int(id)%numTenants)
					q := int(id) % 4
					sql := fmt.Sprintf("SELECT id, val FROM items WHERE cat = $1 AND q = %d", q)

					t0 := time.Now()
					rel, err := sched.Acquire(ctx, tid)
					if err != nil {
						b.Fatal(err)
					}
					budget := mem.BeginQuery(tid, 0)
					_ = budget.Reserve(512)
					_, _ = pc.GetOrTranslateScoped(PlanScope{
						TenantID: tid, Generation: "g1", SchemaVersion: 1, ParamOIDs: []uint32{PGOIDInt4},
					}, sql, func(s string) (string, int, []uint32, []uint32) {
						return s, 1, []uint32{1001}, nil
					})
					budget.Release()
					rel()
					dt := time.Since(t0).Nanoseconds()
					if len(lats) < 4096 {
						latMu.Lock()
						if len(lats) < 4096 {
							lats = append(lats, dt)
						}
						latMu.Unlock()
					}
				}
			})
			b.StopTimer()

			runtime.ReadMemStats(&mAfter)
			sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
			if len(lats) > 0 {
				p50 := float64(lats[len(lats)*50/100]) / 1000.0
				p95 := float64(lats[len(lats)*95/100]) / 1000.0
				p99 := float64(lats[len(lats)*99/100]) / 1000.0
				b.ReportMetric(p50, "p50_us")
				b.ReportMetric(p95, "p95_us")
				b.ReportMetric(p99, "p99_us")
			}
			st := pc.Stats()
			if st.Hits+st.Misses > 0 {
				b.ReportMetric(100.0*float64(st.Hits)/float64(st.Hits+st.Misses), "cache_hit_%")
			}
		})
	}
}

func BenchmarkPhase16J_EndToEnd_CacheHotVsCold_PointLookup(b *testing.B) {
	ctx := context.Background()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(b.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(b.TempDir(), "layers"),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		b.Fatal(err)
	}
	const (
		tenantID  = "t_j_e2e"
		gen       = "g1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(31001)
		lsn       = uint64(0x100)
	)
	ps.SetTenantGeneration(tenantID, gen)
	_ = ps.EnsureTenant(tenantID, 1)

	var tuples [][]byte
	for r := 1; r <= 50; r++ {
		tuples = append(tuples, buildHeapTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, makeInt4TextPayload(int32(r), "payload")))
	}
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, 0, 0, 1, lsn, buildPG17HeapPage(lsn, tuples)); err != nil {
		b.Fatal(err)
	}

	cache, err := NewTieredCacheBackend(ps, CacheConfig{L1MaxBytes: 8 * 1024 * 1024})
	if err != nil {
		b.Fatal(err)
	}
	snapProv := NewStaticSnapshotProvider()
	snapProv.SetSnapshot(tenantID, gen, SnapshotInfo{LSN: lsn})
	ad := NewAdapter(cache, snapProv)
	clog := NewCLOGResolver(cache)
	snap, _ := ad.GetSnapshot(ctx, tenantID, gen)
	defer ad.ReleaseSnapshot(snap)

	conn, err := OpenSQLiteConn(filepath.Join(b.TempDir(), "e2e.db"), false, 2048)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	if err := conn.RegisterAirPGModule(); err != nil {
		b.Fatal(err)
	}

	tbl := DiscoveredTable{
		Schema: "public", Name: "items", RelOID: relNumber,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, RelPages: 1, RelTuples: 50,
		Columns: []DiscoveredColumn{
			{Name: "id", AttNum: 1, PGTypeOID: PGOIDInt4, ColType: ColInt4, SQLiteType: "INTEGER"},
			{Name: "val", AttNum: 2, PGTypeOID: PGOIDText, ColType: ColText, SQLiteType: "TEXT"},
		},
	}
	if _, err := conn.RegisterNativeTableWithOptimizer(tenantID, gen, tbl, ad, clog, NewQueryOptimizer(DefaultCostModelConfig(), nil, nil, cache), NewAdaptiveTracker(64)); err != nil {
		b.Fatal(err)
	}

	b.Run("PreparedCacheHot", func(b *testing.B) {
		oids := []uint32{PGOIDInt4}
		// Warm L1 + prepared stmt cache
		_, _, _ = conn.QueryWithContextPlanTyped(ctx, snap, "SELECT id, val FROM items WHERE id = ?1;", []string{"25"}, oids, 1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, _, err := conn.QueryWithContextPlanTyped(ctx, snap, "SELECT id, val FROM items WHERE id = ?1;", []string{"25"}, oids, 1)
			if err != nil || len(res.Rows) != 1 {
				b.Fatalf("err=%v rows=%d", err, len(res.Rows))
			}
		}
	})
}
