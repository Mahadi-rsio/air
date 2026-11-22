package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/Mahadi-rsio/air/internal/readadapter"
	"github.com/jackc/pgx/v5"
)

type CapResult struct {
	Profile            string  `json:"profile"`
	Tenants            int     `json:"tenants"`
	Workload           string  `json:"workload"`
	DurationSec        float64 `json:"duration_sec"`
	QueriesTotal       uint64  `json:"queries_total"`
	QueriesSucceeded   uint64  `json:"queries_succeeded"`
	QueriesFailed      uint64  `json:"queries_failed"`
	QPS                float64 `json:"qps"`
	P50Ms              float64 `json:"p50_ms"`
	P95Ms              float64 `json:"p95_ms"`
	P99Ms              float64 `json:"p99_ms"`
	NormalP50Ms        float64 `json:"normal_p50_ms,omitempty"`
	NormalP95Ms        float64 `json:"normal_p95_ms,omitempty"`
	NormalP99Ms        float64 `json:"normal_p99_ms,omitempty"`
	AbusiveP50Ms       float64 `json:"abusive_p50_ms,omitempty"`
	AbusiveRejected    uint64  `json:"abusive_rejected,omitempty"`
	RSSMB              float64 `json:"rss_mb"`
	PeakRSSMB          float64 `json:"peak_rss_mb"`
	GoHeapAllocMB      float64 `json:"go_heap_alloc_mb"`
	GoHeapInuseMB      float64 `json:"go_heap_inuse_mb"`
	GoAllocRateMBPerS  float64 `json:"go_alloc_rate_mb_s"`
	NumGC              uint32  `json:"num_gc"`
	GCPauseTotalMs     float64 `json:"gc_pause_total_ms"`
	GCMaxPauseMs       float64 `json:"gc_max_pause_ms"`
	SQLiteMemUsedMB    float64 `json:"sqlite_mem_used_mb"`
	SQLiteMemHighMB    float64 `json:"sqlite_mem_high_mb"`
	L1HitRatePct       float64 `json:"l1_hit_rate_pct"`
	PlanCacheHitPct    float64 `json:"plan_cache_hit_pct"`
	PostRecoveryRSSMB  float64 `json:"post_recovery_rss_mb"`
	PostRecoveryHeapMB float64 `json:"post_recovery_heap_mb"`
}

func readProcStatusMB() (rssMB, hwmMB float64) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
					rssMB = kb / 1024.0
				}
			}
		} else if strings.HasPrefix(line, "VmHWM:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
					hwmMB = kb / 1024.0
				}
			}
		}
	}
	return rssMB, hwmMB
}

func percentileMs(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	cp := append([]float64(nil), samples...)
	sort.Float64s(cp)
	idx := int(float64(len(cp)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

// buildSyntheticHeapPage builds a valid PG17 8KiB page with `rows` tuples of (id int4, cat int4, val int4, note text).
func buildSyntheticHeapPage(startID, rows int) []byte {
	pg := make([]byte, readadapter.PGPageSize)
	binary.LittleEndian.PutUint16(pg[12:14], readadapter.PGPageHeaderSize)
	binary.LittleEndian.PutUint16(pg[14:16], readadapter.PGPageSize)
	binary.LittleEndian.PutUint16(pg[16:18], readadapter.PGPageSize)
	binary.LittleEndian.PutUint16(pg[18:20], readadapter.PGExpectedPageSizeVer)

	upper := readadapter.PGPageSize
	for i := 0; i < rows; i++ {
		id := int32(startID + i)
		cat := int32((id % 10) + 1)
		val := int32(id * 3)
		note := fmt.Sprintf("item_%d", id)
		// 23-byte hdr + 1 (align to 24) + 12 (3x int4) + 1+len(note)
		tupLen := 24 + 12 + 1 + len(note)
		aligned := (tupLen + 7) &^ 7
		if upper-aligned < readadapter.PGPageHeaderSize+(i+1)*4 {
			break
		}
		upper -= aligned
		tup := pg[upper : upper+tupLen]
		binary.LittleEndian.PutUint32(tup[0:4], 2) // FrozenTransactionId
		binary.LittleEndian.PutUint32(tup[4:8], 0)
		binary.LittleEndian.PutUint16(tup[18:20], 4)      // 4 attrs
		binary.LittleEndian.PutUint16(tup[20:22], 0x0B00) // XMIN_FROZEN | XMAX_INVALID
		tup[22] = 24
		binary.LittleEndian.PutUint32(tup[24:28], uint32(id))
		binary.LittleEndian.PutUint32(tup[28:32], uint32(cat))
		binary.LittleEndian.PutUint32(tup[32:36], uint32(val))
		tup[36] = byte(((1 + len(note)) << 1) | 1)
		copy(tup[37:], note)

		lpOff := readadapter.PGPageHeaderSize + i*4
		lp := uint32(upper&0x7FFF) | (uint32(readadapter.LPNormal) << 15) | (uint32(tupLen&0x7FFF) << 17)
		binary.LittleEndian.PutUint32(pg[lpOff:lpOff+4], lp)
		binary.LittleEndian.PutUint16(pg[12:14], uint16(lpOff+4))
		binary.LittleEndian.PutUint16(pg[14:16], uint16(upper))
	}
	return pg
}

// buildSyntheticCatalogTuple builds a PG17 heap tuple for catalog relations.
func buildSyntheticTuple(natts int, body []byte) []byte {
	tup := make([]byte, 24+len(body))
	binary.LittleEndian.PutUint32(tup[0:4], 2) // FrozenTransactionId
	binary.LittleEndian.PutUint32(tup[4:8], 0)
	binary.LittleEndian.PutUint16(tup[18:20], uint16(natts))
	binary.LittleEndian.PutUint16(tup[20:22], 0x0B00) // XMIN_FROZEN | XMAX_INVALID
	tup[22] = 24
	copy(tup[24:], body)
	return tup
}

func makePageWithTuples(tuples ...[]byte) []byte {
	pg := make([]byte, readadapter.PGPageSize)
	binary.LittleEndian.PutUint16(pg[12:14], readadapter.PGPageHeaderSize)
	binary.LittleEndian.PutUint16(pg[14:16], readadapter.PGPageSize)
	binary.LittleEndian.PutUint16(pg[16:18], readadapter.PGPageSize)
	binary.LittleEndian.PutUint16(pg[18:20], readadapter.PGExpectedPageSizeVer)
	upper := readadapter.PGPageSize
	for i, tup := range tuples {
		aligned := (len(tup) + 7) &^ 7
		upper -= aligned
		copy(pg[upper:upper+len(tup)], tup)
		lpOff := readadapter.PGPageHeaderSize + i*4
		lp := uint32(upper&0x7FFF) | (uint32(readadapter.LPNormal) << 15) | (uint32(len(tup)&0x7FFF) << 17)
		binary.LittleEndian.PutUint32(pg[lpOff:lpOff+4], lp)
		binary.LittleEndian.PutUint16(pg[12:14], uint16(lpOff+4))
		binary.LittleEndian.PutUint16(pg[14:16], uint16(upper))
	}
	return pg
}

func buildCatalogTemplates(dbName string) map[uint32][]byte {
	// 1. pg_database (1262): oid(4), datname(64), datdba(4), encoding(4), datlocprovider(1),
	//    datistemplate(1), datallowconn(1), dathasloginevt(1), datconnlimit(4), datfrozenxid(4), datminmxid(4), dattablespace(4)
	dbBody := make([]byte, 96)
	binary.LittleEndian.PutUint32(dbBody[0:4], 5)
	copy(dbBody[4:68], dbName)
	binary.LittleEndian.PutUint32(dbBody[68:72], 10)
	binary.LittleEndian.PutUint32(dbBody[72:76], 6)
	dbBody[76] = 'c'
	dbBody[77] = 0
	dbBody[78] = 1
	dbBody[79] = 0
	binary.LittleEndian.PutUint32(dbBody[80:84], 0xFFFFFFFF)
	binary.LittleEndian.PutUint32(dbBody[84:88], 726)
	binary.LittleEndian.PutUint32(dbBody[88:92], 1)
	binary.LittleEndian.PutUint32(dbBody[92:96], 1663)

	// 2. pg_namespace (2615): oid(4), nspname(64)
	nsBody := make([]byte, 68)
	binary.LittleEndian.PutUint32(nsBody[0:4], 2200)
	copy(nsBody[4:68], "public")

	// 3. pg_class (1259): 17 cols -> oid(4), relname(64), relnamespace(4), reltype(4), reloftype(4), relowner(4),
	//    relam(4), relfilenode(4), reltablespace(4), relpages(4), reltuples(4), relallvisible(4),
	//    reltoastrelid(4), relhasindex(1), relisshared(1), relpersistence(1), relkind(1)
	clsBody := make([]byte, 116)
	binary.LittleEndian.PutUint32(clsBody[0:4], 16384)
	copy(clsBody[4:68], "items")
	binary.LittleEndian.PutUint32(clsBody[68:72], 2200)        // relnamespace = public
	binary.LittleEndian.PutUint32(clsBody[84:88], 0)           // relam
	binary.LittleEndian.PutUint32(clsBody[88:92], 16384)       // relfilenode
	binary.LittleEndian.PutUint32(clsBody[92:96], 1663)        // reltablespace
	binary.LittleEndian.PutUint32(clsBody[96:100], 8)          // relpages
	binary.LittleEndian.PutUint32(clsBody[100:104], 0x43c80000) // reltuples = 400.0
	clsBody[114] = 'p'
	clsBody[115] = 'r'

	// 4. pg_attribute (1249): 18 fixed cols ->
	// attrelid(4), attname(64), atttypid(4), attlen(2), attnum(2), attcacheoff(4), atttypmod(4), attndims(2),
	// attbyval(1), attalign(1), attstorage(1), attcompression(1), attnotnull(1), atthasdef(1), atthasmissing(1),
	// attidentity(1), attgenerated(1), attisdropped(1) = 96 bytes
	makeAtt := func(name string, typOID uint32, attlen int16, attnum int16) []byte {
		b := make([]byte, 96)
		binary.LittleEndian.PutUint32(b[0:4], 16384)
		copy(b[4:68], name)
		binary.LittleEndian.PutUint32(b[68:72], typOID)
		binary.LittleEndian.PutUint16(b[72:74], uint16(attlen))
		binary.LittleEndian.PutUint16(b[74:76], uint16(attnum))
		binary.LittleEndian.PutUint32(b[76:80], 0xFFFFFFFF)
		binary.LittleEndian.PutUint32(b[80:84], 0xFFFFFFFF)
		return buildSyntheticTuple(18, b)
	}

	return map[uint32][]byte{
		readadapter.PGDatabaseRelID:  makePageWithTuples(buildSyntheticTuple(12, dbBody)),
		readadapter.PGNamespaceRelID: makePageWithTuples(buildSyntheticTuple(2, nsBody)),
		readadapter.PGClassRelID:     makePageWithTuples(buildSyntheticTuple(17, clsBody)),
		readadapter.PGAttributeRelID: makePageWithTuples(
			makeAtt("id", 23, 4, 1),
			makeAtt("cat", 23, 4, 2),
			makeAtt("val", 23, 4, 3),
			makeAtt("note", 25, -1, 4),
		),
	}
}

type memBackend struct {
	mu           sync.RWMutex
	pages        map[string][]byte
	catalogPages map[uint32][]byte
	lsn          atomic.Uint64
}

func newMemBackend() *memBackend {
	b := &memBackend{
		pages:        make(map[string][]byte),
		catalogPages: buildCatalogTemplates("postgres"),
	}
	b.lsn.Store(0x1000000)
	return b
}

func (b *memBackend) key(tenantID string, k pageserver.Key) string {
	return fmt.Sprintf("%s:%d:%d:%d:%d:%d", tenantID, k.Kind, k.SpcOID, k.DbOID, k.RelNumber, k.BlockNum)
}

func (b *memBackend) PutPage(tenantID string, k pageserver.Key, pg []byte) {
	b.mu.Lock()
	b.pages[b.key(tenantID, k)] = pg
	b.mu.Unlock()
}

func (b *memBackend) GetPageAtLSN(_ context.Context, tenantID string, key pageserver.Key, _ uint64) ([]byte, error) {
	if key.BlockNum == 0 {
		if catPg, ok := b.catalogPages[key.RelNumber]; ok {
			cp := make([]byte, len(catPg))
			copy(cp, catPg)
			return cp, nil
		}
	}
	b.mu.RLock()
	pg, ok := b.pages[b.key(tenantID, key)]
	b.mu.RUnlock()
	if !ok {
		return nil, pageserver.ErrNotFound
	}
	cp := make([]byte, len(pg))
	copy(cp, pg)
	return cp, nil
}

func (b *memBackend) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	var out []byte
	var count uint32
	for i := uint32(0); i < nblocks; i++ {
		pg, err := b.GetPageAtLSN(ctx, tenantID, pageserver.RelBlockKey(spcOID, dbOID, relNumber, forkNum, startBlock+i), lsn)
		if err != nil {
			break
		}
		out = append(out, pg...)
		count++
	}
	if count == 0 {
		return nil, 0, pageserver.ErrNotFound
	}
	return out, count, nil
}

func (b *memBackend) GetRelSize(_ context.Context, _ string, _, _, relNumber uint32, _ uint8, _ uint64) (uint32, error) {
	if _, ok := b.catalogPages[relNumber]; ok {
		return 1, nil
	}
	if relNumber == 16384 {
		return 8, nil
	}
	return 0, nil
}

func (b *memBackend) GetRelExists(_ context.Context, _ string, _, _, relNumber uint32, _ uint8, _ uint64) (bool, error) {
	if _, ok := b.catalogPages[relNumber]; ok {
		return true, nil
	}
	return relNumber == 16384, nil
}

func (b *memBackend) GetTenantGeneration(_ string) string {
	return "gen-1"
}

type memSnapProv struct {
	lsn *atomic.Uint64
}

func (p *memSnapProv) CurrentSnapshot(_ context.Context, _, _ string) (readadapter.SnapshotInfo, error) {
	return readadapter.SnapshotInfo{
		LSN:  p.lsn.Load(),
		Xmin: 1,
		Xmax: 1000,
	}, nil
}

func main() {
	profile := flag.String("profile", "micro-512mb", "Memory profile name")
	tenants := flag.Int("tenants", 50, "Number of tenants")
	workload := flag.String("workload", "small", "Workload: idle, small, expensive, abusive, mixed")
	duration := flag.Duration("duration", 8*time.Second, "Workload duration")
	l1MB := flag.Int64("l1mb", 16, "L1 cache MB")
	globalMemMB := flag.Int64("globalmem", 128, "Global execution memory MB")
	tenantMemMB := flag.Int64("tenantmem", 32, "Per-tenant execution memory MB")
	cacheKB := flag.Int("cachekb", 1024, "SQLite cache KB per reader")
	readers := flag.Int("readers", 2, "Readers per tenant")
	workers := flag.Int("workers", 8, "Concurrent client workers")
	goMemLimitMB := flag.Int64("gomemlimit", 0, "Optional GOMEMLIMIT in MB")
	flag.Parse()

	if *goMemLimitMB > 0 {
		debug.SetMemoryLimit(*goMemLimitMB * 1024 * 1024)
	}

	tmpDir, err := os.MkdirTemp("", "air_cap17_")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	mb := newMemBackend()
	// Populate shared 8 blocks per tenant (shared page template to avoid harness test-data inflation)
	sharedPages := make([][]byte, 8)
	for blk := 0; blk < 8; blk++ {
		sharedPages[blk] = buildSyntheticHeapPage(blk*50+1, 50)
	}
	for tIdx := 0; tIdx < *tenants; tIdx++ {
		tid := fmt.Sprintf("t_%04d", tIdx)
		for blk := 0; blk < 8; blk++ {
			mb.PutPage(tid, pageserver.RelBlockKey(1663, 5, 16384, 0, uint32(blk)), sharedPages[blk])
		}
	}

	srv, err := readadapter.NewSQLiteReadServer(readadapter.SQLiteServerConfig{
		DataDir:                filepath.Join(tmpDir, "srv"),
		PGPort:                 0,
		CacheKB:                *cacheKB,
		ReadersPerTenant:       *readers,
		L1CacheBytes:           *l1MB * 1024 * 1024,
		CacheDir:               filepath.Join(tmpDir, "l2"),
		NativeBackend:          mb,
		NativeSnapProv:         &memSnapProv{lsn: &mb.lsn},
		GlobalMemoryLimitBytes: *globalMemMB * 1024 * 1024,
		TenantMemoryLimitBytes: *tenantMemMB * 1024 * 1024,
		QueryTimeout:           10 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	if err := srv.Start(); err != nil {
		panic(err)
	}
	defer srv.Stop()

	ctx := context.Background()
	activeTenants := *tenants
	if *workload == "idle" && *tenants > 10 {
		activeTenants = *tenants / 10
	}

	var mStart runtime.MemStats
	runtime.ReadMemStats(&mStart)
	startTime := time.Now()

	var qOK, qFail, abusiveRejected atomic.Uint64
	var latMu sync.Mutex
	var allLat, normalLat, abusiveLat []float64

	stopAt := startTime.Add(*duration)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1700 + workerID)))
			// Maintain per-worker connection cache to avoid TCP handshake dominance
			conns := make(map[string]*pgx.Conn)
			defer func() {
				for _, c := range conns {
					_ = c.Close(ctx)
				}
			}()

			for time.Now().Before(stopAt) {
				var tid string
				isAbusive := false
				if *workload == "abusive" && workerID < *workers/2 {
					tid = "t_0000"
					isAbusive = true
				} else {
					tIdx := rng.Intn(activeTenants)
					if *workload == "abusive" && activeTenants > 1 {
						tIdx = 1 + rng.Intn(activeTenants-1)
					}
					tid = fmt.Sprintf("t_%04d", tIdx)
				}

				c, ok := conns[tid]
				if !ok {
					if len(conns) >= 16 {
						for k, old := range conns {
							_ = old.Close(ctx)
							delete(conns, k)
							break
						}
					}
					var cerr error
					c, cerr = pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=%s sslmode=disable", srv.Port(), tid))
					if cerr != nil {
						qFail.Add(1)
						continue
					}
					conns[tid] = c
				}

				var sql string
				switch *workload {
				case "small", "idle":
					sql = fmt.Sprintf("SELECT id, val, note FROM items WHERE id = %d", rng.Intn(400)+1)
				case "expensive":
					sql = "SELECT a.cat, count(*), sum(a.val) FROM items a JOIN items b ON a.cat = b.cat GROUP BY a.cat ORDER BY sum(a.val) DESC"
				case "abusive":
					if isAbusive {
						sql = "SELECT a.id, b.id, a.note FROM items a JOIN items b ON a.cat = b.cat ORDER BY a.val DESC, b.id ASC LIMIT 100"
					} else {
						sql = fmt.Sprintf("SELECT id, val, note FROM items WHERE id = %d", rng.Intn(400)+1)
					}
				case "mixed":
					if rng.Intn(10) < 3 {
						mb.lsn.Add(64)
					}
					if rng.Intn(5) == 0 {
						sql = "SELECT cat, count(*), sum(val) FROM items GROUP BY cat ORDER BY cat ASC"
					} else {
						sql = fmt.Sprintf("SELECT id, val, note FROM items WHERE id = %d", rng.Intn(400)+1)
					}
				}

				t0 := time.Now()
				rows, qerr := c.Query(ctx, sql)
				if qerr != nil {
					if qFail.Add(1) == 1 {
						fmt.Fprintf(os.Stderr, "first query err: %v\n", qerr)
					}
					if isAbusive {
						abusiveRejected.Add(1)
					}
					continue
				}
				for rows.Next() {
				}
				if rows.Err() != nil {
					qFail.Add(1)
					if isAbusive {
						abusiveRejected.Add(1)
					}
					rows.Close()
					continue
				}
				rows.Close()
				qOK.Add(1)
				ms := float64(time.Since(t0).Microseconds()) / 1000.0
				latMu.Lock()
				if len(allLat) < 50000 {
					allLat = append(allLat, ms)
				}
				if *workload == "abusive" {
					if isAbusive {
						if len(abusiveLat) < 20000 {
							abusiveLat = append(abusiveLat, ms)
						}
					} else if len(normalLat) < 20000 {
						normalLat = append(normalLat, ms)
					}
				}
				latMu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(startTime).Seconds()

	var mEnd runtime.MemStats
	runtime.ReadMemStats(&mEnd)
	rssMB, hwmMB := readProcStatusMB()
	metrics := srv.DetailedMetrics()

	// Measure post-workload memory recovery after GC
	runtime.GC()
	debug.FreeOSMemory()
	var mRec runtime.MemStats
	runtime.ReadMemStats(&mRec)
	postRSS, _ := readProcStatusMB()

	var l1HitPct float64
	if metrics.Cache.L1Hits+metrics.Cache.L1Misses > 0 {
		l1HitPct = 100.0 * float64(metrics.Cache.L1Hits) / float64(metrics.Cache.L1Hits+metrics.Cache.L1Misses)
	}
	var planHitPct float64
	if metrics.PlanCache.Hits+metrics.PlanCache.Misses > 0 {
		planHitPct = 100.0 * float64(metrics.PlanCache.Hits) / float64(metrics.PlanCache.Hits+metrics.PlanCache.Misses)
	}

	var maxPauseNs uint64
	for _, p := range mEnd.PauseNs {
		if p > maxPauseNs {
			maxPauseNs = p
		}
	}

	totalQ := qOK.Load() + qFail.Load()
	res := CapResult{
		Profile:            *profile,
		Tenants:            *tenants,
		Workload:           *workload,
		DurationSec:        elapsed,
		QueriesTotal:       totalQ,
		QueriesSucceeded:   qOK.Load(),
		QueriesFailed:      qFail.Load(),
		QPS:                float64(qOK.Load()) / elapsed,
		P50Ms:              percentileMs(allLat, 0.50),
		P95Ms:              percentileMs(allLat, 0.95),
		P99Ms:              percentileMs(allLat, 0.99),
		NormalP50Ms:        percentileMs(normalLat, 0.50),
		NormalP95Ms:        percentileMs(normalLat, 0.95),
		NormalP99Ms:        percentileMs(normalLat, 0.99),
		AbusiveP50Ms:       percentileMs(abusiveLat, 0.50),
		AbusiveRejected:    abusiveRejected.Load(),
		RSSMB:              rssMB,
		PeakRSSMB:          hwmMB,
		GoHeapAllocMB:      float64(mEnd.HeapAlloc) / (1024 * 1024),
		GoHeapInuseMB:      float64(mEnd.HeapInuse) / (1024 * 1024),
		GoAllocRateMBPerS:  float64(mEnd.TotalAlloc-mStart.TotalAlloc) / (1024 * 1024) / elapsed,
		NumGC:              mEnd.NumGC - mStart.NumGC,
		GCPauseTotalMs:     float64(mEnd.PauseTotalNs-mStart.PauseTotalNs) / 1e6,
		GCMaxPauseMs:       float64(maxPauseNs) / 1e6,
		SQLiteMemUsedMB:    float64(metrics.Memory.SQLiteUsedBytes) / (1024 * 1024),
		SQLiteMemHighMB:    float64(metrics.Memory.SQLiteHighwater) / (1024 * 1024),
		L1HitRatePct:       l1HitPct,
		PlanCacheHitPct:    planHitPct,
		PostRecoveryRSSMB:  postRSS,
		PostRecoveryHeapMB: float64(mRec.HeapAlloc) / (1024 * 1024),
	}
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
}
