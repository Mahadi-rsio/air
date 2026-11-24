package readadapter

import (
	"container/list"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMaxStatsEntries = 2048
	defaultRowsPerPage     = 50.0
)

// ColumnStats holds PostgreSQL pg_statistic-derived column statistics.
// Distinct follows PostgreSQL's stadistinct convention:
//   - > 0: estimated number of distinct values
//   - < 0: negative fraction of rows that are distinct (e.g. -1.0 = unique)
//   - == 0: unknown
type ColumnStats struct {
	AttNum   int16
	NullFrac float32
	AvgWidth int32
	Distinct float32
}

// TableStats holds relation-level and column-level statistics for a tenant table.
type TableStats struct {
	TenantID      string
	Generation    string
	RelOID        uint32
	RelPages      int32
	RelTuples     float32
	RelAllVisible int32
	Columns       map[int16]ColumnStats
	IndexPages    map[uint32]int32
	IndexTuples   map[uint32]float32
	Version       uint64
	UpdatedAt     time.Time
}

// EstimatedRows returns the estimated live row count for the relation,
// falling back conservatively to nblocksFallback * defaultRowsPerPage when
// PostgreSQL statistics are absent or zero.
func (ts *TableStats) EstimatedRows(nblocksFallback uint32) float64 {
	if ts != nil && ts.RelTuples > 0 {
		return float64(ts.RelTuples)
	}
	blocks := nblocksFallback
	if ts != nil && ts.RelPages > 0 && uint32(ts.RelPages) > blocks {
		blocks = uint32(ts.RelPages)
	}
	if blocks == 0 {
		return 1.0
	}
	return float64(blocks) * defaultRowsPerPage
}

// EstimateSelectivity estimates the fraction of rows (in [0.0001, 1.0]) matching
// a single predicate on attNum with SQLite constraint operator op.
func (ts *TableStats) EstimateSelectivity(attNum int16, op int, estRows float64, isUniqueSingleCol bool) float64 {
	if estRows < 1.0 {
		estRows = 1.0
	}
	var cs ColumnStats
	var hasCS bool
	if ts != nil && ts.Columns != nil {
		cs, hasCS = ts.Columns[attNum]
	}

	nullFrac := 0.0
	if hasCS && cs.NullFrac > 0 && cs.NullFrac <= 1.0 {
		nullFrac = float64(cs.NullFrac)
	}
	nonNullFrac := math.Max(0.01, 1.0-nullFrac)

	switch op {
	case sqliteConstraintISNULL:
		if hasCS && cs.NullFrac > 0 {
			return clampSel(nullFrac, 0.001, 0.99)
		}
		return 0.05
	case sqliteConstraintISNOTNULL:
		return clampSel(nonNullFrac, 0.01, 1.0)
	case sqliteConstraintEQ:
		if isUniqueSingleCol {
			return clampSel(1.0/estRows, 0.0001, 1.0)
		}
		if hasCS && cs.Distinct != 0 {
			var ndistinct float64
			if cs.Distinct > 0 {
				ndistinct = float64(cs.Distinct)
			} else {
				ndistinct = math.Max(1.0, -float64(cs.Distinct)*estRows)
			}
			return clampSel(nonNullFrac/ndistinct, 0.0001, 1.0)
		}
		// Conservative equality fallback when pg_statistic is unavailable.
		return clampSel(math.Min(0.05, 10.0/estRows), 0.0001, 0.5)
	case sqliteConstraintNE:
		eqSel := ts.EstimateSelectivity(attNum, sqliteConstraintEQ, estRows, isUniqueSingleCol)
		return clampSel(nonNullFrac-eqSel, 0.01, 1.0)
	case sqliteConstraintGT, sqliteConstraintGE, sqliteConstraintLT, sqliteConstraintLE:
		if hasCS && cs.Distinct != 0 {
			// Higher cardinality columns tend to have tighter range selectivity.
			return clampSel(0.20*nonNullFrac, 0.001, 0.95)
		}
		return 0.25
	default:
		return 0.5
	}
}

func clampSel(v, minV, maxV float64) float64 {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

type statsCacheKey struct {
	TenantID   string
	Generation string
	RelOID     uint32
}

type statsCacheEntry struct {
	key   statsCacheKey
	stats *TableStats
}

// StatsCache is a bounded, tenant-isolated LRU cache of PostgreSQL relation
// and column statistics.
type StatsCache struct {
	mu             sync.Mutex
	maxEntries     int
	ll             *list.List
	entries        map[statsCacheKey]*list.Element
	tenantVersions map[string]uint64

	hits    atomic.Uint64
	misses  atomic.Uint64
	version atomic.Uint64
}

// Version returns a monotonically increasing global version number bumped on stats updates.
func (sc *StatsCache) Version() uint64 {
	if sc == nil {
		return 0
	}
	return sc.version.Load()
}

// TenantVersion returns a monotonically increasing per-tenant stats version number,
// preventing one tenant's statistics updates from invalidating another tenant's plan cache.
func (sc *StatsCache) TenantVersion(tenantID string) uint64 {
	if sc == nil || tenantID == "" {
		return 0
	}
	sc.mu.Lock()
	v := sc.tenantVersions[tenantID]
	sc.mu.Unlock()
	return v
}

// NewStatsCache creates a bounded StatsCache. If maxEntries <= 0, defaults to 2048.
func NewStatsCache(maxEntries int) *StatsCache {
	if maxEntries <= 0 {
		maxEntries = defaultMaxStatsEntries
	}
	return &StatsCache{
		maxEntries:     maxEntries,
		ll:             list.New(),
		entries:        make(map[statsCacheKey]*list.Element),
		tenantVersions: make(map[string]uint64),
	}
}

// Get retrieves cached TableStats for (tenantID, generation, relOID).
func (sc *StatsCache) Get(tenantID, generation string, relOID uint32) (*TableStats, bool) {
	if sc == nil {
		return nil, false
	}
	key := statsCacheKey{TenantID: tenantID, Generation: generation, RelOID: relOID}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if elem, ok := sc.entries[key]; ok {
		sc.ll.MoveToFront(elem)
		sc.hits.Add(1)
		return elem.Value.(*statsCacheEntry).stats, true
	}
	sc.misses.Add(1)
	return nil, false
}

// Put stores or updates TableStats in the bounded LRU cache.
func (sc *StatsCache) Put(ts *TableStats) {
	if sc == nil || ts == nil || ts.TenantID == "" || ts.RelOID == 0 {
		return
	}
	key := statsCacheKey{TenantID: ts.TenantID, Generation: ts.Generation, RelOID: ts.RelOID}
	sc.version.Add(1)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.tenantVersions[ts.TenantID]++
	if elem, ok := sc.entries[key]; ok {
		ent := elem.Value.(*statsCacheEntry)
		if ts.Version >= ent.stats.Version {
			ent.stats = ts
		}
		sc.ll.MoveToFront(elem)
		return
	}
	for sc.ll.Len() >= sc.maxEntries {
		back := sc.ll.Back()
		if back == nil {
			break
		}
		sc.ll.Remove(back)
		delete(sc.entries, back.Value.(*statsCacheEntry).key)
	}
	elem := sc.ll.PushFront(&statsCacheEntry{key: key, stats: ts})
	sc.entries[key] = elem
}

// PutDiscoveredTables populates the cache from discovered catalog tables at catalogVersion.
func (sc *StatsCache) PutDiscoveredTables(tenantID, generation string, tables []DiscoveredTable, catalogVersion uint64) {
	if sc == nil {
		return
	}
	now := time.Now()
	for _, tbl := range tables {
		idxPages := make(map[uint32]int32, len(tbl.Indexes))
		idxTuples := make(map[uint32]float32, len(tbl.Indexes))
		for _, idx := range tbl.Indexes {
			idxPages[idx.IndexRelOID] = idx.RelPages
			idxTuples[idx.IndexRelOID] = idx.RelTuples
		}
		cols := make(map[int16]ColumnStats, len(tbl.ColumnStats))
		for k, v := range tbl.ColumnStats {
			cols[k] = v
		}
		sc.Put(&TableStats{
			TenantID:      tenantID,
			Generation:    generation,
			RelOID:        tbl.RelOID,
			RelPages:      tbl.RelPages,
			RelTuples:     tbl.RelTuples,
			RelAllVisible: tbl.RelAllVisible,
			Columns:       cols,
			IndexPages:    idxPages,
			IndexTuples:   idxTuples,
			Version:       catalogVersion,
			UpdatedAt:     now,
		})
	}
}

// InvalidateTenant removes all cached statistics for tenantID.
func (sc *StatsCache) InvalidateTenant(tenantID string) {
	if sc == nil {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.tenantVersions[tenantID]++
	for k, elem := range sc.entries {
		if k.TenantID == tenantID {
			sc.ll.Remove(elem)
			delete(sc.entries, k)
		}
	}
}

// InvalidateTable removes cached statistics for a single relation of tenantID
// and bumps only that tenant's stats version.
func (sc *StatsCache) InvalidateTable(tenantID string, relOID uint32) {
	if sc == nil || tenantID == "" || relOID == 0 {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.tenantVersions[tenantID]++
	for k, elem := range sc.entries {
		if k.TenantID == tenantID && k.RelOID == relOID {
			sc.ll.Remove(elem)
			delete(sc.entries, k)
		}
	}
}

// Len returns the number of cached relation statistics entries.
func (sc *StatsCache) Len() int {
	if sc == nil {
		return 0
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.ll.Len()
}

// Stats returns cache hit/miss counts.
func (sc *StatsCache) Stats() (hits, misses uint64) {
	if sc == nil {
		return 0, 0
	}
	return sc.hits.Load(), sc.misses.Load()
}

// EstimateBoundSelectivity estimates predicate selectivity with awareness of
// parameter values (NULL checks, unique equality, small/skewed bounds) and table cardinality.
func (ts *TableStats) EstimateBoundSelectivity(attNum int16, op int, estRows float64, paramVal string, isUniqueSingleCol bool) float64 {
	if isUniqueSingleCol && op == 2 { // SQLiteConstraintEQ
		if estRows > 1 {
			return 1.0 / estRows
		}
		return 1.0
	}
	if paramVal != "" && ts != nil {
		if cs, ok := ts.Columns[attNum]; ok {
			if op == 2 && cs.Distinct > 0 { // EQ
				sel := (1.0 - float64(cs.NullFrac)) / float64(cs.Distinct)
				if sel < 0.0001 {
					sel = 0.0001
				}
				return sel
			}
		}
	}
	if ts != nil {
		return ts.EstimateSelectivity(attNum, op, estRows, isUniqueSingleCol)
	}
	if op == 2 {
		return 0.01
	}
	return 0.1
}

// EstimateJoinSelectivity estimates the join selectivity factor for an equi-join
// using the standard 1 / max(NDV_left, NDV_right) heuristic with row-count fallback.
func EstimateJoinSelectivity(leftRows, rightRows, leftNDV, rightNDV float64) float64 {
	maxNDV := leftNDV
	if rightNDV > maxNDV {
		maxNDV = rightNDV
	}
	if maxNDV <= 0 {
		maxNDV = leftRows
		if rightRows > maxNDV {
			maxNDV = rightRows
		}
	}
	if maxNDV <= 1 {
		return 1.0
	}
	sel := 1.0 / maxNDV
	if sel < 1e-6 {
		return 1e-6
	}
	return sel
}
