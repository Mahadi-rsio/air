package readadapter

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	defaultPlanCacheMaxEntries = 4096
	planCacheShards            = 16
)

// PlanCacheStats reports hit/miss/eviction/invalidation/stampede counters for QueryPlanCache.
type PlanCacheStats struct {
	Hits               uint64 `json:"hits"`
	Misses             uint64 `json:"misses"`
	Evictions          uint64 `json:"evictions"`
	Invalidations      uint64 `json:"invalidations"`
	Entries            int    `json:"entries"`
	StampedesPrevented uint64 `json:"stampedes_prevented"`
}

// PlanCacheKey uniquely scopes a cached query plan to a tenant, generation,
// database, search_path, schema/table/stats versions, parameter type signature,
// execution settings, and query shape.
type PlanCacheKey struct {
	TenantID      string
	Generation    string
	DatabaseID    uint32
	SearchPath    string
	CatalogLSN    uint64
	SchemaVersion uint64
	StatsVersion  uint64
	CapabilityVer uint32
	ParamSigHash  uint64
	SettingsHash  uint64
	QueryHash     uint64
}

// PlanScope carries the full invalidation and isolation context for GetOrTranslateScoped.
type PlanScope struct {
	TenantID      string
	Generation    string
	DatabaseID    uint32
	SearchPath    string
	CatalogLSN    uint64
	SchemaVersion uint64
	StatsVersion  uint64
	CapabilityVer uint32
	ParamOIDs     []uint32
	SettingsHash  uint64
}

// CachedQueryPlan holds the translated SQLite SQL, parameter count, and table/index dependencies.
type CachedQueryPlan struct {
	Key        PlanCacheKey
	OrigShape  string
	SQLiteSQL  string
	ParamCount int
	TableDeps  []uint32
	IndexDeps  []uint32
	CreatedAt  time.Time
}

type planCacheEntry struct {
	key  PlanCacheKey
	plan *CachedQueryPlan
}

type planCacheShard struct {
	mu      sync.Mutex
	maxEnts int
	ll      *list.List
	entries map[PlanCacheKey]*list.Element
}

// QueryPlanCache is a sharded, bounded, tenant-isolated LRU cache for
// translated/optimized query plans with targeted table/index/schema invalidation.
type QueryPlanCache struct {
	shards  [planCacheShards]planCacheShard
	sfGroup singleflight.Group

	hits               atomic.Uint64
	misses             atomic.Uint64
	evictions          atomic.Uint64
	invalidations      atomic.Uint64
	stampedesPrevented atomic.Uint64
}

// NewQueryPlanCache creates a sharded QueryPlanCache with total capacity maxEntries.
func NewQueryPlanCache(maxEntries int) *QueryPlanCache {
	if maxEntries <= 0 {
		maxEntries = defaultPlanCacheMaxEntries
	}
	perShard := maxEntries / planCacheShards
	if perShard < 8 {
		perShard = 8
	}
	c := &QueryPlanCache{}
	for i := 0; i < planCacheShards; i++ {
		c.shards[i] = planCacheShard{
			maxEnts: perShard,
			ll:      list.New(),
			entries: make(map[PlanCacheKey]*list.Element),
		}
	}
	return c
}

func hashQueryShape(sql string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sql))
	return h.Sum64()
}

// ParamSignatureHash hashes a parameter OID slice into a compact 64-bit signature
// with zero heap allocations.
func ParamSignatureHash(paramOIDs []uint32) uint64 {
	if len(paramOIDs) == 0 {
		return 0
	}
	var buf [4]byte
	h := fnv.New64a()
	for _, oid := range paramOIDs {
		binary.LittleEndian.PutUint32(buf[:], oid)
		_, _ = h.Write(buf[:])
	}
	return h.Sum64()
}

// ExecSettingsHash hashes relevant session execution settings (e.g. search_path,
// timezone, lc_collate) into a 64-bit fingerprint.
func ExecSettingsHash(searchPath, timezone, collation string) uint64 {
	if searchPath == "" && timezone == "" && collation == "" {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(searchPath))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(timezone))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(collation))
	return h.Sum64()
}

// GetOrTranslate retrieves a cached translated plan or translates and caches it
// using singleflight to prevent stampedes.
func (c *QueryPlanCache) GetOrTranslate(
	tenantID, generation string,
	catalogLSN, statsVersion uint64,
	capabilityVer uint32,
	rawPGSQL string,
	translateFn func(string) (string, int),
) (*CachedQueryPlan, bool) {
	return c.GetOrTranslateScoped(
		PlanScope{
			TenantID:      tenantID,
			Generation:    generation,
			CatalogLSN:    catalogLSN,
			StatsVersion:  statsVersion,
			CapabilityVer: capabilityVer,
		},
		rawPGSQL,
		func(q string) (string, int, []uint32, []uint32) {
			sq, pcnt := translateFn(q)
			return sq, pcnt, nil, nil
		},
	)
}

// GetOrTranslateScoped retrieves a cached plan scoped by tenant, database, search_path,
// schema/stats versions, parameter types, and execution settings, tracking table/index dependencies.
func (c *QueryPlanCache) GetOrTranslateScoped(
	scope PlanScope,
	rawPGSQL string,
	translateFn func(string) (sqliteSQL string, paramCount int, tableDeps []uint32, indexDeps []uint32),
) (*CachedQueryPlan, bool) {
	if c == nil {
		sq, pcnt, tdeps, ideps := translateFn(rawPGSQL)
		return &CachedQueryPlan{
			SQLiteSQL:  sq,
			ParamCount: pcnt,
			TableDeps:  tdeps,
			IndexDeps:  ideps,
		}, false
	}
	qh := hashQueryShape(rawPGSQL)
	paramHash := ParamSignatureHash(scope.ParamOIDs)
	key := PlanCacheKey{
		TenantID:      scope.TenantID,
		Generation:    scope.Generation,
		DatabaseID:    scope.DatabaseID,
		SearchPath:    scope.SearchPath,
		CatalogLSN:    scope.CatalogLSN,
		SchemaVersion: scope.SchemaVersion,
		StatsVersion:  scope.StatsVersion,
		CapabilityVer: scope.CapabilityVer,
		ParamSigHash:  paramHash,
		SettingsHash:  scope.SettingsHash,
		QueryHash:     qh,
	}
	shard := &c.shards[qh%planCacheShards]

	shard.mu.Lock()
	if elem, ok := shard.entries[key]; ok {
		ent := elem.Value.(*planCacheEntry)
		if ent.plan.OrigShape == rawPGSQL {
			shard.ll.MoveToFront(elem)
			shard.mu.Unlock()
			c.hits.Add(1)
			return ent.plan, true
		}
	}
	shard.mu.Unlock()

	c.misses.Add(1)
	sfKey := fmt.Sprintf("%s|%s|%d|%s|%d|%d|%d|%d|%x|%x|%x",
		scope.TenantID, scope.Generation, scope.DatabaseID, scope.SearchPath,
		scope.CatalogLSN, scope.SchemaVersion, scope.StatsVersion, scope.CapabilityVer,
		paramHash, scope.SettingsHash, qh)
	var called atomic.Bool
	v, _, shared := c.sfGroup.Do(sfKey, func() (any, error) {
		called.Store(true)
		sq, pcnt, tdeps, ideps := translateFn(rawPGSQL)
		p := &CachedQueryPlan{
			Key:        key,
			OrigShape:  rawPGSQL,
			SQLiteSQL:  sq,
			ParamCount: pcnt,
			TableDeps:  tdeps,
			IndexDeps:  ideps,
			CreatedAt:  time.Now(),
		}
		shard.mu.Lock()
		defer shard.mu.Unlock()
		if elem, ok := shard.entries[key]; ok {
			ent := elem.Value.(*planCacheEntry)
			if ent.plan.OrigShape == rawPGSQL {
				shard.ll.MoveToFront(elem)
				return ent.plan, nil
			}
		}
		for shard.ll.Len() >= shard.maxEnts {
			back := shard.ll.Back()
			if back == nil {
				break
			}
			shard.ll.Remove(back)
			delete(shard.entries, back.Value.(*planCacheEntry).key)
			c.evictions.Add(1)
		}
		elem := shard.ll.PushFront(&planCacheEntry{key: key, plan: p})
		shard.entries[key] = elem
		return p, nil
	})
	if shared && !called.Load() {
		c.stampedesPrevented.Add(1)
	}
	return v.(*CachedQueryPlan), false
}

// InvalidateTenant removes all cached query plans for tenantID.
func (c *QueryPlanCache) InvalidateTenant(tenantID string) {
	if c == nil || tenantID == "" {
		return
	}
	var removed uint64
	for i := 0; i < planCacheShards; i++ {
		shard := &c.shards[i]
		shard.mu.Lock()
		for k, elem := range shard.entries {
			if k.TenantID == tenantID {
				shard.ll.Remove(elem)
				delete(shard.entries, k)
				removed++
			}
		}
		shard.mu.Unlock()
	}
	if removed > 0 {
		c.invalidations.Add(removed)
	}
}

// InvalidateSchema removes all cached plans for (tenantID, databaseID) without
// flushing other tenants or databases.
func (c *QueryPlanCache) InvalidateSchema(tenantID string, databaseID uint32) int {
	if c == nil || tenantID == "" {
		return 0
	}
	removed := 0
	for i := 0; i < planCacheShards; i++ {
		shard := &c.shards[i]
		shard.mu.Lock()
		for k, elem := range shard.entries {
			if k.TenantID == tenantID && k.DatabaseID == databaseID {
				shard.ll.Remove(elem)
				delete(shard.entries, k)
				removed++
			}
		}
		shard.mu.Unlock()
	}
	if removed > 0 {
		c.invalidations.Add(uint64(removed))
	}
	return removed
}

// InvalidateTable removes cached plans for tenantID that depend on relOID
// (or untracked plans with empty TableDeps for safety), preserving plans on unrelated tables.
func (c *QueryPlanCache) InvalidateTable(tenantID string, relOID uint32) int {
	if c == nil || tenantID == "" || relOID == 0 {
		return 0
	}
	removed := 0
	for i := 0; i < planCacheShards; i++ {
		shard := &c.shards[i]
		shard.mu.Lock()
		for k, elem := range shard.entries {
			if k.TenantID != tenantID {
				continue
			}
			ent := elem.Value.(*planCacheEntry)
			if len(ent.plan.TableDeps) == 0 || slices.Contains(ent.plan.TableDeps, relOID) {
				shard.ll.Remove(elem)
				delete(shard.entries, k)
				removed++
			}
		}
		shard.mu.Unlock()
	}
	if removed > 0 {
		c.invalidations.Add(uint64(removed))
	}
	return removed
}

// InvalidateIndex removes cached plans for tenantID that depend on indexOID
// (or untracked plans with empty IndexDeps/TableDeps), preserving unrelated plans.
func (c *QueryPlanCache) InvalidateIndex(tenantID string, indexOID uint32) int {
	if c == nil || tenantID == "" || indexOID == 0 {
		return 0
	}
	removed := 0
	for i := 0; i < planCacheShards; i++ {
		shard := &c.shards[i]
		shard.mu.Lock()
		for k, elem := range shard.entries {
			if k.TenantID != tenantID {
				continue
			}
			ent := elem.Value.(*planCacheEntry)
			if (len(ent.plan.IndexDeps) == 0 && len(ent.plan.TableDeps) == 0) || slices.Contains(ent.plan.IndexDeps, indexOID) {
				shard.ll.Remove(elem)
				delete(shard.entries, k)
				removed++
			}
		}
		shard.mu.Unlock()
	}
	if removed > 0 {
		c.invalidations.Add(uint64(removed))
	}
	return removed
}

// EvictStaleGeneration removes cached plans for tenantID whose generation does not match activeGen.
func (c *QueryPlanCache) EvictStaleGeneration(tenantID, activeGen string) int {
	if c == nil || tenantID == "" {
		return 0
	}
	removed := 0
	for i := 0; i < planCacheShards; i++ {
		shard := &c.shards[i]
		shard.mu.Lock()
		for k, elem := range shard.entries {
			if k.TenantID == tenantID && k.Generation != activeGen {
				shard.ll.Remove(elem)
				delete(shard.entries, k)
				removed++
			}
		}
		shard.mu.Unlock()
	}
	if removed > 0 {
		c.invalidations.Add(uint64(removed))
	}
	return removed
}

// Stats returns current PlanCacheStats across all shards.
func (c *QueryPlanCache) Stats() PlanCacheStats {
	if c == nil {
		return PlanCacheStats{}
	}
	totalEntries := 0
	for i := 0; i < planCacheShards; i++ {
		shard := &c.shards[i]
		shard.mu.Lock()
		totalEntries += shard.ll.Len()
		shard.mu.Unlock()
	}
	return PlanCacheStats{
		Hits:               c.hits.Load(),
		Misses:             c.misses.Load(),
		Evictions:          c.evictions.Load(),
		Invalidations:      c.invalidations.Load(),
		Entries:            totalEntries,
		StampedesPrevented: c.stampedesPrevented.Load(),
	}
}

// --- Schema, Table, and Index Metadata Version Tracking ---

type schemaSnapshot struct {
	schemaVersion uint64
	tableHashes   map[uint32]uint64
	indexHashes   map[uint32]uint64
	tableNames    map[string]uint32   // lower table name -> RelOID
	tableIndexes  map[uint32][]uint32 // RelOID -> IndexRelOIDs
}

// SchemaVersionRegistry tracks monotonic per-tenant schema, table, and index
// metadata versions so DDL changes trigger targeted invalidation without global flushes.
type SchemaVersionRegistry struct {
	mu          sync.RWMutex
	schemas     map[string]*schemaSnapshot // key: tenantID + "|" + dbID
	tableVers   map[string]uint64          // key: tenantID + "|" + relOID
	indexVers   map[string]uint64          // key: tenantID + "|" + indexOID
}

// NewSchemaVersionRegistry creates a new SchemaVersionRegistry.
func NewSchemaVersionRegistry() *SchemaVersionRegistry {
	return &SchemaVersionRegistry{
		schemas:   make(map[string]*schemaSnapshot),
		tableVers: make(map[string]uint64),
		indexVers: make(map[string]uint64),
	}
}

func schemaKey(tenantID string, dbID uint32) string {
	return fmt.Sprintf("%s|%d", tenantID, dbID)
}

func oidKey(tenantID string, oid uint32) string {
	return fmt.Sprintf("%s|%d", tenantID, oid)
}

// SchemaVersion returns the current monotonic schema version for (tenantID, dbID).
func (r *SchemaVersionRegistry) SchemaVersion(tenantID string, dbID uint32) uint64 {
	if r == nil || tenantID == "" {
		return 0
	}
	r.mu.RLock()
	snap := r.schemas[schemaKey(tenantID, dbID)]
	var v uint64
	if snap != nil {
		v = snap.schemaVersion
	}
	r.mu.RUnlock()
	return v
}

// TableVersion returns the current monotonic metadata version for (tenantID, relOID).
func (r *SchemaVersionRegistry) TableVersion(tenantID string, relOID uint32) uint64 {
	if r == nil || tenantID == "" || relOID == 0 {
		return 0
	}
	r.mu.RLock()
	v := r.tableVers[oidKey(tenantID, relOID)]
	r.mu.RUnlock()
	return v
}

// IndexVersion returns the current monotonic metadata version for (tenantID, indexOID).
func (r *SchemaVersionRegistry) IndexVersion(tenantID string, indexOID uint32) uint64 {
	if r == nil || tenantID == "" || indexOID == 0 {
		return 0
	}
	r.mu.RLock()
	v := r.indexVers[oidKey(tenantID, indexOID)]
	r.mu.RUnlock()
	return v
}

// BumpSchema increments the schema version for (tenantID, dbID) and returns the new version.
func (r *SchemaVersionRegistry) BumpSchema(tenantID string, dbID uint32) uint64 {
	if r == nil || tenantID == "" {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sk := schemaKey(tenantID, dbID)
	snap := r.schemas[sk]
	if snap == nil {
		snap = &schemaSnapshot{
			tableHashes:  make(map[uint32]uint64),
			indexHashes:  make(map[uint32]uint64),
			tableNames:   make(map[string]uint32),
			tableIndexes: make(map[uint32][]uint32),
		}
		r.schemas[sk] = snap
	}
	snap.schemaVersion++
	return snap.schemaVersion
}

// ResolveQueryDeps returns the known table OIDs and index OIDs referenced by rawPGSQL
// for tenantID/dbID, enabling targeted plan invalidation.
func (r *SchemaVersionRegistry) ResolveQueryDeps(tenantID string, dbID uint32, rawPGSQL string) (tableDeps, indexDeps []uint32) {
	if r == nil || tenantID == "" || rawPGSQL == "" {
		return nil, nil
	}
	r.mu.RLock()
	snap := r.schemas[schemaKey(tenantID, dbID)]
	if snap == nil || len(snap.tableNames) == 0 {
		r.mu.RUnlock()
		return nil, nil
	}
	lowerSQL := strings.ToLower(rawPGSQL)
	for name, oid := range snap.tableNames {
		if strings.Contains(lowerSQL, name) {
			tableDeps = append(tableDeps, oid)
			if idxs := snap.tableIndexes[oid]; len(idxs) > 0 {
				indexDeps = append(indexDeps, idxs...)
			}
		}
	}
	r.mu.RUnlock()
	return tableDeps, indexDeps
}

func hashTableSchema(tbl DiscoveredTable) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	_, _ = h.Write([]byte(tbl.Schema))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(tbl.Name))
	binary.LittleEndian.PutUint32(buf[:4], tbl.RelOID)
	_, _ = h.Write(buf[:4])
	for _, col := range tbl.Columns {
		_, _ = h.Write([]byte(col.Name))
		binary.LittleEndian.PutUint32(buf[:4], col.PGTypeOID)
		_, _ = h.Write(buf[:4])
		binary.LittleEndian.PutUint16(buf[:2], uint16(col.AttNum))
		_, _ = h.Write(buf[:2])
		if col.IsDropped {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	}
	for _, idx := range tbl.Indexes {
		binary.LittleEndian.PutUint32(buf[:4], idx.IndexRelOID)
		_, _ = h.Write(buf[:4])
	}
	return h.Sum64()
}

func hashIndexSchema(idx DiscoveredIndex) uint64 {
	h := fnv.New64a()
	var buf [4]byte
	_, _ = h.Write([]byte(idx.Name))
	binary.LittleEndian.PutUint32(buf[:], idx.IndexRelOID)
	_, _ = h.Write(buf[:])
	binary.LittleEndian.PutUint32(buf[:], idx.TableRelOID)
	_, _ = h.Write(buf[:])
	if idx.IsUnique {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
	for _, ka := range idx.KeyAttNums {
		binary.LittleEndian.PutUint16(buf[:2], uint16(ka))
		_, _ = h.Write(buf[:2])
	}
	for _, ko := range idx.KeyOptions {
		binary.LittleEndian.PutUint16(buf[:2], uint16(ko))
		_, _ = h.Write(buf[:2])
	}
	return h.Sum64()
}

// ReconcileCatalog compares discovered catalog tables against the previous snapshot
// for (tenantID, dbID), bumping schema/table/index versions only for entities whose
// structural metadata actually changed.
func (r *SchemaVersionRegistry) ReconcileCatalog(tenantID string, dbID uint32, tables []DiscoveredTable) (changedTables []uint32, changedIndexes []uint32, schemaChanged bool) {
	if r == nil || tenantID == "" {
		return nil, nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	sk := schemaKey(tenantID, dbID)
	prev := r.schemas[sk]
	next := &schemaSnapshot{
		tableHashes:  make(map[uint32]uint64, len(tables)),
		indexHashes:  make(map[uint32]uint64),
		tableNames:   make(map[string]uint32, len(tables)),
		tableIndexes: make(map[uint32][]uint32, len(tables)),
	}
	if prev != nil {
		next.schemaVersion = prev.schemaVersion
	}

	for _, tbl := range tables {
		th := hashTableSchema(tbl)
		next.tableHashes[tbl.RelOID] = th
		next.tableNames[strings.ToLower(tbl.Name)] = tbl.RelOID
		var idxOIDs []uint32
		for _, idx := range tbl.Indexes {
			ih := hashIndexSchema(idx)
			next.indexHashes[idx.IndexRelOID] = ih
			idxOIDs = append(idxOIDs, idx.IndexRelOID)
			if prev == nil || prev.indexHashes[idx.IndexRelOID] != ih {
				r.indexVers[oidKey(tenantID, idx.IndexRelOID)]++
				changedIndexes = append(changedIndexes, idx.IndexRelOID)
				if prev != nil {
					schemaChanged = true
				}
			}
		}
		next.tableIndexes[tbl.RelOID] = idxOIDs
		if prev == nil || prev.tableHashes[tbl.RelOID] != th {
			r.tableVers[oidKey(tenantID, tbl.RelOID)]++
			changedTables = append(changedTables, tbl.RelOID)
			if prev != nil {
				schemaChanged = true
			}
		}
	}

	if prev != nil {
		for oldRelOID := range prev.tableHashes {
			if _, ok := next.tableHashes[oldRelOID]; !ok {
				r.tableVers[oidKey(tenantID, oldRelOID)]++
				changedTables = append(changedTables, oldRelOID)
				schemaChanged = true
			}
		}
		for oldIdxOID := range prev.indexHashes {
			if _, ok := next.indexHashes[oldIdxOID]; !ok {
				r.indexVers[oidKey(tenantID, oldIdxOID)]++
				changedIndexes = append(changedIndexes, oldIdxOID)
				schemaChanged = true
			}
		}
	} else if len(tables) > 0 {
		schemaChanged = true
	}

	if schemaChanged {
		next.schemaVersion++
	}
	r.schemas[sk] = next
	return changedTables, changedIndexes, schemaChanged
}
