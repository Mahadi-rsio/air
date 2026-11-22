package readadapter

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	defaultPlanCacheMaxEntries = 4096
	planCacheShards            = 16
)

// PlanCacheStats reports hit/miss/eviction/stampede counters for QueryPlanCache.
type PlanCacheStats struct {
	Hits               uint64 `json:"hits"`
	Misses             uint64 `json:"misses"`
	Evictions          uint64 `json:"evictions"`
	Entries            int    `json:"entries"`
	StampedesPrevented uint64 `json:"stampedes_prevented"`
}

// PlanCacheKey uniquely scopes a cached query plan to a tenant, generation,
// catalog version, statistics version, and query shape.
type PlanCacheKey struct {
	TenantID      string
	Generation    string
	CatalogLSN    uint64
	StatsVersion  uint64
	CapabilityVer uint32
	QueryHash     uint64
}

// CachedQueryPlan holds the translated SQLite SQL and parameter count for a PG query.
type CachedQueryPlan struct {
	Key        PlanCacheKey
	OrigShape  string
	SQLiteSQL  string
	ParamCount int
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
// translated/optimized query plans.
type QueryPlanCache struct {
	shards  [planCacheShards]planCacheShard
	sfGroup singleflight.Group

	hits               atomic.Uint64
	misses             atomic.Uint64
	evictions          atomic.Uint64
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

// GetOrTranslate retrieves a cached translated plan or translates and caches it
// using singleflight to prevent stampedes.
func (c *QueryPlanCache) GetOrTranslate(
	tenantID, generation string,
	catalogLSN, statsVersion uint64,
	capabilityVer uint32,
	rawPGSQL string,
	translateFn func(string) (string, int),
) (*CachedQueryPlan, bool) {
	if c == nil {
		sq, pcnt := translateFn(rawPGSQL)
		return &CachedQueryPlan{SQLiteSQL: sq, ParamCount: pcnt}, false
	}
	qh := hashQueryShape(rawPGSQL)
	key := PlanCacheKey{
		TenantID:      tenantID,
		Generation:    generation,
		CatalogLSN:    catalogLSN,
		StatsVersion:  statsVersion,
		CapabilityVer: capabilityVer,
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
	sfKey := fmt.Sprintf("%s|%s|%d|%d|%d|%x", tenantID, generation, catalogLSN, statsVersion, capabilityVer, qh)
	var called atomic.Bool
	v, _, shared := c.sfGroup.Do(sfKey, func() (any, error) {
		called.Store(true)
		sq, pcnt := translateFn(rawPGSQL)
		p := &CachedQueryPlan{
			Key:        key,
			OrigShape:  rawPGSQL,
			SQLiteSQL:  sq,
			ParamCount: pcnt,
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
	for i := 0; i < planCacheShards; i++ {
		shard := &c.shards[i]
		shard.mu.Lock()
		for k, elem := range shard.entries {
			if k.TenantID == tenantID {
				shard.ll.Remove(elem)
				delete(shard.entries, k)
			}
		}
		shard.mu.Unlock()
	}
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
		Entries:            totalEntries,
		StampedesPrevented: c.stampedesPrevented.Load(),
	}
}
