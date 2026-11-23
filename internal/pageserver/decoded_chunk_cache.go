package pageserver

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// DecodedChunkKey uniquely identifies a decoded V2 chunk scoped to tenant, generation, layer, chunk index, and DEK epoch.
type DecodedChunkKey struct {
	TenantID   string
	Generation string
	LayerID    string
	ChunkIdx   uint32
	KeyEpoch   uint64
}

// DecodedChunkCacheConfig configures the bounded LRU decoded V2 chunk cache.
type DecodedChunkCacheConfig struct {
	// MaxBytes is the strict memory budget for cached decoded chunks. Defaults to 64 MiB.
	MaxBytes int64
	// MaxEntryBytes caps the maximum size of a single cached chunk. Defaults to MaxBytes/2.
	MaxEntryBytes int64
}

// DecodedChunkCacheStats captures decoded chunk cache performance and memory metrics.
type DecodedChunkCacheStats struct {
	Hits              int64
	Misses            int64
	Evictions         int64
	Bytes             int64
	Entries           int
	DecodeTimeNs      int64
	SingleflightWaits int64
}

type decodedChunkEntry struct {
	key  DecodedChunkKey
	data []byte
	size int64
}

type inflightChunkDecode struct {
	done chan struct{}
	data []byte
	err  error
}

// DecodedChunkCache is a bounded LRU cache of decompressed, decrypted V2 chunks
// with per-key single-flight decoding and tenant+epoch isolation.
type DecodedChunkCache struct {
	mu            sync.Mutex
	maxBytes      int64
	maxEntryBytes int64
	curBytes      int64
	lru           *list.List
	entries       map[DecodedChunkKey]*list.Element
	inflight      map[DecodedChunkKey]*inflightChunkDecode

	hits              atomic.Int64
	misses            atomic.Int64
	evictions         atomic.Int64
	decodeTimeNs      atomic.Int64
	singleflightWaits atomic.Int64
}

// NewDecodedChunkCache creates a DecodedChunkCache with the given byte budget.
func NewDecodedChunkCache(cfg DecodedChunkCacheConfig) *DecodedChunkCache {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 64 * 1024 * 1024
	}
	if cfg.MaxEntryBytes <= 0 || cfg.MaxEntryBytes > cfg.MaxBytes {
		cfg.MaxEntryBytes = cfg.MaxBytes / 2
		if cfg.MaxEntryBytes < 1 {
			cfg.MaxEntryBytes = cfg.MaxBytes
		}
	}
	return &DecodedChunkCache{
		maxBytes:      cfg.MaxBytes,
		maxEntryBytes: cfg.MaxEntryBytes,
		lru:           list.New(),
		entries:       make(map[DecodedChunkKey]*list.Element),
		inflight:      make(map[DecodedChunkKey]*inflightChunkDecode),
	}
}

// GetOrDecode returns a cached decoded chunk or coalesces concurrent misses via single-flight.
// Returned slices must be treated as immutable by callers.
func (c *DecodedChunkCache) GetOrDecode(ctx context.Context, key DecodedChunkKey, decodeFn func() ([]byte, error)) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	if elem, ok := c.entries[key]; ok {
		c.lru.MoveToFront(elem)
		data := elem.Value.(*decodedChunkEntry).data
		c.mu.Unlock()
		c.hits.Add(1)
		return data, nil
	}

	if inf, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		c.singleflightWaits.Add(1)
		select {
		case <-inf.done:
			if inf.err != nil {
				return nil, inf.err
			}
			return inf.data, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	inf := &inflightChunkDecode{done: make(chan struct{})}
	c.inflight[key] = inf
	c.mu.Unlock()

	c.misses.Add(1)
	t0 := time.Now()
	data, err := decodeFn()
	c.decodeTimeNs.Add(time.Since(t0).Nanoseconds())

	c.mu.Lock()
	delete(c.inflight, key)
	inf.data = data
	inf.err = err
	close(inf.done)

	if err == nil && len(data) > 0 && int64(len(data)) <= c.maxEntryBytes {
		c.insertLocked(key, data)
	}
	c.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *DecodedChunkCache) insertLocked(key DecodedChunkKey, data []byte) {
	sz := int64(len(data))
	if elem, ok := c.entries[key]; ok {
		old := elem.Value.(*decodedChunkEntry)
		c.curBytes -= old.size
		c.lru.Remove(elem)
		delete(c.entries, key)
	}
	entry := &decodedChunkEntry{key: key, data: data, size: sz}
	elem := c.lru.PushFront(entry)
	c.entries[key] = elem
	c.curBytes += sz

	for c.curBytes > c.maxBytes && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.lru.Remove(oldest)
		oldEntry := oldest.Value.(*decodedChunkEntry)
		delete(c.entries, oldEntry.key)
		c.curBytes -= oldEntry.size
		c.evictions.Add(1)
	}
}

// Evict removes a specific decoded chunk (e.g. if structural corruption is detected).
func (c *DecodedChunkCache) Evict(key DecodedChunkKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		old := elem.Value.(*decodedChunkEntry)
		c.curBytes -= old.size
		c.lru.Remove(elem)
		delete(c.entries, key)
		c.evictions.Add(1)
	}
}

// InvalidateTenant removes all cached decoded chunks belonging to tenantID.
// Existing references held by in-flight readers remain immutable until GC without racing.
func (c *DecodedChunkCache) InvalidateTenant(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, elem := range c.entries {
		if k.TenantID == tenantID {
			entry := elem.Value.(*decodedChunkEntry)
			c.curBytes -= entry.size
			c.lru.Remove(elem)
			delete(c.entries, k)
			c.evictions.Add(1)
		}
	}
}

// InvalidateTenantGeneration removes cached decoded chunks belonging to (tenantID, generation).
// If generation is empty, it behaves like InvalidateTenant(tenantID).
func (c *DecodedChunkCache) InvalidateTenantGeneration(tenantID, generation string) {
	if generation == "" {
		c.InvalidateTenant(tenantID)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, elem := range c.entries {
		if k.TenantID == tenantID && k.Generation == generation {
			entry := elem.Value.(*decodedChunkEntry)
			c.curBytes -= entry.size
			c.lru.Remove(elem)
			delete(c.entries, k)
			c.evictions.Add(1)
		}
	}
}

// TrimToBytes evicts oldest LRU decoded chunks until curBytes <= targetBytes.
func (c *DecodedChunkCache) TrimToBytes(targetBytes int64) {
	if targetBytes < 0 {
		targetBytes = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.curBytes > targetBytes && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.lru.Remove(oldest)
		oldEntry := oldest.Value.(*decodedChunkEntry)
		delete(c.entries, oldEntry.key)
		c.curBytes -= oldEntry.size
		c.evictions.Add(1)
	}
}

// Stats returns a snapshot of cache usage and hit/miss metrics.
func (c *DecodedChunkCache) Stats() DecodedChunkCacheStats {
	c.mu.Lock()
	curBytes := c.curBytes
	entries := len(c.entries)
	c.mu.Unlock()
	return DecodedChunkCacheStats{
		Hits:              c.hits.Load(),
		Misses:            c.misses.Load(),
		Evictions:         c.evictions.Load(),
		Bytes:             curBytes,
		Entries:           entries,
		DecodeTimeNs:      c.decodeTimeNs.Load(),
		SingleflightWaits: c.singleflightWaits.Load(),
	}
}

