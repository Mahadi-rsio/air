package pageserver

import (
	"container/list"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

const (
	defaultIndexCacheMaxBytes int64 = 64 * 1024 * 1024 // 64 MiB
	sparseIndexStride         int   = 64
	maxRetainedBlooms         int   = 4096
)

type sparseIndexEntry struct {
	firstKey Key
	startIdx int
}

type layerBloom struct {
	bits []uint64
	mask uint64 // numBits - 1 (power of 2)
}

func hashKey64(k Key) (uint64, uint64) {
	h1 := uint64(k.Kind)<<56 | uint64(k.ForkNum)<<48 | uint64(k.SpcOID)<<16 | uint64(k.DbOID>>16)
	h1 ^= uint64(k.DbOID)<<48 ^ uint64(k.RelNumber)*0x9e3779b97f4a7c15 ^ uint64(k.BlockNum)*0xbf58476d1ce4e5b9 ^ uint64(k.Extra)
	h1 ^= h1 >> 30
	h1 *= 0xbf58476d1ce4e5b9
	h1 ^= h1 >> 27
	h1 *= 0x94d049bb133111eb
	h1 ^= h1 >> 31

	h2 := h1*0x9e3779b97f4a7c15 + 0x517cc1b727220a95
	h2 ^= h2 >> 29
	h2 *= 0x94d049bb133111eb
	h2 ^= h2 >> 32
	if h2 == 0 {
		h2 = 1
	}
	return h1, h2
}

func buildLayerBloom(sorted []LayerIndexEntry) *layerBloom {
	if len(sorted) == 0 {
		return nil
	}
	// Count unique keys in sorted entries.
	unique := 1
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Key.Compare(sorted[i-1].Key) != 0 {
			unique++
		}
	}
	needBits := uint64(unique) * 10
	if needBits < 64 {
		needBits = 64
	}
	pow2 := uint64(64)
	for pow2 < needBits && pow2 < (1<<26) {
		pow2 <<= 1
	}
	b := &layerBloom{
		bits: make([]uint64, pow2/64),
		mask: pow2 - 1,
	}
	var prev Key
	for i := range sorted {
		k := sorted[i].Key
		if i > 0 && k.Compare(prev) == 0 {
			continue
		}
		prev = k
		h1, h2 := hashKey64(k)
		for p := uint64(0); p < 3; p++ {
			bit := (h1 + p*h2) & b.mask
			b.bits[bit>>6] |= 1 << (bit & 63)
		}
	}
	return b
}

func (b *layerBloom) mayContain(k Key) bool {
	if b == nil || len(b.bits) == 0 {
		return true
	}
	h1, h2 := hashKey64(k)
	for p := uint64(0); p < 3; p++ {
		bit := (h1 + p*h2) & b.mask
		if (b.bits[bit>>6] & (1 << (bit & 63))) == 0 {
			return false
		}
	}
	return true
}

func buildSparseIndex(sorted []LayerIndexEntry) []sparseIndexEntry {
	if len(sorted) < sparseIndexStride*2 {
		return nil
	}
	n := (len(sorted) + sparseIndexStride - 1) / sparseIndexStride
	out := make([]sparseIndexEntry, 0, n)
	for i := 0; i < len(sorted); i += sparseIndexStride {
		out = append(out, sparseIndexEntry{
			firstKey: sorted[i].Key,
			startIdx: i,
		})
	}
	return out
}

func findKeyStart(entries []LayerIndexEntry, sparse []sparseIndexEntry, key Key) int {
	lo, hi := 0, len(entries)
	if len(sparse) > 0 {
		// Find the last sparse bucket whose firstKey <= key.
		sb := sort.Search(len(sparse), func(i int) bool {
			return sparse[i].firstKey.Compare(key) > 0
		})
		if sb > 0 {
			lo = sparse[sb-1].startIdx
			if sb < len(sparse) {
				hi = sparse[sb].startIdx
			}
			// If key equals sparse[sb-1].firstKey, duplicates may span into the previous bucket.
			if sb > 1 && sparse[sb-1].firstKey.Compare(key) == 0 {
				lo = 0
			}
		} else {
			hi = sparse[0].startIdx
		}
	}
	return lo + sort.Search(hi-lo, func(i int) bool {
		return entries[lo+i].Key.Compare(key) >= 0
	})
}

type layerIndexData struct {
	r2Key    string
	version  uint16
	entries  []LayerIndexEntry
	sparse   []sparseIndexEntry
	bloom    *layerBloom
	chunks   []v2ChunkMeta
	walMeta  v2WALTableMeta
	walMu    sync.Mutex
	walLoaded bool
	walTable [][]byte
	bytes    int64
}

type boundedIndexCache struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	lru      *list.List
	items    map[string]*list.Element
	blooms   map[string]*layerBloom
}

func newBoundedIndexCache(maxBytes int64) *boundedIndexCache {
	if maxBytes <= 0 {
		maxBytes = defaultIndexCacheMaxBytes
	}
	return &boundedIndexCache{
		maxBytes: maxBytes,
		lru:      list.New(),
		items:    make(map[string]*list.Element),
		blooms:   make(map[string]*layerBloom),
	}
}

func (c *boundedIndexCache) get(r2Key string) (*layerIndexData, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[r2Key]; ok {
		c.lru.MoveToFront(elem)
		return elem.Value.(*layerIndexData), true
	}
	return nil, false
}

func (c *boundedIndexCache) getBloom(r2Key string) (*layerBloom, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.blooms[r2Key]
	return b, ok
}

func (c *boundedIndexCache) put(r2Key string, entries []LayerIndexEntry) *layerIndexData {
	return c.putVersioned(r2Key, LayerVersion, entries, nil, v2WALTableMeta{})
}

func (c *boundedIndexCache) putVersioned(r2Key string, version uint16, entries []LayerIndexEntry, chunks []v2ChunkMeta, walMeta v2WALTableMeta) *layerIndexData {
	bloom := buildLayerBloom(entries)
	sparse := buildSparseIndex(entries)
	sz := int64(len(entries))*int64(unsafe.Sizeof(LayerIndexEntry{})) +
		int64(len(sparse))*int64(unsafe.Sizeof(sparseIndexEntry{})) +
		int64(len(chunks))*int64(unsafe.Sizeof(v2ChunkMeta{})) + 64
	if bloom != nil {
		sz += int64(len(bloom.bits)) * 8
	}
	data := &layerIndexData{
		r2Key:   r2Key,
		version: version,
		entries: entries,
		sparse:  sparse,
		bloom:   bloom,
		chunks:  chunks,
		walMeta: walMeta,
		bytes:   sz,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.blooms) >= maxRetainedBlooms {
		// Simple bounded reset of retained blooms for evicted layers.
		c.blooms = make(map[string]*layerBloom)
	}
	if bloom != nil {
		c.blooms[r2Key] = bloom
	}
	if sz > c.maxBytes {
		return data
	}
	if elem, ok := c.items[r2Key]; ok {
		old := elem.Value.(*layerIndexData)
		c.curBytes -= old.bytes
		c.lru.Remove(elem)
		delete(c.items, r2Key)
	}
	elem := c.lru.PushFront(data)
	c.items[r2Key] = elem
	c.curBytes += sz
	for c.curBytes > c.maxBytes && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.lru.Remove(oldest)
		old := oldest.Value.(*layerIndexData)
		delete(c.items, old.r2Key)
		c.curBytes -= old.bytes
	}
	return data
}

func (c *boundedIndexCache) evict(r2Key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[r2Key]; ok {
		old := elem.Value.(*layerIndexData)
		c.curBytes -= old.bytes
		c.lru.Remove(elem)
		delete(c.items, r2Key)
	}
	delete(c.blooms, r2Key)
}

func (c *boundedIndexCache) evictPrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, elem := range c.items {
		if strings.HasPrefix(k, prefix) {
			old := elem.Value.(*layerIndexData)
			c.curBytes -= old.bytes
			c.lru.Remove(elem)
			delete(c.items, k)
		}
	}
	for k := range c.blooms {
		if strings.HasPrefix(k, prefix) {
			delete(c.blooms, k)
		}
	}
}

func (c *boundedIndexCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Init()
	c.items = make(map[string]*list.Element)
	c.blooms = make(map[string]*layerBloom)
	c.curBytes = 0
}
