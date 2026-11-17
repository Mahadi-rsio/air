package pageserver

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// DiskCache is a bounded LRU disk cache for hot layer files and chunks on the local volume.
// Disk I/O (WriteFile, pread, Remove) is performed outside the metadata mutex so concurrent
// readers never block on disk writes or evictions.
type DiskCache struct {
	mu        sync.Mutex
	dir       string
	maxBytes  int64
	curBytes  int64
	lru       *list.List
	entries   map[string]*list.Element
	tmpSeq    atomic.Uint64
	genSeq    uint64
	evictions atomic.Int64
}

type cacheItem struct {
	key           string
	filePath      string
	size          int64
	activeReads   int
	pendingDelete bool
}

// NewDiskCache initializes a bounded LRU disk cache under dir and recovers any
// existing *.layer cache files from a previous run while cleaning up stale *.tmp files.
func NewDiskCache(dir string, maxBytes int64) (*DiskCache, error) {
	if maxBytes <= 0 {
		maxBytes = 256 * 1024 * 1024
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &DiskCache{
		dir:      dir,
		maxBytes: maxBytes,
		lru:      list.New(),
		entries:  make(map[string]*list.Element),
	}
	c.recoverFromDisk()
	return c, nil
}

func (c *DiskCache) recoverFromDisk() {
	dirEntries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	type recoveredFile struct {
		hashKey string
		path    string
		size    int64
		modTime int64
	}
	var found []recoveredFile
	for _, de := range dirEntries {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		fullPath := filepath.Join(c.dir, name)
		if strings.HasSuffix(name, ".tmp") {
			_ = os.Remove(fullPath)
			continue
		}
		if !strings.HasSuffix(name, ".layer") {
			continue
		}
		info, err := de.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		hashKey := strings.TrimSuffix(name, ".layer")
		if dot := strings.IndexByte(hashKey, '.'); dot > 0 {
			hashKey = hashKey[:dot]
		}
		found = append(found, recoveredFile{
			hashKey: hashKey,
			path:    fullPath,
			size:    info.Size(),
			modTime: info.ModTime().UnixNano(),
		})
	}
	// Insert oldest first so newest ends up at Front of LRU.
	sort.Slice(found, func(i, j int) bool {
		return found[i].modTime < found[j].modTime
	})
	for _, rf := range found {
		if rf.size > c.maxBytes {
			_ = os.Remove(rf.path)
			continue
		}
		if prevElem, dup := c.entries[rf.hashKey]; dup {
			c.lru.Remove(prevElem)
			prevItem := prevElem.Value.(*cacheItem)
			c.curBytes -= prevItem.size
			_ = os.Remove(prevItem.filePath)
		}
		item := &cacheItem{key: rf.hashKey, filePath: rf.path, size: rf.size}
		elem := c.lru.PushFront(item)
		c.entries[rf.hashKey] = elem
		c.curBytes += rf.size
	}
	for c.curBytes > c.maxBytes && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.lru.Remove(oldest)
		oldItem := oldest.Value.(*cacheItem)
		delete(c.entries, oldItem.key)
		c.curBytes -= oldItem.size
		_ = os.Remove(oldItem.filePath)
	}
}

func cacheKeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:16])
}

func (c *DiskCache) pathForHash(hashKey string) string {
	return filepath.Join(c.dir, hashKey+".layer")
}

// Put stores data in the disk cache and evicts LRU entries if over maxBytes.
// File writes and evicted file removals happen outside c.mu.
func (c *DiskCache) Put(key string, data []byte) error {
	if int64(len(data)) > c.maxBytes {
		return nil
	}
	hashKey := cacheKeyHash(key)

	c.mu.Lock()
	c.genSeq++
	gen := c.genSeq
	p := c.pathForHash(hashKey)
	if elem, ok := c.entries[hashKey]; ok {
		old := elem.Value.(*cacheItem)
		if old.activeReads > 0 {
			p = filepath.Join(c.dir, fmt.Sprintf("%s.%d.layer", hashKey, gen))
		}
	}
	c.mu.Unlock()

	tmp := fmt.Sprintf("%s.%d.tmp", p, c.tmpSeq.Add(1))
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	var evictPaths []string
	c.mu.Lock()
	if elem, ok := c.entries[hashKey]; ok {
		old := elem.Value.(*cacheItem)
		c.curBytes -= old.size
		c.lru.Remove(elem)
		delete(c.entries, hashKey)
		if old.filePath != p {
			if old.activeReads > 0 {
				old.pendingDelete = true
			} else {
				evictPaths = append(evictPaths, old.filePath)
			}
		}
	}
	item := &cacheItem{key: hashKey, filePath: p, size: int64(len(data))}
	elem := c.lru.PushFront(item)
	c.entries[hashKey] = elem
	c.curBytes += item.size

	for c.curBytes > c.maxBytes && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.lru.Remove(oldest)
		oldItem := oldest.Value.(*cacheItem)
		delete(c.entries, oldItem.key)
		c.curBytes -= oldItem.size
		if oldItem.filePath != p {
			if oldItem.activeReads > 0 {
				oldItem.pendingDelete = true
			} else {
				evictPaths = append(evictPaths, oldItem.filePath)
			}
		}
	}
	c.mu.Unlock()

	for _, ep := range evictPaths {
		_ = os.Remove(ep)
	}
	return nil
}

// GetRange reads [offset, offset+length) if key is cached locally.
// Active reads pin the underlying file so concurrent Evict calls defer deletion until readers finish.
func (c *DiskCache) GetRange(key string, offset, length int64) ([]byte, bool) {
	hashKey := cacheKeyHash(key)
	c.mu.Lock()
	elem, ok := c.entries[hashKey]
	if !ok {
		c.mu.Unlock()
		return nil, false
	}
	c.lru.MoveToFront(elem)
	item := elem.Value.(*cacheItem)
	item.activeReads++
	p := item.filePath
	c.mu.Unlock()

	var deletePath string
	defer func() {
		c.mu.Lock()
		item.activeReads--
		if item.pendingDelete && item.activeReads == 0 {
			deletePath = item.filePath
			item.pendingDelete = false
		}
		c.mu.Unlock()
		if deletePath != "" {
			_ = os.Remove(deletePath)
		}
	}()

	f, err := os.Open(p)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && !(errors.Is(err, io.EOF) && int64(n) == length) {
		return nil, false
	}
	return buf[:n], true
}

// Evict safely removes key from the cache. If readers are currently active on the
// underlying file, deletion is deferred until the last active reader finishes.
func (c *DiskCache) Evict(key string) bool {
	hashKey := cacheKeyHash(key)
	var removePath string
	c.mu.Lock()
	elem, ok := c.entries[hashKey]
	if !ok {
		c.mu.Unlock()
		return false
	}
	c.lru.Remove(elem)
	delete(c.entries, hashKey)
	item := elem.Value.(*cacheItem)
	c.curBytes -= item.size
	if item.activeReads > 0 {
		item.pendingDelete = true
	} else {
		removePath = item.filePath
	}
	c.mu.Unlock()
	c.evictions.Add(1)
	if removePath != "" {
		_ = os.Remove(removePath)
	}
	return true
}

// Evictions returns the number of explicit Evict calls performed on this DiskCache.
func (c *DiskCache) Evictions() int64 {
	return c.evictions.Load()
}

// Stats returns the current cached byte count and entry count.
func (c *DiskCache) Stats() (bytes int64, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes, len(c.entries)
}
