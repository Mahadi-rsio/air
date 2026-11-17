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
	mu       sync.Mutex
	dir      string
	maxBytes int64
	curBytes int64
	lru      *list.List
	entries  map[string]*list.Element
	tmpSeq   atomic.Uint64
}

type cacheItem struct {
	key      string
	filePath string
	size     int64
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
	p := c.pathForHash(hashKey)
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
			evictPaths = append(evictPaths, oldItem.filePath)
		}
	}
	c.mu.Unlock()

	for _, ep := range evictPaths {
		_ = os.Remove(ep)
	}
	return nil
}

// GetRange reads [offset, offset+length) if key is cached locally.
func (c *DiskCache) GetRange(key string, offset, length int64) ([]byte, bool) {
	hashKey := cacheKeyHash(key)
	c.mu.Lock()
	elem, ok := c.entries[hashKey]
	if !ok {
		c.mu.Unlock()
		return nil, false
	}
	c.lru.MoveToFront(elem)
	p := elem.Value.(*cacheItem).filePath
	c.mu.Unlock()

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

// Stats returns the current cached byte count and entry count.
func (c *DiskCache) Stats() (bytes int64, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes, len(c.entries)
}
