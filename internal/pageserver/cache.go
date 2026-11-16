package pageserver

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// DiskCache is a bounded LRU disk cache for hot layer files on the local volume.
type DiskCache struct {
	mu       sync.Mutex
	dir      string
	maxBytes int64
	curBytes int64
	lru      *list.List
	entries  map[string]*list.Element
}

type cacheItem struct {
	key      string
	filePath string
	size     int64
}

// NewDiskCache initializes a bounded LRU disk cache under dir.
func NewDiskCache(dir string, maxBytes int64) (*DiskCache, error) {
	if maxBytes <= 0 {
		maxBytes = 256 * 1024 * 1024
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &DiskCache{
		dir:      dir,
		maxBytes: maxBytes,
		lru:      list.New(),
		entries:  make(map[string]*list.Element),
	}, nil
}

func (c *DiskCache) pathForKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(c.dir, hex.EncodeToString(sum[:16])+".layer")
}

// Put stores data in the disk cache and evicts LRU entries if over maxBytes.
func (c *DiskCache) Put(key string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if int64(len(data)) > c.maxBytes {
		return nil
	}
	if elem, ok := c.entries[key]; ok {
		item := elem.Value.(*cacheItem)
		c.curBytes -= item.size
		c.lru.Remove(elem)
		delete(c.entries, key)
		_ = os.Remove(item.filePath)
	}

	p := c.pathForKey(key)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	item := &cacheItem{key: key, filePath: p, size: int64(len(data))}
	elem := c.lru.PushFront(item)
	c.entries[key] = elem
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
		_ = os.Remove(oldItem.filePath)
	}
	return nil
}

// GetRange reads [offset, offset+length) if key is cached locally.
func (c *DiskCache) GetRange(key string, offset, length int64) ([]byte, bool) {
	c.mu.Lock()
	elem, ok := c.entries[key]
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
