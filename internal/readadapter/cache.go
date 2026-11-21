package readadapter

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"golang.org/x/sync/singleflight"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// CacheConfig configures the multi-level L1 RAM + L2 Local NVMe + L3 Pageserver cache.
type CacheConfig struct {
	L1MaxBytes int64
	L2Dir      string
	L2MaxBytes int64
}

// PageCacheKey uniquely identifies a cached 8 KiB page by tenant, generation,
// relation coordinates, block number, and snapshot LSN.
type PageCacheKey struct {
	TenantID   string
	Generation string
	SpcOID     uint32
	DbOID      uint32
	RelNumber  uint32
	ForkNum    uint8
	BlockNum   uint32
	LSN        uint64
}

func (k PageCacheKey) sfKey() string {
	return fmt.Sprintf("%s|%s|%d|%d|%d|%d|%d|%d",
		k.TenantID, k.Generation, k.SpcOID, k.DbOID, k.RelNumber, k.ForkNum, k.BlockNum, k.LSN)
}

// CacheStats reports L1/L2/L3 hit, miss, eviction, corruption, stampede, and usage counters.
type CacheStats struct {
	L1Hits             uint64
	L1Misses           uint64
	L1Evictions        uint64
	L1Bytes            int64
	L2Hits             uint64
	L2Misses           uint64
	L2Evictions        uint64
	L2Corrupt          uint64
	L2Bytes            int64
	L3Fetches          uint64
	StampedesPrevented uint64
}

type l1Entry struct {
	key  PageCacheKey
	data []byte
}

type l2Entry struct {
	key  PageCacheKey
	path string
	size int64
}

// TieredCacheBackend wraps an authoritative StorageBackend (L3 Pageserver)
// with an L1 in-memory LRU cache and an L2 checksummed local NVMe disk cache.
type TieredCacheBackend struct {
	l3         StorageBackend
	l1MaxBytes int64
	l2Dir      string
	l2MaxBytes int64

	sfGroup singleflight.Group
	tmpSeq  atomic.Uint64

	l1Mu    sync.Mutex
	l1Bytes int64
	l1List  *list.List
	l1Map   map[PageCacheKey]*list.Element

	l2Mu    sync.Mutex
	l2Bytes int64
	l2List  *list.List
	l2Map   map[PageCacheKey]*list.Element

	l1Hits             atomic.Uint64
	l1Misses           atomic.Uint64
	l1Evictions        atomic.Uint64
	l2Hits             atomic.Uint64
	l2Misses           atomic.Uint64
	l2Evictions        atomic.Uint64
	l2Corrupt          atomic.Uint64
	l3Fetches          atomic.Uint64
	stampedesPrevented atomic.Uint64
}

// NewTieredCacheBackend creates a multi-level L1+L2+L3 StorageBackend.
func NewTieredCacheBackend(l3 StorageBackend, cfg CacheConfig) (*TieredCacheBackend, error) {
	if l3 == nil {
		return nil, fmt.Errorf("readadapter: L3 StorageBackend must not be nil")
	}
	if cfg.L1MaxBytes <= 0 {
		cfg.L1MaxBytes = 64 * 1024 * 1024
	}
	if cfg.L2Dir != "" {
		if err := os.MkdirAll(cfg.L2Dir, 0o750); err != nil {
			return nil, fmt.Errorf("readadapter: create L2 cache dir: %w", err)
		}
		if cfg.L2MaxBytes <= 0 {
			cfg.L2MaxBytes = 256 * 1024 * 1024
		}
		reconcileL2DirOnStartup(cfg.L2Dir, cfg.L2MaxBytes)
	}
	return &TieredCacheBackend{
		l3:         l3,
		l1MaxBytes: cfg.L1MaxBytes,
		l2Dir:      cfg.L2Dir,
		l2MaxBytes: cfg.L2MaxBytes,
		l1List:     list.New(),
		l1Map:      make(map[PageCacheKey]*list.Element),
		l2List:     list.New(),
		l2Map:      make(map[PageCacheKey]*list.Element),
	}, nil
}

func reconcileL2DirOnStartup(l2Dir string, maxBytes int64) {
	type diskFile struct {
		path    string
		size    int64
		modTime time.Time
	}
	var files []diskFile
	var total int64
	_ = filepath.WalkDir(l2Dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.Contains(name, ".tmp.") {
			_ = os.Remove(p)
			return nil
		}
		if !strings.HasSuffix(name, ".pg") {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if info.Size() != PGPageSize+4 {
			_ = os.Remove(p)
			return nil
		}
		total += info.Size()
		files = append(files, diskFile{path: p, size: info.Size(), modTime: info.ModTime()})
		return nil
	})
	if total <= maxBytes {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})
	for _, f := range files {
		if total <= maxBytes {
			break
		}
		if os.Remove(f.path) == nil {
			total -= f.size
		}
	}
}

func (c *TieredCacheBackend) GetTenantGeneration(tenantID string) string {
	return c.l3.GetTenantGeneration(tenantID)
}

func (c *TieredCacheBackend) GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	return c.l3.GetRelSize(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, lsn)
}

func (c *TieredCacheBackend) GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error) {
	return c.l3.GetRelExists(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, lsn)
}

func (c *TieredCacheBackend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	if key.Kind != pageserver.KeyKindRelBlock || lsn == 0 || lsn == ^uint64(0) {
		c.l3Fetches.Add(1)
		return c.l3.GetPageAtLSN(ctx, tenantID, key, lsn)
	}

	gen := c.l3.GetTenantGeneration(tenantID)
	ck := PageCacheKey{
		TenantID:   tenantID,
		Generation: gen,
		SpcOID:     key.SpcOID,
		DbOID:      key.DbOID,
		RelNumber:  key.RelNumber,
		ForkNum:    key.ForkNum,
		BlockNum:   key.BlockNum,
		LSN:        lsn,
	}

	if pg, ok := c.getL1(ck); ok {
		c.l1Hits.Add(1)
		return pg, nil
	}
	c.l1Misses.Add(1)

	if c.l2Dir != "" {
		if pg, ok := c.getL2(ck); ok {
			c.l2Hits.Add(1)
			c.putL1(ck, pg)
			return pg, nil
		}
		c.l2Misses.Add(1)
	}

	ch := c.sfGroup.DoChan(ck.sfKey(), func() (any, error) {
		c.l3Fetches.Add(1)
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		pg, err := c.l3.GetPageAtLSN(fetchCtx, tenantID, key, lsn)
		if err != nil {
			return nil, err
		}
		if len(pg) == PGPageSize {
			if c.l2Dir != "" {
				c.putL2(ck, pg)
			}
			c.putL1(ck, pg)
		}
		out := make([]byte, len(pg))
		copy(out, pg)
		return out, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Shared {
			c.stampedesPrevented.Add(1)
		}
		if res.Err != nil {
			return nil, res.Err
		}
		pg := res.Val.([]byte)
		out := make([]byte, len(pg))
		copy(out, pg)
		return out, nil
	}
}

func (c *TieredCacheBackend) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	if nblocks == 0 {
		return nil, 0, nil
	}
	if lsn == 0 || lsn == ^uint64(0) {
		c.l3Fetches.Add(1)
		return c.l3.GetPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, lsn)
	}
	gen := c.l3.GetTenantGeneration(tenantID)
	out := make([]byte, int(nblocks)*PGPageSize)
	cached := make([]bool, nblocks)
	allHit := true

	for i := uint32(0); i < nblocks; i++ {
		ck := PageCacheKey{
			TenantID:   tenantID,
			Generation: gen,
			SpcOID:     spcOID,
			DbOID:      dbOID,
			RelNumber:  relNumber,
			ForkNum:    forkNum,
			BlockNum:   startBlock + i,
			LSN:        lsn,
		}
		if pg, ok := c.getL1(ck); ok {
			c.l1Hits.Add(1)
			copy(out[int(i)*PGPageSize:], pg)
			cached[i] = true
			continue
		}
		c.l1Misses.Add(1)
		if c.l2Dir != "" {
			if pg, ok := c.getL2(ck); ok {
				c.l2Hits.Add(1)
				c.putL1(ck, pg)
				copy(out[int(i)*PGPageSize:], pg)
				cached[i] = true
				continue
			}
			c.l2Misses.Add(1)
		}
		allHit = false
	}

	if allHit {
		return out, nblocks, nil
	}

	c.l3Fetches.Add(1)
	raw, gotCount, err := c.l3.GetPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, lsn)
	if err != nil {
		return nil, 0, err
	}
	for i := uint32(0); i < gotCount; i++ {
		if cached[i] {
			continue
		}
		off := int(i) * PGPageSize
		if off+PGPageSize > len(raw) {
			break
		}
		pg := raw[off : off+PGPageSize]
		copy(out[off:off+PGPageSize], pg)
		ck := PageCacheKey{
			TenantID:   tenantID,
			Generation: gen,
			SpcOID:     spcOID,
			DbOID:      dbOID,
			RelNumber:  relNumber,
			ForkNum:    forkNum,
			BlockNum:   startBlock + i,
			LSN:        lsn,
		}
		if c.l2Dir != "" {
			c.putL2(ck, pg)
		}
		c.putL1(ck, pg)
	}
	return out[:int(gotCount)*PGPageSize], gotCount, nil
}

// InvalidateTenant removes all L1 and L2 cached pages belonging to tenantID.
func (c *TieredCacheBackend) InvalidateTenant(tenantID string) {
	c.invalidateWhere(func(k PageCacheKey) bool {
		return k.TenantID == tenantID
	})
}

// InvalidateGeneration removes all L1 and L2 cached pages for (tenantID, generation).
func (c *TieredCacheBackend) InvalidateGeneration(tenantID, generation string) {
	c.invalidateWhere(func(k PageCacheKey) bool {
		return k.TenantID == tenantID && k.Generation == generation
	})
}

// InvalidateRelation removes all L1 and L2 cached pages for a specific relation fork.
func (c *TieredCacheBackend) InvalidateRelation(tenantID, generation string, spcOID, dbOID, relNumber uint32, forkNum uint8) {
	c.invalidateWhere(func(k PageCacheKey) bool {
		return k.TenantID == tenantID &&
			(generation == "" || k.Generation == generation) &&
			k.SpcOID == spcOID &&
			k.DbOID == dbOID &&
			k.RelNumber == relNumber &&
			k.ForkNum == forkNum
	})
}

// InvalidateBlock removes all L1 and L2 cached versions of a specific block below beforeLSN (or all if beforeLSN == 0).
func (c *TieredCacheBackend) InvalidateBlock(tenantID, generation string, spcOID, dbOID, relNumber uint32, forkNum uint8, blockNum uint32, beforeLSN uint64) {
	c.invalidateWhere(func(k PageCacheKey) bool {
		return k.TenantID == tenantID &&
			(generation == "" || k.Generation == generation) &&
			k.SpcOID == spcOID &&
			k.DbOID == dbOID &&
			k.RelNumber == relNumber &&
			k.ForkNum == forkNum &&
			k.BlockNum == blockNum &&
			(beforeLSN == 0 || k.LSN < beforeLSN)
	})
}

func (c *TieredCacheBackend) invalidateWhere(pred func(PageCacheKey) bool) {
	c.l1Mu.Lock()
	for k, elem := range c.l1Map {
		if pred(k) {
			c.l1List.Remove(elem)
			delete(c.l1Map, k)
			c.l1Bytes -= int64(len(elem.Value.(*l1Entry).data))
		}
	}
	c.l1Mu.Unlock()

	if c.l2Dir != "" {
		c.l2Mu.Lock()
		for k, elem := range c.l2Map {
			if pred(k) {
				ent := elem.Value.(*l2Entry)
				c.l2List.Remove(elem)
				delete(c.l2Map, k)
				c.l2Bytes -= ent.size
				_ = os.Remove(ent.path)
			}
		}
		c.l2Mu.Unlock()
	}
}

// Stats returns a point-in-time snapshot of L1/L2/L3 cache counters.
func (c *TieredCacheBackend) Stats() CacheStats {
	c.l1Mu.Lock()
	l1B := c.l1Bytes
	c.l1Mu.Unlock()

	var l2B int64
	if c.l2Dir != "" {
		c.l2Mu.Lock()
		l2B = c.l2Bytes
		c.l2Mu.Unlock()
	}

	return CacheStats{
		L1Hits:             c.l1Hits.Load(),
		L1Misses:           c.l1Misses.Load(),
		L1Evictions:        c.l1Evictions.Load(),
		L1Bytes:            l1B,
		L2Hits:             c.l2Hits.Load(),
		L2Misses:           c.l2Misses.Load(),
		L2Evictions:        c.l2Evictions.Load(),
		L2Corrupt:          c.l2Corrupt.Load(),
		L2Bytes:            l2B,
		L3Fetches:          c.l3Fetches.Load(),
		StampedesPrevented: c.stampedesPrevented.Load(),
	}
}

func (c *TieredCacheBackend) getL1(k PageCacheKey) ([]byte, bool) {
	c.l1Mu.Lock()
	defer c.l1Mu.Unlock()

	elem, ok := c.l1Map[k]
	if !ok {
		return nil, false
	}
	c.l1List.MoveToFront(elem)
	src := elem.Value.(*l1Entry).data
	cp := make([]byte, len(src))
	copy(cp, src)
	return cp, true
}

func (c *TieredCacheBackend) putL1(k PageCacheKey, pg []byte) {
	if int64(len(pg)) > c.l1MaxBytes {
		return
	}
	cp := make([]byte, len(pg))
	copy(cp, pg)

	c.l1Mu.Lock()
	defer c.l1Mu.Unlock()

	if elem, ok := c.l1Map[k]; ok {
		c.l1List.MoveToFront(elem)
		elem.Value.(*l1Entry).data = cp
		return
	}
	for c.l1Bytes+int64(len(cp)) > c.l1MaxBytes && c.l1List.Len() > 0 {
		back := c.l1List.Back()
		ent := back.Value.(*l1Entry)
		c.l1List.Remove(back)
		delete(c.l1Map, ent.key)
		c.l1Bytes -= int64(len(ent.data))
		c.l1Evictions.Add(1)
	}
	elem := c.l1List.PushFront(&l1Entry{key: k, data: cp})
	c.l1Map[k] = elem
	c.l1Bytes += int64(len(cp))
}

func (c *TieredCacheBackend) l2PathFor(k PageCacheKey) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%s|%d|%d|%d|%d|%d|%d",
		k.TenantID, k.Generation, k.SpcOID, k.DbOID, k.RelNumber, k.ForkNum, k.BlockNum, k.LSN)
	sum := hex.EncodeToString(h.Sum(nil))
	return filepath.Join(c.l2Dir, sum[:2], sum+".pg")
}

func (c *TieredCacheBackend) getL2(k PageCacheKey) ([]byte, bool) {
	c.l2Mu.Lock()
	elem, ok := c.l2Map[k]
	var path string
	if ok {
		ent := elem.Value.(*l2Entry)
		path = ent.path
		c.l2List.MoveToFront(elem)
		c.l2Mu.Unlock()
	} else {
		c.l2Mu.Unlock()
		path = c.l2PathFor(k)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if ok {
			c.evictCorruptL2(k, path, false)
		}
		return nil, false
	}
	if len(raw) != PGPageSize+4 {
		c.evictCorruptL2(k, path, true)
		return nil, false
	}
	gotCRC := binary.LittleEndian.Uint32(raw[PGPageSize:])
	wantCRC := crc32.Checksum(raw[:PGPageSize], crc32cTable)
	if gotCRC != wantCRC {
		c.evictCorruptL2(k, path, true)
		return nil, false
	}
	if !ok {
		c.registerL2Entry(k, path, int64(PGPageSize+4))
	}
	return raw[:PGPageSize], true
}

func (c *TieredCacheBackend) registerL2Entry(k PageCacheKey, path string, fileSize int64) {
	c.l2Mu.Lock()
	defer c.l2Mu.Unlock()

	if elem, ok := c.l2Map[k]; ok {
		c.l2List.MoveToFront(elem)
		return
	}
	for c.l2Bytes+fileSize > c.l2MaxBytes && c.l2List.Len() > 0 {
		back := c.l2List.Back()
		ent := back.Value.(*l2Entry)
		c.l2List.Remove(back)
		delete(c.l2Map, ent.key)
		c.l2Bytes -= ent.size
		_ = os.Remove(ent.path)
		c.l2Evictions.Add(1)
	}
	elem := c.l2List.PushFront(&l2Entry{key: k, path: path, size: fileSize})
	c.l2Map[k] = elem
	c.l2Bytes += fileSize
}

func (c *TieredCacheBackend) evictCorruptL2(k PageCacheKey, path string, countCorrupt bool) {
	if countCorrupt {
		c.l2Corrupt.Add(1)
	}
	c.l2Mu.Lock()
	if elem, ok := c.l2Map[k]; ok {
		ent := elem.Value.(*l2Entry)
		c.l2List.Remove(elem)
		delete(c.l2Map, k)
		c.l2Bytes -= ent.size
	}
	c.l2Mu.Unlock()
	_ = os.Remove(path)
}

func (c *TieredCacheBackend) putL2(k PageCacheKey, pg []byte) {
	if len(pg) != PGPageSize {
		return
	}
	fileSize := int64(PGPageSize + 4)
	if fileSize > c.l2MaxBytes {
		return
	}

	path := c.l2PathFor(k)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return
	}

	buf := make([]byte, PGPageSize+4)
	copy(buf[:PGPageSize], pg)
	binary.LittleEndian.PutUint32(buf[PGPageSize:], crc32.Checksum(pg, crc32cTable))

	tmpPath := fmt.Sprintf("%s.tmp.%d.%d", path, os.Getpid(), c.tmpSeq.Add(1))
	if err := os.WriteFile(tmpPath, buf, 0o640); err != nil {
		return
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return
	}

	c.registerL2Entry(k, path, fileSize)
}
