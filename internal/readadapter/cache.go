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

// BlockCacheKey identifies a cached 8 KiB page independent of snapshot LSN.
type BlockCacheKey struct {
	TenantID   string
	Generation string
	SpcOID     uint32
	DbOID      uint32
	RelNumber  uint32
	ForkNum    uint8
	BlockNum   uint32
}

func (k PageCacheKey) blockKey() BlockCacheKey {
	return BlockCacheKey{
		TenantID:   k.TenantID,
		Generation: k.Generation,
		SpcOID:     k.SpcOID,
		DbOID:      k.DbOID,
		RelNumber:  k.RelNumber,
		ForkNum:    k.ForkNum,
		BlockNum:   k.BlockNum,
	}
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

type relMetaKey struct {
	TenantID   string
	Generation string
	SpcOID     uint32
	DbOID      uint32
	RelNumber  uint32
	ForkNum    uint8
	LSN        uint64
}

type relMetaVal struct {
	existsKnown bool
	exists      bool
	sizeKnown   bool
	nblocks     uint32
}

type relMetaLatestKey struct {
	TenantID   string
	Generation string
	SpcOID     uint32
	DbOID      uint32
	RelNumber  uint32
	ForkNum    uint8
}

type relMetaLatestVal struct {
	fetchedLSN uint64
	val        relMetaVal
}

type tenantSyncState struct {
	syncedLSN  uint64
	catalogLSN uint64
}

const maxRelMetaCacheEntries = 4096

// TieredCacheBackend wraps an authoritative StorageBackend (L3 Pageserver)
// with an L1 in-memory LRU cache and an L2 checksummed local NVMe disk cache.
type TieredCacheBackend struct {
	l3         StorageBackend
	l1MaxBytes int64
	l2Dir      string
	l2MaxBytes int64

	sfGroup singleflight.Group
	syncSF  singleflight.Group
	tmpSeq  atomic.Uint64

	syncMu    sync.Mutex
	syncState map[string]*tenantSyncState

	l1Mu     sync.Mutex
	l1Bytes  int64
	l1List   *list.List
	l1Map    map[PageCacheKey]*list.Element
	l1Latest map[BlockCacheKey]*list.Element

	l2Mu     sync.Mutex
	l2Bytes  int64
	l2List   *list.List
	l2Map    map[PageCacheKey]*list.Element
	l2Latest map[BlockCacheKey]*list.Element

	metaMu     sync.Mutex
	metaMap    map[relMetaKey]relMetaVal
	metaLatest map[relMetaLatestKey]relMetaLatestVal

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
		syncState:  make(map[string]*tenantSyncState),
		l1List:     list.New(),
		l1Map:      make(map[PageCacheKey]*list.Element),
		l1Latest:   make(map[BlockCacheKey]*list.Element),
		l2List:     list.New(),
		l2Map:      make(map[PageCacheKey]*list.Element),
		l2Latest:   make(map[BlockCacheKey]*list.Element),
		metaMap:    make(map[relMetaKey]relMetaVal),
		metaLatest: make(map[relMetaLatestKey]relMetaLatestVal),
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

// ensureSynced pulls incremental modified-block events from L3 (coalesced via singleflight)
// whenever targetLSN exceeds the highest LSN already synced for (tenantID, generation).
func (c *TieredCacheBackend) ensureSynced(ctx context.Context, tenantID, generation string, targetLSN uint64) (syncedLSN, catalogLSN uint64, supported bool) {
	mbp, ok := c.l3.(pageserver.ModifiedBlocksProvider)
	if !ok {
		return 0, 0, false
	}
	stateKey := tenantID + "|" + generation
	c.syncMu.Lock()
	st := c.syncState[stateKey]
	if st != nil && targetLSN <= st.syncedLSN {
		s, cat := st.syncedLSN, st.catalogLSN
		c.syncMu.Unlock()
		return s, cat, true
	}
	c.syncMu.Unlock()

	val, err, _ := c.syncSF.Do(stateKey, func() (any, error) {
		c.syncMu.Lock()
		cur := c.syncState[stateKey]
		var since uint64
		if cur != nil {
			if targetLSN <= cur.syncedLSN {
				out := *cur
				c.syncMu.Unlock()
				return out, nil
			}
			since = cur.syncedLSN
		}
		c.syncMu.Unlock()

		syncCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		res, gerr := mbp.GetModifiedBlocksSince(syncCtx, tenantID, since)
		if gerr != nil {
			return tenantSyncState{}, gerr
		}

		if res.ResetRequired {
			c.InvalidateTenant(tenantID)
		} else {
			for _, ev := range res.Events {
				if ev.IsRelWide {
					c.InvalidateRelation(tenantID, generation, ev.SpcOID, ev.DbOID, ev.RelNumber, ev.ForkNum)
				} else {
					c.InvalidateBlock(tenantID, generation, ev.SpcOID, ev.DbOID, ev.RelNumber, ev.ForkNum, ev.BlockNum, ev.LSN)
					c.invalidateRelMetaIfExtended(tenantID, generation, ev.SpcOID, ev.DbOID, ev.RelNumber, ev.ForkNum, ev.BlockNum)
				}
			}
		}

		c.syncMu.Lock()
		cur = c.syncState[stateKey]
		if cur == nil {
			cur = &tenantSyncState{}
			c.syncState[stateKey] = cur
		}
		if res.CurrentLSN > cur.syncedLSN {
			cur.syncedLSN = res.CurrentLSN
		}
		if res.CatalogLSN > cur.catalogLSN {
			cur.catalogLSN = res.CatalogLSN
		}
		out := *cur
		c.syncMu.Unlock()
		return out, nil
	})
	if err != nil {
		return 0, 0, false
	}
	out := val.(tenantSyncState)
	return out.syncedLSN, out.catalogLSN, true
}

// CatalogLSN returns the latest catalog modification LSN known for (tenantID, generation)
// after syncing up to targetLSN.
func (c *TieredCacheBackend) CatalogLSN(ctx context.Context, tenantID, generation string, targetLSN uint64) (uint64, bool) {
	_, catLSN, ok := c.ensureSynced(ctx, tenantID, generation, targetLSN)
	return catLSN, ok
}

func (c *TieredCacheBackend) invalidateRelMetaIfExtended(tenantID, generation string, spcOID, dbOID, relNumber uint32, forkNum uint8, blockNum uint32) {
	mlk := relMetaLatestKey{
		TenantID: tenantID, Generation: generation,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum,
	}
	c.metaMu.Lock()
	if cur, ok := c.metaLatest[mlk]; ok && cur.val.sizeKnown && blockNum >= cur.val.nblocks {
		delete(c.metaLatest, mlk)
	}
	c.metaMu.Unlock()
}

func (c *TieredCacheBackend) GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	if lsn == 0 || lsn == ^uint64(0) {
		return c.l3.GetRelSize(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, lsn)
	}
	gen := c.l3.GetTenantGeneration(tenantID)
	syncedLSN, _, syncOK := c.ensureSynced(ctx, tenantID, gen, lsn)
	mk := relMetaKey{
		TenantID: tenantID, Generation: gen,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum, LSN: lsn,
	}
	mlk := relMetaLatestKey{
		TenantID: tenantID, Generation: gen,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum,
	}
	c.metaMu.Lock()
	if v, ok := c.metaMap[mk]; ok && v.sizeKnown {
		c.metaMu.Unlock()
		return v.nblocks, nil
	}
	if syncOK && lsn <= syncedLSN {
		if lv, ok := c.metaLatest[mlk]; ok && lv.val.sizeKnown && lv.fetchedLSN <= lsn {
			c.metaMu.Unlock()
			return lv.val.nblocks, nil
		}
	}
	c.metaMu.Unlock()

	sz, err := c.l3.GetRelSize(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, lsn)
	if err != nil {
		return 0, err
	}
	c.metaMu.Lock()
	if len(c.metaMap) >= maxRelMetaCacheEntries {
		clear(c.metaMap)
	}
	if len(c.metaLatest) >= maxRelMetaCacheEntries {
		clear(c.metaLatest)
	}
	v := c.metaMap[mk]
	v.sizeKnown = true
	v.nblocks = sz
	c.metaMap[mk] = v
	if cur, ok := c.metaLatest[mlk]; !ok || lsn >= cur.fetchedLSN {
		nv := cur.val
		nv.sizeKnown = true
		nv.nblocks = sz
		c.metaLatest[mlk] = relMetaLatestVal{fetchedLSN: lsn, val: nv}
	}
	c.metaMu.Unlock()
	return sz, nil
}

func (c *TieredCacheBackend) GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error) {
	if lsn == 0 || lsn == ^uint64(0) {
		return c.l3.GetRelExists(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, lsn)
	}
	gen := c.l3.GetTenantGeneration(tenantID)
	syncedLSN, _, syncOK := c.ensureSynced(ctx, tenantID, gen, lsn)
	mk := relMetaKey{
		TenantID: tenantID, Generation: gen,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum, LSN: lsn,
	}
	mlk := relMetaLatestKey{
		TenantID: tenantID, Generation: gen,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum,
	}
	c.metaMu.Lock()
	if v, ok := c.metaMap[mk]; ok && v.existsKnown {
		c.metaMu.Unlock()
		return v.exists, nil
	}
	if syncOK && lsn <= syncedLSN {
		if lv, ok := c.metaLatest[mlk]; ok && lv.val.existsKnown && lv.fetchedLSN <= lsn {
			c.metaMu.Unlock()
			return lv.val.exists, nil
		}
	}
	c.metaMu.Unlock()

	ex, err := c.l3.GetRelExists(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, lsn)
	if err != nil {
		return false, err
	}
	c.metaMu.Lock()
	if len(c.metaMap) >= maxRelMetaCacheEntries {
		clear(c.metaMap)
	}
	if len(c.metaLatest) >= maxRelMetaCacheEntries {
		clear(c.metaLatest)
	}
	v := c.metaMap[mk]
	v.existsKnown = true
	v.exists = ex
	c.metaMap[mk] = v
	if cur, ok := c.metaLatest[mlk]; !ok || lsn >= cur.fetchedLSN {
		nv := cur.val
		nv.existsKnown = true
		nv.exists = ex
		c.metaLatest[mlk] = relMetaLatestVal{fetchedLSN: lsn, val: nv}
	}
	c.metaMu.Unlock()
	return ex, nil
}

func (c *TieredCacheBackend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	if key.Kind != pageserver.KeyKindRelBlock || lsn == 0 || lsn == ^uint64(0) {
		c.l3Fetches.Add(1)
		return c.l3.GetPageAtLSN(ctx, tenantID, key, lsn)
	}

	gen := c.l3.GetTenantGeneration(tenantID)
	syncedLSN, _, syncOK := c.ensureSynced(ctx, tenantID, gen, lsn)
	if !syncOK {
		syncedLSN = 0
	}
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

	if pg, ok := c.getL1(ck, syncedLSN); ok {
		c.l1Hits.Add(1)
		return pg, nil
	}
	c.l1Misses.Add(1)

	if c.l2Dir != "" {
		if pg, ok := c.getL2(ck, syncedLSN); ok {
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
	syncedLSN, _, syncOK := c.ensureSynced(ctx, tenantID, gen, lsn)
	if !syncOK {
		syncedLSN = 0
	}
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
		if c.getL1Into(ck, out[int(i)*PGPageSize:int(i+1)*PGPageSize], syncedLSN) {
			c.l1Hits.Add(1)
			cached[i] = true
			continue
		}
		c.l1Misses.Add(1)
		if c.l2Dir != "" {
			if pg, ok := c.getL2(ck, syncedLSN); ok {
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

	firstMiss := int(nblocks)
	lastMiss := -1
	for i := 0; i < int(nblocks); i++ {
		if !cached[i] {
			if firstMiss == int(nblocks) {
				firstMiss = i
			}
			lastMiss = i
		}
	}
	fetchStart := startBlock + uint32(firstMiss)
	fetchCount := uint32(lastMiss - firstMiss + 1)

	c.l3Fetches.Add(1)
	raw, gotCount, err := c.l3.GetPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, fetchStart, fetchCount, lsn)
	if err != nil {
		return nil, 0, err
	}
	for j := uint32(0); j < gotCount; j++ {
		i := uint32(firstMiss) + j
		if cached[i] {
			continue
		}
		srcOff := int(j) * PGPageSize
		dstOff := int(i) * PGPageSize
		if srcOff+PGPageSize > len(raw) {
			break
		}
		pg := raw[srcOff : srcOff+PGPageSize]
		copy(out[dstOff:dstOff+PGPageSize], pg)
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
	totalCount := uint32(firstMiss) + gotCount
	if totalCount > nblocks {
		totalCount = nblocks
	}
	return out[:int(totalCount)*PGPageSize], totalCount, nil
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
			bk := k.blockKey()
			if c.l1Latest[bk] == elem {
				delete(c.l1Latest, bk)
			}
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
				bk := k.blockKey()
				if c.l2Latest[bk] == elem {
					delete(c.l2Latest, bk)
				}
				c.l2Bytes -= ent.size
				_ = os.Remove(ent.path)
			}
		}
		c.l2Mu.Unlock()
	}

	c.metaMu.Lock()
	for mk := range c.metaMap {
		if pred(PageCacheKey{
			TenantID:   mk.TenantID,
			Generation: mk.Generation,
			SpcOID:     mk.SpcOID,
			DbOID:      mk.DbOID,
			RelNumber:  mk.RelNumber,
			ForkNum:    mk.ForkNum,
			LSN:        mk.LSN,
		}) {
			delete(c.metaMap, mk)
		}
	}
	for mlk, lv := range c.metaLatest {
		if pred(PageCacheKey{
			TenantID:   mlk.TenantID,
			Generation: mlk.Generation,
			SpcOID:     mlk.SpcOID,
			DbOID:      mlk.DbOID,
			RelNumber:  mlk.RelNumber,
			ForkNum:    mlk.ForkNum,
			LSN:        lv.fetchedLSN,
		}) {
			delete(c.metaLatest, mlk)
		}
	}
	c.metaMu.Unlock()
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

func (c *TieredCacheBackend) getL1(k PageCacheKey, syncedLSN uint64) ([]byte, bool) {
	c.l1Mu.Lock()
	defer c.l1Mu.Unlock()

	elem, ok := c.l1Map[k]
	if !ok && syncedLSN > 0 && k.LSN <= syncedLSN {
		if le, lok := c.l1Latest[k.blockKey()]; lok && le.Value.(*l1Entry).key.LSN <= k.LSN {
			elem = le
			ok = true
		}
	}
	if !ok {
		return nil, false
	}
	c.l1List.MoveToFront(elem)
	src := elem.Value.(*l1Entry).data
	cp := make([]byte, len(src))
	copy(cp, src)
	return cp, true
}

func (c *TieredCacheBackend) getL1Into(k PageCacheKey, dst []byte, syncedLSN uint64) bool {
	c.l1Mu.Lock()
	defer c.l1Mu.Unlock()

	elem, ok := c.l1Map[k]
	if !ok && syncedLSN > 0 && k.LSN <= syncedLSN {
		if le, lok := c.l1Latest[k.blockKey()]; lok && le.Value.(*l1Entry).key.LSN <= k.LSN {
			elem = le
			ok = true
		}
	}
	if !ok {
		return false
	}
	c.l1List.MoveToFront(elem)
	src := elem.Value.(*l1Entry).data
	copy(dst, src)
	return true
}

func (c *TieredCacheBackend) putL1(k PageCacheKey, pg []byte) {
	if int64(len(pg)) > c.l1MaxBytes {
		return
	}
	cp := make([]byte, len(pg))
	copy(cp, pg)

	c.l1Mu.Lock()
	defer c.l1Mu.Unlock()

	bk := k.blockKey()
	if elem, ok := c.l1Map[k]; ok {
		c.l1List.MoveToFront(elem)
		elem.Value.(*l1Entry).data = cp
		if cur, lok := c.l1Latest[bk]; !lok || cur.Value.(*l1Entry).key.LSN <= k.LSN {
			c.l1Latest[bk] = elem
		}
		return
	}
	for c.l1Bytes+int64(len(cp)) > c.l1MaxBytes && c.l1List.Len() > 0 {
		back := c.l1List.Back()
		ent := back.Value.(*l1Entry)
		c.l1List.Remove(back)
		delete(c.l1Map, ent.key)
		ebk := ent.key.blockKey()
		if c.l1Latest[ebk] == back {
			delete(c.l1Latest, ebk)
		}
		c.l1Bytes -= int64(len(ent.data))
		c.l1Evictions.Add(1)
	}
	elem := c.l1List.PushFront(&l1Entry{key: k, data: cp})
	c.l1Map[k] = elem
	if cur, lok := c.l1Latest[bk]; !lok || cur.Value.(*l1Entry).key.LSN <= k.LSN {
		c.l1Latest[bk] = elem
	}
	c.l1Bytes += int64(len(cp))
}

func (c *TieredCacheBackend) l2PathFor(k PageCacheKey) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%s|%d|%d|%d|%d|%d|%d",
		k.TenantID, k.Generation, k.SpcOID, k.DbOID, k.RelNumber, k.ForkNum, k.BlockNum, k.LSN)
	sum := hex.EncodeToString(h.Sum(nil))
	return filepath.Join(c.l2Dir, sum[:2], sum+".pg")
}

func (c *TieredCacheBackend) getL2(k PageCacheKey, syncedLSN uint64) ([]byte, bool) {
	c.l2Mu.Lock()
	elem, ok := c.l2Map[k]
	if !ok && syncedLSN > 0 && k.LSN <= syncedLSN {
		if le, lok := c.l2Latest[k.blockKey()]; lok && le.Value.(*l2Entry).key.LSN <= k.LSN {
			elem = le
			ok = true
		}
	}
	var path string
	actualKey := k
	if ok {
		ent := elem.Value.(*l2Entry)
		path = ent.path
		actualKey = ent.key
		c.l2List.MoveToFront(elem)
		c.l2Mu.Unlock()
	} else {
		c.l2Mu.Unlock()
		path = c.l2PathFor(k)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if ok {
			c.evictCorruptL2(actualKey, path, false)
		}
		return nil, false
	}
	if len(raw) != PGPageSize+4 {
		c.evictCorruptL2(actualKey, path, true)
		return nil, false
	}
	gotCRC := binary.LittleEndian.Uint32(raw[PGPageSize:])
	wantCRC := crc32.Checksum(raw[:PGPageSize], crc32cTable)
	if gotCRC != wantCRC {
		c.evictCorruptL2(actualKey, path, true)
		return nil, false
	}
	if !ok {
		c.registerL2Entry(actualKey, path, int64(PGPageSize+4))
	}
	return raw[:PGPageSize], true
}

func (c *TieredCacheBackend) registerL2Entry(k PageCacheKey, path string, fileSize int64) {
	c.l2Mu.Lock()
	defer c.l2Mu.Unlock()

	bk := k.blockKey()
	if elem, ok := c.l2Map[k]; ok {
		c.l2List.MoveToFront(elem)
		if cur, lok := c.l2Latest[bk]; !lok || cur.Value.(*l2Entry).key.LSN <= k.LSN {
			c.l2Latest[bk] = elem
		}
		return
	}
	for c.l2Bytes+fileSize > c.l2MaxBytes && c.l2List.Len() > 0 {
		back := c.l2List.Back()
		ent := back.Value.(*l2Entry)
		c.l2List.Remove(back)
		delete(c.l2Map, ent.key)
		ebk := ent.key.blockKey()
		if c.l2Latest[ebk] == back {
			delete(c.l2Latest, ebk)
		}
		c.l2Bytes -= ent.size
		_ = os.Remove(ent.path)
		c.l2Evictions.Add(1)
	}
	elem := c.l2List.PushFront(&l2Entry{key: k, path: path, size: fileSize})
	c.l2Map[k] = elem
	if cur, lok := c.l2Latest[bk]; !lok || cur.Value.(*l2Entry).key.LSN <= k.LSN {
		c.l2Latest[bk] = elem
	}
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
		bk := k.blockKey()
		if c.l2Latest[bk] == elem {
			delete(c.l2Latest, bk)
		}
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
