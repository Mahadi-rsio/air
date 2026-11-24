package pageserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Mahadi-rsio/air/internal/crypto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config configures the AIR Pageserver.
type Config struct {
	SocketPath        string
	DisableUnixSocket bool
	LayerDir          string
	CacheDir          string
	CacheMaxBytes     int64
	WalRedoBin        string
	S3Client          *s3.Client
	R2Bucket          string
	KMS               *crypto.EnvelopeKMS
	MemoryBudget      MemoryBudgetConfig
	EnableCAWAL       bool
	Debug             bool
}

// Pageserver manages multi-tenant Timelines and serves air_smgr over a Unix socket and/or gRPC.
type Pageserver struct {
	cfg     Config
	store   *LayerStore
	cache   *DiskCache
	redoer  WALRedoer
	budget  *MemoryBudget
	unixSrv *UnixServer

	mu          sync.RWMutex
	tenants     map[string]*Timeline
	generations map[string]string
}

// New creates a Pageserver instance. Call Start(ctx) to bind the Unix socket if enabled.
func New(cfg Config) (*Pageserver, error) {
	if cfg.Debug {
		SetDebugLogging(true)
	}
	if !cfg.DisableUnixSocket && cfg.SocketPath == "" {
		cfg.SocketPath = "/var/lib/air/pageserver.sock"
	}
	if cfg.CacheMaxBytes <= 0 {
		cfg.CacheMaxBytes = 256 * 1024 * 1024
	}
	var cache *DiskCache
	// Only enable DiskCache when LayerDir is empty or S3/R2 is configured; when local LayerDir
	// is used without R2, layer files already reside directly on disk under LayerDir.
	if cfg.CacheDir != "" && (cfg.LayerDir == "" || (cfg.S3Client != nil && cfg.R2Bucket != "")) {
		var err error
		cache, err = NewDiskCache(cfg.CacheDir, cfg.CacheMaxBytes)
		if err != nil {
			return nil, err
		}
	}
	store := NewLayerStore(cfg.LayerDir, cfg.S3Client, cfg.R2Bucket, cfg.KMS, cache)
	cawal := NewCAWALStore(cfg.LayerDir)
	store.SetCAWALStore(cawal)
	store.SetEnableCAWAL(cfg.EnableCAWAL)
	redoer := NewDefaultWALRedoer(cfg.WalRedoBin)
	ps := &Pageserver{
		cfg:         cfg,
		store:       store,
		cache:       cache,
		redoer:      redoer,
		tenants:     make(map[string]*Timeline),
		generations: make(map[string]string),
	}
	ps.budget = newMemoryBudget(ps, cfg.MemoryBudget)
	store.SetDecodedChunkCache(NewDecodedChunkCache(DecodedChunkCacheConfig{
		MaxBytes: ps.budget.cfg.DecodedChunkCacheBytes,
	}))
	store.SetIndexCacheMaxBytes(ps.budget.cfg.IndexCacheBytes)
	store.SetGenerationChecker(ps.isGenerationActive)
	if !cfg.DisableUnixSocket && cfg.SocketPath != "" {
		ps.unixSrv = NewUnixServer(cfg.SocketPath, ps)
	}
	ps.recoverLocalTenants()
	ps.budget.start()
	debugf("server", "initialized pageserver layer_dir=%q cache_dir=%q cache_max_bytes=%d mem_limit_mb=%d global_soft_inmem_mb=%d tenant_soft_inmem_kb=%d walredo_bin=%q r2_bucket=%q kms=%v unix_socket=%q recovered_tenants=%d",
		cfg.LayerDir, cfg.CacheDir, cfg.CacheMaxBytes, ps.budget.cfg.MemoryLimitBytes/(1024*1024), ps.budget.cfg.GlobalSoftInMemBytes/(1024*1024), ps.budget.cfg.PerTenantSoftInMemBytes/1024, cfg.WalRedoBin, cfg.R2Bucket, cfg.KMS != nil, cfg.SocketPath, len(ps.tenants))
	return ps, nil
}

func parseManifestEpoch(name string) (int64, bool) {
	if !strings.HasPrefix(name, "timeline_") || !strings.HasSuffix(name, "_layer_manifest.json") {
		return 0, false
	}
	mid := strings.TrimSuffix(strings.TrimPrefix(name, "timeline_"), "_layer_manifest.json")
	ep, err := strconv.ParseInt(mid, 10, 64)
	if err != nil || ep <= 0 {
		return 0, false
	}
	return ep, true
}

func (p *Pageserver) recoverLocalTenants() {
	if p.cfg.LayerDir == "" {
		return
	}
	tenantEntries, err := os.ReadDir(p.cfg.LayerDir)
	if err != nil {
		return
	}
	type candidate struct {
		gen      string
		epoch    int64
		manifest TimelineLayerManifest
		lastLSN  uint64
		modTime  time.Time
	}
	for _, te := range tenantEntries {
		if !te.IsDir() {
			continue
		}
		tenantID := te.Name()
		tenantPath := filepath.Join(p.cfg.LayerDir, tenantID)
		var best *candidate

		scanDir := func(dir, gen string) {
			entries, err := os.ReadDir(dir)
			if err != nil {
				return
			}
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				epoch, ok := parseManifestEpoch(e.Name())
				if !ok {
					continue
				}
				fullPath := filepath.Join(dir, e.Name())
				raw, err := os.ReadFile(fullPath)
				if err != nil {
					continue
				}
				var m TimelineLayerManifest
				if err := json.Unmarshal(raw, &m); err != nil {
					continue
				}
				var maxLSN uint64
				for _, lm := range m.Layers {
					if lm.EndLSN > maxLSN {
						maxLSN = lm.EndLSN
					}
				}
				var modTime time.Time
				if info, ierr := e.Info(); ierr == nil {
					modTime = info.ModTime()
				}
				c := &candidate{
					gen:      gen,
					epoch:    epoch,
					manifest: m,
					lastLSN:  maxLSN,
					modTime:  modTime,
				}
				if best == nil ||
					c.epoch > best.epoch ||
					(c.epoch == best.epoch && c.lastLSN > best.lastLSN) ||
					(c.epoch == best.epoch && c.lastLSN == best.lastLSN && c.modTime.After(best.modTime)) {
					best = c
				}
			}
		}

		// Scan direct tenant directory (legacy empty generation) and generation subdirectories.
		scanDir(tenantPath, "")
		subEntries, err := os.ReadDir(tenantPath)
		if err == nil {
			for _, se := range subEntries {
				if se.IsDir() {
					scanDir(filepath.Join(tenantPath, se.Name()), se.Name())
				}
			}
		}

		if best == nil {
			continue
		}
		if p.store != nil && p.store.kms != nil {
			dekPath := filepath.Join(tenantPath, "wrapped_dek.b64")
			if dekRaw, derr := os.ReadFile(dekPath); derr == nil {
				if lerr := p.store.kms.LoadWrappedDEK(tenantID, strings.TrimSpace(string(dekRaw))); lerr != nil {
					debugf("recovery", "LoadWrappedDEK failed tenant=%s err=%v", tenantID, lerr)
				}
			}
		}
		if best.gen != "" {
			p.generations[tenantID] = best.gen
		}
		tl := NewTimeline(tenantID, best.epoch, p.store, p.redoer)
		tl.setMemoryBudget(p.budget)
		if best.gen != "" {
			tl.SetGeneration(best.gen)
		}
		tl.layers = append([]LayerMeta(nil), best.manifest.Layers...)
		tl.layerSeq = best.manifest.MaxSeq
		tl.lastLSN = best.lastLSN
		tl.walLastLSN = best.lastLSN
		p.tenants[tenantID] = tl
		debugf("recovery", "recovered local tenant=%s gen=%q epoch=%d layers=%d max_seq=%d last_lsn=%s",
			tenantID, best.gen, best.epoch, len(tl.layers), tl.layerSeq, formatLSNHex(tl.lastLSN))
	}
}

// PersistTenantWrappedDEK atomically writes the KEK-wrapped tenant DEK to LayerDir/<tenantID>/wrapped_dek.b64
// so Pageserver restarts can decrypt local layers without external re-provisioning.
func (p *Pageserver) PersistTenantWrappedDEK(tenantID, wrappedB64 string) error {
	if p.cfg.LayerDir == "" || tenantID == "" || wrappedB64 == "" {
		return nil
	}
	dir := filepath.Join(p.cfg.LayerDir, tenantID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	target := filepath.Join(dir, "wrapped_dek.b64")
	tmp := target + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(wrappedB64 + "\n"); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, target)
}

// Start binds the Unix domain socket (if enabled) and begins serving air_smgr requests.
func (p *Pageserver) Start(ctx context.Context) error {
	if p.unixSrv == nil {
		return nil
	}
	return p.unixSrv.Start(ctx)
}

// Close shuts down the memory budget monitor, Unix socket server, and WAL redo worker pool.
func (p *Pageserver) Close() error {
	if p.budget != nil {
		p.budget.close()
	}
	var err error
	if p.unixSrv != nil {
		err = p.unixSrv.Close()
	}
	if c, ok := p.redoer.(io.Closer); ok {
		_ = c.Close()
	}
	return err
}

// Store returns the underlying LayerStore.
func (p *Pageserver) Store() *LayerStore {
	return p.store
}

// SetTenantGeneration registers the active immutable generation for tenantID.
// If an existing Timeline belongs to a different generation, it is detached.
func (p *Pageserver) SetTenantGeneration(tenantID, generation string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if generation == "" {
		return
	}
	if prev, ok := p.generations[tenantID]; ok && prev != generation {
		debugf("tenant", "SetTenantGeneration tenant=%s prev=%q new=%q (detaching old timeline)", tenantID, prev, generation)
		if oldTL, exists := p.tenants[tenantID]; exists && p.budget != nil {
			p.budget.addInMemBytes(-oldTL.InMemBytes())
		}
		delete(p.tenants, tenantID)
	} else {
		debugf("tenant", "SetTenantGeneration tenant=%s gen=%q", tenantID, generation)
	}
	p.generations[tenantID] = generation
	if tl, ok := p.tenants[tenantID]; ok && tl.Generation() != generation {
		if tl.Generation() == "" {
			tl.SetGeneration(generation)
		} else {
			if p.budget != nil {
				p.budget.addInMemBytes(-tl.InMemBytes())
			}
			delete(p.tenants, tenantID)
		}
	}
}

// GetTenantGeneration returns the currently registered generation for tenantID (empty if unset).
func (p *Pageserver) GetTenantGeneration(tenantID string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.generations[tenantID]
}

// GetTenantInfo implements TenantInfoProvider, returning the active generation
// and latest ingested WAL LSN for tenantID.
func (p *Pageserver) GetTenantInfo(tenantID string) (string, uint64, bool) {
	p.mu.RLock()
	tl, ok := p.tenants[tenantID]
	gen := p.generations[tenantID]
	p.mu.RUnlock()
	if !ok {
		return gen, 0, false
	}
	if gen == "" {
		gen = tl.Generation()
	}
	return gen, tl.WALLastLSN(), true
}

func (p *Pageserver) isGenerationActive(tenantID, generation string) bool {
	p.mu.RLock()
	cur, ok := p.generations[tenantID]
	p.mu.RUnlock()
	if !ok {
		// If no generation was ever registered (legacy unit tests), allow empty generation.
		return generation == ""
	}
	return cur == generation
}

// PurgeTenant removes in-memory timelines, local layer/cache files, decoded/index/disk cache entries,
// and best-effort R2 objects for (tenantID, generation).
func (p *Pageserver) PurgeTenant(ctx context.Context, tenantID, generation string) error {
	start := time.Now()
	debugf("tenant", "PurgeTenant start tenant=%s gen=%q", tenantID, generation)
	p.mu.Lock()
	curGen := p.generations[tenantID]
	if generation == "" || curGen == generation {
		if oldTL, exists := p.tenants[tenantID]; exists && p.budget != nil {
			p.budget.addInMemBytes(-oldTL.InMemBytes())
		}
		delete(p.generations, tenantID)
		delete(p.tenants, tenantID)
	} else if tl, ok := p.tenants[tenantID]; ok && tl.Generation() == generation {
		if p.budget != nil {
			p.budget.addInMemBytes(-tl.InMemBytes())
		}
		delete(p.tenants, tenantID)
	}
	p.mu.Unlock()

	var deletedR2 int
	if p.store != nil {
		if p.store.decodedChunks != nil {
			p.store.decodedChunks.InvalidateTenantGeneration(tenantID, generation)
		}
		prefix := TenantR2Prefix(tenantID, generation) + "/"
		if p.store.idxCache != nil {
			p.store.idxCache.evictPrefix(prefix)
		}
		if p.cache != nil {
			p.cache.EvictPrefix(prefix)
		}
		if p.store.s3c != nil && p.store.bucket != "" {
			var token *string
			for {
				out, err := p.store.s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
					Bucket:            aws.String(p.store.bucket),
					Prefix:            aws.String(prefix),
					ContinuationToken: token,
				})
				if err != nil {
					debugf("tenant", "PurgeTenant R2 list error tenant=%s prefix=%q err=%v", tenantID, prefix, err)
					break
				}
				for _, obj := range out.Contents {
					if obj.Key != nil {
						_, _ = p.store.s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
							Bucket: aws.String(p.store.bucket),
							Key:    obj.Key,
						})
						deletedR2++
					}
				}
				if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
					break
				}
				token = out.NextContinuationToken
			}
		}
	}

	if p.cfg.LayerDir != "" {
		if generation != "" {
			_ = os.RemoveAll(filepath.Join(p.cfg.LayerDir, tenantID, generation))
		} else {
			_ = os.RemoveAll(filepath.Join(p.cfg.LayerDir, tenantID))
		}
	}
	debugf("tenant", "PurgeTenant done tenant=%s gen=%q cur_gen=%q deleted_r2=%d dur=%s", tenantID, generation, curGen, deletedR2, time.Since(start))
	return nil
}

// EnsureTenant returns an existing Timeline for tenantID or creates one at epoch.
func (p *Pageserver) EnsureTenant(tenantID string, epoch int64) *Timeline {
	p.mu.Lock()
	defer p.mu.Unlock()
	gen := p.generations[tenantID]
	if tl, ok := p.tenants[tenantID]; ok {
		if gen == "" || tl.Generation() == gen {
			debugf("tenant", "EnsureTenant reuse tenant=%s epoch=%d gen=%q last_lsn=%s layers=%d", tenantID, epoch, tl.Generation(), formatLSNHex(tl.LastLSN()), tl.LayerCount())
			return tl
		}
		if p.budget != nil {
			p.budget.addInMemBytes(-tl.InMemBytes())
		}
	}
	tl := NewTimeline(tenantID, epoch, p.store, p.redoer)
	tl.setMemoryBudget(p.budget)
	if gen != "" {
		tl.SetGeneration(gen)
	}
	p.tenants[tenantID] = tl
	debugf("tenant", "EnsureTenant created timeline tenant=%s epoch=%d gen=%q active_tenants=%d", tenantID, epoch, gen, len(p.tenants))
	return tl
}

func (p *Pageserver) snapshotTimelines() []*Timeline {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Timeline, 0, len(p.tenants))
	for _, tl := range p.tenants {
		out = append(out, tl)
	}
	return out
}

func (p *Pageserver) dirtyTenantCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cnt := 0
	for _, tl := range p.tenants {
		if tl.InMemBytes() > 0 {
			cnt++
		}
	}
	return cnt
}

func (p *Pageserver) flushLargestDirtyTenantsAsync(limit int) {
	timelines := p.snapshotTimelines()
	type item struct {
		tl    *Timeline
		bytes int64
	}
	var dirty []item
	for _, tl := range timelines {
		if sz := tl.InMemBytes(); sz > 0 {
			dirty = append(dirty, item{tl: tl, bytes: sz})
		}
	}
	if len(dirty) == 0 {
		return
	}
	sort.Slice(dirty, func(i, j int) bool { return dirty[i].bytes > dirty[j].bytes })
	if limit > 0 && len(dirty) > limit {
		dirty = dirty[:limit]
	}
	for _, d := range dirty {
		d.tl.MaybeFlushAsync()
	}
}

// MemoryStats returns a measurable snapshot of global and per-tenant memory budgets,
// InMemoryLayer consumption, dirty page counts, cache bytes, Go heap stats, and flush counters.
func (p *Pageserver) MemoryStats() MemoryStatsSnapshot {
	timelines := p.snapshotTimelines()
	perTenantBytes := make(map[string]int64, len(timelines))
	perTenantPages := make(map[string]int, len(timelines))
	var globalInMem int64
	var totalDirtyPages int64
	for _, tl := range timelines {
		sz := tl.InMemBytes()
		dp := tl.DirtyPageCount()
		perTenantBytes[tl.tenantID] = sz
		perTenantPages[tl.tenantID] = dp
		globalInMem += sz
		totalDirtyPages += int64(dp)
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	var dcBytes, icBytes int64
	if p.store != nil {
		if dc := p.store.DecodedChunkCache(); dc != nil {
			dcBytes = dc.Stats().Bytes
		}
		if ic := p.store.idxCache; ic != nil {
			icBytes = ic.bytesUsed()
		}
	}

	var snap MemoryStatsSnapshot
	if p.budget != nil {
		soft, hard := p.budget.effectiveTenantLimits()
		snap.MemoryLimitBytes = p.budget.cfg.MemoryLimitBytes
		snap.GoMemoryLimitBytes = p.budget.cfg.GoMemoryLimitBytes
		snap.GlobalSoftInMemBytes = p.budget.cfg.GlobalSoftInMemBytes
		snap.GlobalHardInMemBytes = p.budget.cfg.GlobalHardInMemBytes
		snap.PerTenantSoftInMemBytes = soft
		snap.PerTenantHardInMemBytes = hard
		snap.FlushesCompleted = p.budget.flushesTotal.Load()
		snap.FlushedBytesTotal = p.budget.flushedBytes.Load()
		snap.BackpressureWaits = p.budget.bpWaits.Load()
		snap.BackpressureRejects = p.budget.bpRejects.Load()
		snap.PressureEvictions = p.budget.pressureEvict.Load()
	}
	snap.GlobalInMemBytes = globalInMem
	snap.TotalDirtyPages = totalDirtyPages
	snap.TenantInMemBytes = perTenantBytes
	snap.TenantDirtyPages = perTenantPages
	snap.DecodedChunkCacheBytes = dcBytes
	snap.IndexCacheBytes = icBytes
	snap.HeapAllocBytes = ms.HeapAlloc
	snap.HeapInuseBytes = ms.HeapInuse
	snap.HeapIdleBytes = ms.HeapIdle
	snap.HeapReleasedBytes = ms.HeapReleased
	snap.TotalAllocBytes = ms.TotalAlloc
	snap.NumGC = ms.NumGC
	snap.GCPauseTotalNs = ms.PauseTotalNs
	return snap
}

// FlushTenant flushes the tenant's InMemoryLayer to an immutable L0 layer (e.g. on SuspendTenant).
func (p *Pageserver) FlushTenant(ctx context.Context, tenantID string) (*LayerMeta, error) {
	p.mu.RLock()
	tl, ok := p.tenants[tenantID]
	p.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	return tl.FlushInMemory(ctx)
}

// FlushAll flushes all active tenant InMemoryLayers to immutable L0 layers and returns the first error.
func (p *Pageserver) FlushAll(ctx context.Context) error {
	p.mu.RLock()
	timelines := make([]*Timeline, 0, len(p.tenants))
	for _, tl := range p.tenants {
		timelines = append(timelines, tl)
	}
	p.mu.RUnlock()

	var firstErr error
	for _, tl := range timelines {
		if _, err := tl.FlushInMemory(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (p *Pageserver) getTimeline(tenantID string) (*Timeline, error) {
	p.mu.RLock()
	tl, ok := p.tenants[tenantID]
	p.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return tl, nil
}

// GetPageAtLSN implements Service.
func (p *Pageserver) GetPageAtLSN(ctx context.Context, tenantID string, key Key, lsn uint64) ([]byte, error) {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return nil, err
	}
	return tl.GetPageAtLSN(ctx, key, lsn)
}

// GetPageBatch implements Service.
func (p *Pageserver) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return nil, 0, err
	}
	return tl.GetPageBatch(ctx, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, lsn)
}

// GetRelSize implements Service.
func (p *Pageserver) GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return 0, err
	}
	return tl.GetRelSize(ctx, spcOID, dbOID, relNumber, forkNum, lsn)
}

// GetRelExists implements Service.
func (p *Pageserver) GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error) {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return false, nil
	}
	return tl.GetRelExists(ctx, spcOID, dbOID, relNumber, forkNum, lsn)
}

// GetDBSize implements Service.
func (p *Pageserver) GetModifiedBlocksSince(_ context.Context, tenantID string, sinceLSN uint64) (ModifiedBlocksResult, error) {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return ModifiedBlocksResult{}, err
	}
	return tl.GetModifiedBlocksSince(sinceLSN), nil
}
func (p *Pageserver) GetDBSize(ctx context.Context, tenantID string, spcOID, dbOID uint32, lsn uint64) (uint64, error) {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return 0, err
	}
	return tl.GetDBSize(ctx, spcOID, dbOID, lsn)
}

// PutPageBatch implements Service.
func (p *Pageserver) PutPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64, pages []byte) error {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return err
	}
	return tl.PutPageBatch(ctx, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, lsn, pages)
}

// UnlinkRel implements Service.
func (p *Pageserver) UnlinkRel(_ context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) error {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return err
	}
	tl.UnlinkRel(spcOID, dbOID, relNumber, forkNum, lsn)
	return nil
}

// BootstrapTenantFromPGData imports an existing PGDATA base/ and global/ directory into an Image layer at initLSN
// using bounded-memory streaming file traversal and V2 encoding.
func (p *Pageserver) BootstrapTenantFromPGData(ctx context.Context, tenantID string, epoch int64, pgDataDir string, initLSN uint64) (*LayerMeta, error) {
	start := time.Now()
	if initLSN == 0 {
		initLSN = 1
	}
	debugf("bootstrap", "BootstrapTenantFromPGData start tenant=%s epoch=%d pgdata=%q init_lsn=%s", tenantID, epoch, pgDataDir, formatLSNHex(initLSN))
	tl := p.EnsureTenant(tenantID, epoch)

	type pgDataFile struct {
		path    string
		spcOID  uint32
		dbOID   uint32
		relNum  uint32
		forkNum uint8
		segNum  uint32
	}
	var files []pgDataFile

	// Import global/ (tablespace 1664, db 0) and base/<dbOid>/ (tablespace 1663).
	dirs := []struct {
		root   string
		spcOID uint32
		hasDB  bool
	}{
		{filepath.Join(pgDataDir, "global"), 1664, false},
		{filepath.Join(pgDataDir, "base"), 1663, true},
	}

	for _, d := range dirs {
		if _, err := os.Stat(d.root); err != nil {
			continue
		}
		err := filepath.WalkDir(d.root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() {
				return walkErr
			}
			relPath, err := filepath.Rel(d.root, path)
			if err != nil {
				return err
			}
			var dbOID uint32
			fileName := entry.Name()
			if d.hasDB {
				parts := strings.Split(relPath, string(filepath.Separator))
				if len(parts) != 2 {
					return nil
				}
				v, err := strconv.ParseUint(parts[0], 10, 32)
				if err != nil {
					return nil
				}
				dbOID = uint32(v)
				fileName = parts[1]
			}
			relNum, forkNum, segNum, ok := parsePGDataRelFile(fileName)
			if !ok {
				return nil
			}
			files = append(files, pgDataFile{
				path:    path,
				spcOID:  d.spcOID,
				dbOID:   dbOID,
				relNum:  relNum,
				forkNum: forkNum,
				segNum:  segNum,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if len(files) == 0 {
		return nil, nil
	}

	// Sort in exact RelBlockKey order: (SpcOID, DbOID, RelNumber, ForkNum, segNum).
	sort.Slice(files, func(i, j int) bool {
		if files[i].spcOID != files[j].spcOID {
			return files[i].spcOID < files[j].spcOID
		}
		if files[i].dbOID != files[j].dbOID {
			return files[i].dbOID < files[j].dbOID
		}
		if files[i].relNum != files[j].relNum {
			return files[i].relNum < files[j].relNum
		}
		if files[i].forkNum != files[j].forkNum {
			return files[i].forkNum < files[j].forkNum
		}
		return files[i].segNum < files[j].segNum
	})

	gen := tl.Generation()
	fileName := fmt.Sprintf("img_bootstrap_%016x.layer", initLSN)
	logicalKey := fmt.Sprintf("%s/timeline_%d/layers/%s", TenantR2Prefix(tenantID, gen), epoch, fileName)
	meta := LayerMeta{
		TenantID:   tenantID,
		Generation: gen,
		Epoch:      epoch,
		Kind:       LayerKindImage,
		FileName:   fileName,
		R2Key:      logicalKey,
		AADKey:     logicalKey,
		StartLSN:   initLSN,
		EndLSN:     initLSN,
	}

	var wroteAny bool
	relSizes := make(map[Key]uint32)
	var sizeOrder []Key
	written, err := p.store.writeLayerFromStream(ctx, meta, func(sw *v2StreamWriter) error {
		const segBlocks = 131072
		buf := make([]byte, PageSize)

		for _, rf := range files {
			f, err := os.Open(rf.path)
			if err != nil {
				return err
			}
			var blkIdx uint32
			for {
				n, rerr := io.ReadFull(f, buf)
				if n == PageSize {
					absBlk := rf.segNum*segBlocks + blkIdx
					key := RelBlockKey(rf.spcOID, rf.dbOID, rf.relNum, rf.forkNum, absBlk)
					page := make([]byte, PageSize)
					copy(page, buf)
					if err := sw.Add(LayerRecord{
						Key: key,
						Delta: WALDelta{
							LSN:       initLSN,
							Flags:     EntryFlagPageImage,
							PageImage: page,
						},
					}); err != nil {
						_ = f.Close()
						return err
					}
					wroteAny = true
					blkIdx++
				}
				if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
					break
				}
				if rerr != nil {
					_ = f.Close()
					return rerr
				}
			}
			_ = f.Close()

			sizeKey := RelSizeKey(rf.spcOID, rf.dbOID, rf.relNum, rf.forkNum)
			totalBlocks := rf.segNum*segBlocks + blkIdx
			if _, seen := relSizes[sizeKey]; !seen {
				sizeOrder = append(sizeOrder, sizeKey)
				relSizes[sizeKey] = totalBlocks
			} else if totalBlocks > relSizes[sizeKey] {
				relSizes[sizeKey] = totalBlocks
			}
			if blkIdx > 0 {
				wroteAny = true
			}
		}

		// KeyKindRelSize (2) > KeyKindRelBlock (1), and files were already sorted by (SpcOID, DbOID, RelNumber, ForkNum).
		for _, sizeKey := range sizeOrder {
			if err := sw.Add(LayerRecord{
				Key: sizeKey,
				Delta: WALDelta{
					LSN:     initLSN,
					Flags:   EntryFlagRelSize,
					RelSize: relSizes[sizeKey],
				},
			}); err != nil {
				return err
			}
		}

		// KeyKindSLRU (4) > KeyKindRelSize (2): seed pg_xact (CLOG) segments so native MVCC
		// readers have baseline commit statuses for bootstrapped/pre-existing transactions.
		seedSLRU := func(dirName string, kind uint32) error {
		xactDir := filepath.Join(pgDataDir, dirName)
		if xactEntries, err := os.ReadDir(xactDir); err == nil {
			type xactSeg struct {
				segNo uint32
				path  string
			}
			var segs []xactSeg
			for _, ent := range xactEntries {
				if ent.IsDir() {
					continue
				}
				v, perr := strconv.ParseUint(ent.Name(), 16, 32)
				if perr != nil {
					continue
				}
				segs = append(segs, xactSeg{segNo: uint32(v), path: filepath.Join(xactDir, ent.Name())})
			}
			sort.Slice(segs, func(i, j int) bool { return segs[i].segNo < segs[j].segNo })
			const slruPagesPerSeg = 32 // 256 KiB / 8 KiB
			for _, seg := range segs {
				f, oerr := os.Open(seg.path)
				if oerr != nil {
					continue
				}
				var blkIdx uint32
				for {
					n, rerr := io.ReadFull(f, buf)
					if n == PageSize {
						page := make([]byte, PageSize)
						copy(page, buf)
						slruKey := Key{Kind: KeyKindSLRU, SpcOID: kind, BlockNum: seg.segNo*slruPagesPerSeg + blkIdx}
						tl.SeedSLRUPage(slruKey, page)
						if err := sw.Add(LayerRecord{
							Key: slruKey,
							Delta: WALDelta{
								LSN:       initLSN,
								Flags:     EntryFlagPageImage,
								PageImage: page,
							},
						}); err != nil {
							_ = f.Close()
							return err
						}
						wroteAny = true
						blkIdx++
					}
					if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
						break
					}
					if rerr != nil {
						_ = f.Close()
						return rerr
					}
				}
				_ = f.Close()
			}
		}
			return nil
		}
		for _, d := range []struct {
			dir  string
			kind uint32
		}{{"pg_xact", 0}, {"pg_subtrans", 1}, {"pg_multixact/offsets", SLRUMultiXactOffsetsKind}, {"pg_multixact/members", SLRUMultiXactMembersKind}} {
			if err := seedSLRU(d.dir, d.kind); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !wroteAny {
		debugf("bootstrap", "BootstrapTenantFromPGData tenant=%s no pages written dur=%s", tenantID, time.Since(start))
		return nil, nil
	}
	if err := tl.PublishNewLayer(ctx, written); err != nil {
		debugf("bootstrap", "BootstrapTenantFromPGData publish error tenant=%s err=%v dur=%s", tenantID, err, time.Since(start))
		return nil, err
	}
	tl.mu.Lock()
	for sizeKey, nb := range relSizes {
		tl.relSizes[sizeKey] = nb
	}
	tl.mu.Unlock()
	debugf("bootstrap", "BootstrapTenantFromPGData done tenant=%s files=%d rels=%d layer=%s size=%d sha256=%s end_lsn=%s dur=%s",
		tenantID, len(files), len(relSizes), written.FileName, written.FileSize, shortHash(written.SHA256), formatLSNHex(written.EndLSN), time.Since(start))
	return &written, nil
}

func parsePGDataRelFile(name string) (relNum uint32, forkNum uint8, segNum uint32, ok bool) {
	base := name
	if idx := strings.IndexByte(base, '.'); idx >= 0 {
		segStr := base[idx+1:]
		base = base[:idx]
		v, err := strconv.ParseUint(segStr, 10, 32)
		if err != nil {
			return 0, 0, 0, false
		}
		segNum = uint32(v)
	}
	switch {
	case strings.HasSuffix(base, "_fsm"):
		forkNum = 1
		base = strings.TrimSuffix(base, "_fsm")
	case strings.HasSuffix(base, "_vm"):
		forkNum = 2
		base = strings.TrimSuffix(base, "_vm")
	case strings.HasSuffix(base, "_init"):
		forkNum = 3
		base = strings.TrimSuffix(base, "_init")
	default:
		forkNum = 0
	}
	v, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, 0, 0, false
	}
	return uint32(v), forkNum, segNum, true
}
