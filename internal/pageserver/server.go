package pageserver

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

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
}

// Pageserver manages multi-tenant Timelines and serves air_smgr over a Unix socket and/or gRPC.
type Pageserver struct {
	cfg     Config
	store   *LayerStore
	cache   *DiskCache
	redoer  WALRedoer
	unixSrv *UnixServer

	mu          sync.RWMutex
	tenants     map[string]*Timeline
	generations map[string]string
}

// New creates a Pageserver instance. Call Start(ctx) to bind the Unix socket if enabled.
func New(cfg Config) (*Pageserver, error) {
	if !cfg.DisableUnixSocket && cfg.SocketPath == "" {
		cfg.SocketPath = "/var/lib/air/pageserver.sock"
	}
	if cfg.CacheMaxBytes <= 0 {
		cfg.CacheMaxBytes = 256 * 1024 * 1024
	}
	var cache *DiskCache
	if cfg.CacheDir != "" {
		var err error
		cache, err = NewDiskCache(cfg.CacheDir, cfg.CacheMaxBytes)
		if err != nil {
			return nil, err
		}
	}
	store := NewLayerStore(cfg.LayerDir, cfg.S3Client, cfg.R2Bucket, cfg.KMS, cache)
	redoer := NewDefaultWALRedoer(cfg.WalRedoBin)
	ps := &Pageserver{
		cfg:         cfg,
		store:       store,
		cache:       cache,
		redoer:      redoer,
		tenants:     make(map[string]*Timeline),
		generations: make(map[string]string),
	}
	store.SetGenerationChecker(ps.isGenerationActive)
	if !cfg.DisableUnixSocket && cfg.SocketPath != "" {
		ps.unixSrv = NewUnixServer(cfg.SocketPath, ps)
	}
	return ps, nil
}

// Start binds the Unix domain socket (if enabled) and begins serving air_smgr requests.
func (p *Pageserver) Start(ctx context.Context) error {
	if p.unixSrv == nil {
		return nil
	}
	return p.unixSrv.Start(ctx)
}

// Close shuts down the Unix socket server and WAL redo worker pool.
func (p *Pageserver) Close() error {
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
		delete(p.tenants, tenantID)
	}
	p.generations[tenantID] = generation
	if tl, ok := p.tenants[tenantID]; ok && tl.Generation() != generation {
		if tl.Generation() == "" {
			tl.SetGeneration(generation)
		} else {
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
	return gen, tl.LastLSN(), true
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
	p.mu.Lock()
	curGen := p.generations[tenantID]
	if generation == "" || curGen == generation {
		delete(p.generations, tenantID)
		delete(p.tenants, tenantID)
	} else if tl, ok := p.tenants[tenantID]; ok && tl.Generation() == generation {
		delete(p.tenants, tenantID)
	}
	p.mu.Unlock()

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
					break
				}
				for _, obj := range out.Contents {
					if obj.Key != nil {
						_, _ = p.store.s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
							Bucket: aws.String(p.store.bucket),
							Key:    obj.Key,
						})
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
	return nil
}

// EnsureTenant returns an existing Timeline for tenantID or creates one at epoch.
func (p *Pageserver) EnsureTenant(tenantID string, epoch int64) *Timeline {
	p.mu.Lock()
	defer p.mu.Unlock()
	gen := p.generations[tenantID]
	if tl, ok := p.tenants[tenantID]; ok {
		if gen == "" || tl.Generation() == gen {
			return tl
		}
	}
	tl := NewTimeline(tenantID, epoch, p.store, p.redoer)
	if gen != "" {
		tl.SetGeneration(gen)
	}
	p.tenants[tenantID] = tl
	return tl
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
	if initLSN == 0 {
		initLSN = 1
	}
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
		return nil, nil
	}
	if err := tl.PublishNewLayer(ctx, written); err != nil {
		return nil, err
	}
	tl.mu.Lock()
	for sizeKey, nb := range relSizes {
		tl.relSizes[sizeKey] = nb
	}
	tl.mu.Unlock()
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
