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
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config configures the in-process AIR Pageserver.
type Config struct {
	SocketPath    string
	LayerDir      string
	CacheDir      string
	CacheMaxBytes int64
	WalRedoBin    string
	S3Client      *s3.Client
	R2Bucket      string
	KMS           *crypto.EnvelopeKMS
}

// Pageserver manages multi-tenant Timelines and serves air_smgr over a Unix socket.
type Pageserver struct {
	cfg     Config
	store   *LayerStore
	cache   *DiskCache
	redoer  WALRedoer
	unixSrv *UnixServer

	mu        sync.RWMutex
	tenants   map[string]*Timeline
}

// New creates a Pageserver instance. Call Start(ctx) to bind the Unix socket.
func New(cfg Config) (*Pageserver, error) {
	if cfg.SocketPath == "" {
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
		cfg:     cfg,
		store:   store,
		cache:   cache,
		redoer:  redoer,
		tenants: make(map[string]*Timeline),
	}
	ps.unixSrv = NewUnixServer(cfg.SocketPath, ps)
	return ps, nil
}

// Start binds the Unix domain socket and begins serving air_smgr requests.
func (p *Pageserver) Start(ctx context.Context) error {
	return p.unixSrv.Start(ctx)
}

// Close shuts down the Unix socket server and WAL redo worker pool.
func (p *Pageserver) Close() error {
	err := p.unixSrv.Close()
	if c, ok := p.redoer.(io.Closer); ok {
		_ = c.Close()
	}
	return err
}

// EnsureTenant returns an existing Timeline for tenantID or creates one at epoch.
func (p *Pageserver) EnsureTenant(tenantID string, epoch int64) *Timeline {
	p.mu.Lock()
	defer p.mu.Unlock()
	if tl, ok := p.tenants[tenantID]; ok {
		return tl
	}
	tl := NewTimeline(tenantID, epoch, p.store, p.redoer)
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

	fileName := fmt.Sprintf("img_bootstrap_%016x.layer", initLSN)
	logicalKey := fmt.Sprintf("tenants/%s/timeline_%d/layers/%s", tenantID, epoch, fileName)
	meta := LayerMeta{
		TenantID: tenantID,
		Epoch:    epoch,
		Kind:     LayerKindImage,
		FileName: fileName,
		R2Key:    logicalKey,
		AADKey:   logicalKey,
		StartLSN: initLSN,
		EndLSN:   initLSN,
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
