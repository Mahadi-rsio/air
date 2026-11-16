package pageserver

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// Close shuts down the Unix socket server.
func (p *Pageserver) Close() error {
	return p.unixSrv.Close()
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

// BootstrapTenantFromPGData imports an existing PGDATA base/ and global/ directory into an Image layer at initLSN.
func (p *Pageserver) BootstrapTenantFromPGData(ctx context.Context, tenantID string, epoch int64, pgDataDir string, initLSN uint64) (*LayerMeta, error) {
	if initLSN == 0 {
		initLSN = 1
	}
	tl := p.EnsureTenant(tenantID, epoch)
	var recs []LayerRecord

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
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			const segBlocks = 131072
			var blkIdx uint32
			buf := make([]byte, PageSize)
			for {
				n, rerr := io.ReadFull(f, buf)
				if n == PageSize {
					absBlk := segNum*segBlocks + blkIdx
					key := RelBlockKey(d.spcOID, dbOID, relNum, forkNum, absBlk)
					page := make([]byte, PageSize)
					copy(page, buf)
					recs = append(recs, LayerRecord{
						Key: key,
						Delta: WALDelta{
							LSN:       initLSN,
							Flags:     EntryFlagPageImage,
							PageImage: page,
						},
					})
					blkIdx++
				}
				if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
					break
				}
				if rerr != nil {
					return rerr
				}
			}
			if segNum == 0 {
				sizeKey := RelSizeKey(d.spcOID, dbOID, relNum, forkNum)
				tl.mu.Lock()
				tl.relSizes[sizeKey] = blkIdx
				tl.mu.Unlock()
				recs = append(recs, LayerRecord{
					Key: sizeKey,
					Delta: WALDelta{
						LSN:     initLSN,
						Flags:   EntryFlagRelSize,
						RelSize: blkIdx,
					},
				})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if len(recs) == 0 {
		return nil, nil
	}
	fileName := fmt.Sprintf("img_bootstrap_%016x.layer", initLSN)
	r2Key := fmt.Sprintf("tenants/%s/timeline_%d/layers/%s", tenantID, epoch, fileName)
	meta := LayerMeta{
		TenantID: tenantID,
		Epoch:    epoch,
		Kind:     LayerKindImage,
		FileName: fileName,
		R2Key:    r2Key,
		StartLSN: initLSN,
		EndLSN:   initLSN,
	}
	written, err := p.store.WriteLayer(ctx, meta, recs)
	if err != nil {
		return nil, err
	}
	tl.AddLayer(written)
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
