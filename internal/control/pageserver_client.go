package control

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxControlPSGRPCMsgBytes = 64 * 1024 * 1024

// PageserverClient abstracts in-process *pageserver.Pageserver and remote gRPC air-pageserver.
type PageserverClient interface {
	SetTenantGeneration(ctx context.Context, tenantID, generation string)
	EnsureTenant(ctx context.Context, tenantID string, epoch int64, generation string, loadFromR2 bool, wrappedDEK string) (uint64, error)
	FlushTenant(ctx context.Context, tenantID string) (*pageserver.LayerMeta, error)
	FlushAll(ctx context.Context) error
	PurgeTenant(ctx context.Context, tenantID, generation string) error
	BootstrapTenantFromPGData(ctx context.Context, tenantID string, epoch int64, pgDataDir string, initLSN uint64) (*pageserver.LayerMeta, error)
	HasMetadataSkeleton(ctx context.Context, tenantID string) bool
	SaveMetadataSkeleton(ctx context.Context, tenantID string, epoch int64, pgDataDir string) error
	RestoreMetadataSkeleton(ctx context.Context, tenantID string, epoch int64, pgDataDir string) error
	NewWALReceiver(dsn, tenantID string, epoch int64, generation string, startLSN uint64) *pageserver.WALReceiver
	Close() error
}

// LocalPageserverClient adapts an in-process *pageserver.Pageserver to PageserverClient.
type LocalPageserverClient struct {
	ps *pageserver.Pageserver
}

// NewLocalPageserverClient wraps ps as a PageserverClient.
func NewLocalPageserverClient(ps *pageserver.Pageserver) *LocalPageserverClient {
	return &LocalPageserverClient{ps: ps}
}

func (c *LocalPageserverClient) SetTenantGeneration(_ context.Context, tenantID, generation string) {
	c.ps.SetTenantGeneration(tenantID, generation)
}

func (c *LocalPageserverClient) EnsureTenant(ctx context.Context, tenantID string, epoch int64, generation string, loadFromR2 bool, _ string) (uint64, error) {
	if generation != "" {
		c.ps.SetTenantGeneration(tenantID, generation)
	}
	tl := c.ps.EnsureTenant(tenantID, epoch)
	if loadFromR2 && tl.LastLSN() == 0 {
		_ = tl.LoadLayersFromR2(ctx)
	}
	return tl.LastLSN(), nil
}

func (c *LocalPageserverClient) FlushTenant(ctx context.Context, tenantID string) (*pageserver.LayerMeta, error) {
	return c.ps.FlushTenant(ctx, tenantID)
}

func (c *LocalPageserverClient) FlushAll(ctx context.Context) error {
	return c.ps.FlushAll(ctx)
}

func (c *LocalPageserverClient) PurgeTenant(ctx context.Context, tenantID, generation string) error {
	return c.ps.PurgeTenant(ctx, tenantID, generation)
}

func (c *LocalPageserverClient) BootstrapTenantFromPGData(ctx context.Context, tenantID string, epoch int64, pgDataDir string, initLSN uint64) (*pageserver.LayerMeta, error) {
	return c.ps.BootstrapTenantFromPGData(ctx, tenantID, epoch, pgDataDir, initLSN)
}

func (c *LocalPageserverClient) HasMetadataSkeleton(ctx context.Context, tenantID string) bool {
	return c.ps.HasMetadataSkeleton(ctx, tenantID)
}

func (c *LocalPageserverClient) SaveMetadataSkeleton(ctx context.Context, tenantID string, epoch int64, pgDataDir string) error {
	_, err := c.ps.SaveMetadataSkeleton(ctx, tenantID, epoch, pgDataDir)
	return err
}

func (c *LocalPageserverClient) RestoreMetadataSkeleton(ctx context.Context, tenantID string, epoch int64, pgDataDir string) error {
	_, err := c.ps.RestoreMetadataSkeleton(ctx, tenantID, epoch, pgDataDir)
	return err
}

func (c *LocalPageserverClient) NewWALReceiver(dsn, tenantID string, epoch int64, generation string, startLSN uint64) *pageserver.WALReceiver {
	if generation != "" {
		c.ps.SetTenantGeneration(tenantID, generation)
	}
	tl := c.ps.EnsureTenant(tenantID, epoch)
	if startLSN == 0 {
		startLSN = tl.LastLSN()
	}
	return pageserver.NewWALReceiver(dsn, tl, startLSN)
}

func (c *LocalPageserverClient) Close() error {
	return c.ps.Close()
}

// GRPCPageserverClient connects air-server to a standalone air-pageserver over gRPC.
type GRPCPageserverClient struct {
	addr          string
	internalToken string
	conn          *grpc.ClientConn
	client        pb.PageserverServiceClient
}

// NewGRPCPageserverClient dials a standalone air-pageserver gRPC endpoint.
func NewGRPCPageserverClient(addr, internalToken string) (*GRPCPageserverClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("pageserver gRPC address is required")
	}
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxControlPSGRPCMsgBytes),
			grpc.MaxCallSendMsgSize(maxControlPSGRPCMsgBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("dial pageserver gRPC %s: %w", addr, err)
	}
	return &GRPCPageserverClient{
		addr:          addr,
		internalToken: internalToken,
		conn:          conn,
		client:        pb.NewPageserverServiceClient(conn),
	}, nil
}

func (c *GRPCPageserverClient) withAuth(ctx context.Context) context.Context {
	if c.internalToken == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", c.internalToken)
}

func mapRemotePSErr(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("%w: %v", pageserver.ErrNotFound, err)
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %v", pageserver.ErrStaleGeneration, err)
	default:
		return err
	}
}

func (c *GRPCPageserverClient) SetTenantGeneration(ctx context.Context, tenantID, generation string) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	_, _ = c.client.SetTenantGeneration(rctx, &pb.SetTenantGenerationRequest{
		TenantId:   tenantID,
		Generation: generation,
	})
}

func (c *GRPCPageserverClient) EnsureTenant(ctx context.Context, tenantID string, epoch int64, generation string, loadFromR2 bool, wrappedDEK string) (uint64, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 15*time.Second)
	defer cancel()
	resp, err := c.client.EnsureTenant(rctx, &pb.EnsureTenantRequest{
		TenantId:   tenantID,
		Epoch:      epoch,
		Generation: generation,
		LoadFromR2: loadFromR2,
		WrappedDek: wrappedDEK,
	})
	if err != nil {
		return 0, mapRemotePSErr(err)
	}
	return resp.GetLastLsn(), nil
}

func (c *GRPCPageserverClient) FlushTenant(ctx context.Context, tenantID string) (*pageserver.LayerMeta, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 60*time.Second)
	defer cancel()
	resp, err := c.client.FlushTenant(rctx, &pb.PageserverTenantRequest{TenantId: tenantID})
	if err != nil {
		return nil, mapRemotePSErr(err)
	}
	if !resp.GetFlushed() {
		return nil, nil
	}
	return &pageserver.LayerMeta{
		TenantID: tenantID,
		FileName: resp.GetFileName(),
		StartLSN: resp.GetStartLsn(),
		EndLSN:   resp.GetEndLsn(),
	}, nil
}

func (c *GRPCPageserverClient) FlushAll(ctx context.Context) error {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 60*time.Second)
	defer cancel()
	_, err := c.client.FlushAll(rctx, &pb.PageserverEmpty{})
	return mapRemotePSErr(err)
}

func (c *GRPCPageserverClient) PurgeTenant(ctx context.Context, tenantID, generation string) error {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 30*time.Second)
	defer cancel()
	_, err := c.client.PurgeTenant(rctx, &pb.PurgeTenantRequest{
		TenantId:   tenantID,
		Generation: generation,
	})
	return mapRemotePSErr(err)
}

func (c *GRPCPageserverClient) HasMetadataSkeleton(ctx context.Context, tenantID string) bool {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	resp, err := c.client.HasMetadataSkeleton(rctx, &pb.PageserverTenantRequest{TenantId: tenantID})
	if err != nil {
		return false
	}
	return resp.GetValue()
}

func (c *GRPCPageserverClient) SaveMetadataSkeleton(ctx context.Context, tenantID string, epoch int64, pgDataDir string) error {
	archive, _, err := pageserver.PackMetadataSkeleton(pgDataDir)
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 30*time.Second)
	defer cancel()
	_, err = c.client.SaveMetadataSkeleton(rctx, &pb.SaveSkeletonRequest{
		TenantId:  tenantID,
		Epoch:     epoch,
		PgDataDir: pgDataDir,
		Archive:   archive,
	})
	return mapRemotePSErr(err)
}

func (c *GRPCPageserverClient) RestoreMetadataSkeleton(ctx context.Context, tenantID string, epoch int64, pgDataDir string) error {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 30*time.Second)
	defer cancel()
	resp, err := c.client.RestoreMetadataSkeleton(rctx, &pb.RestoreSkeletonRequest{
		TenantId:      tenantID,
		Epoch:         epoch,
		PgDataDir:     pgDataDir,
		ReturnArchive: true,
	})
	if err != nil {
		return mapRemotePSErr(err)
	}
	if _, err := pageserver.UnpackMetadataSkeleton(resp.GetArchive(), pgDataDir); err != nil {
		return err
	}
	return nil
}

func (c *GRPCPageserverClient) BootstrapTenantFromPGData(ctx context.Context, tenantID string, epoch int64, pgDataDir string, initLSN uint64) (*pageserver.LayerMeta, error) {
	if initLSN == 0 {
		initLSN = 1
	}
	if _, err := c.EnsureTenant(ctx, tenantID, epoch, "", false, ""); err != nil {
		return nil, err
	}

	type pgDataFile struct {
		path    string
		spcOID  uint32
		dbOID   uint32
		relNum  uint32
		forkNum uint8
		segNum  uint32
	}
	var files []pgDataFile
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
			relNum, forkNum, segNum, ok := parsePGDataRelFileName(fileName)
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

	const segBlocks = 131072
	const batchBlocks = 128
	buf := make([]byte, pageserver.PageSize)
	relSizes := make(map[[4]uint32]uint32)

	for _, rf := range files {
		f, err := os.Open(rf.path)
		if err != nil {
			return nil, err
		}
		var blkIdx uint32
		var batch []byte
		var batchStart uint32
		var batchCount uint32

		flushBatch := func() error {
			if batchCount == 0 {
				return nil
			}
			rctx, cancel := context.WithTimeout(c.withAuth(ctx), 30*time.Second)
			defer cancel()
			_, err := c.client.PutPageBatch(rctx, &pb.PutPageBatchRequest{
				TenantId:   tenantID,
				SpcOid:     rf.spcOID,
				DbOid:      rf.dbOID,
				RelNumber:  rf.relNum,
				ForkNum:    uint32(rf.forkNum),
				StartBlock: batchStart,
				Nblocks:    batchCount,
				Lsn:        initLSN,
				Pages:      batch,
			})
			batch = batch[:0]
			batchCount = 0
			return mapRemotePSErr(err)
		}

		for {
			n, rerr := io.ReadFull(f, buf)
			if n == pageserver.PageSize {
				absBlk := rf.segNum*segBlocks + blkIdx
				if batchCount == 0 {
					batchStart = absBlk
				}
				batch = append(batch, buf...)
				batchCount++
				blkIdx++
				if batchCount >= batchBlocks {
					if err := flushBatch(); err != nil {
						_ = f.Close()
						return nil, err
					}
				}
			}
			if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
				break
			}
			if rerr != nil {
				_ = f.Close()
				return nil, rerr
			}
		}
		_ = f.Close()
		if err := flushBatch(); err != nil {
			return nil, err
		}

		k := [4]uint32{rf.spcOID, rf.dbOID, rf.relNum, uint32(rf.forkNum)}
		totalBlocks := rf.segNum*segBlocks + blkIdx
		if totalBlocks > relSizes[k] {
			relSizes[k] = totalBlocks
		}
	}

	if len(relSizes) > 0 {
		entries := make([]*pb.RelSizeEntry, 0, len(relSizes))
		for k, nb := range relSizes {
			entries = append(entries, &pb.RelSizeEntry{
				SpcOid:    k[0],
				DbOid:     k[1],
				RelNumber: k[2],
				ForkNum:   k[3],
				Nblocks:   nb,
				Lsn:       initLSN,
			})
		}
		rctx, cancel := context.WithTimeout(c.withAuth(ctx), 15*time.Second)
		_, err := c.client.SetRelSizes(rctx, &pb.SetRelSizesRequest{
			TenantId: tenantID,
			Entries:  entries,
		})
		cancel()
		if err != nil {
			return nil, mapRemotePSErr(err)
		}
	}

	var slruEntries []*pb.SLRUPageEntry
	const slruPagesPerSeg = 32
	for _, d := range []struct {
		dir  string
		kind uint32
	}{
		{"pg_xact", 0},
		{"pg_subtrans", 1},
		{"pg_multixact/offsets", pageserver.SLRUMultiXactOffsetsKind},
		{"pg_multixact/members", pageserver.SLRUMultiXactMembersKind},
	} {
		xactDir := filepath.Join(pgDataDir, d.dir)
		ents, err := os.ReadDir(xactDir)
		if err != nil {
			continue
		}
		type segFile struct {
			segNo uint32
			path  string
		}
		var segs []segFile
		for _, ent := range ents {
			if ent.IsDir() {
				continue
			}
			v, perr := strconv.ParseUint(ent.Name(), 16, 32)
			if perr != nil {
				continue
			}
			segs = append(segs, segFile{segNo: uint32(v), path: filepath.Join(xactDir, ent.Name())})
		}
		sort.Slice(segs, func(i, j int) bool { return segs[i].segNo < segs[j].segNo })
		for _, seg := range segs {
			f, oerr := os.Open(seg.path)
			if oerr != nil {
				continue
			}
			var blkIdx uint32
			for {
				n, rerr := io.ReadFull(f, buf)
				if n == pageserver.PageSize {
					page := make([]byte, pageserver.PageSize)
					copy(page, buf)
					slruEntries = append(slruEntries, &pb.SLRUPageEntry{
						Kind:     d.kind,
						BlockNum: seg.segNo*slruPagesPerSeg + blkIdx,
						Page:     page,
					})
					blkIdx++
				}
				if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
					break
				}
				if rerr != nil {
					break
				}
			}
			_ = f.Close()
		}
	}
	if len(slruEntries) > 0 {
		rctx, cancel := context.WithTimeout(c.withAuth(ctx), 15*time.Second)
		_, err := c.client.SeedSLRUPages(rctx, &pb.SeedSLRUPagesRequest{
			TenantId: tenantID,
			Entries:  slruEntries,
		})
		cancel()
		if err != nil {
			return nil, mapRemotePSErr(err)
		}
	}

	lm, err := c.FlushTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return lm, nil
}

type grpcWALSink struct {
	client     *GRPCPageserverClient
	tenantID   string
	epoch      int64
	generation string
}

func (s *grpcWALSink) IngestWALBatch(ctx context.Context, recs []*pageserver.DecodedWALRecord, sawCheckpoint bool) (uint64, error) {
	pbRecs := make([]*pb.WALRecordMsg, 0, len(recs))
	for _, rec := range recs {
		blks := make([]*pb.WALRecordBlockMsg, 0, len(rec.Blocks))
		for _, b := range rec.Blocks {
			blks = append(blks, &pb.WALRecordBlockMsg{
				BlockId:      uint32(b.BlockID),
				SpcOid:       b.Rel.SpcOID,
				DbOid:        b.Rel.DbOID,
				RelNumber:    b.Rel.RelNumber,
				ForkNum:      uint32(b.ForkNum),
				BlockNum:     b.BlockNum,
				WillInit:     b.WillInit,
				HasImage:     b.HasImage,
				HasData:      b.HasData,
				ImageApply:   b.ImageApply,
				ImageInfo:    uint32(b.ImageInfo),
				HoleOffset:   uint32(b.HoleOffset),
				HoleLength:   uint32(b.HoleLength),
				ImagePayload: b.ImagePayload,
				BlockData:    b.BlockData,
			})
		}
		pbRecs = append(pbRecs, &pb.WALRecordMsg{
			Lsn:       rec.LSN,
			EndLsn:    rec.EndLSN,
			TotalLen:  rec.TotalLen,
			Xid:       rec.XID,
			PrevLsn:   rec.PrevLSN,
			Info:      uint32(rec.Info),
			Rmid:      uint32(rec.Rmid),
			Crc:       rec.CRC,
			MainData:  rec.MainData,
			Blocks:    blks,
			RawRecord: rec.RawRecord,
		})
	}
	rctx, cancel := context.WithTimeout(s.client.withAuth(ctx), 30*time.Second)
	defer cancel()
	resp, err := s.client.client.IngestWALRecords(rctx, &pb.IngestWALRecordsRequest{
		TenantId:      s.tenantID,
		Epoch:         s.epoch,
		Generation:    s.generation,
		Records:       pbRecs,
		FlushIfNeeded: true,
	})
	if err != nil {
		return 0, mapRemotePSErr(err)
	}
	if sawCheckpoint {
		_, _ = s.client.FlushTenant(ctx, s.tenantID)
	}
	return resp.GetLastLsn(), nil
}

func (c *GRPCPageserverClient) NewWALReceiver(dsn, tenantID string, epoch int64, generation string, startLSN uint64) *pageserver.WALReceiver {
	return pageserver.NewRemoteWALReceiver(dsn, &grpcWALSink{
		client:     c,
		tenantID:   tenantID,
		epoch:      epoch,
		generation: generation,
	}, startLSN)
}

func (c *GRPCPageserverClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func parsePGDataRelFileName(name string) (relNum uint32, forkNum uint8, segNum uint32, ok bool) {
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
