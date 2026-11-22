package pageserver

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"

	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxGRPCMessageBytes = 64 * 1024 * 1024

// ActiveTenantCount returns the number of currently loaded tenant timelines.
func (p *Pageserver) ActiveTenantCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.tenants)
}

// SetRelSizes records relation sizes in the tenant's in-memory layer and size cache.
func (p *Pageserver) SetRelSizes(tenantID string, entries []RelSizeEntry) error {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return err
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for _, e := range entries {
		sizeKey := RelSizeKey(e.SpcOID, e.DbOID, e.RelNumber, e.ForkNum)
		tl.relSizes[sizeKey] = e.NBlocks
		tl.inMem.Put(sizeKey, WALDelta{
			LSN:     e.LSN,
			Flags:   EntryFlagRelSize,
			RelSize: e.NBlocks,
		})
		if e.LSN > tl.lastLSN {
			tl.lastLSN = e.LSN
		}
	}
	return nil
}

// RelSizeEntry describes a relation fork's block count at an LSN.
type RelSizeEntry struct {
	SpcOID    uint32
	DbOID     uint32
	RelNumber uint32
	ForkNum   uint8
	NBlocks   uint32
	LSN       uint64
}

// SLRUPageSeed describes a single 8 KiB SLRU page to seed in a tenant timeline.
type SLRUPageSeed struct {
	Kind     uint32
	BlockNum uint32
	Page     []byte
}

// SeedSLRUPages seeds SLRU pages (pg_xact, pg_subtrans, pg_multixact) on a tenant timeline.
func (p *Pageserver) SeedSLRUPages(tenantID string, entries []SLRUPageSeed) error {
	tl, err := p.getTimeline(tenantID)
	if err != nil {
		return err
	}
	for _, e := range entries {
		key := Key{Kind: KeyKindSLRU, SpcOID: e.Kind, BlockNum: e.BlockNum}
		tl.SeedSLRUPage(key, e.Page)
	}
	return nil
}

// IngestWALRecords applies a batch of decoded PG17 WAL records to a tenant timeline.
func (p *Pageserver) IngestWALRecords(
	ctx context.Context,
	tenantID string,
	epoch int64,
	generation string,
	recs []*DecodedWALRecord,
	flushIfNeeded bool,
) (int, uint64, error) {
	if generation != "" {
		p.SetTenantGeneration(tenantID, generation)
	}
	tl := p.EnsureTenant(tenantID, epoch)
	sawCheckpoint := false
	for _, rec := range recs {
		for _, blk := range rec.Blocks {
			tl.EnsureRelSizeSeeded(ctx, blk.Rel.SpcOID, blk.Rel.DbOID, blk.Rel.RelNumber, blk.ForkNum)
		}
		tl.ApplyWALRecord(rec)
		if rec.Rmid == RmXLogID {
			op := rec.Info & XLogInfoOpMask
			if op == 0x00 || op == 0x10 {
				sawCheckpoint = true
			}
		}
	}
	if sawCheckpoint {
		if _, err := tl.FlushInMemory(ctx); err != nil {
			return len(recs), tl.LastLSN(), err
		}
	} else if flushIfNeeded {
		if err := tl.EnforceInMemBudget(ctx); err != nil {
			return len(recs), tl.LastLSN(), err
		}
	}
	return len(recs), tl.LastLSN(), nil
}

type grpcServer struct {
	pb.UnimplementedPageserverServiceServer
	ps            *Pageserver
	internalToken string
}

// NewGRPCServer constructs a gRPC server exposing PageserverService for air-server and air-read-engine.
func NewGRPCServer(ps *Pageserver, internalToken string) *grpc.Server {
	s := &grpcServer{
		ps:            ps,
		internalToken: internalToken,
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessageBytes),
		grpc.MaxSendMsgSize(maxGRPCMessageBytes),
		grpc.UnaryInterceptor(s.authUnaryInterceptor),
	)
	pb.RegisterPageserverServiceServer(srv, s)
	return srv
}

func (s *grpcServer) authorize(ctx context.Context) error {
	if s.internalToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	for _, key := range []string{"x-air-internal-token", "x-internal-token"} {
		for _, val := range md.Get(key) {
			if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(val)), []byte(s.internalToken)) == 1 {
				return nil
			}
		}
	}
	return status.Error(codes.Unauthenticated, "unauthorized")
}

func (s *grpcServer) authUnaryInterceptor(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func mapPageserverGRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrFenced), errors.Is(err, ErrStaleGeneration):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *grpcServer) Health(_ context.Context, _ *pb.PageserverEmpty) (*pb.PageserverHealthResponse, error) {
	return &pb.PageserverHealthResponse{
		Status:        "ok",
		Service:       "air-pageserver",
		ActiveTenants: int32(s.ps.ActiveTenantCount()),
	}, nil
}

func (s *grpcServer) GetTenantInfo(_ context.Context, req *pb.PageserverTenantRequest) (*pb.PageserverTenantInfoResponse, error) {
	gen, lastLSN, exists := s.ps.GetTenantInfo(req.GetTenantId())
	return &pb.PageserverTenantInfoResponse{
		TenantId:   req.GetTenantId(),
		Generation: gen,
		LastLsn:    lastLSN,
		Exists:     exists,
	}, nil
}

func (s *grpcServer) GetPageAtLSN(ctx context.Context, req *pb.GetPageAtLSNRequest) (*pb.GetPageResponse, error) {
	k := req.GetKey()
	key := Key{
		Kind:      KeyKind(k.GetKind()),
		ForkNum:   uint8(k.GetForkNum()),
		Reserved:  uint16(k.GetReserved()),
		SpcOID:    k.GetSpcOid(),
		DbOID:     k.GetDbOid(),
		RelNumber: k.GetRelNumber(),
		BlockNum:  k.GetBlockNum(),
		Extra:     k.GetExtra(),
	}
	page, err := s.ps.GetPageAtLSN(ctx, req.GetTenantId(), key, req.GetLsn())
	if errors.Is(err, ErrNotFound) {
		return &pb.GetPageResponse{Found: false}, nil
	}
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.GetPageResponse{Page: page, Found: true}, nil
}

func (s *grpcServer) GetPageBatch(ctx context.Context, req *pb.GetPageBatchRequest) (*pb.GetPageBatchResponse, error) {
	pages, nblocks, err := s.ps.GetPageBatch(
		ctx,
		req.GetTenantId(),
		req.GetSpcOid(),
		req.GetDbOid(),
		req.GetRelNumber(),
		uint8(req.GetForkNum()),
		req.GetStartBlock(),
		req.GetNblocks(),
		req.GetLsn(),
	)
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.GetPageBatchResponse{
		Pages:           pages,
		NblocksReturned: nblocks,
	}, nil
}

func (s *grpcServer) GetRelSize(ctx context.Context, req *pb.GetRelRequest) (*pb.GetRelSizeResponse, error) {
	nb, err := s.ps.GetRelSize(
		ctx,
		req.GetTenantId(),
		req.GetSpcOid(),
		req.GetDbOid(),
		req.GetRelNumber(),
		uint8(req.GetForkNum()),
		req.GetLsn(),
	)
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.GetRelSizeResponse{Nblocks: nb}, nil
}

func (s *grpcServer) GetRelExists(ctx context.Context, req *pb.GetRelRequest) (*pb.GetRelExistsResponse, error) {
	exists, err := s.ps.GetRelExists(
		ctx,
		req.GetTenantId(),
		req.GetSpcOid(),
		req.GetDbOid(),
		req.GetRelNumber(),
		uint8(req.GetForkNum()),
		req.GetLsn(),
	)
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.GetRelExistsResponse{Exists: exists}, nil
}

func (s *grpcServer) GetDBSize(ctx context.Context, req *pb.GetDBSizeRequest) (*pb.GetDBSizeResponse, error) {
	sz, err := s.ps.GetDBSize(ctx, req.GetTenantId(), req.GetSpcOid(), req.GetDbOid(), req.GetLsn())
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.GetDBSizeResponse{SizeBytes: sz}, nil
}

func (s *grpcServer) PutPageBatch(ctx context.Context, req *pb.PutPageBatchRequest) (*pb.PageserverStatusResponse, error) {
	err := s.ps.PutPageBatch(
		ctx,
		req.GetTenantId(),
		req.GetSpcOid(),
		req.GetDbOid(),
		req.GetRelNumber(),
		uint8(req.GetForkNum()),
		req.GetStartBlock(),
		req.GetNblocks(),
		req.GetLsn(),
		req.GetPages(),
	)
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "ok", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) UnlinkRel(ctx context.Context, req *pb.UnlinkRelRequest) (*pb.PageserverStatusResponse, error) {
	err := s.ps.UnlinkRel(
		ctx,
		req.GetTenantId(),
		req.GetSpcOid(),
		req.GetDbOid(),
		req.GetRelNumber(),
		uint8(req.GetForkNum()),
		req.GetLsn(),
	)
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "ok", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) GetSLRUPage(ctx context.Context, req *pb.GetSLRUPageRequest) (*pb.GetPageResponse, error) {
	key := Key{
		Kind:     KeyKindSLRU,
		SpcOID:   req.GetKind(),
		BlockNum: req.GetBlockNum(),
	}
	page, err := s.ps.GetPageAtLSN(ctx, req.GetTenantId(), key, req.GetLsn())
	if errors.Is(err, ErrNotFound) {
		return &pb.GetPageResponse{Found: false}, nil
	}
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.GetPageResponse{Page: page, Found: true}, nil
}

func (s *grpcServer) GetModifiedBlocksSince(ctx context.Context, req *pb.GetModifiedBlocksRequest) (*pb.GetModifiedBlocksResponse, error) {
	res, err := s.ps.GetModifiedBlocksSince(ctx, req.GetTenantId(), req.GetSinceLsn())
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	events := make([]*pb.ModifiedBlockEventMsg, 0, len(res.Events))
	for _, ev := range res.Events {
		events = append(events, &pb.ModifiedBlockEventMsg{
			SpcOid:    ev.SpcOID,
			DbOid:     ev.DbOID,
			RelNumber: ev.RelNumber,
			ForkNum:   uint32(ev.ForkNum),
			BlockNum:  ev.BlockNum,
			Lsn:       ev.LSN,
			IsRelWide: ev.IsRelWide,
		})
	}
	return &pb.GetModifiedBlocksResponse{
		CurrentLsn:    res.CurrentLSN,
		CatalogLsn:    res.CatalogLSN,
		ResetRequired: res.ResetRequired,
		Events:        events,
	}, nil
}

func (s *grpcServer) SetTenantGeneration(_ context.Context, req *pb.SetTenantGenerationRequest) (*pb.PageserverStatusResponse, error) {
	s.ps.SetTenantGeneration(req.GetTenantId(), req.GetGeneration())
	return &pb.PageserverStatusResponse{Status: "ok", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) EnsureTenant(ctx context.Context, req *pb.EnsureTenantRequest) (*pb.EnsureTenantResponse, error) {
	if req.GetWrappedDek() != "" && s.ps.store != nil && s.ps.store.kms != nil {
		_ = s.ps.store.kms.LoadWrappedDEK(req.GetTenantId(), req.GetWrappedDek())
	}
	if req.GetGeneration() != "" {
		s.ps.SetTenantGeneration(req.GetTenantId(), req.GetGeneration())
	}
	tl := s.ps.EnsureTenant(req.GetTenantId(), req.GetEpoch())
	if req.GetLoadFromR2() && tl.LastLSN() == 0 {
		_ = tl.LoadLayersFromR2(ctx)
	}
	return &pb.EnsureTenantResponse{
		TenantId:   req.GetTenantId(),
		LastLsn:    tl.LastLSN(),
		Generation: tl.Generation(),
	}, nil
}

func (s *grpcServer) FlushTenant(ctx context.Context, req *pb.PageserverTenantRequest) (*pb.FlushTenantResponse, error) {
	lm, err := s.ps.FlushTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	resp := &pb.FlushTenantResponse{
		TenantId: req.GetTenantId(),
		Flushed:  lm != nil,
	}
	if lm != nil {
		resp.FileName = lm.FileName
		resp.StartLsn = lm.StartLSN
		resp.EndLsn = lm.EndLSN
	}
	return resp, nil
}

func (s *grpcServer) FlushAll(ctx context.Context, _ *pb.PageserverEmpty) (*pb.PageserverStatusResponse, error) {
	if err := s.ps.FlushAll(ctx); err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "flushed"}, nil
}

func (s *grpcServer) PurgeTenant(ctx context.Context, req *pb.PurgeTenantRequest) (*pb.PageserverStatusResponse, error) {
	if err := s.ps.PurgeTenant(ctx, req.GetTenantId(), req.GetGeneration()); err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "purged", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) IngestWALRecords(ctx context.Context, req *pb.IngestWALRecordsRequest) (*pb.IngestWALRecordsResponse, error) {
	recs := make([]*DecodedWALRecord, 0, len(req.GetRecords()))
	for _, r := range req.GetRecords() {
		blks := make([]WALBlockRef, 0, len(r.GetBlocks()))
		for _, b := range r.GetBlocks() {
			blks = append(blks, WALBlockRef{
				BlockID: uint8(b.GetBlockId()),
				Rel: RelFileLocator{
					SpcOID:    b.GetSpcOid(),
					DbOID:     b.GetDbOid(),
					RelNumber: b.GetRelNumber(),
				},
				ForkNum:      uint8(b.GetForkNum()),
				BlockNum:     b.GetBlockNum(),
				WillInit:     b.GetWillInit(),
				HasImage:     b.GetHasImage(),
				HasData:      b.GetHasData(),
				ImageApply:   b.GetImageApply(),
				ImageInfo:    uint8(b.GetImageInfo()),
				HoleOffset:   uint16(b.GetHoleOffset()),
				HoleLength:   uint16(b.GetHoleLength()),
				ImagePayload: b.GetImagePayload(),
				BlockData:    b.GetBlockData(),
			})
		}
		recs = append(recs, &DecodedWALRecord{
			LSN:       r.GetLsn(),
			EndLSN:    r.GetEndLsn(),
			TotalLen:  r.GetTotalLen(),
			XID:       r.GetXid(),
			PrevLSN:   r.GetPrevLsn(),
			Info:      uint8(r.GetInfo()),
			Rmid:      uint8(r.GetRmid()),
			CRC:       r.GetCrc(),
			Blocks:    blks,
			MainData:  r.GetMainData(),
			RawRecord: r.GetRawRecord(),
		})
	}
	applied, lastLSN, err := s.ps.IngestWALRecords(
		ctx,
		req.GetTenantId(),
		req.GetEpoch(),
		req.GetGeneration(),
		recs,
		req.GetFlushIfNeeded(),
	)
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.IngestWALRecordsResponse{
		Applied: int32(applied),
		LastLsn: lastLSN,
	}, nil
}

func (s *grpcServer) BootstrapTenantFromPGData(ctx context.Context, req *pb.BootstrapTenantRequest) (*pb.BootstrapTenantResponse, error) {
	lm, err := s.ps.BootstrapTenantFromPGData(ctx, req.GetTenantId(), req.GetEpoch(), req.GetPgDataDir(), req.GetInitLsn())
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	resp := &pb.BootstrapTenantResponse{
		TenantId:     req.GetTenantId(),
		Bootstrapped: lm != nil,
		InitLsn:      req.GetInitLsn(),
	}
	if lm != nil {
		resp.InitLsn = lm.EndLSN
	}
	return resp, nil
}

func (s *grpcServer) HasMetadataSkeleton(ctx context.Context, req *pb.PageserverTenantRequest) (*pb.PageserverBoolResponse, error) {
	return &pb.PageserverBoolResponse{
		Value: s.ps.HasMetadataSkeleton(ctx, req.GetTenantId()),
	}, nil
}

func (s *grpcServer) SaveMetadataSkeleton(ctx context.Context, req *pb.SaveSkeletonRequest) (*pb.PageserverStatusResponse, error) {
	if _, err := s.ps.SaveMetadataSkeleton(ctx, req.GetTenantId(), req.GetEpoch(), req.GetPgDataDir()); err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "saved", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) RestoreMetadataSkeleton(ctx context.Context, req *pb.RestoreSkeletonRequest) (*pb.PageserverStatusResponse, error) {
	if _, err := s.ps.RestoreMetadataSkeleton(ctx, req.GetTenantId(), req.GetEpoch(), req.GetPgDataDir()); err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "restored", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) SetRelSizes(_ context.Context, req *pb.SetRelSizesRequest) (*pb.PageserverStatusResponse, error) {
	entries := make([]RelSizeEntry, 0, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		entries = append(entries, RelSizeEntry{
			SpcOID:    e.GetSpcOid(),
			DbOID:     e.GetDbOid(),
			RelNumber: e.GetRelNumber(),
			ForkNum:   uint8(e.GetForkNum()),
			NBlocks:   e.GetNblocks(),
			LSN:       e.GetLsn(),
		})
	}
	if err := s.ps.SetRelSizes(req.GetTenantId(), entries); err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "ok", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) SeedSLRUPages(_ context.Context, req *pb.SeedSLRUPagesRequest) (*pb.PageserverStatusResponse, error) {
	entries := make([]SLRUPageSeed, 0, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		entries = append(entries, SLRUPageSeed{
			Kind:     e.GetKind(),
			BlockNum: e.GetBlockNum(),
			Page:     e.GetPage(),
		})
	}
	if err := s.ps.SeedSLRUPages(req.GetTenantId(), entries); err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "ok", TenantId: req.GetTenantId()}, nil
}
