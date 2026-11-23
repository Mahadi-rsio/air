package pageserver

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxGRPCMessageBytes = 64 * 1024 * 1024

// TenantSummary describes a single tenant namespace registered in the pageserver.
type TenantSummary struct {
	TenantID   string `json:"tenant_id"`
	Generation string `json:"generation,omitempty"`
	LastLSN    uint64 `json:"last_lsn"`
}

// ActiveTenantCount returns the number of currently loaded tenant timelines.
func (p *Pageserver) ActiveTenantCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.tenants)
}

// ListTenants returns sorted summaries of all active tenant namespaces.
func (p *Pageserver) ListTenants() []TenantSummary {
	p.mu.RLock()
	ids := make([]string, 0, len(p.tenants))
	for tid := range p.tenants {
		ids = append(ids, tid)
	}
	p.mu.RUnlock()
	sort.Strings(ids)
	out := make([]TenantSummary, 0, len(ids))
	for _, tid := range ids {
		gen, lastLSN, ok := p.GetTenantInfo(tid)
		if ok {
			out = append(out, TenantSummary{
				TenantID:   tid,
				Generation: gen,
				LastLSN:    lastLSN,
			})
		}
	}
	return out
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
		tl.putInMemLocked(sizeKey, WALDelta{
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
	start := time.Now()
	if generation != "" {
		p.SetTenantGeneration(tenantID, generation)
	}
	tl := p.EnsureTenant(tenantID, epoch)
	if flushIfNeeded {
		if err := tl.EnforceInMemBudget(ctx); err != nil {
			return 0, tl.LastLSN(), err
		}
	}
	sawCheckpoint := false
	var firstLSN, lastRecLSN uint64
	var totalBlocks, fpiBlocks int
	for i, rec := range recs {
		if i == 0 {
			firstLSN = rec.LSN
		}
		lastRecLSN = rec.EndLSN
		totalBlocks += len(rec.Blocks)
		for _, blk := range rec.Blocks {
			if blk.HasImage {
				fpiBlocks++
			}
			tl.EnsureRelSizeSeeded(ctx, blk.Rel.SpcOID, blk.Rel.DbOID, blk.Rel.RelNumber, blk.ForkNum)
		}
		tl.ApplyWALRecord(rec)
		if rec.Rmid == RmXLogID {
			op := rec.Info & XLogInfoOpMask
			if op == 0x00 || op == 0x10 {
				sawCheckpoint = true
			}
		}
		if flushIfNeeded && (i+1)%32 == 0 {
			if err := tl.EnforceInMemBudget(ctx); err != nil {
				return i + 1, tl.LastLSN(), err
			}
		}
	}
	debugf("wal-ingest", "tenant=%s epoch=%d gen=%q recs=%d blocks=%d fpi=%d lsn=%s..%s rmids=[%s] checkpoint=%v flush_if_needed=%v inmem_bytes=%d dur=%s",
		tenantID, epoch, generation, len(recs), totalBlocks, fpiBlocks, formatLSNHex(firstLSN), formatLSNHex(lastRecLSN), summarizeRMIDs(recs), sawCheckpoint, flushIfNeeded, tl.InMemBytes(), time.Since(start))
	if sawCheckpoint {
		debugf("wal-ingest", "tenant=%s checkpoint detected, triggering FlushInMemory", tenantID)
		if _, err := tl.FlushInMemory(ctx); err != nil {
			debugf("wal-ingest", "tenant=%s checkpoint FlushInMemory error=%v", tenantID, err)
			return len(recs), tl.LastLSN(), err
		}
	} else if flushIfNeeded {
		if err := tl.EnforceInMemBudget(ctx); err != nil {
			debugf("wal-ingest", "tenant=%s EnforceInMemBudget error=%v", tenantID, err)
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
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	if err := s.authorize(ctx); err != nil {
		debugf("grpc", "method=%s auth=DENIED err=%v dur=%s", info.FullMethod, err, time.Since(start))
		return nil, err
	}
	resp, err := handler(ctx, req)
	if err != nil {
		debugf("grpc", "method=%s status=ERR err=%v dur=%s", info.FullMethod, err, time.Since(start))
	} else if info.FullMethod != "/air.v1.PageserverService/Health" {
		debugf("grpc", "method=%s status=OK dur=%s", info.FullMethod, time.Since(start))
	}
	return resp, err
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
	case errors.Is(err, ErrMemoryBudgetExhausted), errors.Is(err, ErrReadQueueFull):
		return status.Error(codes.ResourceExhausted, err.Error())
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

func (s *grpcServer) ListTenants(_ context.Context, _ *pb.PageserverEmpty) (*pb.PageserverListTenantsResponse, error) {
	list := s.ps.ListTenants()
	tenants := make([]*pb.PageserverTenantInfoResponse, 0, len(list))
	for _, t := range list {
		tenants = append(tenants, &pb.PageserverTenantInfoResponse{
			TenantId:   t.TenantID,
			Generation: t.Generation,
			LastLsn:    t.LastLSN,
			Exists:     true,
		})
	}
	return &pb.PageserverListTenantsResponse{Tenants: tenants}, nil
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
		debugf("grpc-read", "GetPageAtLSN tenant=%s key=%s req_lsn=%s found=false", req.GetTenantId(), key, formatLSNHex(req.GetLsn()))
		return &pb.GetPageResponse{Found: false}, nil
	}
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	debugf("grpc-read", "GetPageAtLSN tenant=%s key=%s req_lsn=%s found=true bytes=%d", req.GetTenantId(), key, formatLSNHex(req.GetLsn()), len(page))
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
	debugf("grpc-read", "GetPageBatch tenant=%s rel=%d/%d/%d.%d blk=%d+%d req_lsn=%s returned=%d bytes=%d",
		req.GetTenantId(), req.GetSpcOid(), req.GetDbOid(), req.GetRelNumber(), req.GetForkNum(), req.GetStartBlock(), req.GetNblocks(), formatLSNHex(req.GetLsn()), nblocks, len(pages))
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
	debugf("grpc-read", "GetRelSize tenant=%s rel=%d/%d/%d.%d req_lsn=%s nblocks=%d",
		req.GetTenantId(), req.GetSpcOid(), req.GetDbOid(), req.GetRelNumber(), req.GetForkNum(), formatLSNHex(req.GetLsn()), nb)
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
	debugf("grpc-read", "GetRelExists tenant=%s rel=%d/%d/%d.%d req_lsn=%s exists=%v",
		req.GetTenantId(), req.GetSpcOid(), req.GetDbOid(), req.GetRelNumber(), req.GetForkNum(), formatLSNHex(req.GetLsn()), exists)
	return &pb.GetRelExistsResponse{Exists: exists}, nil
}

func (s *grpcServer) GetDBSize(ctx context.Context, req *pb.GetDBSizeRequest) (*pb.GetDBSizeResponse, error) {
	sz, err := s.ps.GetDBSize(ctx, req.GetTenantId(), req.GetSpcOid(), req.GetDbOid(), req.GetLsn())
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	debugf("grpc-read", "GetDBSize tenant=%s db=%d/%d req_lsn=%s size_bytes=%d",
		req.GetTenantId(), req.GetSpcOid(), req.GetDbOid(), formatLSNHex(req.GetLsn()), sz)
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
	debugf("grpc-write", "PutPageBatch tenant=%s rel=%d/%d/%d.%d blk=%d+%d lsn=%s bytes=%d",
		req.GetTenantId(), req.GetSpcOid(), req.GetDbOid(), req.GetRelNumber(), req.GetForkNum(), req.GetStartBlock(), req.GetNblocks(), formatLSNHex(req.GetLsn()), len(req.GetPages()))
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
	debugf("grpc-write", "UnlinkRel tenant=%s rel=%d/%d/%d.%d lsn=%s",
		req.GetTenantId(), req.GetSpcOid(), req.GetDbOid(), req.GetRelNumber(), req.GetForkNum(), formatLSNHex(req.GetLsn()))
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
		debugf("grpc-read", "GetSLRUPage tenant=%s kind=%d blk=%d req_lsn=%s found=false", req.GetTenantId(), req.GetKind(), req.GetBlockNum(), formatLSNHex(req.GetLsn()))
		return &pb.GetPageResponse{Found: false}, nil
	}
	if err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	debugf("grpc-read", "GetSLRUPage tenant=%s kind=%d blk=%d req_lsn=%s found=true", req.GetTenantId(), req.GetKind(), req.GetBlockNum(), formatLSNHex(req.GetLsn()))
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
	debugf("grpc-read", "GetModifiedBlocksSince tenant=%s since_lsn=%s current_lsn=%s catalog_lsn=%s reset=%v events=%d",
		req.GetTenantId(), formatLSNHex(req.GetSinceLsn()), formatLSNHex(res.CurrentLSN), formatLSNHex(res.CatalogLSN), res.ResetRequired, len(events))
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
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if s.ps.store != nil && s.ps.store.kms != nil {
		var wrappedDEK string
		if req.GetWrappedDek() != "" {
			if err := s.ps.store.kms.LoadWrappedDEK(req.GetTenantId(), req.GetWrappedDek()); err != nil {
				return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("load wrapped DEK for %s: %v", req.GetTenantId(), err))
			}
			wrappedDEK = req.GetWrappedDek()
		} else {
			w, err := s.ps.store.kms.EnsureTenantDEK(req.GetTenantId())
			if err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("ensure DEK for %s: %v", req.GetTenantId(), err))
			}
			wrappedDEK = w
		}
		if err := s.ps.PersistTenantWrappedDEK(req.GetTenantId(), wrappedDEK); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("persist wrapped DEK for %s: %v", req.GetTenantId(), err))
		}
	}
	if req.GetGeneration() != "" {
		s.ps.SetTenantGeneration(req.GetTenantId(), req.GetGeneration())
	}
	epoch := req.GetEpoch()
	if epoch <= 0 {
		epoch = 1
	}
	tl := s.ps.EnsureTenant(req.GetTenantId(), epoch)
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
	if len(req.GetArchive()) > 0 {
		if _, err := s.ps.SaveMetadataSkeletonArchive(ctx, req.GetTenantId(), req.GetEpoch(), req.GetArchive(), nil); err != nil {
			return nil, mapPageserverGRPCError(err)
		}
		return &pb.PageserverStatusResponse{Status: "saved", TenantId: req.GetTenantId()}, nil
	}
	if _, err := s.ps.SaveMetadataSkeleton(ctx, req.GetTenantId(), req.GetEpoch(), req.GetPgDataDir()); err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.PageserverStatusResponse{Status: "saved", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) RestoreMetadataSkeleton(ctx context.Context, req *pb.RestoreSkeletonRequest) (*pb.RestoreSkeletonResponse, error) {
	if req.GetReturnArchive() {
		archive, _, err := s.ps.LoadMetadataSkeletonArchive(ctx, req.GetTenantId(), req.GetEpoch())
		if err != nil {
			return nil, mapPageserverGRPCError(err)
		}
		return &pb.RestoreSkeletonResponse{
			Status:   "restored",
			TenantId: req.GetTenantId(),
			Archive:  archive,
		}, nil
	}
	if _, err := s.ps.RestoreMetadataSkeleton(ctx, req.GetTenantId(), req.GetEpoch(), req.GetPgDataDir()); err != nil {
		return nil, mapPageserverGRPCError(err)
	}
	return &pb.RestoreSkeletonResponse{Status: "restored", TenantId: req.GetTenantId()}, nil
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
	debugf("grpc-write", "SetRelSizes tenant=%s entries=%d", req.GetTenantId(), len(entries))
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
	debugf("grpc-write", "SeedSLRUPages tenant=%s entries=%d", req.GetTenantId(), len(entries))
	return &pb.PageserverStatusResponse{Status: "ok", TenantId: req.GetTenantId()}, nil
}
