package readadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxPageserverGRPCMsgBytes = 64 * 1024 * 1024

// PageserverGRPCConfig configures the gRPC client to a standalone AIR Pageserver.
type PageserverGRPCConfig struct {
	Addr           string
	InternalToken  string
	RequestTimeout time.Duration
}

// PageserverGRPCBackend implements StorageBackend and SnapshotProvider by
// communicating with a standalone AIR Pageserver over gRPC.
type PageserverGRPCBackend struct {
	cfg    PageserverGRPCConfig
	conn   *grpc.ClientConn
	client pb.PageserverServiceClient

	requestsTotal atomic.Uint64
	failuresTotal atomic.Uint64
	timeoutsTotal atomic.Uint64
	inFlight      atomic.Int64

	genMu    sync.RWMutex
	genCache map[string]cachedTenantInfo
}

// NewPageserverGRPCBackend dials a standalone AIR Pageserver over gRPC.
func NewPageserverGRPCBackend(cfg PageserverGRPCConfig) (*PageserverGRPCBackend, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("readadapter: pageserver gRPC address is required")
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	conn, err := grpc.NewClient(
		cfg.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxPageserverGRPCMsgBytes),
			grpc.MaxCallSendMsgSize(maxPageserverGRPCMsgBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("readadapter: dial pageserver gRPC %s: %w", cfg.Addr, err)
	}
	return &PageserverGRPCBackend{
		cfg:      cfg,
		conn:     conn,
		client:   pb.NewPageserverServiceClient(conn),
		genCache: make(map[string]cachedTenantInfo),
	}, nil
}

// Close closes the underlying gRPC client connection.
func (b *PageserverGRPCBackend) Close() error {
	if b.conn == nil {
		return nil
	}
	return b.conn.Close()
}

// Stats returns live request, in-flight, and failure counters.
func (b *PageserverGRPCBackend) Stats() PageserverSocketStats {
	return PageserverSocketStats{
		ActiveConns:   1,
		InFlight:      b.inFlight.Load(),
		RequestsTotal: b.requestsTotal.Load(),
		FailuresTotal: b.failuresTotal.Load(),
		TimeoutsTotal: b.timeoutsTotal.Load(),
	}
}

func (b *PageserverGRPCBackend) withContext(ctx context.Context) (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc = func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && b.cfg.RequestTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, b.cfg.RequestTimeout)
	}
	if b.cfg.InternalToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", b.cfg.InternalToken)
	}
	return ctx, cancel
}

func (b *PageserverGRPCBackend) mapErr(err error) error {
	if err == nil {
		return nil
	}
	b.failuresTotal.Add(1)
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		b.timeoutsTotal.Add(1)
	}
	switch status.Code(err) {
	case codes.NotFound:
		return ErrNotFound
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %v", pageserver.ErrFenced, err)
	default:
		return err
	}
}

// GetTenantInfo queries the active generation and latest ingested WAL LSN for tenantID.
func (b *PageserverGRPCBackend) GetTenantInfo(ctx context.Context, tenantID string) (string, uint64, error) {
	start := time.Now()
	b.requestsTotal.Add(1)
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)

	rctx, cancel := b.withContext(ctx)
	defer cancel()
	resp, err := b.client.GetTenantInfo(rctx, &pb.PageserverTenantRequest{TenantId: tenantID})
	if err != nil {
		mapped := b.mapErr(err)
		debugf("ps-grpc", "GetTenantInfo tenant=%s err=%v dur=%s", tenantID, mapped, time.Since(start))
		return "", 0, mapped
	}
	if !resp.GetExists() {
		debugf("ps-grpc", "GetTenantInfo tenant=%s exists=false dur=%s", tenantID, time.Since(start))
		return "", 0, ErrNotFound
	}
	b.genMu.Lock()
	b.genCache[tenantID] = cachedTenantInfo{
		generation: resp.GetGeneration(),
		lastLSN:    resp.GetLastLsn(),
		fetchedAt:  time.Now(),
	}
	b.genMu.Unlock()
	debugf("ps-grpc", "GetTenantInfo tenant=%s gen=%s last_lsn=%s dur=%s",
		tenantID, resp.GetGeneration(), formatLSNHex(resp.GetLastLsn()), time.Since(start))
	return resp.GetGeneration(), resp.GetLastLsn(), nil
}

// GetModifiedBlocksSince implements pageserver.ModifiedBlocksProvider over gRPC.
func (b *PageserverGRPCBackend) GetModifiedBlocksSince(ctx context.Context, tenantID string, sinceLSN uint64) (pageserver.ModifiedBlocksResult, error) {
	start := time.Now()
	b.requestsTotal.Add(1)
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)

	rctx, cancel := b.withContext(ctx)
	defer cancel()
	resp, err := b.client.GetModifiedBlocksSince(rctx, &pb.GetModifiedBlocksRequest{
		TenantId: tenantID,
		SinceLsn: sinceLSN,
	})
	if err != nil {
		mapped := b.mapErr(err)
		debugf("ps-grpc", "GetModifiedBlocksSince tenant=%s since=%s err=%v dur=%s",
			tenantID, formatLSNHex(sinceLSN), mapped, time.Since(start))
		return pageserver.ModifiedBlocksResult{}, mapped
	}
	events := make([]pageserver.ModifiedBlockEvent, 0, len(resp.GetEvents()))
	for _, ev := range resp.GetEvents() {
		events = append(events, pageserver.ModifiedBlockEvent{
			SpcOID:    ev.GetSpcOid(),
			DbOID:     ev.GetDbOid(),
			RelNumber: ev.GetRelNumber(),
			ForkNum:   uint8(ev.GetForkNum()),
			BlockNum:  ev.GetBlockNum(),
			LSN:       ev.GetLsn(),
			IsRelWide: ev.GetIsRelWide(),
		})
	}
	debugf("ps-grpc", "GetModifiedBlocksSince tenant=%s since=%s -> current=%s catalog=%s reset=%v events=%d dur=%s",
		tenantID, formatLSNHex(sinceLSN), formatLSNHex(resp.GetCurrentLsn()), formatLSNHex(resp.GetCatalogLsn()),
		resp.GetResetRequired(), len(events), time.Since(start))
	return pageserver.ModifiedBlocksResult{
		CurrentLSN:    resp.GetCurrentLsn(),
		CatalogLSN:    resp.GetCatalogLsn(),
		ResetRequired: resp.GetResetRequired(),
		Events:        events,
	}, nil
}

// GetTenantGeneration implements StorageBackend.
func (b *PageserverGRPCBackend) GetTenantGeneration(tenantID string) string {
	b.genMu.RLock()
	if c, ok := b.genCache[tenantID]; ok && time.Since(c.fetchedAt) < 2*time.Second {
		b.genMu.RUnlock()
		return c.generation
	}
	b.genMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gen, _, err := b.GetTenantInfo(ctx, tenantID)
	if err != nil {
		return ""
	}
	return gen
}

// CurrentSnapshot implements SnapshotProvider.
func (b *PageserverGRPCBackend) CurrentSnapshot(ctx context.Context, tenantID, generation string) (SnapshotInfo, error) {
	gen, lastLSN, err := b.GetTenantInfo(ctx, tenantID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return SnapshotInfo{}, fmt.Errorf("%w: tenant %s not found in pageserver", ErrNotFound, tenantID)
		}
		return SnapshotInfo{}, err
	}
	if generation != "" && gen != "" && gen != generation {
		return SnapshotInfo{}, fmt.Errorf("%w: pageserver generation %q != %q", ErrStaleGeneration, gen, generation)
	}
	if lastLSN == 0 {
		lastLSN = ^uint64(0)
	}
	return SnapshotInfo{
		LSN: lastLSN,
	}, nil
}

// GetPageAtLSN implements StorageBackend.
func (b *PageserverGRPCBackend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	start := time.Now()
	b.requestsTotal.Add(1)
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)

	rctx, cancel := b.withContext(ctx)
	defer cancel()

	if key.Kind == pageserver.KeyKindSLRU {
		resp, err := b.client.GetSLRUPage(rctx, &pb.GetSLRUPageRequest{
			TenantId: tenantID,
			Kind:     key.SpcOID,
			BlockNum: key.BlockNum,
			Lsn:      lsn,
		})
		if err != nil {
			mapped := b.mapErr(err)
			debugf("ps-grpc", "GetSLRUPage tenant=%s kind=%d blk=%d lsn=%s err=%v dur=%s",
				tenantID, key.SpcOID, key.BlockNum, formatLSNHex(lsn), mapped, time.Since(start))
			return nil, mapped
		}
		if !resp.GetFound() {
			debugf("ps-grpc", "GetSLRUPage tenant=%s kind=%d blk=%d lsn=%s found=false dur=%s",
				tenantID, key.SpcOID, key.BlockNum, formatLSNHex(lsn), time.Since(start))
			return nil, ErrNotFound
		}
		debugf("ps-grpc", "GetSLRUPage tenant=%s kind=%d blk=%d lsn=%s found=true bytes=%d dur=%s",
			tenantID, key.SpcOID, key.BlockNum, formatLSNHex(lsn), len(resp.GetPage()), time.Since(start))
		return resp.GetPage(), nil
	}

	resp, err := b.client.GetPageAtLSN(rctx, &pb.GetPageAtLSNRequest{
		TenantId: tenantID,
		Key: &pb.StorageKey{
			Kind:      uint32(key.Kind),
			ForkNum:   uint32(key.ForkNum),
			Reserved:  uint32(key.Reserved),
			SpcOid:    key.SpcOID,
			DbOid:     key.DbOID,
			RelNumber: key.RelNumber,
			BlockNum:  key.BlockNum,
			Extra:     key.Extra,
		},
		Lsn: lsn,
	})
	if err != nil {
		mapped := b.mapErr(err)
		debugf("ps-grpc", "GetPageAtLSN tenant=%s rel=%d/%d/%d.%d blk=%d lsn=%s err=%v dur=%s",
			tenantID, key.SpcOID, key.DbOID, key.RelNumber, key.ForkNum, key.BlockNum, formatLSNHex(lsn), mapped, time.Since(start))
		return nil, mapped
	}
	if !resp.GetFound() {
		debugf("ps-grpc", "GetPageAtLSN tenant=%s rel=%d/%d/%d.%d blk=%d lsn=%s found=false dur=%s",
			tenantID, key.SpcOID, key.DbOID, key.RelNumber, key.ForkNum, key.BlockNum, formatLSNHex(lsn), time.Since(start))
		return nil, ErrNotFound
	}
	debugf("ps-grpc", "GetPageAtLSN tenant=%s rel=%d/%d/%d.%d blk=%d lsn=%s found=true bytes=%d dur=%s",
		tenantID, key.SpcOID, key.DbOID, key.RelNumber, key.ForkNum, key.BlockNum, formatLSNHex(lsn), len(resp.GetPage()), time.Since(start))
	return resp.GetPage(), nil
}

// GetPageBatch implements StorageBackend.
func (b *PageserverGRPCBackend) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	start := time.Now()
	b.requestsTotal.Add(1)
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)

	rctx, cancel := b.withContext(ctx)
	defer cancel()
	resp, err := b.client.GetPageBatch(rctx, &pb.GetPageBatchRequest{
		TenantId:   tenantID,
		SpcOid:     spcOID,
		DbOid:      dbOID,
		RelNumber:  relNumber,
		ForkNum:    uint32(forkNum),
		StartBlock: startBlock,
		Nblocks:    nblocks,
		Lsn:        lsn,
	})
	if err != nil {
		mapped := b.mapErr(err)
		debugf("ps-grpc", "GetPageBatch tenant=%s rel=%d/%d/%d.%d start=%d req=%d lsn=%s err=%v dur=%s",
			tenantID, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, formatLSNHex(lsn), mapped, time.Since(start))
		return nil, 0, mapped
	}
	debugf("ps-grpc", "GetPageBatch tenant=%s rel=%d/%d/%d.%d start=%d req=%d got=%d lsn=%s dur=%s",
		tenantID, spcOID, dbOID, relNumber, forkNum, startBlock, nblocks, resp.GetNblocksReturned(), formatLSNHex(lsn), time.Since(start))
	return resp.GetPages(), resp.GetNblocksReturned(), nil
}

// GetRelSize implements StorageBackend.
func (b *PageserverGRPCBackend) GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	start := time.Now()
	b.requestsTotal.Add(1)
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)

	rctx, cancel := b.withContext(ctx)
	defer cancel()
	resp, err := b.client.GetRelSize(rctx, &pb.GetRelRequest{
		TenantId:  tenantID,
		SpcOid:    spcOID,
		DbOid:     dbOID,
		RelNumber: relNumber,
		ForkNum:   uint32(forkNum),
		Lsn:       lsn,
	})
	if err != nil {
		mapped := b.mapErr(err)
		debugf("ps-grpc", "GetRelSize tenant=%s rel=%d/%d/%d.%d lsn=%s err=%v dur=%s",
			tenantID, spcOID, dbOID, relNumber, forkNum, formatLSNHex(lsn), mapped, time.Since(start))
		return 0, mapped
	}
	debugf("ps-grpc", "GetRelSize tenant=%s rel=%d/%d/%d.%d lsn=%s nblocks=%d dur=%s",
		tenantID, spcOID, dbOID, relNumber, forkNum, formatLSNHex(lsn), resp.GetNblocks(), time.Since(start))
	return resp.GetNblocks(), nil
}

// GetRelExists implements StorageBackend.
func (b *PageserverGRPCBackend) GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error) {
	start := time.Now()
	b.requestsTotal.Add(1)
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)

	rctx, cancel := b.withContext(ctx)
	defer cancel()
	resp, err := b.client.GetRelExists(rctx, &pb.GetRelRequest{
		TenantId:  tenantID,
		SpcOid:    spcOID,
		DbOid:     dbOID,
		RelNumber: relNumber,
		ForkNum:   uint32(forkNum),
		Lsn:       lsn,
	})
	if err != nil {
		mapped := b.mapErr(err)
		debugf("ps-grpc", "GetRelExists tenant=%s rel=%d/%d/%d.%d lsn=%s err=%v dur=%s",
			tenantID, spcOID, dbOID, relNumber, forkNum, formatLSNHex(lsn), mapped, time.Since(start))
		return false, mapped
	}
	debugf("ps-grpc", "GetRelExists tenant=%s rel=%d/%d/%d.%d lsn=%s exists=%v dur=%s",
		tenantID, spcOID, dbOID, relNumber, forkNum, formatLSNHex(lsn), resp.GetExists(), time.Since(start))
	return resp.GetExists(), nil
}
