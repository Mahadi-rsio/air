package readadapter

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestPageserverGRPCBackend_RoundTripAndAuth(t *testing.T) {
	dir := t.TempDir()
	ps, err := pageserver.New(pageserver.Config{
		DisableUnixSocket: true,
		LayerDir:          dir + "/layers",
		CacheDir:          dir + "/cache",
		CacheMaxBytes:     16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const token = "secret-test-token"
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := pageserver.NewGRPCServer(ps, token)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	ps.SetTenantGeneration("tenant_1", "gen_1")
	ps.EnsureTenant("tenant_1", 1)

	page := bytes.Repeat([]byte{0xAB}, pageserver.PageSize)
	if err := ps.PutPageBatch(context.Background(), "tenant_1", 1663, 16384, 1001, 0, 0, 1, 0x2000, page); err != nil {
		t.Fatalf("PutPageBatch: %v", err)
	}
	if err := ps.SetRelSizes("tenant_1", []pageserver.RelSizeEntry{{
		SpcOID: 1663, DbOID: 16384, RelNumber: 1001, ForkNum: 0, NBlocks: 1, LSN: 0x2000,
	}}); err != nil {
		t.Fatalf("SetRelSizes: %v", err)
	}

	// Unauthenticated client should be rejected.
	badClient, err := NewPageserverGRPCBackend(PageserverGRPCConfig{Addr: lis.Addr().String()})
	if err != nil {
		t.Fatalf("NewPageserverGRPCBackend bad: %v", err)
	}
	defer badClient.Close()
	if _, _, err := badClient.GetTenantInfo(context.Background(), "tenant_1"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}

	// Authenticated client should succeed across all operations.
	client, err := NewPageserverGRPCBackend(PageserverGRPCConfig{
		Addr:          lis.Addr().String(),
		InternalToken: token,
	})
	if err != nil {
		t.Fatalf("NewPageserverGRPCBackend: %v", err)
	}
	defer client.Close()

	snap, err := client.CurrentSnapshot(context.Background(), "tenant_1", "gen_1")
	if err != nil {
		t.Fatalf("CurrentSnapshot: %v", err)
	}
	if snap.LSN != 0x2000 {
		t.Fatalf("expected LSN 0x2000, got 0x%x", snap.LSN)
	}

	got, err := client.GetPageAtLSN(context.Background(), "tenant_1", pageserver.RelBlockKey(1663, 16384, 1001, 0, 0), 0x2000)
	if err != nil {
		t.Fatalf("GetPageAtLSN: %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Fatalf("page mismatch")
	}

	nb, err := client.GetRelSize(context.Background(), "tenant_1", 1663, 16384, 1001, 0, 0x2000)
	if err != nil || nb != 1 {
		t.Fatalf("GetRelSize: nb=%d err=%v", nb, err)
	}

	exists, err := client.GetRelExists(context.Background(), "tenant_1", 1663, 16384, 1001, 0, 0x2000)
	if err != nil || !exists {
		t.Fatalf("GetRelExists: exists=%v err=%v", exists, err)
	}
}

type slowTenantInfoServer struct {
	pb.UnimplementedPageserverServiceServer
	calls atomic.Int64
	gate  chan struct{}
}

func (s *slowTenantInfoServer) GetTenantInfo(ctx context.Context, req *pb.PageserverTenantRequest) (*pb.PageserverTenantInfoResponse, error) {
	s.calls.Add(1)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("x-air-internal-token")) == 0 || md.Get("x-air-internal-token")[0] != "tok-sf" {
		return nil, status.Error(codes.Unauthenticated, "missing internal token")
	}
	<-s.gate
	return &pb.PageserverTenantInfoResponse{
		Exists:     true,
		Generation: "gen_sf",
		LastLsn:    0x4000,
	}, nil
}

func TestPageserverGRPCBackend_GetTenantInfoSingleflight(t *testing.T) {
	srv := &slowTenantInfoServer{gate: make(chan struct{})}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterPageserverServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	client, err := NewPageserverGRPCBackend(PageserverGRPCConfig{
		Addr:           lis.Addr().String(),
		InternalToken:  "tok-sf",
		RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewPageserverGRPCBackend: %v", err)
	}
	defer client.Close()

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	lsns := make([]uint64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := context.Background()
			if idx == 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			_, lsn, err := client.GetTenantInfo(ctx, "t_sf")
			lsns[idx] = lsn
			errs[idx] = err
		}(i)
	}

	time.Sleep(50 * time.Millisecond)
	close(srv.gate)
	wg.Wait()

	if got := srv.calls.Load(); got != 1 {
		t.Fatalf("expected 1 coalesced GetTenantInfo RPC, got %d", got)
	}
	if !errors.Is(errs[0], context.DeadlineExceeded) {
		t.Fatalf("expected caller 0 to fail with DeadlineExceeded, got %v", errs[0])
	}
	for i := 1; i < n; i++ {
		if errs[i] != nil || lsns[i] != 0x4000 {
			t.Fatalf("caller %d: lsn=0x%x err=%v", i, lsns[i], errs[i])
		}
	}
}
