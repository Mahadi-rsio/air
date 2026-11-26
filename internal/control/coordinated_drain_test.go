package control

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type coordMockGatewayAdminServer struct {
	pb.UnimplementedGatewayAdminServer
	token string
	err   error
	calls int
}

func (m *coordMockGatewayAdminServer) DrainTenant(ctx context.Context, req *pb.DrainTenantRequest) (*pb.StatusResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("x-air-internal-token")
	if m.token != "" && (len(vals) == 0 || vals[0] != m.token) {
		return nil, status.Error(codes.PermissionDenied, "unauthorized")
	}
	m.calls++
	if m.err != nil {
		return nil, status.Error(codes.FailedPrecondition, m.err.Error())
	}
	return &pb.StatusResponse{Status: "drained", TenantId: req.GetTenantId()}, nil
}

func startCoordMockGatewayAdmin(t *testing.T, token string, drainErr error) (string, *coordMockGatewayAdminServer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	svc := &coordMockGatewayAdminServer{token: token, err: drainErr}
	gs := grpc.NewServer()
	pb.RegisterGatewayAdminServer(gs, svc)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.GracefulStop)
	return ln.Addr().String(), svc
}

func TestCoordinatedGRPCPoolDrain_DefersOnMissingMembershipOrActiveWork(t *testing.T) {
	// 1. Missing membership must defer suspension when requireMembership=true.
	fnNoPeers := NewCoordinatedGRPCPoolDrainFn(nil, "", 9093, "tok", true)
	if err := fnNoPeers("tenant-1", time.Second); err == nil || !strings.Contains(err.Error(), "no gateway instances available") {
		t.Fatalf("expected membership verification error, got %v", err)
	}

	// 2. If one Gateway succeeds and another vetoes (active work), suspension defers.
	addr1, svc1 := startCoordMockGatewayAdmin(t, "tok", nil)
	addr2, svc2 := startCoordMockGatewayAdmin(t, "tok", context.DeadlineExceeded)
	fnMixed := NewCoordinatedGRPCPoolDrainFn([]string{addr1, addr2}, "", 9093, "tok", true)
	if err := fnMixed("tenant-1", time.Second); err == nil {
		t.Fatal("expected coordinated drain to fail when second gateway vetoes")
	}
	if svc1.calls != 1 || svc2.calls != 1 {
		t.Fatalf("expected both gateways to be queried, got calls=(%d,%d)", svc1.calls, svc2.calls)
	}

	// 3. If all Gateways acknowledge drain, suspension proceeds.
	addr3, svc3 := startCoordMockGatewayAdmin(t, "tok", nil)
	fnOK := NewCoordinatedGRPCPoolDrainFn([]string{addr1, addr3}, "", 9093, "tok", true)
	if err := fnOK("tenant-1", time.Second); err != nil {
		t.Fatalf("expected coordinated drain to succeed, got %v", err)
	}
	if svc3.calls != 1 {
		t.Fatalf("expected gateway 3 to be queried once, got %d", svc3.calls)
	}
}
