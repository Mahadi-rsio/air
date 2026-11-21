package gateway

import (
	"context"
	"net"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type gatewayAdminService struct {
	pb.UnimplementedGatewayAdminServer
	internalToken string
	pools         *PoolManager
	metrics       *MetricsRegistry
}

func (s *gatewayAdminService) authorize(ctx context.Context) error {
	if s.internalToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.PermissionDenied, "unauthorized")
	}
	for _, v := range md.Get("x-internal-token") {
		if v == s.internalToken {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "unauthorized")
}

func (s *gatewayAdminService) DrainTenant(ctx context.Context, req *pb.DrainTenantRequest) (*pb.StatusResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	timeoutMs := int(req.TimeoutMs)
	if timeoutMs <= 0 {
		timeoutMs = 3000
	}
	if err := s.pools.RequestDrain(req.TenantId, time.Duration(timeoutMs)*time.Millisecond); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &pb.StatusResponse{Status: "drained", TenantId: req.TenantId}, nil
}

func (s *gatewayAdminService) SetTenantLimits(ctx context.Context, req *pb.SetTenantLimitsRequest) (*pb.StatusResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if req.MaxClientConns > 0 {
		s.metrics.SetTenantClientLimit(req.TenantId, int(req.MaxClientConns))
	}
	if req.MaxBackends > 0 {
		s.pools.SetTenantMaxBackends(req.TenantId, int(req.MaxBackends))
	}
	return &pb.StatusResponse{Status: "updated", TenantId: req.TenantId}, nil
}

// AdminGRPCServer wraps the optional internal GatewayAdmin gRPC server.
type AdminGRPCServer struct {
	listenAddr string
	svc        *gatewayAdminService
	srv        *grpc.Server
	ln         net.Listener
}

func NewAdminGRPCServer(listenAddr, internalToken string, pools *PoolManager, metrics *MetricsRegistry) *AdminGRPCServer {
	return &AdminGRPCServer{
		listenAddr: listenAddr,
		svc: &gatewayAdminService{
			internalToken: internalToken,
			pools:         pools,
			metrics:       metrics,
		},
	}
}

func (s *AdminGRPCServer) Start() (int, error) {
	if s.listenAddr == "" {
		return 0, nil
	}
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return 0, err
	}
	s.ln = ln
	s.srv = grpc.NewServer()
	pb.RegisterGatewayAdminServer(s.srv, s.svc)
	go func() {
		_ = s.srv.Serve(ln)
	}()
	if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok {
		return tcpAddr.Port, nil
	}
	return 0, nil
}

func (s *AdminGRPCServer) Stop() {
	if s.srv != nil {
		s.srv.GracefulStop()
		s.srv = nil
	}
	if s.ln != nil {
		_ = s.ln.Close()
		s.ln = nil
	}
}
