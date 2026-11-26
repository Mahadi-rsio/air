package gateway

import (
	"context"
	"crypto/subtle"
	"net"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/meshtransport"
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
	discovery     *TenantDiscovery
	reGRPC        *ReadEngineGRPCClient
	psGRPC        *PageserverGRPCClient
}

func (s *gatewayAdminService) authorize(ctx context.Context) error {
	if s.internalToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.PermissionDenied, "unauthorized")
	}
	for _, key := range []string{"x-air-internal-token", "x-internal-token"} {
		for _, v := range md.Get(key) {
			if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(v)), []byte(s.internalToken)) == 1 {
				return nil
			}
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

func (s *gatewayAdminService) RegisterNamespace(ctx context.Context, req *pb.RegisterNamespaceRequest) (*pb.NamespaceStatusResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	tid := strings.TrimSpace(req.GetTenantId())
	if tid == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}

	route := TenantRoute{
		TenantID:    tid,
		State:       req.GetState(),
		Host:        req.GetHost(),
		Port:        int(req.GetPort()),
		SockDir:     req.GetSockDir(),
		AppPassword: req.GetAppPassword(),
	}
	if route.State == "" {
		route.State = "ACTIVE"
	}

	// If route endpoint/credentials were omitted, resolve them from air-server via gRPC.
	airConnected := route.Port > 0 && route.AppPassword != ""
	if !airConnected && s.discovery != nil {
		if resolved, _, err := s.discovery.ResolveTenant(tid); err == nil {
			if route.Host == "" {
				route.Host = resolved.Host
			}
			if route.Port == 0 {
				route.Port = resolved.Port
			}
			if route.SockDir == "" {
				route.SockDir = resolved.SockDir
			}
			if route.AppPassword == "" {
				route.AppPassword = resolved.AppPassword
			}
			if resolved.State != "" {
				route.State = resolved.State
			}
			airConnected = route.Port > 0
		}
	}

	if s.discovery != nil {
		s.discovery.UpsertRoute(route)
	}
	if s.pools != nil {
		s.pools.UpsertDiscovered(route)
		if req.GetReadEnginePort() > 0 {
			tp := s.pools.GetOrCreate(route, nil)
			tp.SetReadEngineEndpoint(req.GetReadEngineHost(), "", int(req.GetReadEnginePort()))
		}
	}

	reConnected := false
	if s.reGRPC != nil {
		if _, err := s.reGRPC.EnsureTenant(ctx, tid); err == nil {
			reConnected = true
		}
	}

	psConnected := false
	if s.psGRPC != nil {
		if info, err := s.psGRPC.GetTenantInfo(ctx, tid); err == nil && info.GetExists() {
			psConnected = true
		} else if _, err := s.psGRPC.EnsureTenant(ctx, tid, req.GetPageserverGeneration()); err == nil {
			psConnected = true
		}
	}

	return &pb.NamespaceStatusResponse{
		Status:              "registered",
		TenantId:            tid,
		AirConnected:        airConnected,
		ReadEngineConnected: reConnected,
		PageserverConnected: psConnected,
	}, nil
}

func (s *gatewayAdminService) DeleteNamespace(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	tid := strings.TrimSpace(req.GetTenantId())
	if tid == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if s.discovery != nil {
		s.discovery.EvictCache(tid)
	}
	if s.pools != nil {
		s.pools.RemoveTenant(tid)
	}
	return &pb.StatusResponse{Status: "deleted", TenantId: tid}, nil
}

func (s *gatewayAdminService) GetNamespace(ctx context.Context, req *pb.TenantRequest) (*pb.NamespaceInfoResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	tid := strings.TrimSpace(req.GetTenantId())
	if tid == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	resp := &pb.NamespaceInfoResponse{TenantId: tid}
	if s.discovery != nil {
		for _, r := range s.discovery.ListCached() {
			if r.TenantID == tid {
				resp.State = r.State
				resp.AirHost = r.Host
				resp.AirPort = int32(r.Port)
				break
			}
		}
	}
	if s.reGRPC != nil {
		if r, err := s.reGRPC.GetTenantRoute(ctx, tid); err == nil {
			resp.ReadEngineHost = r.GetHost()
			resp.ReadEnginePort = r.GetPgPort()
		}
	}
	if s.psGRPC != nil {
		if info, err := s.psGRPC.GetTenantInfo(ctx, tid); err == nil {
			resp.PageserverExists = info.GetExists()
			resp.PageserverGeneration = info.GetGeneration()
			resp.PageserverLastLsn = info.GetLastLsn()
		}
	}
	return resp, nil
}

// SetNamespaceBackends wires discovery, read-engine gRPC, and pageserver gRPC clients into GatewayAdmin.
func (s *AdminGRPCServer) SetNamespaceBackends(discovery *TenantDiscovery, reGRPC *ReadEngineGRPCClient, psGRPC *PageserverGRPCClient) {
	s.svc.discovery = discovery
	s.svc.reGRPC = reGRPC
	s.svc.psGRPC = psGRPC
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
	meshTLS, err := meshtransport.LoadTLS()
	if err != nil {
		return 0, err
	}
	if err := meshtransport.RequireForAddress(s.listenAddr, meshTLS); err != nil {
		return 0, err
	}
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return 0, err
	}
	s.ln = ln
	s.srv = grpc.NewServer(meshtransport.ServerOptions(meshTLS)...)
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
