package control

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/pb"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type grpcServer struct {
	pb.UnimplementedAirDBControlServer
	cp *ControlPlane
}

// NewGRPCServer constructs the Air DB gRPC control server for air-gateway.
func NewGRPCServer(cp *ControlPlane) *grpc.Server {
	srv := grpc.NewServer(grpc.UnaryInterceptor(cp.grpcAuthInterceptor))
	pb.RegisterAirDBControlServer(srv, &grpcServer{cp: cp})
	return srv
}

func (cp *ControlPlane) grpcAuthInterceptor(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if cp.internalToken != "" {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		var got string
		if vals := md.Get("x-air-internal-token"); len(vals) > 0 {
			got = strings.TrimSpace(vals[0])
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(cp.internalToken)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "unauthorized")
		}
	}
	return handler(ctx, req)
}

func mapGRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, pgx.ErrNoRows):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrForbidden):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, cell.ErrDrainVetoed):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}

func (s *grpcServer) GetTenantRoute(ctx context.Context, req *pb.TenantRequest) (*pb.TenantRouteResponse, error) {
	state, port, sockDir, appPass, err := s.cp.GetTenantRouteAndCredentials(ctx, req.GetTenantId())
	if err != nil {
		return nil, mapGRPCError(err)
	}
	return &pb.TenantRouteResponse{
		TenantId:    req.GetTenantId(),
		State:       state,
		Host:        s.cp.backendHost,
		Port:        int32(port),
		SockDir:     sockDir,
		AppPassword: appPass,
	}, nil
}

func (s *grpcServer) WakeTenant(ctx context.Context, req *pb.TenantRequest) (*pb.TenantRouteResponse, error) {
	port, sockDir, appPass, err := s.cp.WakeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, mapGRPCError(err)
	}
	s.cp.TouchTenantActivity(req.GetTenantId())
	return &pb.TenantRouteResponse{
		TenantId:    req.GetTenantId(),
		State:       "ACTIVE",
		Host:        s.cp.backendHost,
		Port:        int32(port),
		SockDir:     sockDir,
		AppPassword: appPass,
	}, nil
}

func (s *grpcServer) TouchActivity(_ context.Context, req *pb.TenantRequest) (*pb.Empty, error) {
	if err := tenant.ValidateID(req.GetTenantId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.cp.TouchTenantActivity(req.GetTenantId())
	return &pb.Empty{}, nil
}

func (s *grpcServer) CreateDatabase(ctx context.Context, req *pb.CreateDatabaseRequest) (*pb.CreateDatabaseResponse, error) {
	accountID := strings.TrimSpace(req.GetAccountId())
	if accountID == "" {
		accountID = "acct_default"
	}
	idemKey := strings.TrimSpace(req.GetIdempotencyKey())
	if idemKey == "" {
		idemKey = "gw-" + req.GetTenantId()
	}
	resp, err := s.cp.ProvisionDatabase(ctx, accountID, idemKey, req.GetTenantId())
	if err != nil {
		return nil, mapGRPCError(err)
	}
	return &pb.CreateDatabaseResponse{
		TenantId:   resp.TenantID,
		State:      resp.State,
		DirectPort: int32(resp.DirectPort),
		Host:       s.cp.backendHost,
		User:       resp.Username,
		Password:   resp.Password,
	}, nil
}

func (s *grpcServer) ListDatabases(ctx context.Context, req *pb.ListDatabasesRequest) (*pb.ListDatabasesResponse, error) {
	list, err := s.cp.ListDatabases(ctx, strings.TrimSpace(req.GetAccountId()))
	if err != nil {
		return nil, mapGRPCError(err)
	}
	out := &pb.ListDatabasesResponse{
		Databases: make([]*pb.DatabaseInfo, 0, len(list)),
	}
	for _, d := range list {
		out.Databases = append(out.Databases, &pb.DatabaseInfo{
			AccountId:        d.AccountID,
			TenantId:         d.TenantID,
			DirectPort:       int32(d.DirectPort),
			SystemIdentifier: d.SystemIdentifier,
			FencingEpoch:     d.FencingEpoch,
			State:            d.State,
		})
	}
	return out, nil
}

func (s *grpcServer) GetDatabase(ctx context.Context, req *pb.TenantRequest) (*pb.DatabaseInfo, error) {
	info, err := s.cp.GetDatabase(ctx, "", req.GetTenantId())
	if err != nil {
		return nil, mapGRPCError(err)
	}
	return &pb.DatabaseInfo{
		AccountId:        info.AccountID,
		TenantId:         info.TenantID,
		DirectPort:       int32(info.DirectPort),
		SystemIdentifier: info.SystemIdentifier,
		FencingEpoch:     info.FencingEpoch,
		State:            info.State,
	}, nil
}

func (s *grpcServer) DeleteDatabase(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
	if err := s.cp.DeleteDatabase(ctx, "", req.GetTenantId()); err != nil {
		return nil, mapGRPCError(err)
	}
	return &pb.StatusResponse{Status: "purged", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) SuspendDatabase(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
	if err := s.cp.SuspendTenant(ctx, req.GetTenantId(), 3*time.Second, nil); err != nil {
		return nil, mapGRPCError(err)
	}
	return &pb.StatusResponse{Status: "SUSPENDED", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) StartDatabase(ctx context.Context, req *pb.TenantRequest) (*pb.TenantRouteResponse, error) {
	return s.WakeTenant(ctx, req)
}

func (s *grpcServer) LockDatabase(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
	if err := s.cp.LockDatabase(ctx, req.GetTenantId(), 3*time.Second, nil); err != nil {
		return nil, mapGRPCError(err)
	}
	return &pb.StatusResponse{Status: "LOCKED", TenantId: req.GetTenantId()}, nil
}

func (s *grpcServer) UnlockDatabase(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
	if err := s.cp.UnlockDatabase(ctx, req.GetTenantId()); err != nil {
		return nil, mapGRPCError(err)
	}
	return &pb.StatusResponse{Status: "SUSPENDED", TenantId: req.GetTenantId()}, nil
}
