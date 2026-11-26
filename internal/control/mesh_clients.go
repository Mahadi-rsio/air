package control

import (
	"context"
	"fmt"
	"time"

	"github.com/Mahadi-rsio/air/internal/meshtransport"
	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ReadEngineClient is the gRPC client interface used by air-server to manage tenant namespaces on air-read-engine.
type ReadEngineClient interface {
	EnsureTenant(ctx context.Context, tenantID string) (host string, port int, err error)
	DeleteTenant(ctx context.Context, tenantID string) error
	Close() error
}

// GRPCReadEngineClient connects air-server to air-read-engine over gRPC (ReadEngineService).
type GRPCReadEngineClient struct {
	addr          string
	internalToken string
	conn          *grpc.ClientConn
	client        pb.ReadEngineServiceClient
}

// NewGRPCReadEngineClient dials air-read-engine over gRPC with internal token auth.
func NewGRPCReadEngineClient(addr, internalToken string) (*GRPCReadEngineClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("read-engine gRPC address is required")
	}
	dialOption, err := meshtransport.DialOption(addr)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(addr, dialOption)
	if err != nil {
		return nil, fmt.Errorf("dial read-engine gRPC %s: %w", addr, err)
	}
	return &GRPCReadEngineClient{
		addr:          addr,
		internalToken: internalToken,
		conn:          conn,
		client:        pb.NewReadEngineServiceClient(conn),
	}, nil
}

func (c *GRPCReadEngineClient) withAuth(ctx context.Context) context.Context {
	if c.internalToken == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", c.internalToken)
}

func (c *GRPCReadEngineClient) EnsureTenant(ctx context.Context, tenantID string) (string, int, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 10*time.Second)
	defer cancel()
	resp, err := c.client.EnsureTenant(rctx, &pb.ReadEngineTenantRequest{TenantId: tenantID})
	if err != nil {
		return "", 0, err
	}
	return resp.GetHost(), int(resp.GetPgPort()), nil
}

func (c *GRPCReadEngineClient) DeleteTenant(ctx context.Context, tenantID string) error {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 10*time.Second)
	defer cancel()
	_, err := c.client.DeleteTenant(rctx, &pb.ReadEngineTenantRequest{TenantId: tenantID})
	return err
}

func (c *GRPCReadEngineClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// GatewayNamespaceSpec describes a tenant namespace to register on air-gateway via GatewayAdmin gRPC.
type GatewayNamespaceSpec struct {
	TenantID             string
	State                string
	Host                 string
	Port                 int
	SockDir              string
	AppPassword          string
	ReadEngineHost       string
	ReadEnginePort       int
	PageserverGeneration string
}

// GatewayAdminClient is the gRPC client interface used by air-server to manage tenant namespaces on air-gateway.
type GatewayAdminClient interface {
	RegisterNamespace(ctx context.Context, spec GatewayNamespaceSpec) error
	DeleteNamespace(ctx context.Context, tenantID string) error
	Close() error
}

// GRPCGatewayAdminClient connects air-server to air-gateway over gRPC (GatewayAdmin).
type GRPCGatewayAdminClient struct {
	addr          string
	internalToken string
	conn          *grpc.ClientConn
	client        pb.GatewayAdminClient
}

// NewGRPCGatewayAdminClient dials air-gateway's GatewayAdmin gRPC service with internal token auth.
func NewGRPCGatewayAdminClient(addr, internalToken string) (*GRPCGatewayAdminClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("gateway admin gRPC address is required")
	}
	dialOption, err := meshtransport.DialOption(addr)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(addr, dialOption)
	if err != nil {
		return nil, fmt.Errorf("dial gateway admin gRPC %s: %w", addr, err)
	}
	return &GRPCGatewayAdminClient{
		addr:          addr,
		internalToken: internalToken,
		conn:          conn,
		client:        pb.NewGatewayAdminClient(conn),
	}, nil
}

func (c *GRPCGatewayAdminClient) withAuth(ctx context.Context) context.Context {
	if c.internalToken == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", c.internalToken)
}

func (c *GRPCGatewayAdminClient) RegisterNamespace(ctx context.Context, spec GatewayNamespaceSpec) error {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 10*time.Second)
	defer cancel()
	_, err := c.client.RegisterNamespace(rctx, &pb.RegisterNamespaceRequest{
		TenantId:             spec.TenantID,
		State:                spec.State,
		Host:                 spec.Host,
		Port:                 int32(spec.Port),
		SockDir:              spec.SockDir,
		AppPassword:          spec.AppPassword,
		ReadEngineHost:       spec.ReadEngineHost,
		ReadEnginePort:       int32(spec.ReadEnginePort),
		PageserverGeneration: spec.PageserverGeneration,
	})
	return err
}

func (c *GRPCGatewayAdminClient) DeleteNamespace(ctx context.Context, tenantID string) error {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 10*time.Second)
	defer cancel()
	_, err := c.client.DeleteNamespace(rctx, &pb.TenantRequest{TenantId: tenantID})
	return err
}

func (c *GRPCGatewayAdminClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
