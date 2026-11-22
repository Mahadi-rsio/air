package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// PageserverGRPCClient connects air-gateway to air-pageserver over gRPC (PageserverService).
type PageserverGRPCClient struct {
	addr          string
	internalToken string
	conn          *grpc.ClientConn
	client        pb.PageserverServiceClient
}

// NewPageserverGRPCClient dials air-pageserver over gRPC with internal token authentication.
func NewPageserverGRPCClient(addr, internalToken string) (*PageserverGRPCClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("pageserver gRPC address is required")
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial pageserver gRPC %s: %w", addr, err)
	}
	return &PageserverGRPCClient{
		addr:          addr,
		internalToken: internalToken,
		conn:          conn,
		client:        pb.NewPageserverServiceClient(conn),
	}, nil
}

func (c *PageserverGRPCClient) withAuth(ctx context.Context) context.Context {
	if c.internalToken == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", c.internalToken)
}

// Health queries PageserverService health.
func (c *PageserverGRPCClient) Health(ctx context.Context) (*pb.PageserverHealthResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 3*time.Second)
	defer cancel()
	return c.client.Health(rctx, &pb.PageserverEmpty{})
}

// GetTenantInfo returns the generation, last LSN, and existence of a tenant namespace on air-pageserver.
func (c *PageserverGRPCClient) GetTenantInfo(ctx context.Context, tenantID string) (*pb.PageserverTenantInfoResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	return c.client.GetTenantInfo(rctx, &pb.PageserverTenantRequest{TenantId: tenantID})
}

// EnsureTenant ensures a tenant namespace exists on air-pageserver.
func (c *PageserverGRPCClient) EnsureTenant(ctx context.Context, tenantID, generation string) (*pb.EnsureTenantResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 10*time.Second)
	defer cancel()
	return c.client.EnsureTenant(rctx, &pb.EnsureTenantRequest{
		TenantId:   tenantID,
		Epoch:      1,
		Generation: generation,
	})
}

// Addr returns the configured pageserver gRPC address.
func (c *PageserverGRPCClient) Addr() string {
	return c.addr
}

// Close closes the underlying gRPC connection.
func (c *PageserverGRPCClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
