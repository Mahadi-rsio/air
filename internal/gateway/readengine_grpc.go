package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ReadEngineGRPCClient connects air-gateway to air-read-engine over gRPC (ReadEngineService).
type ReadEngineGRPCClient struct {
	addr   string
	conn   *grpc.ClientConn
	client pb.ReadEngineServiceClient
}

// NewReadEngineGRPCClient dials air-read-engine over gRPC.
func NewReadEngineGRPCClient(addr string) (*ReadEngineGRPCClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("read-engine gRPC address is required")
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial read-engine gRPC %s: %w", addr, err)
	}
	return &ReadEngineGRPCClient{
		addr:   addr,
		conn:   conn,
		client: pb.NewReadEngineServiceClient(conn),
	}, nil
}

// Health queries the ReadEngineService health and PG-wire endpoint.
func (c *ReadEngineGRPCClient) Health(ctx context.Context) (*pb.ReadEngineHealthResponse, error) {
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return c.client.Health(rctx, &pb.ReadEngineEmpty{})
}

// EnsureTenant ensures the tenant database is registered on air-read-engine.
func (c *ReadEngineGRPCClient) EnsureTenant(ctx context.Context, tenantID string) (*pb.ReadEngineTenantRouteResponse, error) {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.client.EnsureTenant(rctx, &pb.ReadEngineTenantRequest{TenantId: tenantID})
}

// Close closes the gRPC client connection.
func (c *ReadEngineGRPCClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
