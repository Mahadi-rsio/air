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

// ReadEngineGRPCClient connects air-gateway to air-read-engine over gRPC (ReadEngineService).
type ReadEngineGRPCClient struct {
	addr          string
	internalToken string
	conn          *grpc.ClientConn
	client        pb.ReadEngineServiceClient
}

// NewReadEngineGRPCClient dials air-read-engine over gRPC.
func NewReadEngineGRPCClient(addr string, internalToken ...string) (*ReadEngineGRPCClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("read-engine gRPC address is required")
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial read-engine gRPC %s: %w", addr, err)
	}
	var tok string
	if len(internalToken) > 0 {
		tok = internalToken[0]
	}
	return &ReadEngineGRPCClient{
		addr:          addr,
		internalToken: tok,
		conn:          conn,
		client:        pb.NewReadEngineServiceClient(conn),
	}, nil
}

func (c *ReadEngineGRPCClient) withAuth(ctx context.Context) context.Context {
	if c.internalToken == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", c.internalToken)
}

// Health queries the ReadEngineService health and PG-wire endpoint.
func (c *ReadEngineGRPCClient) Health(ctx context.Context) (*pb.ReadEngineHealthResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 3*time.Second)
	defer cancel()
	return c.client.Health(rctx, &pb.ReadEngineEmpty{})
}

// EnsureTenant ensures the tenant database is registered on air-read-engine.
func (c *ReadEngineGRPCClient) EnsureTenant(ctx context.Context, tenantID string) (*pb.ReadEngineTenantRouteResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	return c.client.EnsureTenant(rctx, &pb.ReadEngineTenantRequest{TenantId: tenantID})
}

// GetTenantRoute fetches the tenant route from air-read-engine.
func (c *ReadEngineGRPCClient) GetTenantRoute(ctx context.Context, tenantID string) (*pb.ReadEngineTenantRouteResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	return c.client.GetTenantRoute(rctx, &pb.ReadEngineTenantRequest{TenantId: tenantID})
}

// DeleteTenant removes the tenant namespace from air-read-engine.
func (c *ReadEngineGRPCClient) DeleteTenant(ctx context.Context, tenantID string) (*pb.ReadEngineStatusResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	return c.client.DeleteTenant(rctx, &pb.ReadEngineTenantRequest{TenantId: tenantID})
}

// ListTenants lists all tenant IDs registered on air-read-engine.
func (c *ReadEngineGRPCClient) ListTenants(ctx context.Context) (*pb.ReadEngineListTenantsResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	return c.client.ListTenants(rctx, &pb.ReadEngineEmpty{})
}

// SyncTenant triggers SQL or primary_conninfo sync for a tenant on air-read-engine.
func (c *ReadEngineGRPCClient) SyncTenant(ctx context.Context, tenantID, sql, lsn, primaryConninfo string) (*pb.ReadEngineStatusResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 30*time.Second)
	defer cancel()
	return c.client.SyncTenant(rctx, &pb.SyncTenantRequest{
		TenantId:        tenantID,
		Sql:             sql,
		Lsn:             lsn,
		PrimaryConninfo: primaryConninfo,
	})
}

// Close closes the gRPC client connection.
func (c *ReadEngineGRPCClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
