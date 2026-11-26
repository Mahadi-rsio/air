package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/Mahadi-rsio/air/internal/meshtransport"
	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
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
	dialOption, err := meshtransport.DialOption(addr)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(addr, dialOption)
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

// ListTenants lists all tenants known to air-pageserver.
func (c *PageserverGRPCClient) ListTenants(ctx context.Context) (*pb.PageserverListTenantsResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	return c.client.ListTenants(rctx, &pb.PageserverEmpty{})
}

// FlushTenant flushes pending WAL/layers for a tenant on air-pageserver.
func (c *PageserverGRPCClient) FlushTenant(ctx context.Context, tenantID string) (*pb.FlushTenantResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 30*time.Second)
	defer cancel()
	return c.client.FlushTenant(rctx, &pb.PageserverTenantRequest{TenantId: tenantID})
}

// FlushAll flushes all tenants on air-pageserver.
func (c *PageserverGRPCClient) FlushAll(ctx context.Context) (*pb.PageserverStatusResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 60*time.Second)
	defer cancel()
	return c.client.FlushAll(rctx, &pb.PageserverEmpty{})
}

// PurgeTenant purges a tenant namespace from air-pageserver.
func (c *PageserverGRPCClient) PurgeTenant(ctx context.Context, tenantID, generation string) (*pb.PageserverStatusResponse, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 15*time.Second)
	defer cancel()
	return c.client.PurgeTenant(rctx, &pb.PurgeTenantRequest{
		TenantId:   tenantID,
		Generation: generation,
	})
}

// GetTenantStorageBytes returns the total logical storage bytes tracked for tenantID on air-pageserver.
func (c *PageserverGRPCClient) GetTenantStorageBytes(ctx context.Context, tenantID string) (uint64, error) {
	rctx, cancel := context.WithTimeout(c.withAuth(ctx), 5*time.Second)
	defer cancel()
	resp, err := c.client.GetDBSize(rctx, &pb.GetDBSizeRequest{
		TenantId: tenantID,
		SpcOid:   0,
		DbOid:    0,
		Lsn:      0,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetSizeBytes(), nil
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
