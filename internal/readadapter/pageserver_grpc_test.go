package readadapter

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
	"google.golang.org/grpc/codes"
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
