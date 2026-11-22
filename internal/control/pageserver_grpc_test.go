package control

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestGRPCPageserverClient_LifecycleAndWAL(t *testing.T) {
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

	const token = "ctrl-token"
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := pageserver.NewGRPCServer(ps, token)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	client, err := NewGRPCPageserverClient(lis.Addr().String(), token)
	if err != nil {
		t.Fatalf("NewGRPCPageserverClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	client.SetTenantGeneration(ctx, "tenant_a", "gen_a")
	lastLSN, err := client.EnsureTenant(ctx, "tenant_a", 1, "gen_a", false, "")
	if err != nil {
		t.Fatalf("EnsureTenant: %v", err)
	}
	if lastLSN != 0 {
		t.Fatalf("expected 0 initial LSN, got %d", lastLSN)
	}

	sink := &grpcWALSink{
		client:     client,
		tenantID:   "tenant_a",
		epoch:      1,
		generation: "gen_a",
	}
	pageImg := bytes.Repeat([]byte{0x5A}, pageserver.PageSize)
	appliedLSN, err := sink.IngestWALBatch(ctx, []*pageserver.DecodedWALRecord{{
		LSN:    0x3000,
		EndLSN: 0x3040,
		Blocks: []pageserver.WALBlockRef{{
			BlockID:      0,
			Rel:          pageserver.RelFileLocator{SpcOID: 1663, DbOID: 16384, RelNumber: 2002},
			ForkNum:      0,
			BlockNum:     0,
			HasImage:     true,
			ImagePayload: pageImg,
		}},
	}}, false)
	if err != nil {
		t.Fatalf("IngestWALBatch: %v", err)
	}
	if appliedLSN < 0x3040 {
		t.Fatalf("expected LSN >= 0x3040, got 0x%x", appliedLSN)
	}

	got, err := ps.GetPageAtLSN(ctx, "tenant_a", pageserver.RelBlockKey(1663, 16384, 2002, 0, 0), appliedLSN)
	if err != nil {
		t.Fatalf("GetPageAtLSN: %v", err)
	}
	if !bytes.Equal(got, pageImg) {
		t.Fatalf("WAL ingested page mismatch")
	}

	if _, err := client.FlushTenant(ctx, "tenant_a"); err != nil {
		t.Fatalf("FlushTenant: %v", err)
	}
	if err := client.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if err := client.PurgeTenant(ctx, "tenant_a", "gen_a"); err != nil {
		t.Fatalf("PurgeTenant: %v", err)
	}
}
