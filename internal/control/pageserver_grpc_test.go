package control

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
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

func TestGRPCPageserverClient_SkeletonRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ps, err := pageserver.New(pageserver.Config{
		DisableUnixSocket: true,
		LayerDir:          filepath.Join(dir, "layers"),
		CacheDir:          filepath.Join(dir, "cache"),
		CacheMaxBytes:     16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const token = "skel-token"
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
	client.SetTenantGeneration(ctx, "tenant_skel", "gen_skel")
	if _, err := client.EnsureTenant(ctx, "tenant_skel", 1, "gen_skel", false, ""); err != nil {
		t.Fatalf("EnsureTenant: %v", err)
	}

	srcPGData := filepath.Join(dir, "src_pgdata")
	if err := os.MkdirAll(filepath.Join(srcPGData, "global"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(srcPGData, "base", "16385"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcPGData, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcPGData, "global", "pg_control"), []byte("ctrl-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Relation fork file should be skipped by PackMetadataSkeleton.
	if err := os.WriteFile(filepath.Join(srcPGData, "base", "16385", "12345"), bytes.Repeat([]byte{0xAB}, 8192), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := client.SaveMetadataSkeleton(ctx, "tenant_skel", 1, srcPGData); err != nil {
		t.Fatalf("SaveMetadataSkeleton: %v", err)
	}
	if !client.HasMetadataSkeleton(ctx, "tenant_skel") {
		t.Fatalf("expected HasMetadataSkeleton to be true")
	}

	// Remove source PGDATA to prove restore works without shared disk.
	if err := os.RemoveAll(srcPGData); err != nil {
		t.Fatal(err)
	}

	dstPGData := filepath.Join(dir, "dst_pgdata")
	if err := client.RestoreMetadataSkeleton(ctx, "tenant_skel", 1, dstPGData); err != nil {
		t.Fatalf("RestoreMetadataSkeleton: %v", err)
	}
	gotVer, err := os.ReadFile(filepath.Join(dstPGData, "PG_VERSION"))
	if err != nil || string(gotVer) != "17\n" {
		t.Fatalf("unexpected PG_VERSION: %q err=%v", string(gotVer), err)
	}
	gotCtrl, err := os.ReadFile(filepath.Join(dstPGData, "global", "pg_control"))
	if err != nil || string(gotCtrl) != "ctrl-bytes" {
		t.Fatalf("unexpected pg_control: %q err=%v", string(gotCtrl), err)
	}
	if _, err := os.Stat(filepath.Join(dstPGData, "base", "16385", "12345")); !os.IsNotExist(err) {
		t.Fatalf("expected relation fork file to be skipped, stat err=%v", err)
	}
}
