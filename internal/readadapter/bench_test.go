package readadapter

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func BenchmarkReadEngineL1HotPage(b *testing.B) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(b.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(b.TempDir(), "layers"),
	})
	if err != nil {
		b.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID = "bench-l1"
		gen      = "gen-1"
		lsn      = uint64(0x10000)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)
	pg := bytes.Repeat([]byte{0x55}, PGPageSize)
	if err := ps.PutPageBatch(context.Background(), tenantID, 1663, 5, 60001, 0, 0, 1, lsn, pg); err != nil {
		b.Fatalf("PutPageBatch: %v", err)
	}

	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: 16 * PGPageSize,
	})
	if err != nil {
		b.Fatalf("NewTieredCacheBackend: %v", err)
	}

	ctx := context.Background()
	key := pageserver.RelBlockKey(1663, 5, 60001, 0, 0)
	if _, err := cache.GetPageAtLSN(ctx, tenantID, key, lsn); err != nil {
		b.Fatalf("warm L1: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cache.GetPageAtLSN(ctx, tenantID, key, lsn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadEngineL2NVMePage(b *testing.B) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(b.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(b.TempDir(), "layers"),
	})
	if err != nil {
		b.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID = "bench-l2"
		gen      = "gen-1"
		lsn      = uint64(0x10000)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)
	pg := bytes.Repeat([]byte{0x66}, PGPageSize)
	if err := ps.PutPageBatch(context.Background(), tenantID, 1663, 5, 60002, 0, 0, 1, lsn, pg); err != nil {
		b.Fatalf("PutPageBatch: %v", err)
	}

	// L1MaxBytes = 1 byte disables L1 storage so every read exercises L2 NVMe + CRC32C.
	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: 1,
		L2Dir:      filepath.Join(b.TempDir(), "l2"),
		L2MaxBytes: 16 * PGPageSize,
	})
	if err != nil {
		b.Fatalf("NewTieredCacheBackend: %v", err)
	}

	ctx := context.Background()
	key := pageserver.RelBlockKey(1663, 5, 60002, 0, 0)
	if _, err := cache.GetPageAtLSN(ctx, tenantID, key, lsn); err != nil {
		b.Fatalf("warm L2: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cache.GetPageAtLSN(ctx, tenantID, key, lsn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadEngineL3PageserverPage(b *testing.B) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(b.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(b.TempDir(), "layers"),
	})
	if err != nil {
		b.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID = "bench-l3"
		gen      = "gen-1"
		lsn      = uint64(0x10000)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)
	pg := bytes.Repeat([]byte{0x77}, PGPageSize)
	if err := ps.PutPageBatch(context.Background(), tenantID, 1663, 5, 60003, 0, 0, 1, lsn, pg); err != nil {
		b.Fatalf("PutPageBatch: %v", err)
	}

	ctx := context.Background()
	key := pageserver.RelBlockKey(1663, 5, 60003, 0, 0)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ps.GetPageAtLSN(ctx, tenantID, key, lsn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadEngineMVCCWithCLOG(b *testing.B) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(b.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(b.TempDir(), "layers"),
	})
	if err != nil {
		b.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID = "bench-mvcc"
		gen      = "gen-1"
		lsn      = uint64(0x10000)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)

	xactPg := make([]byte, PGPageSize)
	_ = SetCLOGStatus(xactPg, 100, XactStatusCommitted)

	backend := &slruMockBackend{
		Pageserver: ps,
		slruPages: map[slruCacheKey][]byte{
			{tenantID: tenantID, lsn: lsn, slruKind: SLRUXactKind, pageNum: 0}: xactPg,
		},
	}
	coord := NewSnapshotCoordinator()
	coord.Publish(tenantID, gen, SnapshotInfo{LSN: lsn, Xmin: 99, Xmax: 105})
	ad := NewAdapter(backend, coord)
	resolver := NewCLOGResolver(backend)
	ctx := context.Background()
	snap, err := ad.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		b.Fatalf("GetSnapshot: %v", err)
	}
	defer ad.ReleaseSnapshot(snap)

	tup := HeapTupleHeader{Xmin: 100}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		vis, err := IsVisibleWithCLOG(ctx, tup, snap, resolver)
		if err != nil || !vis {
			b.Fatalf("unexpected vis=%v err=%v", vis, err)
		}
	}
}

func BenchmarkReadEngineCommitPublish(b *testing.B) {
	coords := NewSnapshotCoordinator()
	pub, err := NewCommitPublisher(coords, nil)
	if err != nil {
		b.Fatalf("NewCommitPublisher: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := pub.PublishCommit(CommitEvent{
			TenantID:   "bench-pub",
			Generation: "gen-1",
			CommitLSN:  uint64(i + 1),
			Xmin:       1,
			Xmax:       uint32(i + 2),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadEngineSQLiteQuery(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench.sqlite3")
	writer, err := OpenSQLiteConn(dbPath, false, 32768)
	if err != nil {
		b.Fatalf("OpenSQLiteConn writer: %v", err)
	}
	defer writer.Close()

	if err := writer.Exec(`
		CREATE TABLE items (id INT4 PRIMARY KEY, val TEXT);
		INSERT INTO items VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');
	`); err != nil {
		b.Fatalf("seed: %v", err)
	}

	reader, err := OpenSQLiteConn(dbPath, true, 32768)
	if err != nil {
		b.Fatalf("OpenSQLiteConn reader: %v", err)
	}
	defer reader.Close()

	args := []string{"2"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := reader.Query("SELECT val FROM items WHERE id = ?1", args)
		if err != nil || len(res.Rows) != 1 {
			b.Fatalf("Query failed: err=%v rows=%d", err, len(res.Rows))
		}
	}
}

