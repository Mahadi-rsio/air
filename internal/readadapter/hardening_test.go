package readadapter

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestHardeningConcurrentReadsWritesInvalidations(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantA   = "hardening-a"
		tenantB   = "hardening-b"
		gen       = "gen-1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(50001)
		forkNum   = uint8(0)
		numBlocks = uint32(8)
		numEpochs = 40
	)
	ps.EnsureTenant(tenantA, 1)
	ps.SetTenantGeneration(tenantA, gen)
	ps.EnsureTenant(tenantB, 1)
	ps.SetTenantGeneration(tenantB, gen)

	ctx := context.Background()
	for b := uint32(0); b < numBlocks; b++ {
		pgA := bytes.Repeat([]byte{byte(0x10 + b)}, PGPageSize)
		pgB := bytes.Repeat([]byte{byte(0x80 + b)}, PGPageSize)
		if err := ps.PutPageBatch(ctx, tenantA, spcOID, dbOID, relNumber, forkNum, b, 1, 0x1000, pgA); err != nil {
			t.Fatalf("init A: %v", err)
		}
		if err := ps.PutPageBatch(ctx, tenantB, spcOID, dbOID, relNumber, forkNum, b, 1, 0x1000, pgB); err != nil {
			t.Fatalf("init B: %v", err)
		}
	}

	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: 4 * PGPageSize,
		L2Dir:      filepath.Join(t.TempDir(), "l2"),
		L2MaxBytes: 12 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend: %v", err)
	}

	coords := NewSnapshotCoordinator()
	pub, err := NewCommitPublisher(coords, cache)
	if err != nil {
		t.Fatalf("NewCommitPublisher: %v", err)
	}
	_ = pub.PublishCommit(CommitEvent{TenantID: tenantA, Generation: gen, CommitLSN: 0x1000, Xmin: 1, Xmax: 2})
	_ = pub.PublishCommit(CommitEvent{TenantID: tenantB, Generation: gen, CommitLSN: 0x1000, Xmin: 1, Xmax: 2})

	adapter := NewAdapter(cache, coords)

	var wg sync.WaitGroup
	var errCount atomic.Int64

	// Concurrent writers advancing LSNs and invalidating changed blocks.
	for _, tid := range []string{tenantA, tenantB} {
		tid := tid
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := byte(0x10)
			if tid == tenantB {
				base = 0x80
			}
			for ep := 1; ep <= numEpochs; ep++ {
				lsn := uint64(0x1000 + ep*0x100)
				blk := uint32(ep) % numBlocks
				pg := bytes.Repeat([]byte{base + byte(blk)}, PGPageSize)
				if err := ps.PutPageBatch(ctx, tid, spcOID, dbOID, relNumber, forkNum, blk, 1, lsn, pg); err != nil {
					errCount.Add(1)
					return
				}
				if err := pub.PublishCommit(CommitEvent{
					TenantID:   tid,
					Generation: gen,
					CommitLSN:  lsn,
					Xmin:       1,
					Xmax:       uint32(2 + ep),
					ChangedBlocks: []BlockCoord{
						{SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum, BlockNum: blk},
					},
				}); err != nil {
					errCount.Add(1)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Concurrent readers verifying tenant isolation and exact page contents.
	for r := 0; r < 12; r++ {
		tid := tenantA
		base := byte(0x10)
		if r%2 == 1 {
			tid = tenantB
			base = 0x80
		}
		wg.Add(1)
		go func(id int, tenant string, expectedBase byte) {
			defer wg.Done()
			rel, err := adapter.OpenRelation(ctx, RelationSpec{
				TenantID:   tenant,
				Generation: gen,
				SpcOID:     spcOID,
				DbOID:      dbOID,
				RelNumber:  relNumber,
				ForkNum:    forkNum,
			})
			if err != nil {
				errCount.Add(1)
				return
			}
			defer adapter.CloseRelation(rel)

			for iter := 0; iter < 30; iter++ {
				snap, err := adapter.GetSnapshot(ctx, tenant, gen)
				if err != nil {
					errCount.Add(1)
					return
				}
				blk := uint32((id + iter) % int(numBlocks))
				pg, err := adapter.ReadPage(ctx, rel, snap, blk)
				_ = adapter.ReleaseSnapshot(snap)
				if err != nil {
					errCount.Add(1)
					return
				}
				wantByte := expectedBase + byte(blk)
				if len(pg.Data) != PGPageSize || pg.Data[0] != wantByte || pg.Data[PGPageSize-1] != wantByte {
					errCount.Add(1)
					return
				}
			}
		}(r, tid, base)
	}

	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("observed %d errors during concurrent storm", errCount.Load())
	}
	if adapter.ActiveSnapshotCount() != 0 {
		t.Fatalf("expected 0 leaked snapshots, got %d", adapter.ActiveSnapshotCount())
	}
}

func TestHardeningGenerationFenceAndL2CrashRecovery(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID  = "hardening-fence"
		gen1      = "gen-1"
		gen2      = "gen-2"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(50002)
		forkNum   = uint8(0)
		lsn1      = uint64(0x10000)
		lsn2      = uint64(0x20000)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen1)

	pg1 := bytes.Repeat([]byte{0x41}, PGPageSize)
	pg2 := bytes.Repeat([]byte{0x42}, PGPageSize)
	ctx := context.Background()
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, 0, 1, lsn1, pg1); err != nil {
		t.Fatalf("PutPageBatch 1: %v", err)
	}

	l2Dir := filepath.Join(t.TempDir(), "l2-crash")
	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: PGPageSize,
		L2Dir:      l2Dir,
		L2MaxBytes: 8 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend: %v", err)
	}
	coords := NewSnapshotCoordinator()
	pub, err := NewCommitPublisher(coords, cache)
	if err != nil {
		t.Fatalf("NewCommitPublisher: %v", err)
	}
	_ = pub.PublishCommit(CommitEvent{TenantID: tenantID, Generation: gen1, CommitLSN: lsn1, Xmin: 1, Xmax: 2})

	adapter := NewAdapter(cache, coords)
	snap1, err := adapter.GetSnapshot(ctx, tenantID, gen1)
	if err != nil {
		t.Fatalf("GetSnapshot gen1: %v", err)
	}
	rel1, err := adapter.OpenRelation(ctx, RelationSpec{
		TenantID: tenantID, Generation: gen1, SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum,
	})
	if err != nil {
		t.Fatalf("OpenRelation gen1: %v", err)
	}
	if _, err := adapter.ReadPage(ctx, rel1, snap1, 0); err != nil {
		t.Fatalf("ReadPage gen1: %v", err)
	}

	// Simulate primary failover / fencing gen1 -> gen2.
	ps.SetTenantGeneration(tenantID, gen2)
	ps.EnsureTenant(tenantID, 2)
	if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, forkNum, 0, 1, lsn2, pg2); err != nil {
		t.Fatalf("PutPageBatch 2: %v", err)
	}
	pub.OnGenerationFence(tenantID, gen1, gen2, SnapshotInfo{LSN: lsn2, Xmin: 1, Xmax: 3})

	// Stale gen1 handle must fail with ErrStaleGeneration.
	if _, err := adapter.ReadPage(ctx, rel1, snap1, 0); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration for fenced gen1 read, got %v", err)
	}
	_ = adapter.ReleaseSnapshot(snap1)
	_ = adapter.CloseRelation(rel1)

	// New gen2 handle succeeds and reads pg2.
	snap2, err := adapter.GetSnapshot(ctx, tenantID, gen2)
	if err != nil {
		t.Fatalf("GetSnapshot gen2: %v", err)
	}
	defer adapter.ReleaseSnapshot(snap2)
	rel2, err := adapter.OpenRelation(ctx, RelationSpec{
		TenantID: tenantID, Generation: gen2, SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum,
	})
	if err != nil {
		t.Fatalf("OpenRelation gen2: %v", err)
	}
	defer adapter.CloseRelation(rel2)

	got, err := adapter.ReadPage(ctx, rel2, snap2, 0)
	if err != nil || !bytes.Equal(got.Data, pg2) {
		t.Fatalf("gen2 read failed: err=%v", err)
	}

	// Simulate L2 NVMe crash truncation on gen2's L2 file + evict L1 so L2 is hit.
	ck := PageCacheKey{
		TenantID: tenantID, Generation: gen2,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum, BlockNum: 0, LSN: lsn2,
	}
	l2Path := cache.l2PathFor(ck)
	if err := os.Truncate(l2Path, 128); err != nil {
		t.Fatalf("truncate L2: %v", err)
	}
	// Evict from L1 by putting another dummy key into L1 directly.
	cache.putL1(PageCacheKey{TenantID: "evict", BlockNum: 99}, make([]byte, PGPageSize))

	recovered, err := adapter.ReadPage(ctx, rel2, snap2, 0)
	if err != nil || !bytes.Equal(recovered.Data, pg2) {
		t.Fatalf("expected automatic recovery after L2 truncation, err=%v", err)
	}
	if cache.Stats().L2Corrupt < 1 {
		t.Fatalf("expected L2Corrupt >= 1 after truncation, got %+v", cache.Stats())
	}
}

