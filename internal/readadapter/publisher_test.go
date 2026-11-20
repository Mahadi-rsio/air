package readadapter

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestCommitPublisherInvalidationAndSessionReadYourOwnWrite(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID  = "tenant-pub"
		gen1      = "gen-1"
		gen2      = "gen-2"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(40001)
		forkNum   = uint8(0)
		lsn1      = uint64(0x10000)
		lsn2      = uint64(0x20000)
		sessID    = "sess-42"
	)

	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen1)

	pageV1 := bytes.Repeat([]byte{0x11}, PGPageSize)
	pageV2 := bytes.Repeat([]byte{0x22}, PGPageSize)
	if err := ps.PutPageBatch(context.Background(), tenantID, spcOID, dbOID, relNumber, forkNum, 0, 1, lsn1, pageV1); err != nil {
		t.Fatalf("PutPageBatch v1: %v", err)
	}
	if err := ps.PutPageBatch(context.Background(), tenantID, spcOID, dbOID, relNumber, forkNum, 0, 1, lsn2, pageV2); err != nil {
		t.Fatalf("PutPageBatch v2: %v", err)
	}

	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: 16 * PGPageSize,
		L2Dir:      filepath.Join(t.TempDir(), "l2"),
		L2MaxBytes: 32 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend: %v", err)
	}

	coords := NewSnapshotCoordinator()
	pub, err := NewCommitPublisher(coords, cache)
	if err != nil {
		t.Fatalf("NewCommitPublisher: %v", err)
	}

	// Publish initial commit at lsn1 and warm cache.
	if err := pub.PublishCommit(CommitEvent{
		TenantID:   tenantID,
		Generation: gen1,
		CommitLSN:  lsn1,
		Xmin:       100,
		Xmax:       101,
	}); err != nil {
		t.Fatalf("PublishCommit lsn1: %v", err)
	}

	ctx := context.Background()
	key0 := pageserver.RelBlockKey(spcOID, dbOID, relNumber, forkNum, 0)
	got1, err := cache.GetPageAtLSN(ctx, tenantID, key0, lsn1)
	if err != nil || !bytes.Equal(got1, pageV1) {
		t.Fatalf("expected pageV1 at lsn1, err=%v", err)
	}

	// Session performs a write at lsn2 on Write Engine; Read Engine hasn't published lsn2 yet.
	pub.RecordSessionWrite(sessID, lsn2)

	shortCtx, shortCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer shortCancel()
	if _, err := pub.AwaitSessionRead(shortCtx, tenantID, gen1, sessID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded before lsn2 publish, got %v", err)
	}

	// Publish commit at lsn2 asynchronously with block 0 invalidation.
	go func() {
		time.Sleep(15 * time.Millisecond)
		_ = pub.PublishCommit(CommitEvent{
			TenantID:   tenantID,
			Generation: gen1,
			CommitLSN:  lsn2,
			Xmin:       100,
			Xmax:       102,
			ChangedBlocks: []BlockCoord{
				{SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum, BlockNum: 0},
			},
		})
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, time.Second)
	defer waitCancel()
	snapInfo, err := pub.AwaitSessionRead(waitCtx, tenantID, gen1, sessID)
	if err != nil || snapInfo.LSN != lsn2 {
		t.Fatalf("AwaitSessionRead failed: info=%+v err=%v", snapInfo, err)
	}

	// Reading block 0 at lsn2 returns pageV2, and old lsn1 entry was invalidated.
	got2, err := cache.GetPageAtLSN(ctx, tenantID, key0, lsn2)
	if err != nil || !bytes.Equal(got2, pageV2) {
		t.Fatalf("expected pageV2 at lsn2, err=%v", err)
	}

	// Generation fence invalidates gen1 and publishes gen2 baseline.
	ps.SetTenantGeneration(tenantID, gen2)
	pub.OnGenerationFence(tenantID, gen1, gen2, SnapshotInfo{LSN: lsn2, Xmin: 100, Xmax: 102})

	st := pub.Stats()
	if st.CommitsPublished != 2 || st.BlocksInvalidated != 1 || st.FencesProcessed != 1 {
		t.Fatalf("unexpected publisher stats: %+v", st)
	}
}
