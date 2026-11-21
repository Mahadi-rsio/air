package readadapter

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestPageserverBackendAndStaticSnapshotProvider(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const tenantID = "tenant-ro-bridge"
	const generation = "gen-3"
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, generation)

	const (
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(20001)
		forkNum   = uint8(0)
		commitLSN = uint64(0x20000)
	)

	page0 := make([]byte, pageserver.PageSize)
	if err := ps.PutPageBatch(context.Background(), tenantID, spcOID, dbOID, relNumber, forkNum, 0, 1, commitLSN, page0); err != nil {
		t.Fatalf("PutPageBatch: %v", err)
	}

	backend, err := NewPageserverBackend(ps)
	if err != nil {
		t.Fatalf("NewPageserverBackend: %v", err)
	}

	snaps := NewStaticSnapshotProvider()
	snaps.SetSnapshot(tenantID, generation, SnapshotInfo{
		LSN:  commitLSN,
		Xmin: 100,
		Xmax: 101,
	})

	adapter := NewAdapter(backend, snaps)
	snap, err := adapter.GetSnapshot(context.Background(), tenantID, generation)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	defer adapter.ReleaseSnapshot(snap)

	rel, err := adapter.OpenRelation(context.Background(), RelationSpec{
		TenantID:   tenantID,
		Generation: generation,
		SpcOID:     spcOID,
		DbOID:      dbOID,
		RelNumber:  relNumber,
		ForkNum:    forkNum,
	})
	if err != nil {
		t.Fatalf("OpenRelation: %v", err)
	}
	defer adapter.CloseRelation(rel)

	gotPage, err := adapter.ReadPage(context.Background(), rel, snap, 0)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if len(gotPage.Data) != pageserver.PageSize {
		t.Fatalf("expected %d-byte page, got %d", pageserver.PageSize, len(gotPage.Data))
	}

	meta, err := adapter.GetRelationMetadata(context.Background(), rel, snap)
	if err != nil {
		t.Fatalf("GetRelationMetadata: %v", err)
	}
	if meta.NBlocks != 1 {
		t.Fatalf("expected NBlocks=1, got %d", meta.NBlocks)
	}
}

func TestPageserverSocketBackendConcurrencyAndOverload(t *testing.T) {
	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "ps.sock")
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: sockPath,
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()
	if err := ps.Start(context.Background()); err != nil {
		t.Fatalf("ps.Start: %v", err)
	}

	const tenantID = "tenant-sock-pool"
	const spcOID = uint32(1663)
	const dbOID = uint32(5)
	const relNum = uint32(20050)
	const lsn = uint64(0x20000)

	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, "gen-1")

	const numBlocks = 16
	batch := make([]byte, numBlocks*pageserver.PageSize)
	for blk := 0; blk < numBlocks; blk++ {
		off := blk * pageserver.PageSize
		// Stamp block number in first 4 bytes to verify zero cross-talk/corruption.
		batch[off] = byte(blk)
		batch[off+1] = byte(blk >> 8)
		batch[off+2] = 0xAA
		batch[off+3] = 0x55
	}
	if err := ps.PutPageBatch(context.Background(), tenantID, spcOID, dbOID, relNum, 0, 0, numBlocks, lsn, batch); err != nil {
		t.Fatalf("PutPageBatch: %v", err)
	}

	sb := NewPageserverSocketBackendWithConfig(PageserverSocketConfig{
		SocketPath:     sockPath,
		MaxConns:       4,
		MaxInFlight:    4,
		QueueCapacity:  64,
		RequestTimeout: 5 * 1e9,
	})
	defer sb.Close()

	// Fire 48 concurrent page reads across the 4-connection pool and verify exact block correlation.
	const workers = 48
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		blk := uint32(w % numBlocks)
		go func(b uint32) {
			pg, err := sb.GetPageAtLSN(context.Background(), tenantID, pageserver.RelBlockKey(spcOID, dbOID, relNum, 0, b), lsn)
			if err != nil {
				errCh <- err
				return
			}
			if len(pg) != pageserver.PageSize || pg[0] != byte(b) || pg[2] != 0xAA || pg[3] != 0x55 {
				errCh <- context.DeadlineExceeded
				return
			}
			errCh <- nil
		}(blk)
	}
	for i := 0; i < workers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent GetPageAtLSN failed: %v", err)
		}
	}

	st := sb.Stats()
	if st.ActiveConns < 1 || st.ActiveConns > 4 {
		t.Fatalf("expected ActiveConns in [1,4], got %d", st.ActiveConns)
	}
	if st.RequestsTotal != workers {
		t.Fatalf("expected RequestsTotal=%d, got %d", workers, st.RequestsTotal)
	}
}
