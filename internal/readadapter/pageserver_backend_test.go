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
