package readadapter

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

type slruMockBackend struct {
	*pageserver.Pageserver
	slruPages map[slruCacheKey][]byte
}

func (m *slruMockBackend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	if key.Kind == pageserver.KeyKindSLRU {
		if pg, ok := m.slruPages[slruCacheKey{
			tenantID: tenantID,
			lsn:      lsn,
			slruKind: key.SpcOID,
			pageNum:  key.BlockNum,
		}]; ok {
			return pg, nil
		}
		return nil, ErrNotFound
	}
	return m.Pageserver.GetPageAtLSN(ctx, tenantID, key, lsn)
}

func TestMVCCVisibilityWithPGXactAndSubtrans(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const tenantID = "tenant-mvcc"
	const gen = "gen-1"
	const lsn1 = uint64(0x10000)
	const lsn2 = uint64(0x20000)

	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)

	// Build pg_xact page 0 at lsn1:
	// - XID 100: committed
	// - XID 101: aborted
	// - XID 102: in-progress at lsn1
	// - XID 103: sub-committed (parent XID 100 via pg_subtrans)
	xactL1 := make([]byte, PGPageSize)
	_ = SetCLOGStatus(xactL1, 100, XactStatusCommitted)
	_ = SetCLOGStatus(xactL1, 101, XactStatusAborted)
	_ = SetCLOGStatus(xactL1, 102, XactStatusInProgress)
	_ = SetCLOGStatus(xactL1, 103, XactStatusSubCommitted)

	subL1 := make([]byte, PGPageSize)
	_ = SetSubtransParent(subL1, 103, 100)

	// At lsn2, XID 102 commits.
	xactL2 := append([]byte(nil), xactL1...)
	_ = SetCLOGStatus(xactL2, 102, XactStatusCommitted)

	backend := &slruMockBackend{
		Pageserver: ps,
		slruPages: map[slruCacheKey][]byte{
			{tenantID: tenantID, lsn: lsn1, slruKind: SLRUXactKind, pageNum: 0}:     xactL1,
			{tenantID: tenantID, lsn: lsn1, slruKind: SLRUSubtransKind, pageNum: 0}: subL1,
			{tenantID: tenantID, lsn: lsn2, slruKind: SLRUXactKind, pageNum: 0}:     xactL2,
			{tenantID: tenantID, lsn: lsn2, slruKind: SLRUSubtransKind, pageNum: 0}: subL1,
		},
	}

	coord := NewSnapshotCoordinator()
	coord.Publish(tenantID, gen, SnapshotInfo{
		LSN:        lsn1,
		Xmin:       99,
		Xmax:       105,
		ActiveXIPs: []uint32{104},
	})

	adapter := NewAdapter(backend, coord)
	resolver := NewCLOGResolver(backend)
	ctx := context.Background()

	snap1, err := adapter.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatalf("GetSnapshot lsn1: %v", err)
	}
	defer adapter.ReleaseSnapshot(snap1)

	// Unhinted tuple (Infomask == 0) inserted by committed XID 100 -> VISIBLE.
	vis, err := IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 100}, snap1, resolver)
	if err != nil || !vis {
		t.Fatalf("expected unhinted XID 100 to be visible at lsn1, got vis=%v err=%v", vis, err)
	}

	// Aborted XID 101 -> INVISIBLE.
	vis, err = IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 101}, snap1, resolver)
	if err != nil || vis {
		t.Fatalf("expected aborted XID 101 to be invisible, got vis=%v err=%v", vis, err)
	}

	// Uncommitted XID 102 at lsn1 -> INVISIBLE (even though it commits later at lsn2).
	vis, err = IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 102}, snap1, resolver)
	if err != nil || vis {
		t.Fatalf("expected XID 102 to be invisible at lsn1, got vis=%v err=%v", vis, err)
	}

	// Subtransaction XID 103 whose parent is committed XID 100 -> VISIBLE.
	vis, err = IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 103}, snap1, resolver)
	if err != nil || !vis {
		t.Fatalf("expected subcommitted XID 103 (parent 100) to be visible, got vis=%v err=%v", vis, err)
	}

	// Active in-flight XID 104 -> INVISIBLE even if hint bit were set.
	vis, err = IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 104, Infomask: HeapXminCommitted}, snap1, resolver)
	if err != nil || vis {
		t.Fatalf("expected active XID 104 to be invisible, got vis=%v err=%v", vis, err)
	}

	// Frozen tuple -> VISIBLE.
	vis, err = IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 50, Infomask: HeapXminFrozen}, snap1, resolver)
	if err != nil || !vis {
		t.Fatalf("expected frozen tuple to be visible, got vis=%v err=%v", vis, err)
	}

	// Tuple inserted by XID 100 and deleted by committed XID 100 -> INVISIBLE.
	vis, err = IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 100, Xmax: 100}, snap1, resolver)
	if err != nil || vis {
		t.Fatalf("expected deleted tuple (xmax=100) to be invisible, got vis=%v err=%v", vis, err)
	}

	// Tuple locked FOR SHARE/KEY SHARE (HeapXmaxLockOnly) -> VISIBLE.
	vis, err = IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 100, Xmax: 100, Infomask: HeapXmaxLockOnly}, snap1, resolver)
	if err != nil || !vis {
		t.Fatalf("expected lock-only tuple to be visible, got vis=%v err=%v", vis, err)
	}

	// Advance snapshot to lsn2: now XID 102 is visible.
	coord.Publish(tenantID, gen, SnapshotInfo{LSN: lsn2, Xmin: 99, Xmax: 105})
	snap2, err := adapter.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatalf("GetSnapshot lsn2: %v", err)
	}
	defer adapter.ReleaseSnapshot(snap2)

	vis, err = IsVisibleWithCLOG(ctx, HeapTupleHeader{Xmin: 102}, snap2, resolver)
	if err != nil || !vis {
		t.Fatalf("expected XID 102 to be visible at lsn2, got vis=%v err=%v", vis, err)
	}
}

func TestSnapshotCoordinatorAwaitLSN(t *testing.T) {
	coord := NewSnapshotCoordinator()
	const tenantID = "tenant-causal"
	const gen = "gen-2"

	coord.Publish(tenantID, gen, SnapshotInfo{LSN: 100, Xmin: 10, Xmax: 20})

	// Causal wait for LSN 200 published asynchronously.
	go func() {
		time.Sleep(25 * time.Millisecond)
		coord.Publish(tenantID, gen, SnapshotInfo{LSN: 200, Xmin: 10, Xmax: 21})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	info, err := coord.AwaitLSN(ctx, tenantID, gen, 200)
	if err != nil {
		t.Fatalf("AwaitLSN failed: %v", err)
	}
	if info.LSN != 200 {
		t.Fatalf("expected LSN 200, got %d", info.LSN)
	}

	// Timeout when LSN never reaches target.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	if _, err := coord.AwaitLSN(shortCtx, tenantID, gen, 999); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}
