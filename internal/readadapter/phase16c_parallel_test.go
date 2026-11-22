package readadapter

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func makeTwoInt4Payload(a, b int32) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(a))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(b))
	return buf
}

func TestPhase16C_CostBasedParallelSelection(t *testing.T) {
	opt := NewQueryOptimizer(DefaultCostModelConfig(), nil, nil, nil)

	// Small table (10 blocks): must stay serial
	smallDec := opt.ChooseBestPlan(
		DiscoveredTable{RelOID: 1, RelPages: 10, RelTuples: 500},
		nil, []ColumnType{ColInt4, ColText}, nil, nil, 2, 10, "t1", "g1",
	)
	if smallDec.Selected.ParallelWorkers != 1 {
		t.Fatalf("expected small table to stay serial (ParallelWorkers=1), got %d", smallDec.Selected.ParallelWorkers)
	}

	// Large table (1024 blocks): on <=2 vCPU nodes, measured parallel overhead exceeds
	// serial scan for L1-resident pages so optimizer retains serial (1 worker);
	// on >=4 vCPU nodes, optimizer enables >=2 workers.
	largeDec := opt.ChooseBestPlan(
		DiscoveredTable{RelOID: 2, RelPages: 1024, RelTuples: 50000},
		nil, []ColumnType{ColInt4, ColText}, nil, nil, 2, 1024, "t1", "g1",
	)
	if runtime.GOMAXPROCS(0) >= 4 && largeDec.Selected.ParallelWorkers < 2 {
		t.Fatalf("expected large 1024-page scan on >=4 CPUs to select ParallelWorkers >= 2, got %d", largeDec.Selected.ParallelWorkers)
	}
	if runtime.GOMAXPROCS(0) < 4 && largeDec.Selected.ParallelWorkers != 1 {
		t.Fatalf("expected <=2 CPU node to retain serial execution, got %d", largeDec.Selected.ParallelWorkers)
	}
	if okPar, _ := opt.ShouldParallelize(largeDec.Selected, runtime.GOMAXPROCS(0)); okPar {
		t.Fatalf("expected ShouldParallelize to return false when activeGlobalQueries >= GOMAXPROCS")
	}
}

func TestPhase16C_ParallelScanAndAggregateCorrectness(t *testing.T) {
	ctx := context.Background()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	const (
		tenantID  = "t_par"
		gen       = "g1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(29001)
		lsn       = uint64(0x100)
		nBlocks   = 80
		rowsPerPg = 20
	)
	ps.SetTenantGeneration(tenantID, gen)
	_ = ps.EnsureTenant(tenantID, 1)

	cols := []ColumnType{ColInt4, ColInt4}
	var expectedSum int64
	for blk := uint32(0); blk < nBlocks; blk++ {
		var tuples [][]byte
		for r := 0; r < rowsPerPg; r++ {
			id := int32(int(blk)*rowsPerPg + r + 1)
			val := int32((r + 1) * 10)
			expectedSum += int64(val)
			tuples = append(tuples, buildHeapTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, makeTwoInt4Payload(id, val)))
		}
		pg := buildPG17HeapPage(lsn, tuples)
		if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, 0, blk, 1, lsn, pg); err != nil {
			t.Fatal(err)
		}
	}

	snapProv := NewStaticSnapshotProvider()
	snapProv.SetSnapshot(tenantID, gen, SnapshotInfo{LSN: lsn})
	ad := NewAdapter(ps, snapProv)
	clog := NewCLOGResolver(ps)
	snap, err := ad.GetSnapshot(ctx, tenantID, gen)
	if err != nil {
		t.Fatal(err)
	}
	defer ad.ReleaseSnapshot(snap)

	rel, err := ad.OpenRelation(ctx, RelationSpec{
		TenantID: tenantID, Generation: gen, SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ad.CloseRelation(rel)

	// 1. ParallelHeapAggregate
	cnt, sum, err := ParallelHeapAggregate(ctx, ad, clog, rel, snap, cols, 1, nBlocks, 2)
	if err != nil {
		t.Fatalf("ParallelHeapAggregate: %v", err)
	}
	wantCnt := int64(nBlocks * rowsPerPg)
	if cnt != wantCnt || sum != expectedSum {
		t.Fatalf("ParallelHeapAggregate mismatch: got (cnt=%d, sum=%d) want (cnt=%d, sum=%d)", cnt, sum, wantCnt, expectedSum)
	}

	// 2. parallelScanIter
	it := newParallelScanIter(ctx, ad, clog, rel, snap, cols, []bool{true, true}, nil, nil, nil, nBlocks, 2)
	defer it.Close()
	gotRows := 0
	for {
		_, n, ok, err := it.Next()
		if err != nil {
			t.Fatalf("parallelScanIter.Next: %v", err)
		}
		if !ok {
			break
		}
		gotRows += n
	}
	if gotRows != int(wantCnt) {
		t.Fatalf("parallelScanIter row count mismatch: got %d want %d", gotRows, wantCnt)
	}
}

func BenchmarkPhase16C_SerialVsParallelHeapAggregate(b *testing.B) {
	ctx := context.Background()
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(b.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(b.TempDir(), "layers"),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer ps.Close()
	if err := ps.Start(ctx); err != nil {
		b.Fatal(err)
	}
	const (
		tenantID  = "t_par_b"
		gen       = "g1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(29002)
		lsn       = uint64(0x100)
		nBlocks   = 128
		rowsPerPg = 40
	)
	ps.SetTenantGeneration(tenantID, gen)
	_ = ps.EnsureTenant(tenantID, 1)
	cols := []ColumnType{ColInt4, ColInt4}
	for blk := uint32(0); blk < nBlocks; blk++ {
		var tuples [][]byte
		for r := 0; r < rowsPerPg; r++ {
			id := int32(int(blk)*rowsPerPg + r + 1)
			tuples = append(tuples, buildHeapTuple(100, 0, HeapXminCommitted|HeapXmaxInvalid, makeTwoInt4Payload(id, int32(r+1))))
		}
		if err := ps.PutPageBatch(ctx, tenantID, spcOID, dbOID, relNumber, 0, blk, 1, lsn, buildPG17HeapPage(lsn, tuples)); err != nil {
			b.Fatal(err)
		}
	}
	snapProv := NewStaticSnapshotProvider()
	snapProv.SetSnapshot(tenantID, gen, SnapshotInfo{LSN: lsn})
	ad := NewAdapter(ps, snapProv)
	clog := NewCLOGResolver(ps)
	snap, _ := ad.GetSnapshot(ctx, tenantID, gen)
	defer ad.ReleaseSnapshot(snap)
	rel, _ := ad.OpenRelation(ctx, RelationSpec{
		TenantID: tenantID, Generation: gen, SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber,
	})
	defer ad.CloseRelation(rel)

	b.Run("Serial_1Worker_128Pages", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cnt, _, err := ParallelHeapAggregate(ctx, ad, clog, rel, snap, cols, 1, nBlocks, 1)
			if err != nil || cnt != nBlocks*rowsPerPg {
				b.Fatal(err)
			}
		}
	})
	b.Run("Parallel_2Workers_128Pages", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cnt, _, err := ParallelHeapAggregate(ctx, ad, clog, rel, snap, cols, 1, nBlocks, 2)
			if err != nil || cnt != nBlocks*rowsPerPg {
				b.Fatal(err)
			}
		}
	})
}
