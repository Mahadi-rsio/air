package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

func TestPhase12StorageIntrospectionSizesAcrossForksDropsTruncateAndRestart(t *testing.T) {
	ctx := context.Background()
	r2 := newMemR2Server()
	defer r2.Close()

	dir := t.TempDir()
	kms, err := crypto.LoadOrCreateEnvelopeKMS(filepath.Join(dir, "master.key"))
	if err != nil {
		t.Fatal(err)
	}
	tenantID := "t_introspect"
	gen := "gen_intro_1"
	if _, err := kms.EnsureTenantDEK(tenantID); err != nil {
		t.Fatal(err)
	}

	store := NewLayerStore(filepath.Join(dir, "layers"), r2.Client(), "air-bucket", kms, nil)
	tl := NewTimeline(tenantID, 1, store, nil)
	tl.SetGeneration(gen)

	const spcOID = 1663
	const db1OID = 16385
	const db2OID = 16386
	const relHeap = 30001
	const relIdx = 30002
	const relToast = 30003
	const relOtherDB = 30004
	const relMultiSeg = 30005

	// 1. Heap: main fork (8 blocks), FSM fork (2 blocks), VM fork (1 block), init fork (1 block)
	if err := tl.PutPageBatch(ctx, spcOID, db1OID, relHeap, 0, 0, 8, 100, bytes.Repeat([]byte{0x11}, 8*PageSize)); err != nil {
		t.Fatal(err)
	}
	if err := tl.PutPageBatch(ctx, spcOID, db1OID, relHeap, 1, 0, 2, 110, bytes.Repeat([]byte{0x12}, 2*PageSize)); err != nil {
		t.Fatal(err)
	}
	if err := tl.PutPageBatch(ctx, spcOID, db1OID, relHeap, 2, 0, 1, 120, bytes.Repeat([]byte{0x13}, PageSize)); err != nil {
		t.Fatal(err)
	}
	if err := tl.PutPageBatch(ctx, spcOID, db1OID, relHeap, 3, 0, 1, 130, bytes.Repeat([]byte{0x14}, PageSize)); err != nil {
		t.Fatal(err)
	}

	// 2. Index: 4 blocks main fork
	if err := tl.PutPageBatch(ctx, spcOID, db1OID, relIdx, 0, 0, 4, 140, bytes.Repeat([]byte{0x21}, 4*PageSize)); err != nil {
		t.Fatal(err)
	}

	// 3. TOAST: 6 blocks main fork + 1 block FSM
	if err := tl.PutPageBatch(ctx, spcOID, db1OID, relToast, 0, 0, 6, 150, bytes.Repeat([]byte{0x31}, 6*PageSize)); err != nil {
		t.Fatal(err)
	}
	if err := tl.PutPageBatch(ctx, spcOID, db1OID, relToast, 1, 0, 1, 155, bytes.Repeat([]byte{0x32}, PageSize)); err != nil {
		t.Fatal(err)
	}

	// 4. Multi-segment relation: write block 131072 (segment 1, block 0 -> 131073 blocks total)
	if err := tl.PutPageBatch(ctx, spcOID, db1OID, relMultiSeg, 0, 131072, 1, 160, bytes.Repeat([]byte{0x41}, PageSize)); err != nil {
		t.Fatal(err)
	}

	// 5. Second database (db2OID): 3 blocks
	if err := tl.PutPageBatch(ctx, spcOID, db2OID, relOtherDB, 0, 0, 3, 170, bytes.Repeat([]byte{0x51}, 3*PageSize)); err != nil {
		t.Fatal(err)
	}

	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}

	checkFork := func(tlCheck *Timeline, db, rel uint32, fork uint8, want uint32) {
		t.Helper()
		got, err := tlCheck.GetRelSize(ctx, spcOID, db, rel, fork, 0)
		if err != nil {
			t.Fatalf("GetRelSize(db=%d rel=%d fork=%d): %v", db, rel, fork, err)
		}
		if got != want {
			t.Fatalf("GetRelSize(db=%d rel=%d fork=%d) = %d blocks, want %d", db, rel, fork, got, want)
		}
	}

	checkFork(tl, db1OID, relHeap, 0, 8)
	checkFork(tl, db1OID, relHeap, 1, 2)
	checkFork(tl, db1OID, relHeap, 2, 1)
	checkFork(tl, db1OID, relHeap, 3, 1)
	checkFork(tl, db1OID, relIdx, 0, 4)
	checkFork(tl, db1OID, relToast, 0, 6)
	checkFork(tl, db1OID, relToast, 1, 1)
	checkFork(tl, db1OID, relMultiSeg, 0, 131073)
	checkFork(tl, db2OID, relOtherDB, 0, 3)

	// Expected DB1 size = (8+2+1+1 + 4 + 6+1 + 131073) * PageSize = 131096 * 8192
	wantDB1Bytes := uint64(8+2+1+1+4+6+1+131073) * uint64(PageSize)
	gotDB1Bytes, err := tl.GetDBSize(ctx, spcOID, db1OID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if gotDB1Bytes != wantDB1Bytes {
		t.Fatalf("GetDBSize(db1) = %d, want %d", gotDB1Bytes, wantDB1Bytes)
	}
	gotDB2Bytes, err := tl.GetDBSize(ctx, spcOID, db2OID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if gotDB2Bytes != uint64(3*PageSize) {
		t.Fatalf("GetDBSize(db2) = %d, want %d", gotDB2Bytes, 3*PageSize)
	}

	// Truncate relHeap main fork from 8 blocks -> 3 blocks via XLogSmgrTruncate WAL record.
	truncData := make([]byte, 20)
	binary.LittleEndian.PutUint32(truncData[0:4], 3) // blkno = 3
	binary.LittleEndian.PutUint32(truncData[4:8], spcOID)
	binary.LittleEndian.PutUint32(truncData[8:12], db1OID)
	binary.LittleEndian.PutUint32(truncData[12:16], relHeap)
	binary.LittleEndian.PutUint32(truncData[16:20], 1<<0) // main fork
	tl.ApplyWALRecord(&DecodedWALRecord{
		Rmid:     RmSmgrID,
		Info:     XLogSmgrTruncate,
		EndLSN:   200,
		MainData: truncData,
	})
	checkFork(tl, db1OID, relHeap, 0, 3)

	// Unlink (drop) relMultiSeg main fork.
	tl.UnlinkRel(spcOID, db1OID, relMultiSeg, 0, 210)
	exists, err := tl.GetRelExists(ctx, spcOID, db1OID, relMultiSeg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected relMultiSeg to be unlinked (exists=false)")
	}

	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}

	// Compact and then simulate full restart (cold LayerStore from R2).
	for i := 0; i < 3; i++ {
		lsn := uint64(300 + i*10)
		if err := tl.PutPageBatch(ctx, spcOID, db1OID, relIdx, 0, uint32(i), 1, lsn, bytes.Repeat([]byte{0x22}, PageSize)); err != nil {
			t.Fatal(err)
		}
		if _, err := tl.FlushInMemory(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tl.CompactIfNeeded(ctx, 4, true); err != nil {
		t.Fatalf("CompactIfNeeded: %v", err)
	}

	storeRestart := NewLayerStore(filepath.Join(dir, "restart"), r2.Client(), "air-bucket", kms, nil)
	tlRestart := NewTimeline(tenantID, 1, storeRestart, nil)
	tlRestart.SetGeneration(gen)
	if err := tlRestart.LoadLayersFromR2(ctx); err != nil {
		t.Fatal(err)
	}

	checkFork(tlRestart, db1OID, relHeap, 0, 3) // truncated size preserved
	checkFork(tlRestart, db1OID, relHeap, 1, 2)
	checkFork(tlRestart, db1OID, relIdx, 0, 4)
	checkFork(tlRestart, db1OID, relToast, 0, 6)

	existsRestart, err := tlRestart.GetRelExists(ctx, spcOID, db1OID, relMultiSeg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if existsRestart {
		t.Fatal("expected relMultiSeg to remain unlinked after compaction + restart")
	}

	wantDB1After := uint64(3+2+1+1+4+6+1) * uint64(PageSize)
	gotDB1After, err := tlRestart.GetDBSize(ctx, spcOID, db1OID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if gotDB1After != wantDB1After {
		t.Fatalf("GetDBSize(db1) after truncate+unlink+compact+restart = %d, want %d", gotDB1After, wantDB1After)
	}
}
