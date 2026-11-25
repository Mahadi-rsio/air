package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func buildReproHeapHotUpdateWAL(xid uint32, key Key, oldOff, newOff uint16, payload []byte) []byte {
	// xl_heap_update: old_xmax(4) + old_offnum(2) + old_infobits_set(1) + flags(1) + new_xmax(4) + new_offnum(2) = 14B
	mainData := make([]byte, 14)
	binary.LittleEndian.PutUint32(mainData[0:4], xid)
	binary.LittleEndian.PutUint16(mainData[4:6], oldOff)
	mainData[6] = 0
	mainData[7] = 0
	binary.LittleEndian.PutUint32(mainData[8:12], 0)
	binary.LittleEndian.PutUint16(mainData[12:14], newOff)

	blkData := make([]byte, 5+len(payload))
	binary.LittleEndian.PutUint16(blkData[0:2], 0)
	binary.LittleEndian.PutUint16(blkData[2:4], 0)
	blkData[4] = pgSizeOfHeapTupleHdr
	copy(blkData[5:], payload)

	return buildTestWALRecord(RmHeapID, XLogHeapHotUpdate, xid, key, mainData, blkData)
}

// buildReproHeap2PruneWithFreezePlans builds a PG17 XLOG_HEAP2_PRUNE_ON_ACCESS record
// with xlhpHasFreezePlans set using PG17's exact packed layout (uint16 nplans + plans[nplans]).
func buildReproHeap2PruneWithFreezePlans(xid uint32, key Key, freezeXmax uint32, freezeOffnum, deadOffnum uint16) []byte {
	mainData := []byte{0, xlhpCleanupLock | xlhpHasFreezePlans | xlhpHasDeadItems}
	var blkData []byte
	// PG17 log_heap_prune_and_freeze: uint16 nplans (2 bytes, no padding)
	blkData = binary.LittleEndian.AppendUint16(blkData, 1)
	// xlhp_freeze_plan (12 bytes): xmax(4), t_infomask2(2), t_infomask(2), frzflags(1), pad(1), ntuples(2)
	blkData = binary.LittleEndian.AppendUint32(blkData, freezeXmax)
	blkData = binary.LittleEndian.AppendUint16(blkData, 0)
	blkData = binary.LittleEndian.AppendUint16(blkData, 0x0100|0x0200) // HEAP_XMIN_FROZEN
	blkData = append(blkData, xlhFreezeXvac, 0)
	blkData = binary.LittleEndian.AppendUint16(blkData, 1) // ntuples = 1
	// ndead = 1, deadOffnum
	blkData = binary.LittleEndian.AppendUint16(blkData, 1)
	blkData = binary.LittleEndian.AppendUint16(blkData, deadOffnum)
	// frozen[0] = freezeOffnum
	blkData = binary.LittleEndian.AppendUint16(blkData, freezeOffnum)

	return buildTestWALRecord(RmHeap2ID, XLogHeap2PruneOnAccess, xid, key, mainData, blkData)
}

func TestRepro_HotBlockWALRedoAndDeltaChain(t *testing.T) {
	dir := t.TempDir()
	layerDir := filepath.Join(dir, "layers")
	binPath := filepath.Join(dir, "air-walredo")
	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/air-walredo")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build air-walredo: %v\n%s", err, out)
	}

	ps, err := New(Config{
		LayerDir:          layerDir,
		DisableUnixSocket: true,
		WalRedoBin:        binPath,
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}

	ctx := context.Background()
	tenant := "k1"
	tl := ps.EnsureTenant(tenant, 1)
	key := RelBlockKey(1663, 16385, 16396, 0, 0)

	// 1. Seed initial FPI at LSN 100 (empty initialized heap page + tuple 1).
	basePage := make([]byte, PageSize)
	initPGHeapPage(basePage)
	tup1 := buildTupleFromXLHeader(10, 0, 0, 1, 0, pgHeapXmaxInvalid, pgSizeOfHeapTupleHdr, []byte("v000"))
	pageAddHeapItem(basePage, 1, tup1)
	tl.PutPage(key, 100, basePage)

	// 2. Accumulate >300 updates across multiple L0 flushes + XLOG_HEAP2_PRUNE_ON_ACCESS (with freeze plan)
	// and RmXLogID FPI_FOR_HINT records.
	lsn := uint64(200)
	curOff := uint16(1)
	var prePruneLSN, postPruneLSN uint64
	var prePruneOff, frozenOff uint16
	var prePrunePayload []byte

	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 30; i++ {
			nextOff := curOff + 1
			if nextOff > 20 {
				nextOff = 2
			}
			payload := []byte{byte((batch*30 + i) & 0xFF)}
			raw := buildReproHeapHotUpdateWAL(uint32(100+batch*30+i), key, curOff, nextOff, payload)
			rec, derr := DecodeWALRecord(lsn, raw)
			if derr != nil {
				t.Fatalf("DecodeWALRecord: %v", derr)
			}
			tl.ApplyWALRecord(rec)
			curOff = nextOff
			if batch == 4 && i == 29 {
				prePruneLSN = rec.EndLSN
				prePruneOff = curOff
				prePrunePayload = append([]byte(nil), payload...)
			}
			lsn = rec.EndLSN + 10
		}
		if batch == 5 {
			// Also include an XLOG_FPI_FOR_HINT record for another block (mixed RMID chain).
			hintRec := buildTestWALRecord(RmXLogID, XLogXLogFPIHint, 499, RelBlockKey(1663, 16385, 16396, 0, 99), nil, nil)
			tl.ApplyWALRecord(&DecodedWALRecord{LSN: lsn, EndLSN: lsn + 1, Rmid: RmXLogID, Info: XLogXLogFPIHint, RawRecord: hintRec, Blocks: []WALBlockRef{{Rel: RelFileLocator{1663, 16385, 16396}, ForkNum: 0, BlockNum: 0}}})
			lsn += 10

			// Simulate PG17 HOT prune with freeze plan on hot block 16396.0#0.
			frozenOff = curOff
			pruneRaw := buildReproHeap2PruneWithFreezePlans(500, key, 200, frozenOff, 1)
			rec, derr := DecodeWALRecord(lsn, pruneRaw)
			if derr != nil {
				t.Fatalf("DecodeWALRecord(prune): %v", derr)
			}
			tl.ApplyWALRecord(rec)
			postPruneLSN = rec.EndLSN
			lsn = rec.EndLSN + 10
		}
		if _, err := tl.FlushInMemory(ctx); err != nil {
			t.Fatalf("FlushInMemory batch %d: %v", batch, err)
		}
	}

	// Trigger L0 -> L1 batch compaction (same path used by MemoryBudget).
	// With > 256 deltas in the compacted batch, L1 must materialize a checkpoint FPI.
	l1Meta, err := tl.CompactL0Batch(ctx, 4, 8)
	if err != nil {
		t.Fatalf("CompactL0Batch: %v", err)
	}
	if l1Meta == nil {
		t.Fatalf("expected L1 compaction to produce a layer")
	}
	l1Deltas, err := ps.Store().LookupKeyInLayer(ctx, *l1Meta, key, 0)
	if err != nil {
		t.Fatalf("LookupKeyInLayer(L1): %v", err)
	}
	if _, hasFPI := latestFPILSN(l1Deltas); !hasFPI {
		t.Fatalf("expected L1 compaction to materialize a checkpoint FPI for bounded delta chain (deltas=%d)", len(l1Deltas))
	}

	verifySnapshots := func(target *Timeline) {
		// Check pre-prune MVCC snapshot.
		pgPre, err := target.GetPageAtLSN(ctx, key, prePruneLSN)
		if err != nil {
			t.Fatalf("GetPageAtLSN(prePruneLSN): %v", err)
		}
		gotPre := pageGetHeapTuple(pgPre, prePruneOff)
		if gotPre == nil || !bytes.Equal(gotPre[pgSizeOfHeapTupleHdr:], prePrunePayload) {
			t.Fatalf("prePrune snapshot mismatch at off %d", prePruneOff)
		}

		// Check post-prune MVCC snapshot: frozenOff must have xmin == 2 (FrozenTransactionId) and LP 1 == LP_DEAD.
		pgPost, err := target.GetPageAtLSN(ctx, key, postPruneLSN)
		if err != nil {
			t.Fatalf("GetPageAtLSN(postPruneLSN): %v", err)
		}
		gotFrozen := pageGetHeapTuple(pgPost, frozenOff)
		if gotFrozen == nil {
			t.Fatalf("missing frozen tuple at offnum %d", frozenOff)
		}
		if xvac := binary.LittleEndian.Uint32(gotFrozen[8:12]); xvac != 2 {
			t.Fatalf("expected frozen xvac=2 at offnum %d, got %d", frozenOff, xvac)
		}
		lp1 := binary.LittleEndian.Uint32(pgPost[pgPageHeaderSize : pgPageHeaderSize+4])
		if ((lp1 >> 15) & 0x03) != pgLPDead {
			t.Fatalf("expected LP 1 to be LP_DEAD after prune, got flags=%d", (lp1>>15)&0x03)
		}

		// Check latest snapshot.
		pgLatest, err := target.GetPageAtLSN(ctx, key, 0)
		if err != nil {
			t.Fatalf("GetPageAtLSN(latest): %v", err)
		}
		latestTup := pageGetHeapTuple(pgLatest, curOff)
		wantLatest := []byte{byte((9*30 + 29) & 0xFF)}
		if latestTup == nil || !bytes.Equal(latestTup[pgSizeOfHeapTupleHdr:], wantLatest) {
			t.Fatalf("latest snapshot mismatch at off %d: got=%v want=%v", curOff, latestTup, wantLatest)
		}
	}

	verifySnapshots(tl)

	// 4. Simulate crash/restart recovery: close Pageserver and reopen on same LayerDir.
	if err := ps.Close(); err != nil {
		t.Fatalf("ps.Close: %v", err)
	}
	ps2, err := New(Config{
		LayerDir:          layerDir,
		DisableUnixSocket: true,
		WalRedoBin:        binPath,
	})
	if err != nil {
		t.Fatalf("pageserver.New(restart): %v", err)
	}
	defer ps2.Close()

	tl2 := ps2.EnsureTenant(tenant, 1)
	verifySnapshots(tl2)
}

func TestWALRedo_WorkerErrorIncludesStderr(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "air-walredo")
	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/air-walredo")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build air-walredo: %v\n%s", err, out)
	}
	redoer := NewWALRedoPool(WALRedoPoolConfig{
		WorkerBin: binPath,
		Workers:   1,
		QueueSize: 4,
	})
	defer redoer.Close()

	key := RelBlockKey(1663, 16385, 16396, 0, 0)
	base := make([]byte, PageSize)
	badRec := buildTestWALRecord(99, 0x00, 1, key, []byte{1}, nil)
	_, err := redoer.Redo(context.Background(), key, base, []WALDelta{{LSN: 100, Flags: EntryFlagWALRecord, WALRecord: badRec}})
	if err == nil {
		t.Fatalf("expected error for unsupported RMID 99")
	}
	if !strings.Contains(err.Error(), "air-walredo:") {
		t.Fatalf("expected worker stderr details in error, got: %v", err)
	}
}

func BenchmarkWALRedo_HotBlock(b *testing.B) {
	key := RelBlockKey(1663, 16385, 16396, 0, 0)
	base := make([]byte, PageSize)
	initPGHeapPage(base)
	pageAddHeapItem(base, 1, buildTupleFromXLHeader(10, 0, 0, 1, 0, pgHeapXmaxInvalid, pgSizeOfHeapTupleHdr, []byte("init")))

	const chainLen = 256
	records := make([][]byte, chainLen)
	curOff := uint16(1)
	for i := 0; i < chainLen; i++ {
		nextOff := curOff + 1
		if nextOff > 20 {
			nextOff = 2
		}
		records[i] = buildReproHeapHotUpdateWAL(uint32(100+i), key, curOff, nextOff, []byte{byte(i), 1, 2, 3})
		curOff = nextOff
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, ok := ApplyWALRecordsInGo(key, base, records)
		if !ok || len(out) != PageSize {
			b.Fatalf("ApplyWALRecordsInGo failed")
		}
	}
}
