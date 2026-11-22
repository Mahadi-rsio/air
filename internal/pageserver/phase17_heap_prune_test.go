package pageserver

import (
	"context"
	"encoding/binary"
	"testing"
)

func buildTestWALRecord(rmid, info uint8, xid uint32, key Key, mainData, blkData []byte) []byte {
	// Build a minimal valid PG17 XLogRecord with block 0 and mainData.
	// Header (24 bytes) + block 0 header + data_short/long + blkData + mainData
	var blkHdr []byte
	blkHdr = append(blkHdr, 0) // block_id = 0
	flags := uint8(BkpBlockForkMask & key.ForkNum)
	if len(blkData) > 0 {
		flags |= BkpBlockHasData
	}
	blkHdr = append(blkHdr, flags)
	if len(blkData) > 0 {
		var lenBuf [2]byte
		binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(blkData)))
		blkHdr = append(blkHdr, lenBuf[:]...)
	}
	var relBuf [12]byte
	binary.LittleEndian.PutUint32(relBuf[0:4], key.SpcOID)
	binary.LittleEndian.PutUint32(relBuf[4:8], key.DbOID)
	binary.LittleEndian.PutUint32(relBuf[8:12], key.RelNumber)
	blkHdr = append(blkHdr, relBuf[:]...)
	var blkNumBuf [4]byte
	binary.LittleEndian.PutUint32(blkNumBuf[:], key.BlockNum)
	blkHdr = append(blkHdr, blkNumBuf[:]...)

	var dataHdr []byte
	if len(mainData) < 256 {
		dataHdr = []byte{XLRBlockIDDataShort, byte(len(mainData))}
	} else {
		dataHdr = make([]byte, 5)
		dataHdr[0] = XLRBlockIDDataLong
		binary.LittleEndian.PutUint32(dataHdr[1:5], uint32(len(mainData)))
	}

	totLen := SizeOfXLogRecord + len(blkHdr) + len(dataHdr) + len(blkData) + len(mainData)
	raw := make([]byte, totLen)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(totLen))
	binary.LittleEndian.PutUint32(raw[4:8], xid)
	raw[16] = info
	raw[17] = rmid

	off := SizeOfXLogRecord
	copy(raw[off:], blkHdr)
	off += len(blkHdr)
	copy(raw[off:], dataHdr)
	off += len(dataHdr)
	copy(raw[off:], blkData)
	off += len(blkData)
	copy(raw[off:], mainData)
	return raw
}

func TestPhase17_Heap2PruneOnAccess_Replay(t *testing.T) {
	key := RelBlockKey(1663, 5, 16384, 0, 0)
	page := make([]byte, PageSize)
	initPGHeapPage(page)

	// Add 3 tuples at offnum 1, 2, 3
	tup1 := buildTupleFromXLHeader(100, 101, 0, 1, 0x4000, 0x0100, pgSizeOfHeapTupleHdr, []byte{1, 2, 3, 4})
	tup2 := buildTupleFromXLHeader(101, 0, 0, 2, 0x8000, 0x0900, pgSizeOfHeapTupleHdr, []byte{5, 6, 7, 8})
	tup3 := buildTupleFromXLHeader(102, 103, 0, 3, 0x0000, 0x0500, pgSizeOfHeapTupleHdr, []byte{9, 10, 11, 12})
	pageAddHeapItem(page, 1, tup1)
	pageAddHeapItem(page, 2, tup2)
	pageAddHeapItem(page, 3, tup3)

	// Build XLOG_HEAP2_PRUNE_ON_ACCESS:
	// - redirect offnum 1 -> 2
	// - mark offnum 3 -> LP_DEAD
	mainData := []byte{0, xlhpCleanupLock | xlhpHasRedirections | xlhpHasDeadItems}
	var blkData []byte
	// nredirected = 1, (1 -> 2)
	blkData = binary.LittleEndian.AppendUint16(blkData, 1)
	blkData = binary.LittleEndian.AppendUint16(blkData, 1)
	blkData = binary.LittleEndian.AppendUint16(blkData, 2)
	// ndead = 1, (3)
	blkData = binary.LittleEndian.AppendUint16(blkData, 1)
	blkData = binary.LittleEndian.AppendUint16(blkData, 3)

	raw := buildTestWALRecord(RmHeap2ID, XLogHeap2PruneOnAccess, 105, key, mainData, blkData)
	redoer := NewDefaultWALRedoer("")
	out, err := redoer.Redo(context.Background(), key, page, []WALDelta{{LSN: 200, Flags: EntryFlagWALRecord, WALRecord: raw}})
	if err != nil {
		t.Fatalf("Redo HEAP2_PRUNE_ON_ACCESS failed: %v", err)
	}

	// Verify LP 1 is LP_REDIRECT -> 2
	lp1 := binary.LittleEndian.Uint32(out[pgPageHeaderSize : pgPageHeaderSize+4])
	if ((lp1 >> 15) & 0x03) != pgLPRedirect || (lp1&0x7FFF) != 2 {
		t.Fatalf("expected LP 1 to be LP_REDIRECT->2, got flags=%d off=%d", (lp1>>15)&0x03, lp1&0x7FFF)
	}
	// Verify LP 2 is still LP_NORMAL
	lp2 := binary.LittleEndian.Uint32(out[pgPageHeaderSize+4 : pgPageHeaderSize+8])
	if ((lp2 >> 15) & 0x03) != pgLPNormal {
		t.Fatalf("expected LP 2 to remain LP_NORMAL, got flags=%d", (lp2>>15)&0x03)
	}
	// Verify LP 3 is LP_DEAD
	lp3 := binary.LittleEndian.Uint32(out[pgPageHeaderSize+8 : pgPageHeaderSize+12])
	if ((lp3 >> 15) & 0x03) != pgLPDead {
		t.Fatalf("expected LP 3 to be LP_DEAD, got flags=%d", (lp3>>15)&0x03)
	}
}

func TestPhase17_UnsupportedWALRecord_NotSilentlyIgnored(t *testing.T) {
	key := RelBlockKey(1663, 5, 16384, 0, 0)
	base := make([]byte, PageSize)
	initPGHeapPage(base)

	// Unsupported RMID 99
	raw := buildTestWALRecord(99, 0x10, 105, key, []byte{1, 2}, nil)
	redoer := NewDefaultWALRedoer("")
	if _, err := redoer.Redo(context.Background(), key, base, []WALDelta{{LSN: 200, Flags: EntryFlagWALRecord, WALRecord: raw}}); err == nil {
		t.Fatalf("expected explicit error for unsupported WAL record, got nil")
	}
}
