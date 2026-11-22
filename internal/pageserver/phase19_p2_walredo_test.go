package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestP2_CompressedFPIDecoding(t *testing.T) {
	// Build a realistic 8192-byte page with a zeroed hole [128:4096].
	origPage := make([]byte, PageSize)
	for i := 0; i < 128; i++ {
		origPage[i] = byte(i ^ 0x5A)
	}
	for i := 4096; i < PageSize; i++ {
		origPage[i] = byte((i * 7) & 0xFF)
	}
	uncompWithHole := append([]byte(nil), origPage[:128]...)
	uncompWithHole = append(uncompWithHole, origPage[4096:]...)

	// 1. ZSTD compressed FPI with hole
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	zstdPayload := enc.EncodeAll(uncompWithHole, nil)
	_ = enc.Close()

	refZstd := WALBlockRef{
		HasImage:     true,
		ImageInfo:    BkpImageHasHole | BkpImageCompressZSTD,
		HoleOffset:   128,
		HoleLength:   4096 - 128,
		ImagePayload: zstdPayload,
	}
	gotZstd, err := RestoreBkpImage(refZstd)
	if err != nil {
		t.Fatalf("RestoreBkpImage(ZSTD) failed: %v", err)
	}
	if !bytes.Equal(gotZstd, origPage) {
		t.Fatalf("ZSTD reconstructed page mismatch")
	}

	// 2. LZ4 compressed FPI (literal-only block for deterministic test)
	// Raw LZ4 block: token=(15<<4)|0, ext len, literals...
	var lz4Buf bytes.Buffer
	lit := uncompWithHole
	if len(lit) >= 15 {
		lz4Buf.WriteByte(0xF0)
		rem := len(lit) - 15
		for rem >= 255 {
			lz4Buf.WriteByte(255)
			rem -= 255
		}
		lz4Buf.WriteByte(byte(rem))
	} else {
		lz4Buf.WriteByte(byte(len(lit) << 4))
	}
	lz4Buf.Write(lit)

	refLZ4 := WALBlockRef{
		HasImage:     true,
		ImageInfo:    BkpImageHasHole | BkpImageCompressLZ4,
		HoleOffset:   128,
		HoleLength:   4096 - 128,
		ImagePayload: lz4Buf.Bytes(),
	}
	gotLZ4, err := RestoreBkpImage(refLZ4)
	if err != nil {
		t.Fatalf("RestoreBkpImage(LZ4) failed: %v", err)
	}
	if !bytes.Equal(gotLZ4, origPage) {
		t.Fatalf("LZ4 reconstructed page mismatch")
	}

	// 3. PGLZ compressed FPI (literal-only chunks: ctrl=0x00 followed by 8 literal bytes)
	var pglzBuf bytes.Buffer
	for i := 0; i < len(uncompWithHole); i += 8 {
		end := i + 8
		if end > len(uncompWithHole) {
			end = len(uncompWithHole)
		}
		pglzBuf.WriteByte(0x00)
		pglzBuf.Write(uncompWithHole[i:end])
	}
	refPGLZ := WALBlockRef{
		HasImage:     true,
		ImageInfo:    BkpImageHasHole | BkpImageCompressPGLZ,
		HoleOffset:   128,
		HoleLength:   4096 - 128,
		ImagePayload: pglzBuf.Bytes(),
	}
	gotPGLZ, err := RestoreBkpImage(refPGLZ)
	if err != nil {
		t.Fatalf("RestoreBkpImage(PGLZ) failed: %v", err)
	}
	if !bytes.Equal(gotPGLZ, origPage) {
		t.Fatalf("PGLZ reconstructed page mismatch")
	}
}

func TestP2_SubtransSLRUIngestion(t *testing.T) {
	store := NewLayerStore(t.TempDir(), nil, "", nil, nil)
	tl := NewTimeline("t-subtrans", 1, store, NewDefaultWALRedoer(""))

	// Build XLogXactAssignment record: xtop=500, nsubxacts=2, subxacts=[501, 2050]
	mainData := make([]byte, 16)
	binary.LittleEndian.PutUint32(mainData[0:4], 500)
	binary.LittleEndian.PutUint32(mainData[4:8], 2)
	binary.LittleEndian.PutUint32(mainData[8:12], 501)
	binary.LittleEndian.PutUint32(mainData[12:16], 2050)

	rec := &DecodedWALRecord{
		LSN:      1000,
		EndLSN:   1064,
		XID:      500,
		Rmid:     RmXactID,
		Info:     XLogXactAssignment,
		MainData: mainData,
	}
	tl.ApplyWALRecord(rec)

	// Verify pg_subtrans page 0 (for subxid 501) and page 1 (for subxid 2050)
	ctx := context.Background()
	pg0, err := tl.GetPageAtLSN(ctx, Key{Kind: KeyKindSLRU, SpcOID: 1, BlockNum: 0}, 1064)
	if err != nil {
		t.Fatalf("GetPageAtLSN(subtrans page 0): %v", err)
	}
	if got := binary.LittleEndian.Uint32(pg0[501*4 : 501*4+4]); got != 500 {
		t.Fatalf("subxid 501 parent: got %d, want 500", got)
	}

	pg1, err := tl.GetPageAtLSN(ctx, Key{Kind: KeyKindSLRU, SpcOID: 1, BlockNum: 1}, 1064)
	if err != nil {
		t.Fatalf("GetPageAtLSN(subtrans page 1): %v", err)
	}
	slot := (2050 % 2048) * 4
	if got := binary.LittleEndian.Uint32(pg1[slot : slot+4]); got != 500 {
		t.Fatalf("subxid 2050 parent: got %d, want 500", got)
	}
}

func buildP2TestWALRecord(rmid, info uint8, rel RelFileLocator, blkNum uint32, blkData, mainData []byte) []byte {
	var body bytes.Buffer
	if blkData != nil {
		body.WriteByte(0) // block id 0
		body.WriteByte(BkpBlockHasData)
		var dl [2]byte
		binary.LittleEndian.PutUint16(dl[:], uint16(len(blkData)))
		body.Write(dl[:])
		var rb [16]byte
		binary.LittleEndian.PutUint32(rb[0:4], rel.SpcOID)
		binary.LittleEndian.PutUint32(rb[4:8], rel.DbOID)
		binary.LittleEndian.PutUint32(rb[8:12], rel.RelNumber)
		binary.LittleEndian.PutUint32(rb[12:16], blkNum)
		body.Write(rb[:])
	}
	if len(mainData) > 0 {
		if len(mainData) <= 255 {
			body.WriteByte(XLRBlockIDDataShort)
			body.WriteByte(byte(len(mainData)))
		} else {
			body.WriteByte(XLRBlockIDDataLong)
			var ml [4]byte
			binary.LittleEndian.PutUint32(ml[:], uint32(len(mainData)))
			body.Write(ml[:])
		}
	}
	if blkData != nil {
		body.Write(blkData)
	}
	if len(mainData) > 0 {
		body.Write(mainData)
	}

	totLen := uint32(SizeOfXLogRecord + body.Len())
	hdr := make([]byte, SizeOfXLogRecord)
	binary.LittleEndian.PutUint32(hdr[0:4], totLen)
	hdr[16] = info
	hdr[17] = rmid
	return append(hdr, body.Bytes()...)
}

func TestP2_SandboxedWALRedoWorkerAndUnsupportedFuzz(t *testing.T) {
	binPath := filepath.Join(t.TempDir(), "air-walredo")
	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/air-walredo")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build air-walredo failed: %v\n%s", err, out)
	}

	redoer := NewWALRedoPool(WALRedoPoolConfig{
		WorkerBin: binPath,
		Workers:   2,
		QueueSize: 8,
	})
	defer redoer.Close()

	ctx := context.Background()
	rel := RelFileLocator{SpcOID: 1663, DbOID: 5, RelNumber: 16384}
	key := RelBlockKey(rel.SpcOID, rel.DbOID, rel.RelNumber, 0, 0)
	base := make([]byte, PageSize)
	base[0] = 0xAA

	// 1. GenericXLog WAL record (RMID 17) must apply byte-range delta cleanly.
	var genData bytes.Buffer
	_ = binary.Write(&genData, binary.LittleEndian, uint16(100))
	_ = binary.Write(&genData, binary.LittleEndian, uint16(4))
	genData.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	genRec := buildP2TestWALRecord(RmGenericID, 0x00, rel, 0, genData.Bytes(), nil)

	page, err := redoer.Redo(ctx, key, base, []WALDelta{{LSN: 200, Flags: EntryFlagWALRecord, WALRecord: genRec}})
	if err != nil {
		t.Fatalf("GenericXLog redo failed: %v", err)
	}
	if !bytes.Equal(page[100:104], []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Fatalf("GenericXLog did not update page bytes: %x", page[100:104])
	}

	// 2. Unsupported RMID (e.g. 99) or corrupt/truncated WAL record MUST fail loudly, NEVER return stale base page.
	unsupportedRec := buildP2TestWALRecord(99, 0x00, rel, 0, []byte{1, 2, 3}, nil)
	if got, err := redoer.Redo(ctx, key, base, []WALDelta{{LSN: 300, Flags: EntryFlagWALRecord, WALRecord: unsupportedRec}}); err == nil || got != nil {
		t.Fatalf("expected error and nil page for unsupported RMID 99, got page=%v err=%v", got != nil, err)
	}

	// 3. Worker must automatically recover after error and still serve valid subsequent requests!
	page2, err := redoer.Redo(ctx, key, base, []WALDelta{{LSN: 400, Flags: EntryFlagWALRecord, WALRecord: genRec}})
	if err != nil {
		t.Fatalf("recovery after unsupported record failed: %v", err)
	}
	if !bytes.Equal(page2[100:104], []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Fatalf("post-recovery GenericXLog mismatch")
	}

	// 4. Truncated/corrupt fuzz payloads must never panic or serve stale base page.
	for _, bad := range [][]byte{{}, {1, 2, 3}, make([]byte, 24)} {
		if got, err := redoer.Redo(ctx, key, base, []WALDelta{{LSN: 500, Flags: EntryFlagWALRecord, WALRecord: bad}}); err == nil || got != nil {
			t.Fatalf("expected error on corrupt WAL record %x, got page=%v err=%v", bad, got != nil, err)
		}
	}
}
