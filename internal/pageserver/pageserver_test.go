package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

func buildTestWALPageWithFPI(pageAddr uint64, spc, db, rel uint32, fork uint8, blk uint32, pagePattern byte) []byte {
	// Construct an 8192-byte page with a zero hole [100..200).
	holeOff := 100
	holeLen := 100
	fullPage := make([]byte, PageSize)
	for i := range fullPage {
		if i < holeOff || i >= holeOff+holeLen {
			fullPage[i] = pagePattern
		}
	}
	imgPayload := append(append([]byte(nil), fullPage[:holeOff]...), fullPage[holeOff+holeLen:]...)

	// Build XLogRecord: 24B header + block header (1+1+2 + 5 imgHdr + 12 rel + 4 blk) + 2B data_short + imgPayload
	hdrLen := SizeOfXLogRecord + 4 + 5 + 12 + 4 + 2
	totLen := hdrLen + len(imgPayload)
	rec := make([]byte, totLen)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(totLen))
	binary.LittleEndian.PutUint32(rec[4:8], 42)
	binary.LittleEndian.PutUint64(rec[8:16], pageAddr)
	rec[16] = 0x00 // info
	rec[17] = 10   // RM_HEAP_ID

	off := SizeOfXLogRecord
	rec[off] = 0 // block id 0
	rec[off+1] = (fork & BkpBlockForkMask) | BkpBlockHasImage
	binary.LittleEndian.PutUint16(rec[off+2:off+4], 0) // data_length
	off += 4

	binary.LittleEndian.PutUint16(rec[off:off+2], uint16(len(imgPayload)))
	binary.LittleEndian.PutUint16(rec[off+2:off+4], uint16(holeOff))
	rec[off+4] = BkpImageHasHole | BkpImageApply
	off += 5

	binary.LittleEndian.PutUint32(rec[off:off+4], spc)
	binary.LittleEndian.PutUint32(rec[off+4:off+8], db)
	binary.LittleEndian.PutUint32(rec[off+8:off+12], rel)
	off += 12

	binary.LittleEndian.PutUint32(rec[off:off+4], blk)
	off += 4

	rec[off] = XLRBlockIDDataShort
	rec[off+1] = 0
	off += 2

	copy(rec[off:], imgPayload)

	// Wrap inside an 8192-byte PG17 WAL page with LongPageHeader.
	walPage := make([]byte, XLogPageSize)
	binary.LittleEndian.PutUint16(walPage[0:2], XLogPageMagic)
	binary.LittleEndian.PutUint16(walPage[2:4], XLPLongHeader)
	binary.LittleEndian.PutUint32(walPage[4:8], 1) // tli
	binary.LittleEndian.PutUint64(walPage[8:16], pageAddr)
	binary.LittleEndian.PutUint32(walPage[SizeOfShortPHD:SizeOfShortPHD+4], uint32(XLogPageSize))
	copy(walPage[SizeOfLongPHD:], rec)
	return walPage
}

func TestWALParserAndFPI(t *testing.T) {
	walPage := buildTestWALPageWithFPI(0x1000000, 1663, 5, 16384, 0, 7, 0xAB)
	recs, err := ParseWALPages(walPage)
	if err != nil {
		t.Fatalf("ParseWALPages: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 WAL record, got %d", len(recs))
	}
	if len(recs[0].Blocks) != 1 {
		t.Fatalf("expected 1 block ref, got %d", len(recs[0].Blocks))
	}

	tmpDir := t.TempDir()
	store := NewLayerStore(tmpDir, nil, "", nil, nil)
	tl := NewTimeline("tenant_a", 1, store, nil)
	tl.ApplyWALRecord(recs[0])

	if _, err := tl.FlushInMemory(context.Background()); err != nil {
		t.Fatalf("FlushInMemory: %v", err)
	}

	key := RelBlockKey(1663, 5, 16384, 0, 7)
	got, err := tl.GetPageAtLSN(context.Background(), key, 0)
	if err != nil {
		t.Fatalf("GetPageAtLSN: %v", err)
	}
	if len(got) != PageSize || got[0] != 0xAB || got[150] != 0x00 || got[250] != 0xAB {
		t.Fatalf("unexpected restored FPI page bytes: [0]=0x%x [150]=0x%x [250]=0x%x", got[0], got[150], got[250])
	}
}

func TestEncryptedLayerRangeReadAndCompaction(t *testing.T) {
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	if _, err := kms.EnsureTenantDEK("tenant_enc"); err != nil {
		t.Fatalf("EnsureTenantDEK: %v", err)
	}

	tmpDir := t.TempDir()
	cache, err := NewDiskCache(filepath.Join(tmpDir, "cache"), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	store := NewLayerStore(filepath.Join(tmpDir, "layers"), nil, "", kms, cache)
	tl := NewTimeline("tenant_enc", 3, store, nil)

	for i := 0; i < 4; i++ {
		page := bytes.Repeat([]byte{byte(0x10 + i)}, PageSize)
		tl.PutPage(RelBlockKey(1663, 5, 20000, 0, uint32(i)), uint64(100+i*10), page)
		if _, err := tl.FlushInMemory(context.Background()); err != nil {
			t.Fatalf("FlushInMemory %d: %v", i, err)
		}
	}

	if tl.LayerCount() != 4 {
		t.Fatalf("expected 4 L0 layers, got %d", tl.LayerCount())
	}

	imgMeta, err := tl.CompactIfNeeded(context.Background(), 4, true)
	if err != nil {
		t.Fatalf("CompactIfNeeded: %v", err)
	}
	if imgMeta == nil || imgMeta.Kind != LayerKindImage || !imgMeta.Encrypted {
		t.Fatalf("expected encrypted Image layer, got %+v", imgMeta)
	}
	if tl.LayerCount() != 1 {
		t.Fatalf("expected 1 compacted layer, got %d", tl.LayerCount())
	}

	for i := 0; i < 4; i++ {
		got, err := tl.GetPageAtLSN(context.Background(), RelBlockKey(1663, 5, 20000, 0, uint32(i)), 0)
		if err != nil {
			t.Fatalf("GetPageAtLSN blk %d: %v", i, err)
		}
		if got[0] != byte(0x10+i) || got[PageSize-1] != byte(0x10+i) {
			t.Fatalf("corrupt decrypted page for blk %d", i)
		}
	}
}

func TestProtocolAndUnixSocket(t *testing.T) {
	tmpDir := t.TempDir()
	sock := filepath.Join(tmpDir, "ps.sock")
	ps, err := New(Config{
		SocketPath: sock,
		LayerDir:   filepath.Join(tmpDir, "layers"),
		CacheDir:   filepath.Join(tmpDir, "cache"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ps.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ps.Close()

	tl := ps.EnsureTenant("tenant_sock", 1)
	p0 := bytes.Repeat([]byte{0x5A}, PageSize)
	p1 := bytes.Repeat([]byte{0x6B}, PageSize)
	tl.PutPage(RelBlockKey(1663, 5, 999, 0, 0), 50, p0)
	tl.PutPage(RelBlockKey(1663, 5, 999, 0, 1), 60, p1)

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	sendReq := func(op uint16, blk, nblk uint32) (uint16, uint32, []byte) {
		var req [ReqHeaderSize]byte
		binary.LittleEndian.PutUint32(req[0:4], ProtocolMagic)
		binary.LittleEndian.PutUint16(req[4:6], ProtocolVersion)
		binary.LittleEndian.PutUint16(req[6:8], op)
		copy(req[8:8+TenantIDMaxLen], "tenant_sock")
		binary.LittleEndian.PutUint32(req[72:76], 1663)
		binary.LittleEndian.PutUint32(req[76:80], 5)
		binary.LittleEndian.PutUint32(req[80:84], 999)
		req[84] = 0
		binary.LittleEndian.PutUint32(req[88:92], blk)
		binary.LittleEndian.PutUint32(req[92:96], nblk)

		if _, err := conn.Write(req[:]); err != nil {
			t.Fatalf("Write: %v", err)
		}
		var resp [RespHeaderSize]byte
		if _, err := io.ReadFull(conn, resp[:]); err != nil {
			t.Fatalf("ReadFull hdr: %v", err)
		}
		status := binary.LittleEndian.Uint16(resp[6:8])
		pLen := binary.LittleEndian.Uint32(resp[8:12])
		val := binary.LittleEndian.Uint32(resp[12:16])
		var payload []byte
		if pLen > 0 {
			payload = make([]byte, pLen)
			if _, err := io.ReadFull(conn, payload); err != nil {
				t.Fatalf("ReadFull payload: %v", err)
			}
		}
		return status, val, payload
	}

	st, val, _ := sendReq(OpGetRelExists, 0, 0)
	if st != StatusOK || val != 1 {
		t.Fatalf("OpGetRelExists got st=%d val=%d", st, val)
	}
	st, val, _ = sendReq(OpGetRelSize, 0, 0)
	if st != StatusOK || val != 2 {
		t.Fatalf("OpGetRelSize got st=%d val=%d", st, val)
	}
	st, val, pay := sendReq(OpGetPageBatch, 0, 2)
	if st != StatusOK || val != 2 || len(pay) != 2*PageSize {
		t.Fatalf("OpGetPageBatch got st=%d val=%d len=%d", st, val, len(pay))
	}
	if pay[0] != 0x5A || pay[PageSize] != 0x6B {
		t.Fatalf("OpGetPageBatch payload mismatch")
	}
	st, _, _ = sendReq(OpGetPageAtLSN, 99, 1)
	if st != StatusNotFound {
		t.Fatalf("expected StatusNotFound for blk 99, got %d", st)
	}
}

func TestDiskCacheLRUEviction(t *testing.T) {
	dc, err := NewDiskCache(t.TempDir(), 200)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	_ = dc.Put("k1", bytes.Repeat([]byte("a"), 100))
	_ = dc.Put("k2", bytes.Repeat([]byte("b"), 100))
	if _, ok := dc.GetRange("k1", 0, 10); !ok {
		t.Fatalf("expected k1 hit")
	}
	// Adding k3 (100B) should evict k2 (LRU) while keeping k1.
	_ = dc.Put("k3", bytes.Repeat([]byte("c"), 100))
	if _, ok := dc.GetRange("k2", 0, 10); ok {
		t.Fatalf("expected k2 to be evicted")
	}
	if _, ok := dc.GetRange("k1", 0, 10); !ok {
		t.Fatalf("expected k1 to remain cached")
	}
}

func TestBootstrapFromPGData(t *testing.T) {
	tmpDir := t.TempDir()
	pgData := filepath.Join(tmpDir, "pgdata")
	if err := os.MkdirAll(filepath.Join(pgData, "base", "5"), 0o755); err != nil {
		t.Fatal(err)
	}
	relData := append(bytes.Repeat([]byte{0x11}, PageSize), bytes.Repeat([]byte{0x22}, PageSize)...)
	if err := os.WriteFile(filepath.Join(pgData, "base", "5", "16385"), relData, 0o600); err != nil {
		t.Fatal(err)
	}

	ps, err := New(Config{
		SocketPath: filepath.Join(tmpDir, "ps.sock"),
		LayerDir:   filepath.Join(tmpDir, "layers"),
	})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := ps.BootstrapTenantFromPGData(context.Background(), "tenant_boot", 1, pgData, 100)
	if err != nil || meta == nil {
		t.Fatalf("BootstrapTenantFromPGData: meta=%v err=%v", meta, err)
	}
	nb, err := ps.GetRelSize(context.Background(), "tenant_boot", 1663, 5, 16385, 0, 0)
	if err != nil || nb != 2 {
		t.Fatalf("GetRelSize got nb=%d err=%v", nb, err)
	}
	p1, err := ps.GetPageAtLSN(context.Background(), "tenant_boot", RelBlockKey(1663, 5, 16385, 0, 1), 0)
	if err != nil || p1[0] != 0x22 {
		t.Fatalf("GetPageAtLSN blk 1 got err=%v", err)
	}
}

func TestDiskCacheEvictPrefix(t *testing.T) {
	dc, err := NewDiskCache(t.TempDir(), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	if err := dc.Put("tenants/t_a/gen1/layers/l0_1.layer", []byte("aaaa")); err != nil {
		t.Fatal(err)
	}
	if err := dc.Put("tenants/t_a/gen1/layers/l0_2.layer#chunk:0", []byte("bbbb")); err != nil {
		t.Fatal(err)
	}
	if err := dc.Put("tenants/t_b/gen1/layers/l0_1.layer", []byte("cccc")); err != nil {
		t.Fatal(err)
	}

	removed := dc.EvictPrefix("tenants/t_a/gen1/")
	if removed != 2 {
		t.Fatalf("EvictPrefix removed=%d, want 2", removed)
	}
	if _, ok := dc.GetRange("tenants/t_a/gen1/layers/l0_1.layer", 0, 4); ok {
		t.Fatalf("expected t_a layer to be evicted")
	}
	if _, ok := dc.GetRange("tenants/t_a/gen1/layers/l0_2.layer#chunk:0", 0, 4); ok {
		t.Fatalf("expected t_a chunk to be evicted")
	}
	if got, ok := dc.GetRange("tenants/t_b/gen1/layers/l0_1.layer", 0, 4); !ok || string(got) != "cccc" {
		t.Fatalf("expected t_b layer to remain, got=%q ok=%v", string(got), ok)
	}
}
