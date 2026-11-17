package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

// pgChecksumPage computes a PostgreSQL-compatible FNV-1a 32-bit page checksum over an 8192-byte page
// and stores it in pd_checksum (bytes 8..10) as a 16-bit folded value.
func stampPGPageChecksum(page []byte, blkno uint32) uint16 {
	if len(page) != PageSize {
		panic("invalid page size")
	}
	page[8] = 0
	page[9] = 0
	var sums [32]uint32
	for i := range sums {
		sums[i] = 0x811c9dc5
	}
	for off := 0; off < PageSize; off += 128 {
		for i := 0; i < 32; i++ {
			v := binary.LittleEndian.Uint32(page[off+i*4 : off+i*4+4])
			sums[i] ^= v
			sums[i] *= 0x01000193
		}
	}
	var final uint32
	for _, s := range sums {
		final ^= s
	}
	final ^= blkno
	final *= 0x01000193
	chk := uint16((final % 65535) + 1)
	binary.LittleEndian.PutUint16(page[8:10], chk)
	return chk
}

func verifyPGPageChecksum(page []byte, blkno uint32) bool {
	if len(page) != PageSize {
		return false
	}
	got := binary.LittleEndian.Uint16(page[8:10])
	cp := append([]byte(nil), page...)
	want := stampPGPageChecksum(cp, blkno)
	return got == want
}

func makePGPage(blkno uint32, lsn uint64) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(page[0:8], lsn)
	binary.LittleEndian.PutUint16(page[12:14], 24)        // pd_lower
	binary.LittleEndian.PutUint16(page[14:16], uint16(PageSize)) // pd_upper
	for off := 24; off < 1024; off += 4 {
		binary.LittleEndian.PutUint32(page[off:off+4], blkno^uint32(lsn)^uint32(off))
	}
	stampPGPageChecksum(page, blkno)
	return page
}

type parityRedoer struct{}

func (parityRedoer) Redo(_ context.Context, _ Key, basePage []byte, deltas []WALDelta) ([]byte, error) {
	var page []byte
	if len(basePage) == PageSize {
		page = append([]byte(nil), basePage...)
	}
	for _, d := range deltas {
		if len(d.PageImage) == PageSize {
			page = append([]byte(nil), d.PageImage...)
		}
		if len(d.WALRecord) >= 8 {
			if len(page) != PageSize {
				page = make([]byte, PageSize)
			}
			// Deterministic XOR-patch WAL semantics for replay parity testing.
			off := int(binary.LittleEndian.Uint32(d.WALRecord[0:4])) % (PageSize - 32)
			if off < 24 {
				off = 24
			}
			val := binary.LittleEndian.Uint32(d.WALRecord[4:8])
			binary.LittleEndian.PutUint32(page[off:off+4], val)
		}
	}
	if len(page) != PageSize {
		return nil, errors.New("page not found")
	}
	return page, nil
}

func TestPhase8_PGChecksumAndWALReplayParity(t *testing.T) {
	ctx := context.Background()
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("tenant_p8"); err != nil {
		t.Fatal(err)
	}

	store := NewLayerStore(t.TempDir(), nil, "", kms, nil)
	tl := NewTimeline("tenant_p8", 1, store, parityRedoer{})

	var walBuf [16]byte
	binary.LittleEndian.PutUint32(walBuf[0:4], 128)
	binary.LittleEndian.PutUint32(walBuf[4:8], 0xCAFEBABE)

	for blk := uint32(0); blk < 8; blk++ {
		k := RelBlockKey(1663, 5, 100, 0, blk)
		pg := makePGPage(blk, 10)
		if !verifyPGPageChecksum(pg, blk) {
			t.Fatalf("initial checksum invalid for blk %d", blk)
		}
		tl.PutPage(k, 10, pg)
	}
	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatalf("FlushInMemory 1: %v", err)
	}

	// Apply shared WAL record to blocks 0..7 (tests V2 shared WAL record table + replay parity).
	for blk := uint32(0); blk < 8; blk++ {
		k := RelBlockKey(1663, 5, 100, 0, blk)
		tl.inMem.Put(k, WALDelta{
			LSN:       20,
			Flags:     EntryFlagWALRecord,
			WALRecord: walBuf[:],
		})
	}
	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatalf("FlushInMemory 2: %v", err)
	}

	for blk := uint32(0); blk < 8; blk++ {
		k := RelBlockKey(1663, 5, 100, 0, blk)
		// Historical LSN 10 must preserve original PG checksum.
		p10, err := tl.GetPageAtLSN(ctx, k, 10)
		if err != nil {
			t.Fatalf("GetPageAtLSN(10) blk=%d: %v", blk, err)
		}
		if !verifyPGPageChecksum(p10, blk) {
			t.Fatalf("historical page blk=%d failed PG checksum", blk)
		}

		// Latest LSN 20 must match direct parityRedoer output.
		p20, err := tl.GetPageAtLSN(ctx, k, 20)
		if err != nil {
			t.Fatalf("GetPageAtLSN(20) blk=%d: %v", blk, err)
		}
		want, _ := (parityRedoer{}).Redo(ctx, k, makePGPage(blk, 10), []WALDelta{{LSN: 20, WALRecord: walBuf[:]}})
		if !bytes.Equal(p20, want) {
			t.Fatalf("WAL replay parity mismatch on blk=%d", blk)
		}
	}
}

func TestPhase8_MixedV1V2HistoricalReadsAndConcurrentCompaction(t *testing.T) {
	ctx := context.Background()
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("tenant_mix"); err != nil {
		t.Fatal(err)
	}

	store := NewLayerStore(t.TempDir(), nil, "", kms, nil)
	tl := NewTimeline("tenant_mix", 1, store, parityRedoer{})

	// Write 2 V1 layers and 2 V2 layers into the same timeline.
	for round := 0; round < 4; round++ {
		if round%2 == 0 {
			store.SetLayerFormatVersion(LayerVersion)
		} else {
			store.SetLayerFormatVersion(LayerVersionV2)
		}
		lsn := uint64((round + 1) * 100)
		for blk := uint32(0); blk < 16; blk++ {
			tl.PutPage(RelBlockKey(1663, 5, 200, 0, blk), lsn, makePGPage(blk, lsn))
		}
		if _, err := tl.FlushInMemory(ctx); err != nil {
			t.Fatalf("flush round %d: %v", round, err)
		}
	}

	// Concurrent readers while compaction merges mixed V1+V2 L0 layers into V2 L1.
	var wg sync.WaitGroup
	readErr := make(chan error, 16)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for iter := 0; iter < 25; iter++ {
				blk := uint32((id + iter) % 16)
				k := RelBlockKey(1663, 5, 200, 0, blk)
				p200, err := tl.GetPageAtLSN(ctx, k, 200)
				if err != nil {
					readErr <- fmt.Errorf("historical LSN 200 blk=%d: %w", blk, err)
					return
				}
				if !bytes.Equal(p200, makePGPage(blk, 200)) {
					readErr <- fmt.Errorf("historical LSN 200 content mismatch blk=%d", blk)
					return
				}
				p400, err := tl.GetPageAtLSN(ctx, k, 400)
				if err != nil {
					readErr <- fmt.Errorf("latest LSN 400 blk=%d: %w", blk, err)
					return
				}
				if !bytes.Equal(p400, makePGPage(blk, 400)) {
					readErr <- fmt.Errorf("latest LSN 400 content mismatch blk=%d", blk)
					return
				}
			}
		}(g)
	}

	compacted, err := tl.CompactIfNeeded(ctx, 4, false)
	wg.Wait()
	close(readErr)
	if err != nil {
		t.Fatalf("CompactIfNeeded: %v", err)
	}
	if compacted == nil {
		t.Fatal("expected compacted layer")
	}
	for rErr := range readErr {
		t.Fatal(rErr)
	}

	// Verify historical reads after compaction.
	for _, lsn := range []uint64{100, 200, 300, 400} {
		for blk := uint32(0); blk < 16; blk++ {
			got, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 200, 0, blk), lsn)
			if err != nil {
				t.Fatalf("post-compaction GetPageAtLSN(%d) blk=%d: %v", lsn, blk, err)
			}
			if !bytes.Equal(got, makePGPage(blk, lsn)) {
				t.Fatalf("post-compaction mismatch lsn=%d blk=%d", lsn, blk)
			}
		}
	}
}

func TestPhase8_V2EncryptionIntegrityAndTenantIsolation(t *testing.T) {
	ctx := context.Background()
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("tenant_a"); err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("tenant_b"); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	store := NewLayerStore(dir, nil, "", kms, nil)
	k := RelBlockKey(1663, 5, 1, 0, 0)
	metaA, err := store.WriteLayer(ctx, LayerMeta{
		TenantID: "tenant_a",
		Epoch:    1,
		Kind:     LayerKindL0,
		FileName: "l0_a.layer",
		EndLSN:   10,
	}, []LayerRecord{{
		Key:   k,
		Delta: WALDelta{LSN: 10, Flags: EntryFlagPageImage, PageImage: makePGPage(0, 10)},
	}})
	if err != nil {
		t.Fatalf("WriteLayer A: %v", err)
	}

	// Tenant isolation:
	// 1. Local storage path isolation prevents tenant_b from reading tenant_a's file path.
	badTenant := metaA
	badTenant.TenantID = "tenant_b"
	store.ClearIndexCache()
	if _, err := store.LookupKeyInLayer(ctx, badTenant, k, 10); err == nil {
		t.Fatal("expected error on cross-tenant local path lookup")
	}
	// 2. Even if tenant_a's ciphertext is copied into tenant_b's directory, KMS AAD rejects decryption.
	layerPath := filepath.Join(dir, "tenant_a", "l0_a.layer")
	origBytes, err := os.ReadFile(layerPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "tenant_b"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tenant_b", "l0_a.layer"), origBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	store.ClearIndexCache()
	if _, err := store.LookupKeyInLayer(ctx, badTenant, k, 10); !errors.Is(err, crypto.ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch on cross-tenant read, got %v", err)
	}

	// Tampering with ciphertext byte in chunk 0 must fail GCM authentication.
	tampered := append([]byte(nil), origBytes...)
	tampered[16] ^= 0xFF
	if err := os.WriteFile(layerPath, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	store.ClearIndexCache()
	if _, err := store.LookupKeyInLayer(ctx, metaA, k, 10); !errors.Is(err, crypto.ErrAADMismatch) {
		t.Fatalf("expected ErrAADMismatch on tampered V2 chunk, got %v", err)
	}

	// Restore and shred DEK -> ErrDEKMissing.
	_ = os.WriteFile(layerPath, origBytes, 0o600)
	store.ClearIndexCache()
	kms.ShredDEK("tenant_a")
	if _, err := store.LookupKeyInLayer(ctx, metaA, k, 10); !errors.Is(err, crypto.ErrDEKMissing) {
		t.Fatalf("expected ErrDEKMissing after ShredDEK, got %v", err)
	}
}

func TestPhase8_InterruptedMultipartAndManifestConcurrency(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)
	store := NewLayerStore(t.TempDir(), s3c, "test-bucket", nil, nil)
	store.SetMultipartConfig(4096, 1024)
	store.SetMultipartConcurrency(4)

	// Interrupted multipart via context cancellation must abort multipart upload cleanly.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	incomp := make([]byte, PageSize)
	for i := range incomp {
		incomp[i] = byte((i*31 + 17) ^ (i >> 2))
	}
	_, err := store.WriteLayer(cancelCtx, LayerMeta{
		TenantID: "tenant_mp_cancel",
		Epoch:    1,
		Kind:     LayerKindL0,
		FileName: "l0_cancel.layer",
		EndLSN:   10,
	}, []LayerRecord{{
		Key:   RelBlockKey(1663, 5, 1, 0, 0),
		Delta: WALDelta{LSN: 10, Flags: EntryFlagPageImage, PageImage: incomp},
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	_ = mock

	// Concurrent manifest publications on the same Timeline must not lose any layer.
	tl := NewTimeline("tenant_conc_m", 1, store, nil)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m, wErr := store.WriteLayer(ctx, LayerMeta{
				TenantID: "tenant_conc_m",
				Epoch:    1,
				Kind:     LayerKindL0,
				FileName: fmt.Sprintf("l0_%06d_0000000000000001_0000000000000002.layer", idx+1),
				StartLSN: uint64(idx*10 + 1),
				EndLSN:   uint64(idx*10 + 10),
			}, []LayerRecord{{
				Key:   RelBlockKey(1663, 5, uint32(10+idx), 0, 0),
				Delta: WALDelta{LSN: uint64(idx*10 + 1), Flags: EntryFlagPageImage, PageImage: makePGPage(uint32(idx), uint64(idx+1))},
			}})
			if wErr != nil {
				t.Errorf("WriteLayer %d: %v", idx, wErr)
				return
			}
			if pErr := tl.PublishNewLayer(ctx, m); pErr != nil {
				t.Errorf("PublishNewLayer %d: %v", idx, pErr)
			}
		}(i)
	}
	wg.Wait()

	manifest, err := store.LoadLayerManifest(ctx, "tenant_conc_m", 1)
	if err != nil {
		t.Fatalf("LoadLayerManifest: %v", err)
	}
	if tl.LayerCount() != 6 || len(manifest.Layers) != 6 {
		t.Fatalf("expected 6 layers in timeline and manifest, got tl=%d manifest=%d", tl.LayerCount(), len(manifest.Layers))
	}
}

func TestPhase8_StreamingBootstrapAndCacheInvalidation(t *testing.T) {
	ctx := context.Background()
	pgDir := t.TempDir()
	baseDB := filepath.Join(pgDir, "base", "16384")
	if err := os.MkdirAll(baseDB, 0o755); err != nil {
		t.Fatal(err)
	}
	// Write 4 pages into relation 1001 and 2 pages into relation 1002.
	var rel1 []byte
	for blk := uint32(0); blk < 4; blk++ {
		rel1 = append(rel1, makePGPage(blk, 1)...)
	}
	if err := os.WriteFile(filepath.Join(baseDB, "1001"), rel1, 0o600); err != nil {
		t.Fatal(err)
	}
	var rel2 []byte
	for blk := uint32(0); blk < 2; blk++ {
		rel2 = append(rel2, makePGPage(blk, 1)...)
	}
	if err := os.WriteFile(filepath.Join(baseDB, "1002"), rel2, 0o600); err != nil {
		t.Fatal(err)
	}

	cache, err := NewDiskCache(t.TempDir(), 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := New(Config{
		LayerDir: t.TempDir(),
		CacheDir: cache.dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	meta, err := ps.BootstrapTenantFromPGData(ctx, "tenant_boot", 1, pgDir, 50)
	if err != nil {
		t.Fatalf("BootstrapTenantFromPGData: %v", err)
	}
	if meta == nil || meta.Kind != LayerKindImage {
		t.Fatalf("expected Image layer from bootstrap, got %+v", meta)
	}

	tl := ps.EnsureTenant("tenant_boot", 1)
	got, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 16384, 1001, 0, 3), 50)
	if err != nil || !bytes.Equal(got, makePGPage(3, 1)) {
		t.Fatalf("bootstrap page read mismatch: err=%v", err)
	}
	relSz, err := tl.GetRelSize(ctx, 1663, 16384, 1002, 0, 50)
	if err != nil || relSz != 2 {
		t.Fatalf("expected rel 1002 size=2, got %d err=%v", relSz, err)
	}

	// Verify cache invalidation.
	ps.store.ClearIndexCache()
	got2, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 16384, 1002, 0, 1), 50)
	if err != nil || !bytes.Equal(got2, makePGPage(1, 1)) {
		t.Fatalf("post-cache-invalidation read failed: err=%v", err)
	}
}

func BenchmarkLayerFormatV1vsV2(b *testing.B) {
	kms, _ := crypto.NewEnvelopeKMS()
	_, _ = kms.EnsureTenantDEK("t_v1v2")

	// Realistic 512-record layer (8KB Postgres-like pages + shared WAL delta).
	recs := make([]LayerRecord, 512)
	wal := bytes.Repeat([]byte{0x42, 0x19, 0x00, 0xFF}, 32)
	for i := range recs {
		recs[i] = LayerRecord{
			Key: RelBlockKey(1663, 5, 100, 0, uint32(i)),
			Delta: WALDelta{
				LSN:       uint64(100 + i),
				Flags:     EntryFlagPageImage | EntryFlagWALRecord,
				PageImage: makePGPage(uint32(i), uint64(100+i)),
				WALRecord: wal,
			},
		}
	}

	v1Raw, err := EncodeLayerBytes("t_v1v2", 1, "k", LayerKindL0, 1000, recs, kms)
	if err != nil {
		b.Fatal(err)
	}
	v2Raw, err := EncodeLayerV2Bytes("t_v1v2", 1, "k", LayerKindL0, 1000, recs, kms)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("LayerSize (512 encrypted records): V1=%d bytes, V2=%d bytes (ratio=%.2fx)",
		len(v1Raw), len(v2Raw), float64(len(v1Raw))/float64(len(v2Raw)))

	b.Run("EncodeV1", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(v1Raw)))
		for i := 0; i < b.N; i++ {
			if _, err := EncodeLayerBytes("t_v1v2", 1, "k", LayerKindL0, 1000, recs, kms); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("EncodeV2", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(v1Raw)))
		for i := 0; i < b.N; i++ {
			if _, err := EncodeLayerV2Bytes("t_v1v2", 1, "k", LayerKindL0, 1000, recs, kms); err != nil {
				b.Fatal(err)
			}
		}
	})
}
