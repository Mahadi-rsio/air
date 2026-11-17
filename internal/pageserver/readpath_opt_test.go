package pageserver

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

func TestDiskCacheRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	c1, err := NewDiskCache(dir, 32*1024)
	if err != nil {
		t.Fatalf("NewDiskCache 1: %v", err)
	}
	d1 := bytes.Repeat([]byte{0xAA}, 8192)
	d2 := bytes.Repeat([]byte{0xBB}, 8192)
	if err := c1.Put("layer-a", d1); err != nil {
		t.Fatal(err)
	}
	if err := c1.Put("layer-b", d2); err != nil {
		t.Fatal(err)
	}
	// Leave a stale .tmp file that should be cleaned up on restart.
	if err := os.WriteFile(filepath.Join(dir, "stale.tmp"), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}

	c2, err := NewDiskCache(dir, 32*1024)
	if err != nil {
		t.Fatalf("NewDiskCache 2: %v", err)
	}
	sz, cnt := c2.Stats()
	if cnt != 2 || sz != 16384 {
		t.Fatalf("recovered stats got=(%d, %d) want=(16384, 2)", sz, cnt)
	}
	got, ok := c2.GetRange("layer-a", 0, 8192)
	if !ok || !bytes.Equal(got, d1) {
		t.Fatalf("GetRange layer-a after restart: ok=%v len=%d", ok, len(got))
	}
	if _, err := os.Stat(filepath.Join(dir, "stale.tmp")); !os.IsNotExist(err) {
		t.Fatalf("expected stale.tmp to be removed on restart")
	}
}

func TestBoundedIndexCacheAndBloom(t *testing.T) {
	ctx := context.Background()
	_, s3c, mock := newMockR2Server(t)
	store := NewLayerStore("", s3c, "test-bucket", nil, nil)
	// Tiny index cache budget to force eviction across layers.
	store.SetIndexCacheMaxBytes(1024)
	tl := NewTimeline("t_idx", 1, store, nil)

	for l := 0; l < 4; l++ {
		for p := 0; p < 16; p++ {
			blk := uint32(l*16 + p)
			tl.PutPage(RelBlockKey(1663, 5, 16384, 0, blk), uint64(100+blk), bytes.Repeat([]byte{byte(blk)}, PageSize))
		}
		if _, err := tl.FlushInMemory(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Read all 64 blocks via GetPageBatch; even with eviction, all blocks must resolve accurately.
	pages, count, err := tl.GetPageBatch(ctx, 1663, 5, 16384, 0, 0, 64, 0)
	if err != nil || count != 64 || len(pages) != 64*PageSize {
		t.Fatalf("GetPageBatch: err=%v count=%d len=%d", err, count, len(pages))
	}
	for b := 0; b < 64; b++ {
		if pages[b*PageSize] != byte(b) {
			t.Fatalf("block %d byte mismatch: got=%d want=%d", b, pages[b*PageSize], byte(b))
		}
	}
	if mock.getCount.Load() == 0 {
		t.Fatalf("expected mock R2 GETs")
	}
}

func TestGetPageBatchHistoricalLSNAndEncryptedCoalesce(t *testing.T) {
	ctx := context.Background()
	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kms.EnsureTenantDEK("t_hist"); err != nil {
		t.Fatal(err)
	}

	_, s3c, mock := newMockR2Server(t)
	cache, err := NewDiskCache(filepath.Join(t.TempDir(), "cache"), 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	store := NewLayerStore("", s3c, "test-bucket", kms, cache)
	tl := NewTimeline("t_hist", 1, store, nil)

	// Layer 1 (LSN 100): blocks 0..7 = 0x11
	for b := uint32(0); b < 8; b++ {
		tl.PutPage(RelBlockKey(1663, 5, 30000, 0, b), 100, bytes.Repeat([]byte{0x11}, PageSize))
	}
	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}

	// Layer 2 (LSN 200): blocks 2..5 = 0x22
	for b := uint32(2); b < 6; b++ {
		tl.PutPage(RelBlockKey(1663, 5, 30000, 0, b), 200, bytes.Repeat([]byte{0x22}, PageSize))
	}
	if _, err := tl.FlushInMemory(ctx); err != nil {
		t.Fatal(err)
	}

	// Historical batch read at LSN 150 must see 0x11 for all 8 blocks.
	pages150, cnt150, err := tl.GetPageBatch(ctx, 1663, 5, 30000, 0, 0, 8, 150)
	if err != nil || cnt150 != 8 {
		t.Fatalf("GetPageBatch@150: err=%v cnt=%d", err, cnt150)
	}
	for b := 0; b < 8; b++ {
		if pages150[b*PageSize] != 0x11 {
			t.Fatalf("LSN 150 block %d: got=0x%x want=0x11", b, pages150[b*PageSize])
		}
	}

	// Latest batch read (lsn=0) must see 0x22 for blocks 2..5 and 0x11 for 0,1,6,7.
	store.ClearIndexCache()
	coldCache, _ := NewDiskCache(filepath.Join(t.TempDir(), "cache2"), 64*1024*1024)
	store.cache = coldCache
	mock.getCount.Store(0)

	pagesLatest, cntLatest, err := tl.GetPageBatch(ctx, 1663, 5, 30000, 0, 0, 8, 0)
	if err != nil || cntLatest != 8 {
		t.Fatalf("GetPageBatch@latest: err=%v cnt=%d", err, cntLatest)
	}
	for b := 0; b < 8; b++ {
		want := byte(0x11)
		if b >= 2 && b <= 5 {
			want = 0x22
		}
		if pagesLatest[b*PageSize] != want {
			t.Fatalf("latest block %d: got=0x%x want=0x%x", b, pagesLatest[b*PageSize], want)
		}
	}
	// Coalesced reads across 2 small layers should require at most 4 R2 GETs (not 16+).
	if gets := mock.getCount.Load(); gets > 6 {
		t.Fatalf("expected coalesced R2 GETs <= 6, got %d", gets)
	}
}
