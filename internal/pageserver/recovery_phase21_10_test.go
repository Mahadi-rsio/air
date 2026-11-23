package pageserver

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestPageserverRestart_RecoversZeroExtendAndRejectsMissingPage(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	layerDir := filepath.Join(tmp, "ps_layers")
	cacheDir := filepath.Join(tmp, "ps_cache")

	ps1, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		CacheDir:          cacheDir,
		CacheMaxBytes:     16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New ps1: %v", err)
	}
	ps1.SetTenantGeneration("t_zero", "gen_1")
	ps1.EnsureTenant("t_zero", 1)

	zeroPage := make([]byte, PageSize)
	nonZeroPage := bytes.Repeat([]byte{0xAB}, PageSize)
	pages := append(append([]byte(nil), zeroPage...), nonZeroPage...)

	if err := ps1.PutPageBatch(ctx, "t_zero", 1663, 16385, 2690, 0, 4, 2, 0x2000100, pages); err != nil {
		t.Fatalf("PutPageBatch: %v", err)
	}
	if err := ps1.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if err := ps1.Close(); err != nil {
		t.Fatalf("Close ps1: %v", err)
	}

	// Simulate Pageserver restart with a fresh empty cache dir.
	ps2, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          layerDir,
		CacheDir:          filepath.Join(tmp, "ps_cache_2"),
		CacheMaxBytes:     16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New ps2: %v", err)
	}
	defer ps2.Close()

	// Valid zero-extended block 4 must be recovered automatically without EnsureTenant.
	got4, err := ps2.GetPageAtLSN(ctx, "t_zero", RelBlockKey(1663, 16385, 2690, 0, 4), 0)
	if err != nil {
		t.Fatalf("GetPageAtLSN(blk=4) after restart: %v", err)
	}
	if !bytes.Equal(got4, zeroPage) {
		t.Fatalf("blk=4 mismatch: expected valid zero page")
	}

	// Non-zero block 5 must be recovered intact.
	got5, err := ps2.GetPageAtLSN(ctx, "t_zero", RelBlockKey(1663, 16385, 2690, 0, 5), 0)
	if err != nil {
		t.Fatalf("GetPageAtLSN(blk=5) after restart: %v", err)
	}
	if !bytes.Equal(got5, nonZeroPage) {
		t.Fatalf("blk=5 mismatch: expected 0xAB page")
	}

	// Missing block 99 must return ErrNotFound, never a silent zero page.
	_, err = ps2.GetPageAtLSN(ctx, "t_zero", RelBlockKey(1663, 16385, 2690, 0, 99), 0)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetPageAtLSN(blk=99) expected ErrNotFound, got %v", err)
	}
}
