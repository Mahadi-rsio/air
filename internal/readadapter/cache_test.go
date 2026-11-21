package readadapter

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

func TestTieredCacheL1L2L3AndCorruptionRecovery(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantA   = "tenant-cache-a"
		tenantB   = "tenant-cache-b"
		gen1      = "gen-1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(30001)
		forkNum   = uint8(0)
		lsn       = uint64(0x10000)
	)
	ps.EnsureTenant(tenantA, 1)
	ps.SetTenantGeneration(tenantA, gen1)
	ps.EnsureTenant(tenantB, 1)
	ps.SetTenantGeneration(tenantB, gen1)

	pageA0 := bytes.Repeat([]byte{0xAA}, PGPageSize)
	pageA1 := bytes.Repeat([]byte{0xBB}, PGPageSize)
	pageB0 := bytes.Repeat([]byte{0xCC}, PGPageSize)

	if err := ps.PutPageBatch(context.Background(), tenantA, spcOID, dbOID, relNumber, forkNum, 0, 1, lsn, pageA0); err != nil {
		t.Fatalf("PutPageBatch A0: %v", err)
	}
	if err := ps.PutPageBatch(context.Background(), tenantA, spcOID, dbOID, relNumber, forkNum, 1, 1, lsn, pageA1); err != nil {
		t.Fatalf("PutPageBatch A1: %v", err)
	}
	if err := ps.PutPageBatch(context.Background(), tenantB, spcOID, dbOID, relNumber, forkNum, 0, 1, lsn, pageB0); err != nil {
		t.Fatalf("PutPageBatch B0: %v", err)
	}

	l2Dir := filepath.Join(t.TempDir(), "l2-nvme")
	// L1 holds only 1 page (8192 bytes) so reading block 1 evicts block 0 from L1 into L2.
	cache, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: PGPageSize,
		L2Dir:      l2Dir,
		L2MaxBytes: 16 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend: %v", err)
	}

	ctx := context.Background()
	keyA0 := pageserver.RelBlockKey(spcOID, dbOID, relNumber, forkNum, 0)
	keyA1 := pageserver.RelBlockKey(spcOID, dbOID, relNumber, forkNum, 1)

	// 1. Cold read A0 -> L3 fetch.
	got, err := cache.GetPageAtLSN(ctx, tenantA, keyA0, lsn)
	if err != nil || !bytes.Equal(got, pageA0) {
		t.Fatalf("cold A0 mismatch: err=%v", err)
	}
	if st := cache.Stats(); st.L3Fetches != 1 {
		t.Fatalf("expected L3Fetches=1, got %+v", st)
	}

	// 2. Hot read A0 -> L1 hit (no L3 fetch).
	got, err = cache.GetPageAtLSN(ctx, tenantA, keyA0, lsn)
	if err != nil || !bytes.Equal(got, pageA0) {
		t.Fatalf("hot A0 mismatch: err=%v", err)
	}
	if st := cache.Stats(); st.L1Hits != 1 || st.L3Fetches != 1 {
		t.Fatalf("expected L1Hits=1 L3Fetches=1, got %+v", st)
	}

	// 3. Read A1 -> L3 fetch + evicts A0 from L1.
	got, err = cache.GetPageAtLSN(ctx, tenantA, keyA1, lsn)
	if err != nil || !bytes.Equal(got, pageA1) {
		t.Fatalf("cold A1 mismatch: err=%v", err)
	}
	if st := cache.Stats(); st.L1Evictions != 1 || st.L3Fetches != 2 {
		t.Fatalf("expected L1Evictions=1 L3Fetches=2, got %+v", st)
	}

	// 4. Re-read A0 -> L1 miss, L2 NVMe hit (still no L3 fetch!).
	got, err = cache.GetPageAtLSN(ctx, tenantA, keyA0, lsn)
	if err != nil || !bytes.Equal(got, pageA0) {
		t.Fatalf("L2 A0 mismatch: err=%v", err)
	}
	if st := cache.Stats(); st.L2Hits != 1 || st.L3Fetches != 2 {
		t.Fatalf("expected L2Hits=1 L3Fetches=2, got %+v", st)
	}

	// 5. Tenant isolation: tenantB block 0 returns pageB0, not pageA0.
	got, err = cache.GetPageAtLSN(ctx, tenantB, keyA0, lsn)
	if err != nil || !bytes.Equal(got, pageB0) {
		t.Fatalf("tenantB isolation mismatch: err=%v", err)
	}

	// 6. Corrupt L2 file for A1 -> must detect CRC32C mismatch, self-heal from L3.
	ckA1 := PageCacheKey{
		TenantID: tenantA, Generation: gen1,
		SpcOID: spcOID, DbOID: dbOID, RelNumber: relNumber, ForkNum: forkNum, BlockNum: 1, LSN: lsn,
	}
	l2PathA1 := cache.l2PathFor(ckA1)
	raw, err := os.ReadFile(l2PathA1)
	if err != nil {
		t.Fatalf("ReadFile L2: %v", err)
	}
	raw[10] ^= 0xFF
	if err := os.WriteFile(l2PathA1, raw, 0o640); err != nil {
		t.Fatalf("corrupt L2: %v", err)
	}
	// Evict A1 from L1 by reading tenantB again so A1 hits L2.
	_, _ = cache.GetPageAtLSN(ctx, tenantB, keyA0, lsn)
	got, err = cache.GetPageAtLSN(ctx, tenantA, keyA1, lsn)
	if err != nil || !bytes.Equal(got, pageA1) {
		t.Fatalf("L2 corruption recovery failed: err=%v", err)
	}
	if st := cache.Stats(); st.L2Corrupt != 1 {
		t.Fatalf("expected L2Corrupt=1, got %+v", st)
	}

	// 7. Concurrent batch + single reads under -race.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				b, cnt, err := cache.GetPageBatch(ctx, tenantA, spcOID, dbOID, relNumber, forkNum, 0, 2, lsn)
				if err != nil || cnt != 2 || !bytes.Equal(b[:PGPageSize], pageA0) || !bytes.Equal(b[PGPageSize:], pageA1) {
					t.Errorf("concurrent GetPageBatch failed: cnt=%d err=%v", cnt, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// 8. InvalidateTenant clears L1 & L2 entries for tenantA.
	cache.InvalidateTenant(tenantA)
	beforeFetches := cache.Stats().L3Fetches
	_, err = cache.GetPageAtLSN(ctx, tenantA, keyA0, lsn)
	if err != nil {
		t.Fatalf("post-invalidate read: %v", err)
	}
	if cache.Stats().L3Fetches != beforeFetches+1 {
		t.Fatalf("expected L3 fetch after InvalidateTenant")
	}
}

func TestTieredCacheStampedeAndRestartRecovery(t *testing.T) {
	ps, err := pageserver.New(pageserver.Config{
		SocketPath: filepath.Join(t.TempDir(), "ps.sock"),
		LayerDir:   filepath.Join(t.TempDir(), "layers"),
	})
	if err != nil {
		t.Fatalf("pageserver.New: %v", err)
	}
	defer ps.Close()

	const (
		tenantID  = "tenant-stampede"
		gen       = "gen-1"
		spcOID    = uint32(1663)
		dbOID     = uint32(5)
		relNumber = uint32(30009)
		lsn       = uint64(0x10000)
	)
	ps.EnsureTenant(tenantID, 1)
	ps.SetTenantGeneration(tenantID, gen)
	page0 := bytes.Repeat([]byte{0x5A}, PGPageSize)
	if err := ps.PutPageBatch(context.Background(), tenantID, spcOID, dbOID, relNumber, 0, 0, 1, lsn, page0); err != nil {
		t.Fatalf("PutPageBatch: %v", err)
	}

	l2Dir := filepath.Join(t.TempDir(), "l2-restart")
	cache1, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: PGPageSize,
		L2Dir:      l2Dir,
		L2MaxBytes: 16 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend 1: %v", err)
	}

	// Fire 20 concurrent misses for the exact same page -> singleflight collapses to 1 L3 fetch.
	const N = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	key := pageserver.RelBlockKey(spcOID, dbOID, relNumber, 0, 0)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			pg, err := cache1.GetPageAtLSN(context.Background(), tenantID, key, lsn)
			if err != nil || !bytes.Equal(pg, page0) {
				t.Errorf("GetPageAtLSN failed: err=%v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	st1 := cache1.Stats()
	if st1.L3Fetches != 1 {
		t.Fatalf("expected L3Fetches=1 under concurrent miss stampede, got %d", st1.L3Fetches)
	}

	// Simulate restart: open a brand-new TieredCacheBackend pointing to the same l2Dir.
	cache2, err := NewTieredCacheBackend(ps, CacheConfig{
		L1MaxBytes: PGPageSize,
		L2Dir:      l2Dir,
		L2MaxBytes: 16 * PGPageSize,
	})
	if err != nil {
		t.Fatalf("NewTieredCacheBackend 2: %v", err)
	}
	pg, err := cache2.GetPageAtLSN(context.Background(), tenantID, key, lsn)
	if err != nil || !bytes.Equal(pg, page0) {
		t.Fatalf("restart L2 recovery failed: err=%v", err)
	}
	st2 := cache2.Stats()
	if st2.L2Hits != 1 || st2.L3Fetches != 0 {
		t.Fatalf("expected L2Hits=1 and L3Fetches=0 after restart, got %+v", st2)
	}
}
