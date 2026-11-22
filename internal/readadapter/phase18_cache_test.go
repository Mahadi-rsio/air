package readadapter

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

type mockL3Backend struct {
	pages map[uint32][]byte
	calls atomic.Uint64
}

func newMockL3(nPages int) *mockL3Backend {
	m := &mockL3Backend{pages: make(map[uint32][]byte, nPages)}
	for i := 0; i < nPages; i++ {
		pg := make([]byte, PGPageSize)
		binary.LittleEndian.PutUint32(pg[0:4], uint32(i+1))
		binary.LittleEndian.PutUint32(pg[PGPageSize-4:], uint32(0xDEAD0000+i))
		m.pages[uint32(i)] = pg
	}
	return m
}

func (m *mockL3Backend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	m.calls.Add(1)
	if pg, ok := m.pages[key.BlockNum]; ok {
		cp := make([]byte, PGPageSize)
		copy(cp, pg)
		return cp, nil
	}
	return nil, os.ErrNotExist
}

func (m *mockL3Backend) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	m.calls.Add(1)
	out := make([]byte, int(nblocks)*PGPageSize)
	var got uint32
	for i := uint32(0); i < nblocks; i++ {
		if pg, ok := m.pages[startBlock+i]; ok {
			copy(out[int(i)*PGPageSize:], pg)
			got++
		} else {
			break
		}
	}
	return out[:int(got)*PGPageSize], got, nil
}

func (m *mockL3Backend) GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	return uint32(len(m.pages)), nil
}

func (m *mockL3Backend) GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error) {
	return true, nil
}

func (m *mockL3Backend) GetTenantGeneration(tenantID string) string { return "gen1" }

func pgKey(blk uint32) pageserver.Key {
	return pageserver.Key{Kind: pageserver.KeyKindRelBlock, SpcOID: 1663, DbOID: 1, RelNumber: 100, ForkNum: 0, BlockNum: blk}
}

func TestPhase18D_StrictlyBoundedL2AndBypass(t *testing.T) {
	ctx := context.Background()
	l3 := newMockL3(20)
	l2Dir := t.TempDir()

	// Strict 4-entry cap and 100KB byte cap
	c, err := NewTieredCacheBackend(l3, CacheConfig{
		L1MaxBytes:        2 * PGPageSize,
		L2Dir:             l2Dir,
		L2MaxBytes:        100 * 1024,
		L2MaxEntries:      4,
		L2MaxEntryBytes:   16384,
		L2MaxConcurrentIO: 4,
		L2DropPageCache:   true,
	})
	if err != nil {
		t.Fatal(err)
	}

	for blk := uint32(0); blk < 10; blk++ {
		pg, err := c.GetPageAtLSN(ctx, "t1", pgKey(blk), 1000)
		if err != nil {
			t.Fatalf("GetPageAtLSN(%d): %v", blk, err)
		}
		if binary.LittleEndian.Uint32(pg[0:4]) != blk+1 {
			t.Fatalf("unexpected page content for blk %d", blk)
		}
	}

	st := c.Stats()
	if st.L2Entries > 4 {
		t.Fatalf("L2Entries=%d exceeded L2MaxEntries=4", st.L2Entries)
	}
	if st.L2Bytes > 4*(PGPageSize+4) {
		t.Fatalf("L2Bytes=%d exceeded 4 entries", st.L2Bytes)
	}
	if st.L2Evictions == 0 {
		t.Fatalf("expected L2 evictions > 0")
	}

	// Test bypass under memory pressure
	var underPressure atomic.Bool
	underPressure.Store(true)
	l2DirP := t.TempDir()
	cp, err := NewTieredCacheBackend(l3, CacheConfig{
		L1MaxBytes:            2 * PGPageSize,
		L2Dir:                 l2DirP,
		L2MaxBytes:            1024 * 1024,
		L2BypassUnderPressure: true,
		MemPressureFn:         underPressure.Load,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = cp.GetPageAtLSN(ctx, "t1", pgKey(0), 1000)
	if err != nil {
		t.Fatal(err)
	}
	pst := cp.Stats()
	if pst.L2Entries != 0 || pst.L2Bypasses == 0 {
		t.Fatalf("expected L2 bypass under memory pressure, got entries=%d bypasses=%d", pst.L2Entries, pst.L2Bypasses)
	}

	// Test L2Disabled
	cd, err := NewTieredCacheBackend(l3, CacheConfig{
		L1MaxBytes: 2 * PGPageSize,
		L2Dir:      t.TempDir(),
		L2Disabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cd.GetPageAtLSN(ctx, "t1", pgKey(0), 1000)
	if cd.Stats().L2Entries != 0 || cd.Stats().L2Bytes != 0 {
		t.Fatalf("expected 0 L2 usage when L2Disabled=true")
	}
}

func TestPhase18E_DirectIOAndFallback(t *testing.T) {
	ctx := context.Background()
	l3 := newMockL3(6)
	l2Dir := t.TempDir()

	c, err := NewTieredCacheBackend(l3, CacheConfig{
		L1MaxBytes: PGPageSize, // tiny L1 so reads hit L2
		L2Dir:      l2Dir,
		L2MaxBytes: 1024 * 1024,
		L2DirectIO: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for blk := uint32(0); blk < 4; blk++ {
		if _, err := c.GetPageAtLSN(ctx, "t1", pgKey(blk), 1000); err != nil {
			t.Fatal(err)
		}
	}
	// Evict from L1 by reading blk 4, then re-read blk 0 (must hit L2 or fallback cleanly)
	_, _ = c.GetPageAtLSN(ctx, "t1", pgKey(4), 1000)
	pg, err := c.GetPageAtLSN(ctx, "t1", pgKey(0), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(pg[0:4]) != 1 {
		t.Fatalf("data mismatch after O_DIRECT / fallback L2 read")
	}
	st := c.Stats()
	if st.L2DirectIOOps == 0 && st.L2DirectIOFallbacks == 0 {
		t.Fatalf("expected either O_DIRECT ops or clean fallback counter > 0")
	}
}

func TestPhase18I_CacheSafetyAndCorruptionRecovery(t *testing.T) {
	ctx := context.Background()
	l3 := newMockL3(8)
	l2Dir := t.TempDir()

	// Seed stale .tmp. and truncated .pg files before startup
	sub := filepath.Join(l2Dir, "ab")
	_ = os.MkdirAll(sub, 0o750)
	_ = os.WriteFile(filepath.Join(sub, "stale.pg.tmp.1.1"), []byte("partial"), 0o640)
	_ = os.WriteFile(filepath.Join(sub, "trunc.pg"), []byte("short"), 0o640)

	c, err := NewTieredCacheBackend(l3, CacheConfig{
		L1MaxBytes:   PGPageSize,
		L2Dir:        l2Dir,
		L2MaxBytes:   128 * 1024,
		L2MaxEntries: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(sub, "stale.pg.tmp.1.1")); !os.IsNotExist(err) {
		t.Fatalf("expected stale tmp file to be cleaned on startup")
	}
	if _, err := os.Stat(filepath.Join(sub, "trunc.pg")); !os.IsNotExist(err) {
		t.Fatalf("expected truncated .pg file to be cleaned on startup")
	}

	// Populate L2 for blk 0, then corrupt its file on disk
	if _, err := c.GetPageAtLSN(ctx, "t1", pgKey(0), 1000); err != nil {
		t.Fatal(err)
	}
	// Evict blk 0 from L1 by reading blk 1 & 2
	_, _ = c.GetPageAtLSN(ctx, "t1", pgKey(1), 1000)
	_, _ = c.GetPageAtLSN(ctx, "t1", pgKey(2), 1000)

	k0 := PageCacheKey{TenantID: "t1", Generation: "gen1", SpcOID: 1663, DbOID: 1, RelNumber: 100, ForkNum: 0, BlockNum: 0, LSN: 1000}
	p0 := c.l2PathFor(k0)
	raw, err := os.ReadFile(p0)
	if err != nil {
		t.Fatal(err)
	}
	raw[10] ^= 0xFF
	if err := os.WriteFile(p0, raw, 0o640); err != nil {
		t.Fatal(err)
	}

	// Reading blk 0 must detect CRC mismatch, discard corrupt L2 entry, and recover from L3
	pg, err := c.GetPageAtLSN(ctx, "t1", pgKey(0), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(pg[0:4]) != 1 {
		t.Fatalf("expected clean L3 recovery after L2 corruption")
	}
	if c.Stats().L2Corrupt == 0 {
		t.Fatalf("expected L2Corrupt > 0 after bit-flip")
	}

	// Concurrent eviction + reads stress test
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				blk := uint32((id*7 + i) % 8)
				_, _ = c.GetPageAtLSN(ctx, "t1", pgKey(blk), 1000)
			}
		}(g)
	}
	wg.Wait()
	if st := c.Stats(); st.L2Entries > 4 {
		t.Fatalf("concurrent eviction violated L2MaxEntries=4: got %d", st.L2Entries)
	}

	// Malformed extended type payloads must error cleanly without panic
	if _, err := DecodePGNumeric([]byte{0x01}); err == nil {
		t.Fatalf("expected error on truncated numeric")
	}
	if _, err := DecodePGInet([]byte{2, 99, 1, 2, 3, 4}, false); err == nil {
		t.Fatalf("expected error on invalid ipv4 prefix")
	}
	if _, err := DecodePGArray([]byte{1, 0, 0, 0}, PGOIDInt4); err == nil {
		t.Fatalf("expected error on truncated array")
	}
}

func TestPhase18F_PrometheusMetrics(t *testing.T) {
	srv, err := NewSQLiteReadServer(SQLiteServerConfig{
		DataDir:      t.TempDir(),
		PGPort:       0,
		L1CacheBytes: 4 * 1024 * 1024,
		CacheDir:     t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	prom := srv.PrometheusMetrics()
	required := []string{
		"air_read_queries_total",
		"air_native_read_queries_total",
		"air_postgres_fallback_queries_total",
		"air_postgres_fallback_total{reason=",
		"air_cache_l1_hits_total",
		"air_cache_l2_hits_total",
		"air_cache_misses_total",
		"air_cache_evictions_total",
		"air_l2_cache_bytes",
		"air_l2_cache_entries",
		"air_tenant_memory_bytes",
		"air_cgroup_memory_bytes",
	}
	for _, metric := range required {
		if !containsStr(prom, metric) {
			t.Fatalf("missing required Prometheus metric %q in:\n%s", metric, prom)
		}
	}
}

func containsStr(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}())
}
