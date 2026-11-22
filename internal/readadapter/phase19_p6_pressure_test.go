package readadapter

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
)

func TestP6_CgroupMemoryPressureParserAndAdaptiveDirectIO(t *testing.T) {
	lowPSI := "some avg10=0.25 avg60=0.10 avg300=0.05 total=100\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
	if ParseCgroupMemoryPressure(lowPSI) {
		t.Fatalf("expected low PSI to return false")
	}
	highPSI := "some avg10=14.50 avg60=5.00 avg300=1.00 total=5000\nfull avg10=0.50 avg60=0.10 avg300=0.00 total=100\n"
	if !ParseCgroupMemoryPressure(highPSI) {
		t.Fatalf("expected high PSI (some avg10=14.50) to return true")
	}
	highFullPSI := "some avg10=3.00 avg60=1.00 avg300=0.50 total=500\nfull avg10=2.50 avg60=1.00 avg300=0.50 total=200\n"
	if !ParseCgroupMemoryPressure(highFullPSI) {
		t.Fatalf("expected high full PSI (full avg10=2.50) to return true")
	}

	// Verify LowMemory512MBProfile wires DefaultCgroupMemoryPressureFn automatically.
	l3 := newMockL3(4)
	cProf, err := NewTieredCacheBackend(l3, LowMemory512MBProfile(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if cProf.memPressureFn == nil || !cProf.l2AdaptiveDirectIO {
		t.Fatalf("expected LowMemory512MBProfile to wire memPressureFn and l2AdaptiveDirectIO")
	}

	// Verify adaptive Direct I/O: buffered when not under pressure, O_DIRECT when under pressure.
	var underPressure atomic.Bool
	c, err := NewTieredCacheBackend(l3, CacheConfig{
		L1MaxBytes:         PGPageSize,
		L2Dir:              t.TempDir(),
		L2MaxBytes:         1024 * 1024,
		L2MaxEntries:       64,
		L2AdaptiveDirectIO: true,
		MemPressureFn:      underPressure.Load,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// 1. Not under pressure -> buffered I/O (zero DirectIO ops/fallbacks)
	if _, err := c.GetPageAtLSN(ctx, "t1", pgKey(0), 1000); err != nil {
		t.Fatal(err)
	}
	if st := c.Stats(); st.L2DirectIOOps != 0 || st.L2DirectIOFallbacks != 0 {
		t.Fatalf("expected buffered I/O when not under pressure, got ops=%d fb=%d", st.L2DirectIOOps, st.L2DirectIOFallbacks)
	}

	// 2. Under pressure -> O_DIRECT (or clean EINVAL fallback on tmpfs)
	underPressure.Store(true)
	pg, err := c.GetPageAtLSN(ctx, "t1", pgKey(1), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(pg[0:4]) != 2 {
		t.Fatalf("unexpected page data")
	}
	if st := c.Stats(); st.L2DirectIOOps == 0 && st.L2DirectIOFallbacks == 0 {
		t.Fatalf("expected O_DIRECT attempt under pressure, got ops=0 fb=0")
	}
}
