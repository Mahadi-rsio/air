package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestInMemoryLayerSameLSNCoalescing(t *testing.T) {
	im := NewInMemoryLayer(100)
	key := RelBlockKey(1663, 5, 1000, 0, 0)

	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(page[0:8], 42)
	rawRec := make([]byte, 512)

	// First put with both PageImage and redundant WALRecord should strip WALRecord.
	d1 := im.Put(key, WALDelta{
		LSN:       100,
		Flags:     EntryFlagPageImage | EntryFlagWALRecord,
		PageImage: page,
		WALRecord: rawRec,
	})
	if d1 <= 0 || im.ApproxBytes != d1 {
		t.Fatalf("unexpected initial InMemoryLayer bytes: d1=%d, bytes=%d", d1, im.ApproxBytes)
	}
	got := im.Lookup(key, 100)
	if len(got) != 1 {
		t.Fatalf("expected 1 delta, got %d", len(got))
	}
	if len(got[0].WALRecord) != 0 {
		t.Fatalf("expected redundant WALRecord stripped, got len=%d", len(got[0].WALRecord))
	}

	// Second put at the same LSN (e.g. PutPageBatch + WALReceiver FPI) should coalesce in place.
	page2 := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(page2[0:8], 99)
	d2 := im.Put(key, WALDelta{
		LSN:       100,
		Flags:     EntryFlagPageImage,
		PageImage: page2,
	})
	if d2 != 0 {
		t.Fatalf("expected 0 byte delta on same-LSN coalesce, got %d", d2)
	}
	got = im.Lookup(key, 100)
	if len(got) != 1 {
		t.Fatalf("expected coalesced length 1, got %d", len(got))
	}
	if binary.LittleEndian.Uint64(got[0].PageImage[0:8]) != 99 {
		t.Fatalf("expected updated page image after coalesce")
	}

	// Monotonic RelSize updates should also coalesce in place.
	sizeKey := RelSizeKey(1663, 5, 1000, 0)
	im.Put(sizeKey, WALDelta{LSN: 100, Flags: EntryFlagRelSize, RelSize: 1})
	beforeSize := im.ApproxBytes
	im.Put(sizeKey, WALDelta{LSN: 200, Flags: EntryFlagRelSize, RelSize: 2})
	if im.ApproxBytes != beforeSize {
		t.Fatalf("expected RelSize update to coalesce in place: before=%d after=%d", beforeSize, im.ApproxBytes)
	}
	if szList := im.Lookup(sizeKey, 200); len(szList) != 1 || szList[0].RelSize != 2 {
		t.Fatalf("unexpected RelSize coalesced entries: %+v", szList)
	}
}

func TestMemoryBudgetDynamicTenantScaling(t *testing.T) {
	ps, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          t.TempDir(),
		MemoryBudget: MemoryBudgetConfig{
			MemoryLimitBytes:        256 * 1024 * 1024,
			GlobalSoftInMemBytes:    24 * 1024 * 1024,
			GlobalHardInMemBytes:    48 * 1024 * 1024,
			PerTenantSoftInMemBytes: 2 * 1024 * 1024,
			PerTenantHardInMemBytes: 4 * 1024 * 1024,
			ApplyGoRuntimeLimit:     false,
		},
	})
	if err != nil {
		t.Fatalf("NewPageserver: %v", err)
	}
	defer ps.Close()

	page := make([]byte, PageSize)
	for _, count := range []int{1, 4, 8, 10, 20} {
		for i := 0; i < count; i++ {
			tid := fmt.Sprintf("t-%02d", i)
			tl := ps.EnsureTenant(tid, 1)
			_ = tl.PutPageBatch(context.Background(), 1663, 5, 2000, 0, uint32(i), 1, 100, page)
		}
		soft, hard := ps.budget.effectiveTenantLimits()
		if count <= 10 && soft != 2*1024*1024 {
			t.Fatalf("count=%d expected soft=2MiB, got %d", count, soft)
		}
		if count == 20 {
			expectedSoft := int64(24*1024*1024) / 20
			if soft != expectedSoft {
				t.Fatalf("count=20 expected fair-share soft=%d, got %d", expectedSoft, soft)
			}
			if hard < soft*2 {
				t.Fatalf("count=20 expected hard >= 2*soft (%d), got %d", soft*2, hard)
			}
		}
	}
}

func TestMemoryBudgetGlobalAndPerTenantEnforcement(t *testing.T) {
	ps, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          t.TempDir(),
		MemoryBudget: MemoryBudgetConfig{
			MemoryLimitBytes:         64 * 1024 * 1024,
			GlobalSoftInMemBytes:     512 * 1024,
			GlobalHardInMemBytes:     1024 * 1024,
			PerTenantSoftInMemBytes:  128 * 1024,
			PerTenantHardInMemBytes:  256 * 1024,
			MaxConcurrentFlushes:     2,
			MaxConcurrentCompactions: 1,
			IdleFlushAge:             200 * time.Millisecond,
			MonitorInterval:          50 * time.Millisecond,
			ApplyGoRuntimeLimit:      false,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ps.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numTenants = 8
	const pagesPerTenant = 48 // 48 * 8 KiB = 384 KiB per tenant > PerTenantHard (256 KiB), total 3 MiB > GlobalHard (1 MiB)

	for p := uint32(0); p < pagesPerTenant; p++ {
		for tidx := 0; tidx < numTenants; tidx++ {
			tid := fmt.Sprintf("tenant-%d", tidx)
			tl := ps.EnsureTenant(tid, 1)
			page := make([]byte, PageSize)
			binary.LittleEndian.PutUint64(page[0:8], uint64(tidx*1000+int(p)+1))
			if err := tl.PutPageBatch(ctx, 1663, 5, 3000, 0, p, 1, uint64(100+p), page); err != nil {
				t.Fatalf("PutPageBatch(%s, %d): %v", tid, p, err)
			}
		}
	}

	snap := ps.MemoryStats()
	if snap.GlobalInMemBytes > snap.GlobalHardInMemBytes+int64(numTenants*PageSize*2) {
		t.Fatalf("GlobalInMemBytes exceeded hard budget unexpectedly: global=%d hard=%d", snap.GlobalInMemBytes, snap.GlobalHardInMemBytes)
	}
	if snap.FlushesCompleted == 0 {
		t.Fatalf("expected proactive background flushes, got FlushesCompleted=0")
	}

	// Wait for idle flush to drain remaining dirty pages to 0.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ps.MemoryStats().GlobalInMemBytes == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := ps.MemoryStats().GlobalInMemBytes; got != 0 {
		t.Fatalf("expected GlobalInMemBytes to drain to 0 after idle flush, got %d", got)
	}

	// Verify 100% data integrity across all tenants and pages after flush & eviction.
	for tidx := 0; tidx < numTenants; tidx++ {
		tid := fmt.Sprintf("tenant-%d", tidx)
		tl := ps.EnsureTenant(tid, 1)
		for p := uint32(0); p < pagesPerTenant; p++ {
			got, err := tl.GetPageAtLSN(ctx, RelBlockKey(1663, 5, 3000, 0, p), uint64(100+pagesPerTenant))
			if err != nil {
				t.Fatalf("GetPageAtLSN(%s, %d): %v", tid, p, err)
			}
			wantVal := uint64(tidx*1000 + int(p) + 1)
			if gotVal := binary.LittleEndian.Uint64(got[0:8]); gotVal != wantVal {
				t.Fatalf("data corruption for %s page %d: got %d, want %d", tid, p, gotVal, wantVal)
			}
		}
	}
}

func TestMemoryBudgetBackpressureAndNoDataLoss(t *testing.T) {
	ps, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          t.TempDir(),
		MemoryBudget: MemoryBudgetConfig{
			MemoryLimitBytes:        64 * 1024 * 1024,
			GlobalSoftInMemBytes:    16 * 1024,
			GlobalHardInMemBytes:    24 * 1024,
			PerTenantSoftInMemBytes: 16 * 1024,
			PerTenantHardInMemBytes: 24 * 1024,
			MaxConcurrentFlushes:    1,
			MonitorInterval:         time.Hour,
			ApplyGoRuntimeLimit:     false,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ps.Close()

	// Occupy the single flush slot so flush cannot complete before the short deadline.
	if err := ps.budget.acquireFlushSlot(context.Background()); err != nil {
		t.Fatalf("acquireFlushSlot: %v", err)
	}

	tl := ps.EnsureTenant("bp-tenant", 1)
	page := bytes.Repeat([]byte{0xAB}, PageSize)

	// Fill past hard watermark (24 KiB -> 3 pages = ~25 KiB).
	for p := uint32(0); p < 3; p++ {
		if err := tl.PutPageBatch(context.Background(), 1663, 5, 4000, 0, p, 1, uint64(100+p), page); err != nil {
			t.Fatalf("PutPageBatch(%d): %v", p, err)
		}
	}

	// Next write with short deadline should fail with ErrMemoryBudgetExhausted, not discard dirty pages.
	shortCtx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err = tl.PutPageBatch(shortCtx, 1663, 5, 4000, 0, 3, 1, 103, page)
	if !errors.Is(err, ErrMemoryBudgetExhausted) {
		t.Fatalf("expected ErrMemoryBudgetExhausted, got %v", err)
	}

	// Release flush slot and verify previously written 3 dirty pages were NOT discarded.
	ps.budget.releaseFlushSlot()
	if _, err := tl.FlushInMemory(context.Background()); err != nil {
		t.Fatalf("FlushInMemory: %v", err)
	}
	for p := uint32(0); p < 3; p++ {
		got, err := tl.GetPageAtLSN(context.Background(), RelBlockKey(1663, 5, 4000, 0, p), 103)
		if err != nil || !bytes.Equal(got, page) {
			t.Fatalf("dirty page %d lost after backpressure rejection: err=%v", p, err)
		}
	}
}

func TestSLRUEvictionAndWarmingAfterFlush(t *testing.T) {
	ps, err := New(Config{
		DisableUnixSocket: true,
		LayerDir:          t.TempDir(),
		MemoryBudget: MemoryBudgetConfig{
			MemoryLimitBytes:    64 * 1024 * 1024,
			ApplyGoRuntimeLimit: false,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ps.Close()

	tl := ps.EnsureTenant("slru-tenant", 1)

	// Simulate two committed transactions on pg_xact (xid=500, then flush, then xid=501 on same SLRU page).
	tl.ApplyWALRecord(&DecodedWALRecord{
		Rmid:   RmXactID,
		Info:   XLogXactCommit,
		XID:    500,
		EndLSN: 200,
	})
	if _, err := tl.FlushInMemory(context.Background()); err != nil {
		t.Fatalf("FlushInMemory: %v", err)
	}

	// Verify slruPages cache was evicted after durable L0 flush.
	tl.mu.Lock()
	cachedAfterFlush := len(tl.slruPages)
	tl.mu.Unlock()
	if cachedAfterFlush != 0 {
		t.Fatalf("expected slruPages to be evicted after flush, got %d entries", cachedAfterFlush)
	}

	// Commit xid=501 on the same SLRU page: warmXactSLRU must reload xid=500's status from L0 before setting xid=501.
	tl.ApplyWALRecord(&DecodedWALRecord{
		Rmid:   RmXactID,
		Info:   XLogXactCommit,
		XID:    501,
		EndLSN: 300,
	})

	slruRel := Key{Kind: KeyKindSLRU, SpcOID: 0, BlockNum: 0}
	page, err := tl.GetPageAtLSN(context.Background(), slruRel, 300)
	if err != nil {
		t.Fatalf("GetPageAtLSN(slru): %v", err)
	}
	// Each byte holds 4 XIDs (2 bits per XID). 0x01 = XactStatusCommitted.
	status500 := (page[500/4] >> ((500 % 4) * 2)) & 0x03
	status501 := (page[501/4] >> ((501 % 4) * 2)) & 0x03
	if status500 != 0x01 || status501 != 0x01 {
		t.Fatalf("expected both xid 500 and 501 committed after SLRU warm: got 500=%d 501=%d", status500, status501)
	}
}
