package pageserver

import (
	"context"
	"errors"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrMemoryBudgetExhausted is returned when the global or per-tenant hard memory budget
	// remains exhausted after flushing and waiting until ctx deadline/cancellation.
	ErrMemoryBudgetExhausted = errors.New("pageserver: memory budget exhausted")
)

// MemoryBudgetConfig configures global and per-tenant memory budgets for the AIR Pageserver.
type MemoryBudgetConfig struct {
	// MemoryLimitBytes is the container/process memory cap. If <= 0, auto-detected from
	// cgroup v2 /sys/fs/cgroup/memory.max (defaulting to 256 MiB if unlimited/unreadable).
	MemoryLimitBytes int64
	// GoMemoryLimitBytes configures debug.SetMemoryLimit so Go's GC and scavenger promptly
	// return HeapIdle spans to the OS. Defaults to 72% of MemoryLimitBytes.
	GoMemoryLimitBytes int64
	// GlobalSoftInMemBytes triggers proactive background L0 flushing across dirty timelines.
	// Defaults to 10% of MemoryLimitBytes (e.g. 24 MiB on a 256 MiB Pageserver).
	GlobalSoftInMemBytes int64
	// GlobalHardInMemBytes applies admission backpressure to writers until in-flight flushes complete.
	// Defaults to 20% of MemoryLimitBytes (e.g. 48 MiB on a 256 MiB Pageserver).
	GlobalHardInMemBytes int64
	// PerTenantSoftInMemBytes triggers per-tenant async L0 flushing.
	// Defaults to 2 MiB on a 256 MiB Pageserver (4 MiB on >= 512 MiB).
	PerTenantSoftInMemBytes int64
	// PerTenantHardInMemBytes applies per-tenant backpressure until the tenant's L0 flush completes.
	// Defaults to 2 * PerTenantSoftInMemBytes.
	PerTenantHardInMemBytes int64
	// DecodedChunkCacheBytes bounds the decompressed V2 chunk cache.
	// Defaults to 16 MiB on a 256 MiB Pageserver (32 MiB on >= 512 MiB).
	DecodedChunkCacheBytes int64
	// IndexCacheBytes bounds the in-memory layer index cache.
	// Defaults to 16 MiB on a 256 MiB Pageserver (32 MiB on >= 512 MiB).
	IndexCacheBytes int64
	// MaxConcurrentFlushes bounds simultaneous L0 flushes across tenants. Defaults to 2.
	MaxConcurrentFlushes int
	// MaxConcurrentCompactions bounds simultaneous L0->L1 compactions across tenants. Defaults to 1.
	MaxConcurrentCompactions int
	// IdleFlushAge flushes any dirty InMemoryLayer that has not received writes for this duration.
	// Defaults to 750ms.
	IdleFlushAge time.Duration
	// MonitorInterval is the tick interval of the memory-pressure monitor loop. Defaults to 200ms.
	MonitorInterval time.Duration
	// ApplyGoRuntimeLimit controls whether debug.SetMemoryLimit is applied globally.
	ApplyGoRuntimeLimit bool
}

// MemoryStatsSnapshot reports measurable point-in-time memory metrics and budget state.
type MemoryStatsSnapshot struct {
	MemoryLimitBytes        int64            `json:"memory_limit_bytes"`
	GoMemoryLimitBytes      int64            `json:"go_memory_limit_bytes"`
	GlobalSoftInMemBytes    int64            `json:"global_soft_inmem_bytes"`
	GlobalHardInMemBytes    int64            `json:"global_hard_inmem_bytes"`
	PerTenantSoftInMemBytes int64            `json:"per_tenant_soft_inmem_bytes"`
	PerTenantHardInMemBytes int64            `json:"per_tenant_hard_inmem_bytes"`
	GlobalInMemBytes        int64            `json:"global_inmem_bytes"`
	TotalDirtyPages         int64            `json:"total_dirty_pages"`
	TenantInMemBytes        map[string]int64 `json:"tenant_inmem_bytes"`
	TenantDirtyPages        map[string]int   `json:"tenant_dirty_pages"`
	DecodedChunkCacheBytes  int64            `json:"decoded_chunk_cache_bytes"`
	IndexCacheBytes         int64            `json:"index_cache_bytes"`
	HeapAllocBytes          uint64           `json:"heap_alloc_bytes"`
	HeapInuseBytes          uint64           `json:"heap_inuse_bytes"`
	HeapIdleBytes           uint64           `json:"heap_idle_bytes"`
	HeapReleasedBytes       uint64           `json:"heap_released_bytes"`
	TotalAllocBytes         uint64           `json:"total_alloc_bytes"`
	NumGC                   uint32           `json:"num_gc"`
	GCPauseTotalNs          uint64           `json:"gc_pause_total_ns"`
	FlushesCompleted        int64            `json:"flushes_completed"`
	FlushedBytesTotal       int64            `json:"flushed_bytes_total"`
	BackpressureWaits       int64            `json:"backpressure_waits"`
	BackpressureRejects     int64            `json:"backpressure_rejects"`
	PressureEvictions       int64            `json:"pressure_evictions"`
}

func detectCgroupMemoryMaxBytes() int64 {
	if v := strings.TrimSpace(os.Getenv("AIR_PAGESERVER_MEMORY_MB")); v != "" {
		if mb, err := strconv.ParseInt(v, 10, 64); err == nil && mb > 0 {
			return mb * 1024 * 1024
		}
	}
	raw, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err == nil {
		s := strings.TrimSpace(string(raw))
		if s != "" && s != "max" {
			if b, err := strconv.ParseInt(s, 10, 64); err == nil && b > 0 && b <= 64*1024*1024*1024 {
				return b
			}
		}
	}
	return 256 * 1024 * 1024
}

func readCgroupMemoryCurrentBytes() int64 {
	raw, err := os.ReadFile("/sys/fs/cgroup/memory.current")
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

// ResolveMemoryBudgetConfig fills zero-value fields with safe defaults scaled to MemoryLimitBytes.
func ResolveMemoryBudgetConfig(cfg MemoryBudgetConfig) MemoryBudgetConfig {
	if cfg.MemoryLimitBytes <= 0 {
		cfg.MemoryLimitBytes = detectCgroupMemoryMaxBytes()
	}
	const mib = 1024 * 1024
	if cfg.MemoryLimitBytes < 64*mib {
		cfg.MemoryLimitBytes = 64 * mib
	}
	if cfg.GoMemoryLimitBytes <= 0 {
		cfg.GoMemoryLimitBytes = (cfg.MemoryLimitBytes * 52) / 100
	}
	if cfg.GlobalSoftInMemBytes <= 0 {
		cfg.GlobalSoftInMemBytes = (cfg.MemoryLimitBytes * 625) / 10000 // 16 MiB on 256 MiB, 32 MiB on 512 MiB
		if cfg.GlobalSoftInMemBytes < 8*mib {
			cfg.GlobalSoftInMemBytes = 8 * mib
		}
		if cfg.GlobalSoftInMemBytes > 48*mib {
			cfg.GlobalSoftInMemBytes = 48 * mib
		}
	}
	if cfg.GlobalHardInMemBytes <= 0 {
		cfg.GlobalHardInMemBytes = cfg.GlobalSoftInMemBytes * 2
	}
	if cfg.PerTenantSoftInMemBytes <= 0 {
		if cfg.MemoryLimitBytes <= 256*mib {
			cfg.PerTenantSoftInMemBytes = 2 * mib
		} else {
			cfg.PerTenantSoftInMemBytes = 4 * mib
		}
	}
	if cfg.PerTenantHardInMemBytes <= 0 {
		cfg.PerTenantHardInMemBytes = cfg.PerTenantSoftInMemBytes * 2
	}
	if cfg.DecodedChunkCacheBytes <= 0 {
		if cfg.MemoryLimitBytes <= 256*mib {
			cfg.DecodedChunkCacheBytes = 8 * mib
		} else {
			cfg.DecodedChunkCacheBytes = 16 * mib
		}
	}
	if cfg.IndexCacheBytes <= 0 {
		if cfg.MemoryLimitBytes <= 256*mib {
			cfg.IndexCacheBytes = 8 * mib
		} else {
			cfg.IndexCacheBytes = 16 * mib
		}
	}
	if cfg.MaxConcurrentFlushes <= 0 {
		cfg.MaxConcurrentFlushes = 2
	}
	if cfg.MaxConcurrentCompactions <= 0 {
		cfg.MaxConcurrentCompactions = 1
	}
	if cfg.IdleFlushAge <= 0 {
		cfg.IdleFlushAge = 750 * time.Millisecond
	}
	if cfg.MonitorInterval <= 0 {
		cfg.MonitorInterval = 200 * time.Millisecond
	}
	return cfg
}

// MemoryBudget coordinates global and per-tenant InMemoryLayer budgets, bounded background flush
// and compaction semaphores, memory-pressure eviction, and writer admission backpressure.
type MemoryBudget struct {
	cfg           MemoryBudgetConfig
	ps            *Pageserver
	globalInMem   atomic.Int64
	flushesTotal  atomic.Int64
	flushedBytes  atomic.Int64
	bpWaits       atomic.Int64
	bpRejects     atomic.Int64
	pressureEvict atomic.Int64
	lastScavenge  atomic.Int64

	flushSem   chan struct{}
	compactSem chan struct{}

	notifyMu sync.Mutex
	notifyCh chan struct{}

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func newMemoryBudget(ps *Pageserver, cfg MemoryBudgetConfig) *MemoryBudget {
	cfg = ResolveMemoryBudgetConfig(cfg)
	if cfg.ApplyGoRuntimeLimit && cfg.GoMemoryLimitBytes > 0 {
		debug.SetMemoryLimit(cfg.GoMemoryLimitBytes)
	}
	return &MemoryBudget{
		cfg:        cfg,
		ps:         ps,
		flushSem:   make(chan struct{}, cfg.MaxConcurrentFlushes),
		compactSem: make(chan struct{}, cfg.MaxConcurrentCompactions),
		notifyCh:   make(chan struct{}),
		stopCh:     make(chan struct{}),
	}
}

func (b *MemoryBudget) start() {
	b.wg.Add(1)
	go b.monitorLoop()
}

func (b *MemoryBudget) close() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
	})
	b.wg.Wait()
}

func (b *MemoryBudget) addInMemBytes(delta int64) int64 {
	next := b.globalInMem.Add(delta)
	if next < 0 {
		b.globalInMem.Store(0)
		return 0
	}
	return next
}

func (b *MemoryBudget) notifyFlushCompleted(flushedBytes int64) {
	if flushedBytes > 0 {
		b.flushesTotal.Add(1)
		b.flushedBytes.Add(flushedBytes)
	}
	b.notifyMu.Lock()
	close(b.notifyCh)
	b.notifyCh = make(chan struct{})
	b.notifyMu.Unlock()
}

func (b *MemoryBudget) waitCh() <-chan struct{} {
	b.notifyMu.Lock()
	ch := b.notifyCh
	b.notifyMu.Unlock()
	return ch
}

func (b *MemoryBudget) acquireFlushSlot(ctx context.Context) error {
	select {
	case b.flushSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *MemoryBudget) releaseFlushSlot() {
	<-b.flushSem
}

func (b *MemoryBudget) tryAcquireCompactSlot() bool {
	select {
	case b.compactSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (b *MemoryBudget) releaseCompactSlot() {
	<-b.compactSem
}

// effectiveTenantLimits dynamically scales per-tenant soft/hard limits when many tenants
// are simultaneously dirty so their sum stays within GlobalSoftInMemBytes / GlobalHardInMemBytes.
func (b *MemoryBudget) effectiveTenantLimits() (soft, hard int64) {
	soft = b.cfg.PerTenantSoftInMemBytes
	hard = b.cfg.PerTenantHardInMemBytes
	if b.ps == nil {
		return soft, hard
	}
	dirtyTenants := b.ps.dirtyTenantCount()
	if dirtyTenants > 1 {
		fairSoft := b.cfg.GlobalSoftInMemBytes / int64(dirtyTenants)
		const minSoft = 512 * 1024 // 512 KiB floor
		if fairSoft < minSoft {
			fairSoft = minSoft
		}
		if fairSoft < soft {
			soft = fairSoft
		}
		fairHard := b.cfg.GlobalHardInMemBytes / int64(dirtyTenants)
		if fairHard < soft*2 {
			fairHard = soft * 2
		}
		if fairHard < hard {
			hard = fairHard
		}
	}
	return soft, hard
}

// enforceBeforeWrite checks per-tenant and global InMemoryLayer watermarks before admitting writes.
// At soft watermarks it schedules background flushes; at hard watermarks it flushes and blocks until
// memory drops below the hard watermark or ctx expires.
func (b *MemoryBudget) enforceBeforeWrite(ctx context.Context, t *Timeline) error {
	if t == nil || t.store == nil {
		return nil
	}
	soft, hard := b.effectiveTenantLimits()
	if t.maxInMemBytes > 0 && t.maxInMemBytes < soft {
		soft = t.maxInMemBytes
		hard = t.hardMaxInMemBytes()
	}

	curTenant := t.InMemBytes()
	curGlobal := b.globalInMem.Load()

	if curTenant >= soft {
		t.MaybeFlushAsync()
	}
	if curGlobal >= b.cfg.GlobalSoftInMemBytes && b.ps != nil {
		b.ps.flushLargestDirtyTenantsAsync(2)
	}

	if curTenant < hard && curGlobal < b.cfg.GlobalHardInMemBytes {
		return nil
	}

	b.bpWaits.Add(1)
	for curTenant >= hard || curGlobal >= b.cfg.GlobalHardInMemBytes {
		if err := ctx.Err(); err != nil {
			b.bpRejects.Add(1)
			return errors.Join(ErrMemoryBudgetExhausted, err)
		}
		ch := b.waitCh()
		if curTenant > 0 {
			t.MaybeFlushAsync()
		}
		if curGlobal >= b.cfg.GlobalSoftInMemBytes && b.ps != nil {
			b.ps.flushLargestDirtyTenantsAsync(2)
		}

		// Check if our own timeline has an active flush or if we can flush synchronously.
		t.bgFlushMu.Lock()
		flushing := t.bgFlushing
		done := t.flushDoneCh
		lastErr := t.lastFlushErr
		t.bgFlushMu.Unlock()

		if !flushing && lastErr != nil {
			return lastErr
		}

		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ch:
			timer.Stop()
		case <-done:
			timer.Stop()
		case <-timer.C:
		case <-b.stopCh:
			timer.Stop()
			return nil
		case <-ctx.Done():
			timer.Stop()
			b.bpRejects.Add(1)
			return errors.Join(ErrMemoryBudgetExhausted, ctx.Err())
		}

		soft, hard = b.effectiveTenantLimits()
		if t.maxInMemBytes > 0 && t.maxInMemBytes < soft {
			soft = t.maxInMemBytes
			hard = t.hardMaxInMemBytes()
		}
		curTenant = t.InMemBytes()
		curGlobal = b.globalInMem.Load()
	}
	return nil
}

func (b *MemoryBudget) monitorLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.cfg.MonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.runMaintenanceStep()
		}
	}
}

func (b *MemoryBudget) runMaintenanceStep() {
	if b.ps == nil {
		return
	}
	now := time.Now()
	timelines := b.ps.snapshotTimelines()

	type dirtyCandidate struct {
		tl    *Timeline
		bytes int64
		idle  time.Duration
	}
	var candidates []dirtyCandidate
	var totalInMem int64

	for _, tl := range timelines {
		sz := tl.InMemBytes()
		totalInMem += sz
		if sz > 0 {
			lastW := tl.lastWriteTime()
			var idle time.Duration
			if !lastW.IsZero() {
				idle = now.Sub(lastW)
			}
			candidates = append(candidates, dirtyCandidate{tl: tl, bytes: sz, idle: idle})
		}
	}
	b.globalInMem.Store(totalInMem)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	unreleasedIdle := int64(ms.HeapIdle) - int64(ms.HeapReleased)
	liveAndIdleAnon := int64(ms.HeapInuse) + unreleasedIdle + int64(ms.StackInuse) + int64(ms.GCSys)
	cgroupCur := readCgroupMemoryCurrentBytes()

	// Check memory pressure against GoMemoryLimitBytes, process anon, and container cgroup memory.
	highPressure := totalInMem >= b.cfg.GlobalSoftInMemBytes ||
		int64(ms.HeapAlloc) >= (b.cfg.GoMemoryLimitBytes*75)/100 ||
		liveAndIdleAnon >= (b.cfg.MemoryLimitBytes*65)/100 ||
		(cgroupCur > 0 && cgroupCur >= (b.cfg.MemoryLimitBytes*78)/100)

	soft, _ := b.effectiveTenantLimits()
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].bytes > candidates[j].bytes
	})

	flushedAny := 0
	for _, c := range candidates {
		if c.bytes >= soft || c.idle >= b.cfg.IdleFlushAge || (highPressure && flushedAny < b.cfg.MaxConcurrentFlushes) {
			c.tl.MaybeFlushAsync()
			flushedAny++
			if highPressure {
				b.pressureEvict.Add(1)
			}
		}
	}

	// Background L0 compaction when a clean timeline has >= 8 L0 layers and memory headroom is ample.
	canCompact := !highPressure &&
		liveAndIdleAnon < (b.cfg.MemoryLimitBytes*50)/100 &&
		(cgroupCur == 0 || cgroupCur < (b.cfg.MemoryLimitBytes*70)/100)
	if canCompact {
		for _, tl := range timelines {
			if tl.InMemBytes() == 0 && tl.L0LayerCount() >= 8 {
				if b.tryAcquireCompactSlot() {
					go func(target *Timeline) {
						defer b.releaseCompactSlot()
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						_, _ = target.CompactL0Batch(ctx, 8, 4)
					}(tl)
				}
				break
			}
		}
	}

	// Under high memory pressure or unreleased HeapIdle (> 12 MiB), trim caches and scavenge OS memory.
	if highPressure || unreleasedIdle > 12*1024*1024 {
		last := b.lastScavenge.Load()
		if now.UnixNano()-last > int64(500*time.Millisecond) && b.lastScavenge.CompareAndSwap(last, now.UnixNano()) {
			if highPressure && b.ps.store != nil {
				if dc := b.ps.store.DecodedChunkCache(); dc != nil {
					dc.TrimToBytes(b.cfg.DecodedChunkCacheBytes / 4)
				}
				if ic := b.ps.store.idxCache; ic != nil {
					ic.trimToBytes(b.cfg.IndexCacheBytes / 4)
				}
			}
			debug.FreeOSMemory()
		}
	}
}
