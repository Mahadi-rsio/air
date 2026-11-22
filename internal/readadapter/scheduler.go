package readadapter

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

// QueryPriority defines scheduling priority for multi-tenant admission control.
type QueryPriority int

const (
	PriorityBatch       QueryPriority = 0
	PriorityNormal      QueryPriority = 1
	PriorityInteractive QueryPriority = 2
)

var (
	// ErrSchedulerQueueFull is returned when the bounded admission queue is saturated.
	ErrSchedulerQueueFull = errors.New("readadapter: workload scheduler queue full (backpressure)")
	// ErrMemoryPressure is returned when memory admission control rejects a new query.
	ErrMemoryPressure = errors.New("readadapter: admission rejected due to memory pressure")
)

type priorityCtxKey struct{}

// ContextWithQueryPriority attaches a QueryPriority to ctx.
func ContextWithQueryPriority(ctx context.Context, p QueryPriority) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, priorityCtxKey{}, p)
}

// QueryPriorityFromContext retrieves the QueryPriority from ctx (defaults to PriorityNormal).
func QueryPriorityFromContext(ctx context.Context) QueryPriority {
	if ctx == nil {
		return PriorityNormal
	}
	if p, ok := ctx.Value(priorityCtxKey{}).(QueryPriority); ok {
		return p
	}
	return PriorityNormal
}

// SchedulerConfig configures the shared, CPU- and memory-aware fair tenant scheduler.
type SchedulerConfig struct {
	MaxGlobalConcurrency     int
	DefaultTenantConcurrency int
	MaxQueueDepth            int
	MemoryManager            *MemoryManager
	MemorySoftLimitRatio     float64 // e.g. 0.90 of global/tenant limit
}

// SchedulerStats reports admission, queueing, and backpressure metrics.
type SchedulerStats struct {
	ActiveGlobal         int    `json:"active_global"`
	QueuedGlobal         int    `json:"queued_global"`
	MaxGlobalConcurrency int    `json:"max_global_concurrency"`
	AdmittedTotal        uint64 `json:"admitted_total"`
	BackpressureTotal    uint64 `json:"backpressure_total"`
	CancelledWaitTotal   uint64 `json:"cancelled_wait_total"`
}

// TenantSchedulerStats reports per-tenant active/queued/admitted counts.
type TenantSchedulerStats struct {
	Active   int    `json:"active"`
	Queued   int    `json:"queued"`
	Admitted uint64 `json:"admitted"`
	Rejected uint64 `json:"rejected"`
}

type schedWaiter struct {
	tenantID string
	priority QueryPriority
	seq      uint64
	ch       chan error
	cancelled bool
}

// TenantScheduler provides shared, CPU-aware, memory-aware, fair multi-tenant
// admission control with bounded queues and zero background goroutines.
type TenantScheduler struct {
	maxGlobal     int
	defaultTenant int
	maxQueue      int
	memMgr        *MemoryManager
	memSoftRatio  float64

	mu           sync.Mutex
	activeGlobal int
	tenantActive map[string]int
	tenantLimits map[string]int
	tenantQueued map[string]int
	tenantAdmit  map[string]uint64
	tenantReject map[string]uint64
	waiters      []*schedWaiter
	nextSeq      uint64

	admittedTotal      atomic.Uint64
	backpressureTotal  atomic.Uint64
	cancelledWaitTotal atomic.Uint64
}

// NewTenantScheduler creates a shared TenantScheduler with bounded queues.
func NewTenantScheduler(cfg SchedulerConfig) *TenantScheduler {
	if cfg.MaxGlobalConcurrency <= 0 {
		cpus := runtime.GOMAXPROCS(0)
		if cpus < 2 {
			cpus = 2
		}
		cfg.MaxGlobalConcurrency = cpus * 4
	}
	if cfg.DefaultTenantConcurrency <= 0 {
		cfg.DefaultTenantConcurrency = max(2, cfg.MaxGlobalConcurrency/2)
	}
	if cfg.MaxQueueDepth <= 0 {
		cfg.MaxQueueDepth = 128
	}
	if cfg.MemorySoftLimitRatio <= 0 || cfg.MemorySoftLimitRatio > 1.0 {
		cfg.MemorySoftLimitRatio = 0.90
	}
	return &TenantScheduler{
		maxGlobal:     cfg.MaxGlobalConcurrency,
		defaultTenant: cfg.DefaultTenantConcurrency,
		maxQueue:      cfg.MaxQueueDepth,
		memMgr:        cfg.MemoryManager,
		memSoftRatio:  cfg.MemorySoftLimitRatio,
		tenantActive:  make(map[string]int),
		tenantLimits:  make(map[string]int),
		tenantQueued:  make(map[string]int),
		tenantAdmit:   make(map[string]uint64),
		tenantReject:  make(map[string]uint64),
	}
}

// SetTenantConcurrency overrides the concurrency limit for tenantID.
func (s *TenantScheduler) SetTenantConcurrency(tenantID string, limit int) {
	if s == nil || tenantID == "" || limit <= 0 {
		return
	}
	s.mu.Lock()
	s.tenantLimits[tenantID] = limit
	s.mu.Unlock()
}

func (s *TenantScheduler) tenantLimitLocked(tenantID string) int {
	if lim, ok := s.tenantLimits[tenantID]; ok && lim > 0 {
		return lim
	}
	return s.defaultTenant
}

func (s *TenantScheduler) checkMemoryAdmission(tenantID string, prio QueryPriority) error {
	if s.memMgr == nil {
		return nil
	}
	if s.memMgr.globalLimit > 0 {
		softGlobal := int64(float64(s.memMgr.globalLimit) * s.memSoftRatio)
		if s.memMgr.GlobalUsed() >= softGlobal && prio < PriorityInteractive {
			return ErrMemoryPressure
		}
	}
	if tenantID != "" {
		_, tLim := s.memMgr.getTenantSlot(tenantID)
		if tLim > 0 {
			softTenant := int64(float64(tLim) * s.memSoftRatio)
			if s.memMgr.TenantUsed(tenantID) >= softTenant && prio < PriorityInteractive {
				return ErrMemoryPressure
			}
		}
	}
	return nil
}

// Acquire admits a query for tenantID, blocking fairly in the bounded queue if
// global or per-tenant capacity is currently full. Returns a release func.
func (s *TenantScheduler) Acquire(ctx context.Context, tenantID string) (func(), error) {
	if s == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prio := QueryPriorityFromContext(ctx)
	if err := s.checkMemoryAdmission(tenantID, prio); err != nil {
		s.mu.Lock()
		s.tenantReject[tenantID]++
		s.mu.Unlock()
		s.backpressureTotal.Add(1)
		return nil, err
	}

	s.mu.Lock()
	tLim := s.tenantLimitLocked(tenantID)
	if s.activeGlobal < s.maxGlobal && s.tenantActive[tenantID] < tLim && len(s.waiters) == 0 {
		s.activeGlobal++
		s.tenantActive[tenantID]++
		s.tenantAdmit[tenantID]++
		s.mu.Unlock()
		s.admittedTotal.Add(1)
		return s.releaseFunc(tenantID), nil
	}

	if len(s.waiters) >= s.maxQueue {
		s.tenantReject[tenantID]++
		s.mu.Unlock()
		s.backpressureTotal.Add(1)
		return nil, ErrSchedulerQueueFull
	}

	s.nextSeq++
	w := &schedWaiter{
		tenantID: tenantID,
		priority: prio,
		seq:      s.nextSeq,
		ch:       make(chan error, 1),
	}
	s.waiters = append(s.waiters, w)
	s.tenantQueued[tenantID]++
	s.mu.Unlock()

	select {
	case err := <-w.ch:
		if err != nil {
			return nil, err
		}
		return s.releaseFunc(tenantID), nil
	case <-ctx.Done():
		s.mu.Lock()
		if !w.cancelled {
			w.cancelled = true
			for i, item := range s.waiters {
				if item == w {
					s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
					s.tenantQueued[tenantID]--
					break
				}
			}
		}
		s.mu.Unlock()
		// If a slot was handed to w right as ctx cancelled, drain and release it.
		select {
		case err := <-w.ch:
			if err == nil {
				s.release(tenantID)
			}
		default:
		}
		s.cancelledWaitTotal.Add(1)
		return nil, ctx.Err()
	}
}

func (s *TenantScheduler) releaseFunc(tenantID string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.release(tenantID)
		})
	}
}

func (s *TenantScheduler) release(tenantID string) {
	s.mu.Lock()
	if s.activeGlobal > 0 {
		s.activeGlobal--
	}
	if s.tenantActive[tenantID] > 0 {
		s.tenantActive[tenantID]--
	}
	s.dispatchLocked()
	s.mu.Unlock()
}

// dispatchLocked selects the next eligible waiter using fair tenant selection:
// highest priority first, then fewest active queries for that tenant, then FIFO seq.
func (s *TenantScheduler) dispatchLocked() {
	for s.activeGlobal < s.maxGlobal && len(s.waiters) > 0 {
		bestIdx := -1
		var best *schedWaiter
		for i, w := range s.waiters {
			if s.tenantActive[w.tenantID] >= s.tenantLimitLocked(w.tenantID) {
				continue
			}
			if best == nil {
				bestIdx = i
				best = w
				continue
			}
			if w.priority != best.priority {
				if w.priority > best.priority {
					bestIdx = i
					best = w
				}
				continue
			}
			wAct := s.tenantActive[w.tenantID]
			bAct := s.tenantActive[best.tenantID]
			if wAct != bAct {
				if wAct < bAct {
					bestIdx = i
					best = w
				}
				continue
			}
			if w.seq < best.seq {
				bestIdx = i
				best = w
			}
		}
		if bestIdx < 0 {
			break
		}
		s.waiters = append(s.waiters[:bestIdx], s.waiters[bestIdx+1:]...)
		s.tenantQueued[best.tenantID]--
		best.cancelled = true // mark removed from waiters slice
		s.activeGlobal++
		s.tenantActive[best.tenantID]++
		s.tenantAdmit[best.tenantID]++
		s.admittedTotal.Add(1)
		best.ch <- nil
	}
}

// Stats returns global scheduler metrics.
func (s *TenantScheduler) Stats() SchedulerStats {
	if s == nil {
		return SchedulerStats{}
	}
	s.mu.Lock()
	act := s.activeGlobal
	q := len(s.waiters)
	s.mu.Unlock()
	return SchedulerStats{
		ActiveGlobal:         act,
		QueuedGlobal:         q,
		MaxGlobalConcurrency: s.maxGlobal,
		AdmittedTotal:        s.admittedTotal.Load(),
		BackpressureTotal:    s.backpressureTotal.Load(),
		CancelledWaitTotal:   s.cancelledWaitTotal.Load(),
	}
}

// TenantStats returns per-tenant scheduler metrics.
func (s *TenantScheduler) TenantStats(tenantID string) TenantSchedulerStats {
	if s == nil || tenantID == "" {
		return TenantSchedulerStats{}
	}
	s.mu.Lock()
	st := TenantSchedulerStats{
		Active:   s.tenantActive[tenantID],
		Queued:   s.tenantQueued[tenantID],
		Admitted: s.tenantAdmit[tenantID],
		Rejected: s.tenantReject[tenantID],
	}
	s.mu.Unlock()
	return st
}
