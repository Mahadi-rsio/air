package pageserver

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	// ErrReadQueueFull is returned when the bounded read admission queue is at capacity.
	ErrReadQueueFull = errors.New("pageserver: read admission queue full")
	// ErrReadLimiterClosed is returned when read admission is shutting down.
	ErrReadLimiterClosed = errors.New("pageserver: read limiter closed")
)

// ReadLimiterConfig configures bounded, tenant-fair read admission control.
type ReadLimiterConfig struct {
	// MaxActive is the maximum number of concurrent active read operations.
	// Defaults to max(GOMAXPROCS*8, 16).
	MaxActive int
	// MaxPending is the maximum number of queued read waiters across all tenants.
	// Defaults to 256.
	MaxPending int
	// MaxPerTenantActive caps active reads for a single tenant when other tenants
	// are waiting (or always if StrictPerTenantCap is true). Defaults to max(MaxActive/2, 1).
	MaxPerTenantActive int
	// StrictPerTenantCap enforces MaxPerTenantActive even when no other tenants are waiting.
	StrictPerTenantCap bool
}

// ReadLimiterStats captures point-in-time counters for read admission control.
type ReadLimiterStats struct {
	Active       int
	Pending      int
	Admitted     int64
	RejectedFull int64
	Cancelled    int64
}

type readWaiter struct {
	tenantID  string
	ready     chan struct{}
	granted   bool
	cancelled bool
}

// ReadLimiter enforces bounded concurrent read execution, bounded queueing,
// context-aware cancellation, graceful shutdown, and round-robin fairness across tenants.
type ReadLimiter struct {
	cfg ReadLimiterConfig

	mu              sync.Mutex
	closed          bool
	shutdownCh      chan struct{}
	drainedCh       chan struct{}
	active          int
	pending         int
	perTenantActive map[string]int
	tenantQueues    map[string][]*readWaiter
	tenantOrder     []string
	rrCursor        int

	admitted     atomic.Int64
	rejectedFull atomic.Int64
	cancelled    atomic.Int64
}

// NewReadLimiter creates a ReadLimiter with sensible defaults for zero-value fields.
func NewReadLimiter(cfg ReadLimiterConfig) *ReadLimiter {
	if cfg.MaxActive <= 0 {
		cfg.MaxActive = runtime.GOMAXPROCS(0) * 8
		if cfg.MaxActive < 16 {
			cfg.MaxActive = 16
		}
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = 256
	}
	if cfg.MaxPerTenantActive <= 0 {
		cfg.MaxPerTenantActive = cfg.MaxActive / 2
		if cfg.MaxPerTenantActive < 1 {
			cfg.MaxPerTenantActive = 1
		}
	}
	if cfg.MaxPerTenantActive > cfg.MaxActive {
		cfg.MaxPerTenantActive = cfg.MaxActive
	}
	return &ReadLimiter{
		cfg:             cfg,
		shutdownCh:      make(chan struct{}),
		drainedCh:       make(chan struct{}),
		perTenantActive: make(map[string]int),
		tenantQueues:    make(map[string][]*readWaiter),
	}
}

func (rl *ReadLimiter) canAdmitLocked(tenantID string) bool {
	if rl.active >= rl.cfg.MaxActive {
		return false
	}
	ta := rl.perTenantActive[tenantID]
	if ta >= rl.cfg.MaxPerTenantActive {
		if rl.cfg.StrictPerTenantCap || rl.hasOtherWaitingLocked(tenantID) {
			return false
		}
	}
	return true
}

func (rl *ReadLimiter) hasOtherWaitingLocked(tenantID string) bool {
	for tid, q := range rl.tenantQueues {
		if tid != tenantID && len(q) > 0 {
			return true
		}
	}
	return false
}

// Acquire blocks until a read slot is granted, ctx is cancelled, the queue is full,
// or the limiter is closed. The returned release function must be called exactly once.
func (rl *ReadLimiter) Acquire(ctx context.Context, tenantID string) (func(), error) {
	if err := ctx.Err(); err != nil {
		rl.cancelled.Add(1)
		return nil, err
	}

	rl.mu.Lock()
	if rl.closed {
		rl.mu.Unlock()
		return nil, ErrReadLimiterClosed
	}

	if len(rl.tenantQueues[tenantID]) == 0 && rl.canAdmitLocked(tenantID) {
		rl.active++
		rl.perTenantActive[tenantID]++
		rl.mu.Unlock()
		rl.admitted.Add(1)
		return rl.releaseFunc(tenantID), nil
	}

	if rl.pending >= rl.cfg.MaxPending {
		rl.mu.Unlock()
		rl.rejectedFull.Add(1)
		return nil, ErrReadQueueFull
	}

	w := &readWaiter{
		tenantID: tenantID,
		ready:    make(chan struct{}),
	}
	if _, exists := rl.tenantQueues[tenantID]; !exists {
		rl.tenantOrder = append(rl.tenantOrder, tenantID)
	}
	rl.tenantQueues[tenantID] = append(rl.tenantQueues[tenantID], w)
	rl.pending++
	rl.mu.Unlock()

	select {
	case <-w.ready:
		rl.admitted.Add(1)
		return rl.releaseFunc(tenantID), nil
	case <-rl.shutdownCh:
		if rl.removeCancelledWaiter(w) {
			rl.release(tenantID)
		}
		return nil, ErrReadLimiterClosed
	case <-ctx.Done():
		granted := rl.removeCancelledWaiter(w)
		if granted {
			// Slot was granted concurrently with ctx cancellation; release it immediately.
			rl.release(tenantID)
		}
		rl.cancelled.Add(1)
		return nil, ctx.Err()
	}
}

func (rl *ReadLimiter) removeCancelledWaiter(w *readWaiter) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if w.granted {
		return true
	}
	if w.cancelled {
		return false
	}
	w.cancelled = true
	q := rl.tenantQueues[w.tenantID]
	for i, item := range q {
		if item == w {
			q = append(q[:i], q[i+1:]...)
			rl.pending--
			break
		}
	}
	if len(q) == 0 {
		delete(rl.tenantQueues, w.tenantID)
		rl.removeTenantFromOrderLocked(w.tenantID)
	} else {
		rl.tenantQueues[w.tenantID] = q
	}
	return false
}

func (rl *ReadLimiter) removeTenantFromOrderLocked(tenantID string) {
	for i, tid := range rl.tenantOrder {
		if tid == tenantID {
			rl.tenantOrder = append(rl.tenantOrder[:i], rl.tenantOrder[i+1:]...)
			if len(rl.tenantOrder) == 0 {
				rl.rrCursor = 0
			} else if i < rl.rrCursor {
				rl.rrCursor--
			} else if rl.rrCursor >= len(rl.tenantOrder) {
				rl.rrCursor = 0
			}
			return
		}
	}
}

func (rl *ReadLimiter) releaseFunc(tenantID string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			rl.release(tenantID)
		})
	}
}

func (rl *ReadLimiter) release(tenantID string) {
	rl.mu.Lock()
	if rl.active > 0 {
		rl.active--
	}
	if cur := rl.perTenantActive[tenantID]; cur <= 1 {
		delete(rl.perTenantActive, tenantID)
	} else {
		rl.perTenantActive[tenantID] = cur - 1
	}

	if !rl.closed {
		rl.dispatchLocked()
	} else if rl.active == 0 {
		select {
		case <-rl.drainedCh:
		default:
			close(rl.drainedCh)
		}
	}
	rl.mu.Unlock()
}

func (rl *ReadLimiter) dispatchLocked() {
	for rl.active < rl.cfg.MaxActive && len(rl.tenantOrder) > 0 {
		n := len(rl.tenantOrder)
		dispatched := false
		for step := 0; step < n; step++ {
			idx := (rl.rrCursor + step) % n
			tid := rl.tenantOrder[idx]
			if !rl.canAdmitLocked(tid) {
				continue
			}
			q := rl.tenantQueues[tid]
			if len(q) == 0 {
				delete(rl.tenantQueues, tid)
				rl.removeTenantFromOrderLocked(tid)
				dispatched = true
				break
			}
			w := q[0]
			q = q[1:]
			rl.pending--
			if len(q) == 0 {
				delete(rl.tenantQueues, tid)
				rl.removeTenantFromOrderLocked(tid)
			} else {
				rl.tenantQueues[tid] = q
				rl.rrCursor = (idx + 1) % len(rl.tenantOrder)
			}
			w.granted = true
			rl.active++
			rl.perTenantActive[tid]++
			close(w.ready)
			dispatched = true
			break
		}
		if !dispatched {
			break
		}
	}
}

// Shutdown rejects new and queued reads immediately and waits for in-flight reads
// to complete or until ctx expires.
func (rl *ReadLimiter) Shutdown(ctx context.Context) error {
	rl.mu.Lock()
	if !rl.closed {
		rl.closed = true
		close(rl.shutdownCh)
		// Clear pending queue; blocked goroutines wake on <-rl.shutdownCh.
		for tid, q := range rl.tenantQueues {
			for _, w := range q {
				w.cancelled = true
			}
			delete(rl.tenantQueues, tid)
		}
		rl.tenantOrder = nil
		rl.pending = 0
		if rl.active == 0 {
			close(rl.drainedCh)
		}
	}
	rl.mu.Unlock()

	select {
	case <-rl.drainedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns a snapshot of current ReadLimiter metrics.
func (rl *ReadLimiter) Stats() ReadLimiterStats {
	rl.mu.Lock()
	active := rl.active
	pending := rl.pending
	rl.mu.Unlock()
	return ReadLimiterStats{
		Active:       active,
		Pending:      pending,
		Admitted:     rl.admitted.Load(),
		RejectedFull: rl.rejectedFull.Load(),
		Cancelled:    rl.cancelled.Load(),
	}
}
