package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrWriteQueueFull    = errors.New("write governor queue is full")
	ErrWriteQueueTimeout = errors.New("write governor queue wait timeout exceeded")
)

// ControllerState represents the operating state of the Adaptive Write Governor.
type ControllerState int32

const (
	ControllerStateStatic ControllerState = iota
	ControllerStateHealthy
	ControllerStatePressured
	ControllerStateCritical
	ControllerStateStaleFallback
)

func (s ControllerState) String() string {
	switch s {
	case ControllerStateStatic:
		return "static"
	case ControllerStateHealthy:
		return "healthy"
	case ControllerStatePressured:
		return "pressured"
	case ControllerStateCritical:
		return "critical"
	case ControllerStateStaleFallback:
		return "stale_fallback"
	default:
		return "unknown"
	}
}

type writeWaiter struct {
	tenantID string
	bytes    int64
	enqueued time.Time
	readyCh  chan struct{}
	granted  bool
	removed  bool
}

type tenantWriteState struct {
	tenantID    string
	active      int
	maxOverride int
	weight      int
	deficit     int
	queuedBytes int64
	waiters     []*writeWaiter
}

func (t *tenantWriteState) effectiveLimit(defaultMax int) int {
	if t.maxOverride > 0 {
		return t.maxOverride
	}
	return defaultMax
}

// WriteGovernorSnapshot captures aggregate, low-cardinality metrics for the Write Governor.
type WriteGovernorSnapshot struct {
	Enabled              bool
	ActiveWrites         int64
	QueueDepth           int64
	QueueBytes           int64
	EffectiveGlobalLimit int
	MaxGlobal            int
	MinGlobal            int
	MaxPerTenant         int
	ControllerState      ControllerState
	CompletedTotal       uint64
	RejectedTotal        uint64
	CancelledTotal       uint64
	TimeoutTotal         uint64
	ErrorsTotal          uint64
	OpenTransactions     int64
	QueueWaitP50Us       uint64
	QueueWaitP95Us       uint64
	QueueWaitP99Us       uint64
	ExecP50Us            uint64
	ExecP95Us            uint64
	ExecP99Us            uint64
	TxDurP50Us           uint64
	TxDurP95Us           uint64
	TxDurP99Us           uint64
}

// WritePermit represents an admitted write execution slot.
// Release MUST be called when execution finishes (idempotent).
type WritePermit struct {
	gov      *WriteGovernor
	tenantID string
	started  time.Time
	waitUs   uint64
	released atomic.Bool
}

// WaitUs returns the time spent waiting in the write queue in microseconds.
func (p *WritePermit) WaitUs() uint64 {
	if p == nil {
		return 0
	}
	return p.waitUs
}

// Release frees the write slot and records execution duration. Safe to call multiple times.
func (p *WritePermit) Release(hadErr bool) {
	if p == nil || p.gov == nil {
		return
	}
	if !p.released.CompareAndSwap(false, true) {
		return
	}
	execUs := uint64(time.Since(p.started).Microseconds())
	p.gov.releaseSlot(p.tenantID, execUs, hadErr)
}

// WriteGovernor enforces global and per-tenant write concurrency limits with
// bounded queuing, Deficit Round Robin (DRR) tenant fairness, and cancellation support.
type WriteGovernor struct {
	enabled atomic.Bool

	mu                   sync.Mutex
	maxGlobal            int
	minGlobal            int
	effectiveGlobalLimit int
	defaultMaxPerTenant  int
	queueMax             int
	queueMaxBytes        int64
	queueTimeout         time.Duration

	activeWrites  int
	queueDepth    int
	queueBytes    int64
	tenants       map[string]*tenantWriteState
	activeTenants []string
	rrCursor      int

	controllerState atomic.Int32
	openTxCount     atomic.Int64

	completedTotal atomic.Uint64
	rejectedTotal  atomic.Uint64
	cancelledTotal atomic.Uint64
	timeoutTotal   atomic.Uint64
	errorsTotal    atomic.Uint64

	histMu        sync.Mutex
	queueWaitHist LatencyHistogram
	execHist      LatencyHistogram
	txDurHist     LatencyHistogram
}

// NewWriteGovernor constructs a WriteGovernor from Config.
func NewWriteGovernor(cfg Config) *WriteGovernor {
	cfg.NormalizeWriteGovernor()
	g := &WriteGovernor{
		maxGlobal:            cfg.WriteMaxGlobal,
		minGlobal:            cfg.WriteMinGlobal,
		effectiveGlobalLimit: cfg.WriteMaxGlobal,
		defaultMaxPerTenant:  cfg.WriteMaxPerTenant,
		queueMax:             cfg.WriteQueueMax,
		queueMaxBytes:        cfg.WriteQueueMaxBytes,
		queueTimeout:         cfg.WriteQueueTimeout,
		tenants:              make(map[string]*tenantWriteState),
	}
	g.enabled.Store(cfg.WriteGovernorEnabled)
	if cfg.WriteAdaptiveEnabled {
		// Start conservatively on startup/restart rather than jumping to maxGlobal.
		initLimit := cfg.WriteMaxGlobal / 2
		if initLimit < cfg.WriteMinGlobal {
			initLimit = cfg.WriteMinGlobal
		}
		g.effectiveGlobalLimit = initLimit
		g.controllerState.Store(int32(ControllerStateHealthy))
	} else {
		g.controllerState.Store(int32(ControllerStateStatic))
	}
	return g
}

func (g *WriteGovernor) SetEnabled(enabled bool) {
	if g != nil {
		g.enabled.Store(enabled)
	}
}

func (g *WriteGovernor) IsEnabled() bool {
	return g != nil && g.enabled.Load()
}

func (g *WriteGovernor) SetControllerState(state ControllerState) {
	if g != nil {
		g.controllerState.Store(int32(state))
	}
}

func (g *WriteGovernor) EffectiveGlobalLimit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.effectiveGlobalLimit
}

// SetEffectiveGlobalLimit updates the active global write concurrency cap (clamped to [minGlobal, maxGlobal])
// and dispatches any queued waiters if capacity increased.
func (g *WriteGovernor) SetEffectiveGlobalLimit(limit int) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if limit < g.minGlobal {
		limit = g.minGlobal
	}
	if limit > g.maxGlobal {
		limit = g.maxGlobal
	}
	g.effectiveGlobalLimit = limit
	g.dispatchLocked()
	return limit
}

// SetTenantLimit configures a per-tenant write concurrency override and scheduling weight.
func (g *WriteGovernor) SetTenantLimit(tenantID string, maxWrites int, weight int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	t := g.getOrCreateTenantLocked(tenantID)
	t.maxOverride = maxWrites
	if weight > 0 {
		t.weight = weight
	}
	g.dispatchLocked()
}

func (g *WriteGovernor) getOrCreateTenantLocked(tenantID string) *tenantWriteState {
	t, ok := g.tenants[tenantID]
	if !ok {
		t = &tenantWriteState{
			tenantID: tenantID,
			weight:   1,
		}
		g.tenants[tenantID] = t
	}
	return t
}

func (g *WriteGovernor) addActiveTenantLocked(tenantID string) {
	for _, id := range g.activeTenants {
		if id == tenantID {
			return
		}
	}
	g.activeTenants = append(g.activeTenants, tenantID)
}

func (g *WriteGovernor) removeActiveTenantLocked(tenantID string) {
	for i, id := range g.activeTenants {
		if id == tenantID {
			g.activeTenants = append(g.activeTenants[:i], g.activeTenants[i+1:]...)
			if g.rrCursor >= len(g.activeTenants) && len(g.activeTenants) > 0 {
				g.rrCursor = g.rrCursor % len(g.activeTenants)
			} else if len(g.activeTenants) == 0 {
				g.rrCursor = 0
			}
			return
		}
	}
}

// Acquire acquires a write execution permit for tenantID, queuing fairly if capacity is full.
func (g *WriteGovernor) Acquire(ctx context.Context, tenantID string, reqBytes int64) (*WritePermit, error) {
	if !g.IsEnabled() {
		return &WritePermit{started: time.Now()}, nil
	}
	if err := ctx.Err(); err != nil {
		g.cancelledTotal.Add(1)
		return nil, err
	}

	now := time.Now()
	g.mu.Lock()
	t := g.getOrCreateTenantLocked(tenantID)
	tLimit := t.effectiveLimit(g.defaultMaxPerTenant)

	// Fast path: global and per-tenant capacity available and no waiters queued for this tenant.
	if g.activeWrites < g.effectiveGlobalLimit && t.active < tLimit && len(t.waiters) == 0 {
		g.activeWrites++
		t.active++
		g.mu.Unlock()
		g.recordQueueWait(0)
		return &WritePermit{
			gov:      g,
			tenantID: tenantID,
			started:  now,
			waitUs:   0,
		}, nil
	}

	// Fast overload rejection if bounded queue is full.
	if g.queueMax == 0 || g.queueDepth >= g.queueMax ||
		(g.queueMaxBytes > 0 && g.queueBytes+reqBytes > g.queueMaxBytes) {
		g.mu.Unlock()
		g.rejectedTotal.Add(1)
		return nil, ErrWriteQueueFull
	}

	w := &writeWaiter{
		tenantID: tenantID,
		bytes:    reqBytes,
		enqueued: now,
		readyCh:  make(chan struct{}, 1),
	}
	t.waiters = append(t.waiters, w)
	t.queuedBytes += reqBytes
	g.queueDepth++
	g.queueBytes += reqBytes
	g.addActiveTenantLocked(tenantID)
	g.mu.Unlock()

	timeout := g.queueTimeout
	if dl, ok := ctx.Deadline(); ok {
		if rem := time.Until(dl); rem < timeout {
			timeout = rem
		}
	}
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-w.readyCh:
		waitUs := uint64(time.Since(now).Microseconds())
		g.recordQueueWait(waitUs)
		return &WritePermit{
			gov:      g,
			tenantID: tenantID,
			started:  time.Now(),
			waitUs:   waitUs,
		}, nil

	case <-timer.C:
		return nil, g.abortWaiter(w, true, ErrWriteQueueTimeout)

	case <-ctx.Done():
		return nil, g.abortWaiter(w, false, ctx.Err())
	}
}

func (g *WriteGovernor) abortWaiter(w *writeWaiter, isTimeout bool, retErr error) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if w.granted {
		// Race: dispatcher already granted the slot to w right as timeout/cancel fired.
		// Release the slot immediately so capacity is never leaked.
		if g.activeWrites > 0 {
			g.activeWrites--
		}
		if t, ok := g.tenants[w.tenantID]; ok && t.active > 0 {
			t.active--
		}
		g.dispatchLocked()
	} else if !w.removed {
		w.removed = true
		if t, ok := g.tenants[w.tenantID]; ok {
			for i, candidate := range t.waiters {
				if candidate == w {
					t.waiters = append(t.waiters[:i], t.waiters[i+1:]...)
					break
				}
			}
			t.queuedBytes -= w.bytes
			if t.queuedBytes < 0 {
				t.queuedBytes = 0
			}
			if len(t.waiters) == 0 {
				g.removeActiveTenantLocked(w.tenantID)
			}
		}
		if g.queueDepth > 0 {
			g.queueDepth--
		}
		g.queueBytes -= w.bytes
		if g.queueBytes < 0 {
			g.queueBytes = 0
		}
	}

	if isTimeout {
		g.timeoutTotal.Add(1)
	} else {
		g.cancelledTotal.Add(1)
	}
	return retErr
}

func (g *WriteGovernor) releaseSlot(tenantID string, execUs uint64, hadErr bool) {
	g.completedTotal.Add(1)
	if hadErr {
		g.errorsTotal.Add(1)
	}
	g.histMu.Lock()
	g.execHist.Record(execUs)
	g.histMu.Unlock()

	g.mu.Lock()
	if g.activeWrites > 0 {
		g.activeWrites--
	}
	if t, ok := g.tenants[tenantID]; ok && t.active > 0 {
		t.active--
	}
	g.dispatchLocked()
	g.mu.Unlock()
}

// dispatchLocked grants write slots to waiting tenants using Deficit Round Robin (DRR).
// Must be called with g.mu held.
func (g *WriteGovernor) dispatchLocked() {
	for g.activeWrites < g.effectiveGlobalLimit && len(g.activeTenants) > 0 {
		n := len(g.activeTenants)
		dispatched := false

		for checked := 0; checked < n; checked++ {
			if g.rrCursor >= len(g.activeTenants) {
				g.rrCursor = 0
			}
			tid := g.activeTenants[g.rrCursor]
			t := g.tenants[tid]
			if t == nil || len(t.waiters) == 0 {
				g.removeActiveTenantLocked(tid)
				n = len(g.activeTenants)
				if n == 0 {
					break
				}
				continue
			}

			tLimit := t.effectiveLimit(g.defaultMaxPerTenant)
			if t.active >= tLimit {
				g.rrCursor = (g.rrCursor + 1) % len(g.activeTenants)
				continue
			}

			if t.deficit <= 0 {
				w := t.weight
				if w <= 0 {
					w = 1
				}
				t.deficit += w
			}

			w := t.waiters[0]
			t.waiters = t.waiters[1:]
			t.queuedBytes -= w.bytes
			if t.queuedBytes < 0 {
				t.queuedBytes = 0
			}
			if g.queueDepth > 0 {
				g.queueDepth--
			}
			g.queueBytes -= w.bytes
			if g.queueBytes < 0 {
				g.queueBytes = 0
			}

			w.granted = true
			w.removed = true
			g.activeWrites++
			t.active++
			t.deficit--

			if len(t.waiters) == 0 {
				g.removeActiveTenantLocked(tid)
			} else if t.deficit <= 0 {
				g.rrCursor = (g.rrCursor + 1) % len(g.activeTenants)
			}

			select {
			case w.readyCh <- struct{}{}:
			default:
			}
			dispatched = true
			break
		}

		if !dispatched {
			break
		}
	}
}

func (g *WriteGovernor) recordQueueWait(waitUs uint64) {
	g.histMu.Lock()
	g.queueWaitHist.Record(waitUs)
	g.histMu.Unlock()
}

// RecordTxStart increments open transaction count.
func (g *WriteGovernor) RecordTxStart() {
	if g != nil {
		g.openTxCount.Add(1)
	}
}

// RecordTxEnd decrements open transaction count and records transaction duration.
func (g *WriteGovernor) RecordTxEnd(dur time.Duration) {
	if g == nil {
		return
	}
	if g.openTxCount.Add(-1) < 0 {
		g.openTxCount.Store(0)
	}
	g.histMu.Lock()
	g.txDurHist.Record(uint64(dur.Microseconds()))
	g.histMu.Unlock()
}

// Snapshot returns an aggregate, low-cardinality snapshot of governor state.
func (g *WriteGovernor) Snapshot() WriteGovernorSnapshot {
	if g == nil {
		return WriteGovernorSnapshot{}
	}
	g.mu.Lock()
	activeW := int64(g.activeWrites)
	qDepth := int64(g.queueDepth)
	qBytes := g.queueBytes
	effLimit := g.effectiveGlobalLimit
	maxG := g.maxGlobal
	minG := g.minGlobal
	maxT := g.defaultMaxPerTenant
	g.mu.Unlock()

	g.histMu.Lock()
	qwP50 := g.queueWaitHist.PercentileUs(0.50)
	qwP95 := g.queueWaitHist.PercentileUs(0.95)
	qwP99 := g.queueWaitHist.PercentileUs(0.99)
	exP50 := g.execHist.PercentileUs(0.50)
	exP95 := g.execHist.PercentileUs(0.95)
	exP99 := g.execHist.PercentileUs(0.99)
	txP50 := g.txDurHist.PercentileUs(0.50)
	txP95 := g.txDurHist.PercentileUs(0.95)
	txP99 := g.txDurHist.PercentileUs(0.99)
	g.histMu.Unlock()

	return WriteGovernorSnapshot{
		Enabled:              g.enabled.Load(),
		ActiveWrites:         activeW,
		QueueDepth:           qDepth,
		QueueBytes:           qBytes,
		EffectiveGlobalLimit: effLimit,
		MaxGlobal:            maxG,
		MinGlobal:            minG,
		MaxPerTenant:         maxT,
		ControllerState:      ControllerState(g.controllerState.Load()),
		CompletedTotal:       g.completedTotal.Load(),
		RejectedTotal:        g.rejectedTotal.Load(),
		CancelledTotal:       g.cancelledTotal.Load(),
		TimeoutTotal:         g.timeoutTotal.Load(),
		ErrorsTotal:          g.errorsTotal.Load(),
		OpenTransactions:     g.openTxCount.Load(),
		QueueWaitP50Us:       qwP50,
		QueueWaitP95Us:       qwP95,
		QueueWaitP99Us:       qwP99,
		ExecP50Us:            exP50,
		ExecP95Us:            exP95,
		ExecP99Us:            exP99,
		TxDurP50Us:           txP50,
		TxDurP95Us:           txP95,
		TxDurP99Us:           txP99,
	}
}
