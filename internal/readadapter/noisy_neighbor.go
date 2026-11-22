package readadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrTenantCPUQuotaExceeded is returned when a tenant exhausts its CPU-time quota window.
	ErrTenantCPUQuotaExceeded = errors.New("readadapter: tenant CPU-time quota exceeded")
	// ErrPlanCostRejected is returned when cost-based admission control rejects an overly expensive plan.
	ErrPlanCostRejected = errors.New("readadapter: query plan cost exceeds admission limit")
)

// NoisyNeighborConfig configures per-tenant CPU-time quotas, hard query timeouts,
// and cost-based admission control on top of TenantScheduler and MemoryManager.
type NoisyNeighborConfig struct {
	TenantCPUQuota      time.Duration // max CPU/execution time per tenant per QuotaWindow (0 = disabled)
	QuotaWindow         time.Duration // sliding window duration (default 1s)
	HardQueryTimeout    time.Duration // absolute ceiling on any query's execution deadline (0 = disabled)
	MaxPlanCost         float64       // reject plans with estimated cost > MaxPlanCost immediately (0 = disabled)
	ExpensivePlanCost   float64       // demote plans with cost > ExpensivePlanCost to PriorityBatch (0 = disabled)
}

type tenantCPUWindow struct {
	windowStart time.Time
	usedNs      int64
	rejected    uint64
	costRejects uint64
}

// NoisyNeighborController enforces per-tenant CPU quotas, hard query timeouts,
// and cost-based admission control.
type NoisyNeighborController struct {
	cfg NoisyNeighborConfig

	mu      sync.Mutex
	tenants map[string]*tenantCPUWindow

	cpuRejectsTotal  atomic.Uint64
	costRejectsTotal atomic.Uint64
	demotedTotal     atomic.Uint64
}

// NewNoisyNeighborController creates a NoisyNeighborController.
func NewNoisyNeighborController(cfg NoisyNeighborConfig) *NoisyNeighborController {
	if cfg.QuotaWindow <= 0 {
		cfg.QuotaWindow = time.Second
	}
	return &NoisyNeighborController{
		cfg:     cfg,
		tenants: make(map[string]*tenantCPUWindow),
	}
}

// ApplyHardTimeout clamps ctx with HardQueryTimeout if configured.
func (nc *NoisyNeighborController) ApplyHardTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if nc == nil || nc.cfg.HardQueryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, nc.cfg.HardQueryTimeout)
}

// Admit checks cost-based admission and per-tenant CPU-time quota, possibly demoting
// expensive plans to PriorityBatch. Returns (adjustedCtx, error).
func (nc *NoisyNeighborController) Admit(ctx context.Context, tenantID string, estCost float64) (context.Context, error) {
	if nc == nil {
		return ctx, nil
	}
	if nc.cfg.MaxPlanCost > 0 && estCost > nc.cfg.MaxPlanCost {
		nc.costRejectsTotal.Add(1)
		nc.mu.Lock()
		tw := nc.getOrCreateLocked(tenantID, time.Now())
		tw.costRejects++
		nc.mu.Unlock()
		return ctx, fmt.Errorf("%w: est_cost=%.1f max=%.1f", ErrPlanCostRejected, estCost, nc.cfg.MaxPlanCost)
	}
	if nc.cfg.ExpensivePlanCost > 0 && estCost > nc.cfg.ExpensivePlanCost {
		nc.demotedTotal.Add(1)
		ctx = ContextWithQueryPriority(ctx, PriorityBatch)
	}
	if nc.cfg.TenantCPUQuota > 0 {
		now := time.Now()
		nc.mu.Lock()
		tw := nc.getOrCreateLocked(tenantID, now)
		if now.Sub(tw.windowStart) >= nc.cfg.QuotaWindow {
			tw.windowStart = now
			tw.usedNs = 0
		}
		over := tw.usedNs >= nc.cfg.TenantCPUQuota.Nanoseconds()
		if over {
			tw.rejected++
		}
		nc.mu.Unlock()
		if over {
			nc.cpuRejectsTotal.Add(1)
			return ctx, fmt.Errorf("%w: tenant=%s quota=%s", ErrTenantCPUQuotaExceeded, tenantID, nc.cfg.TenantCPUQuota)
		}
	}
	return ctx, nil
}

// RecordExecution charges elapsed execution time against tenantID's CPU quota window.
func (nc *NoisyNeighborController) RecordExecution(tenantID string, elapsed time.Duration) {
	if nc == nil || nc.cfg.TenantCPUQuota <= 0 || elapsed <= 0 {
		return
	}
	now := time.Now()
	nc.mu.Lock()
	tw := nc.getOrCreateLocked(tenantID, now)
	if now.Sub(tw.windowStart) >= nc.cfg.QuotaWindow {
		tw.windowStart = now
		tw.usedNs = 0
	}
	tw.usedNs += elapsed.Nanoseconds()
	nc.mu.Unlock()
}

func (nc *NoisyNeighborController) getOrCreateLocked(tenantID string, now time.Time) *tenantCPUWindow {
	tw, ok := nc.tenants[tenantID]
	if !ok {
		tw = &tenantCPUWindow{windowStart: now}
		nc.tenants[tenantID] = tw
	}
	return tw
}

// Stats returns aggregate noisy-neighbor enforcement counters.
func (nc *NoisyNeighborController) Stats() (cpuRejects, costRejects, demoted uint64) {
	if nc == nil {
		return 0, 0, 0
	}
	return nc.cpuRejectsTotal.Load(), nc.costRejectsTotal.Load(), nc.demotedTotal.Load()
}
