package readadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	defaultGlobalExecMemoryBytes int64 = 256 * 1024 * 1024 // 256 MiB
	defaultTenantExecMemoryBytes int64 = 64 * 1024 * 1024  // 64 MiB
	defaultQueryExecMemoryBytes  int64 = 16 * 1024 * 1024  // 16 MiB
)

// ErrMemoryBudgetExceeded is returned when a query, tenant, or global execution
// memory budget is exhausted.
var ErrMemoryBudgetExceeded = errors.New("readadapter: execution memory budget exceeded")

// ErrResultLimitExceeded is returned when a query exceeds its maximum allowed
// intermediate/returned row count.
var ErrResultLimitExceeded = errors.New("readadapter: query result row limit exceeded")

type memBudgetCtxKey struct{}
type resLimitsCtxKey struct{}

// QueryResourceLimits configures per-query execution bounds.
type QueryResourceLimits struct {
	MaxResultRows int64
}

// ContextWithResourceLimits attaches QueryResourceLimits to ctx.
func ContextWithResourceLimits(ctx context.Context, lim QueryResourceLimits) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, resLimitsCtxKey{}, lim)
}

// ResourceLimitsFromContext retrieves QueryResourceLimits from ctx.
func ResourceLimitsFromContext(ctx context.Context) (QueryResourceLimits, bool) {
	if ctx == nil {
		return QueryResourceLimits{}, false
	}
	lim, ok := ctx.Value(resLimitsCtxKey{}).(QueryResourceLimits)
	return lim, ok
}

// ContextWithMemoryBudget attaches a QueryMemoryBudget to ctx.
func ContextWithMemoryBudget(ctx context.Context, b *QueryMemoryBudget) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, memBudgetCtxKey{}, b)
}

// MemoryBudgetFromContext retrieves the QueryMemoryBudget attached to ctx, if any.
func MemoryBudgetFromContext(ctx context.Context) *QueryMemoryBudget {
	if ctx == nil {
		return nil
	}
	b, _ := ctx.Value(memBudgetCtxKey{}).(*QueryMemoryBudget)
	return b
}

// MemoryManagerConfig configures bounded execution memory budgets.
type MemoryManagerConfig struct {
	GlobalLimitBytes        int64
	DefaultTenantLimitBytes int64
	DefaultQueryLimitBytes  int64
}

// MemoryManagerStats reports current execution memory accounting across queries and tenants.
type MemoryManagerStats struct {
	GlobalUsedBytes  int64  `json:"global_used_bytes"`
	GlobalLimitBytes int64  `json:"global_limit_bytes"`
	SQLiteUsedBytes  int64  `json:"sqlite_used_bytes"`
	SQLiteHighwater  int64  `json:"sqlite_highwater_bytes"`
	Exhaustions      uint64 `json:"exhaustions"`
}

// MemoryManager enforces per-query, per-tenant, and global execution memory limits.
type MemoryManager struct {
	globalLimit        int64
	defaultTenantLimit int64
	defaultQueryLimit  int64

	globalUsed  atomic.Int64
	exhaustions atomic.Uint64

	mu           sync.RWMutex
	tenantUsed   map[string]*atomic.Int64
	tenantLimits map[string]int64
}

// NewMemoryManager creates a MemoryManager with bounded global, tenant, and query defaults.
func NewMemoryManager(cfg MemoryManagerConfig) *MemoryManager {
	if cfg.GlobalLimitBytes <= 0 {
		cfg.GlobalLimitBytes = defaultGlobalExecMemoryBytes
	}
	if cfg.DefaultTenantLimitBytes <= 0 {
		cfg.DefaultTenantLimitBytes = defaultTenantExecMemoryBytes
	}
	if cfg.DefaultQueryLimitBytes <= 0 {
		cfg.DefaultQueryLimitBytes = defaultQueryExecMemoryBytes
	}
	return &MemoryManager{
		globalLimit:        cfg.GlobalLimitBytes,
		defaultTenantLimit: cfg.DefaultTenantLimitBytes,
		defaultQueryLimit:  cfg.DefaultQueryLimitBytes,
		tenantUsed:         make(map[string]*atomic.Int64),
		tenantLimits:       make(map[string]int64),
	}
}

// SetTenantLimit overrides the execution memory limit for tenantID.
func (m *MemoryManager) SetTenantLimit(tenantID string, limitBytes int64) {
	if m == nil || tenantID == "" || limitBytes <= 0 {
		return
	}
	m.mu.Lock()
	m.tenantLimits[tenantID] = limitBytes
	m.mu.Unlock()
}

func (m *MemoryManager) getTenantSlot(tenantID string) (*atomic.Int64, int64) {
	m.mu.RLock()
	counter := m.tenantUsed[tenantID]
	tLimit := m.tenantLimits[tenantID]
	m.mu.RUnlock()
	if counter != nil {
		if tLimit <= 0 {
			tLimit = m.defaultTenantLimit
		}
		return counter, tLimit
	}
	m.mu.Lock()
	counter = m.tenantUsed[tenantID]
	if counter == nil {
		counter = &atomic.Int64{}
		m.tenantUsed[tenantID] = counter
	}
	tLimit = m.tenantLimits[tenantID]
	if tLimit <= 0 {
		tLimit = m.defaultTenantLimit
	}
	m.mu.Unlock()
	return counter, tLimit
}

// BeginQuery creates a QueryMemoryBudget for a single query execution.
// Callers must defer budget.Release().
func (m *MemoryManager) BeginQuery(tenantID string, queryLimitOverride int64) *QueryMemoryBudget {
	if m == nil {
		return nil
	}
	qlimit := m.defaultQueryLimit
	if queryLimitOverride > 0 {
		qlimit = queryLimitOverride
	}
	var tCounter *atomic.Int64
	var tLimit int64
	if tenantID != "" {
		tCounter, tLimit = m.getTenantSlot(tenantID)
	}
	return &QueryMemoryBudget{
		mgr:         m,
		tenantID:    tenantID,
		queryLimit:  qlimit,
		tenantCtr:   tCounter,
		tenantLimit: tLimit,
	}
}

// GlobalUsed returns total currently reserved execution memory across all tenants.
func (m *MemoryManager) GlobalUsed() int64 {
	if m == nil {
		return 0
	}
	return m.globalUsed.Load()
}

// TenantUsed returns currently reserved execution memory for tenantID.
func (m *MemoryManager) TenantUsed(tenantID string) int64 {
	if m == nil || tenantID == "" {
		return 0
	}
	m.mu.RLock()
	c := m.tenantUsed[tenantID]
	m.mu.RUnlock()
	if c == nil {
		return 0
	}
	return c.Load()
}

// Stats returns current MemoryManagerStats including SQLite C heap accounting.
func (m *MemoryManager) Stats() MemoryManagerStats {
	st := MemoryManagerStats{
		SQLiteUsedBytes: SQLiteMemoryUsed(),
		SQLiteHighwater: SQLiteMemoryHighwater(),
	}
	if m != nil {
		st.GlobalUsedBytes = m.globalUsed.Load()
		st.GlobalLimitBytes = m.globalLimit
		st.Exhaustions = m.exhaustions.Load()
	}
	return st
}

// QueryMemoryBudget tracks and enforces memory reservations for one query.
type QueryMemoryBudget struct {
	mgr         *MemoryManager
	tenantID    string
	queryLimit  int64
	tenantCtr   *atomic.Int64
	tenantLimit int64

	mu       sync.Mutex
	used     int64
	released bool
}

// Reserve attempts to reserve n bytes for the query. Returns ErrMemoryBudgetExceeded
// if the per-query, per-tenant, or global limit would be exceeded.
func (b *QueryMemoryBudget) Reserve(n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.released {
		return nil
	}
	if b.queryLimit > 0 && b.used+n > b.queryLimit {
		if b.mgr != nil {
			b.mgr.exhaustions.Add(1)
		}
		return fmt.Errorf("%w: query limit (%d + %d > %d bytes)", ErrMemoryBudgetExceeded, b.used, n, b.queryLimit)
	}
	if b.tenantCtr != nil && b.tenantLimit > 0 {
		for {
			cur := b.tenantCtr.Load()
			if cur+n > b.tenantLimit {
				if b.mgr != nil {
					b.mgr.exhaustions.Add(1)
				}
				return fmt.Errorf("%w: tenant %s limit (%d + %d > %d bytes)", ErrMemoryBudgetExceeded, b.tenantID, cur, n, b.tenantLimit)
			}
			if b.tenantCtr.CompareAndSwap(cur, cur+n) {
				break
			}
		}
	}
	if b.mgr != nil && b.mgr.globalLimit > 0 {
		for {
			cur := b.mgr.globalUsed.Load()
			if cur+n > b.mgr.globalLimit {
				if b.tenantCtr != nil {
					b.tenantCtr.Add(-n)
				}
				b.mgr.exhaustions.Add(1)
				return fmt.Errorf("%w: global limit (%d + %d > %d bytes)", ErrMemoryBudgetExceeded, cur, n, b.mgr.globalLimit)
			}
			if b.mgr.globalUsed.CompareAndSwap(cur, cur+n) {
				break
			}
		}
	}
	b.used += n
	return nil
}

// Used returns the bytes currently reserved by this query.
func (b *QueryMemoryBudget) Used() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	u := b.used
	b.mu.Unlock()
	return u
}

// Release returns all reserved bytes back to the tenant and global budgets.
// Safe to call multiple times.
func (b *QueryMemoryBudget) Release() {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.released {
		b.mu.Unlock()
		return
	}
	b.released = true
	toRelease := b.used
	b.used = 0
	b.mu.Unlock()

	if toRelease > 0 {
		if b.tenantCtr != nil {
			b.tenantCtr.Add(-toRelease)
		}
		if b.mgr != nil {
			b.mgr.globalUsed.Add(-toRelease)
		}
	}
}
