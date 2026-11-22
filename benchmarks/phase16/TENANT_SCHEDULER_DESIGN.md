# AIR Phase 16G — Tenant-Aware Workload Scheduling (`TENANT_SCHEDULER_DESIGN.md`)

## 1. Objective

Prevent noisy-neighbor starvation and CPU/memory exhaustion across multi-tenant workloads using a single shared, zero-background-goroutine admission controller.

---

## 2. Architecture (`TenantScheduler` in [scheduler.go](file:///workspaces/air/internal/readadapter/scheduler.go))

1. **Shared Infrastructure**: A single `TenantScheduler` instance in `SQLiteReadServer` serves all tenants with zero per-tenant goroutines or dedicated pools.
2. **CPU-Aware & Per-Tenant Limits**:
   - `MaxGlobalConcurrency` (default `max(2, GOMAXPROCS) * 4`)
   - `DefaultTenantConcurrency` (default `max(2, MaxGlobalConcurrency/2)`), overridable via `SetTenantConcurrency`.
3. **Memory-Aware Admission Control**:
   - Checks `MemoryManager` global and per-tenant usage against `MemorySoftLimitRatio` (default `0.90`).
   - Under memory pressure, `PriorityBatch` and `PriorityNormal` queries fail fast with `ErrMemoryPressure`, while `PriorityInteractive` queries can still proceed up to the hard `MemoryManager` limit.
4. **Fair Dispatch & Priority**:
   - On slot release, `dispatchLocked` selects the next eligible waiter by:
     1. Highest `QueryPriority` (`PriorityInteractive` > `PriorityNormal` > `PriorityBatch`)
     2. Fewest currently active queries for that tenant (fairness / anti-noisy-neighbor)
     3. FIFO sequence number
5. **Bounded Queues & Backpressure**:
   - Waiting queue is bounded by `MaxQueueDepth` (default `128`), returning `ErrSchedulerQueueFull` immediately on saturation.
   - Waiters respect `ctx.Done()` cancellation cleanly without leaking slots.

---

## 3. Verification

- `TestPhase16G_FairSchedulingAndNoisyNeighborPrevention`
- `TestPhase16G_QueueBackpressureAndMemoryAdmission`
