# AIR Phase 17D — Multi-Tenant Isolation & Resource Protection (`TENANT_ISOLATION_REPORT.md`)

## 1. Shared Resource Audit

| Shared Resource | Protection Mechanism | Bounded? | Verified Under Adversarial Load? |
|---|---|---|---|
| **Query Execution Memory** | `MemoryManager` (`memory_budget.go`) hierarchical per-query, per-tenant, global atomic budgets | Yes (configurable via `SQLiteServerConfig`) | Yes (`TestPhase16B_*`, `cap_harness`) |
| **CPU Scheduling & Concurrency** | `TenantScheduler` (`scheduler.go`) fair-share dispatch by fewest active queries per tenant + priority | Yes (`MaxGlobalConcurrency`, `DefaultTenantConcurrency`) | Yes (`TestPhase16G_*`, `abusive` cgroup test) |
| **Query Queues** | `TenantScheduler.MaxQueueDepth` (default 128) returning `ErrSchedulerQueueFull` | Yes | Yes |
| **L1 Page Cache** | `TieredCacheBackend` bounded LRU (`L1MaxBytes`) | Yes | Yes |
| **SLRU MVCC Cache** | `CLOGResolver.cache` bounded (`max=4096` pages) | Yes | Yes |
| **Query Plan Cache** | `QueryPlanCache` 16-shard bounded LRU (`4096` entries) | Yes | Yes |
| **Prepared Statement Cache** | Per-connection 64-entry LRU (`sqlite_cgo.go`) | Yes | Yes |
| **Result Rows** | `MaxResultRows` (`ErrResultLimitExceeded`) | Yes | Yes |
| **Connection Slots** | `TenantPool.maxBackends` + `ReadersPerTenant` | Yes | Yes |

---

## 2. Adversarial Isolation Benchmark (`micro-512mb`, 50 Tenants, `abusive` Workload)

- **Setup**: 4 concurrent workers continuously hammer tenant `t_0000` with expensive 400×400 cross-table joins + `ORDER BY`, while 4 concurrent workers execute normal point lookups across tenants `t_0001..t_0049` inside a **512 MB cgroup**.
- **Measured Results**:
  - **Normal Tenants (`t_0001..t_0049`)**: **p50 = 3.30 ms**, **p95 = 29.76 ms**, **p99 = 52.51 ms**
  - **Abusive Tenant (`t_0000`)**: **p50 = 440.97 ms**
  - **Failures / OOMs**: `0` OOMs, `0` failed normal queries, peak RSS `58.2 MB`.
  - **Conclusion**: `TenantScheduler`'s fewest-active-tenant-first dispatch and `MemoryManager`'s per-tenant/per-query budgets isolate normal tenants from noisy-neighbor join/sort storms on a 512 MB node.
