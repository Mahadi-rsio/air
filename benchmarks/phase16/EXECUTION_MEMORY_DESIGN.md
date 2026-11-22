# AIR Phase 16B — Query Execution Memory Management (`EXECUTION_MEMORY_DESIGN.md`)

## 1. Objective

Minimize heap allocations, GC pressure, and OOM risk on low-RAM multi-tenant nodes (512 MB–2 GB) by introducing bounded execution memory budgets, arena-style row materialization, fast numeric formatting, and bounded buffer pooling.

---

## 2. Architecture & Mechanisms

### 2.1 Bounded Execution Memory Manager ([memory_budget.go](file:///workspaces/air/internal/readadapter/memory_budget.go))
- `MemoryManager` enforces three hierarchical limits:
  - **Per-Query Limit** (`DefaultQueryLimitBytes`, default 16 MiB)
  - **Per-Tenant Limit** (`DefaultTenantLimitBytes`, default 64 MiB)
  - **Global Limit** (`GlobalLimitBytes`, default 256 MiB)
- `QueryMemoryBudget.Reserve(n)` atomically reserves bytes across query → tenant → global counters (rolling back cleanly on failure) and returns `ErrMemoryBudgetExceeded` for graceful exhaustion.
- `defer budget.Release()` guarantees idempotent cleanup even on error or cancellation.
- `MemoryManager.Stats()` also exposes SQLite C heap usage (`sqlite3_memory_used`, `sqlite3_memory_highwater`).

### 2.2 Arena-Style Row Materialization & Allocation Reduction ([sqlite_cgo.go](file:///workspaces/air/internal/readadapter/sqlite_cgo.go))
- Replaced per-row `make([]SQLiteValue, colCount)` with a slab/arena allocator (`allocRow`) growing in bounded chunks (16 → 128 rows), reducing row slice allocations from `O(N)` to `O(log N)`.
- Replaced `fmt.Sprintf("%d", ...)` / `fmt.Sprintf("%g", ...)` with `strconv.FormatInt` / `strconv.FormatFloat` (avoiding reflection and leveraging Go's small-int fast path).

### 2.3 Bounded Buffer Reuse & Wire Encoding ([sqlite_vtab.go](file:///workspaces/air/internal/readadapter/sqlite_vtab.go), [sqlite_server.go](file:///workspaces/air/internal/readadapter/sqlite_server.go))
- `pgVTabCursor.resetResources()` clears references (`clear(cur.rows)`, `clear(cur.rowBuf)`) so GC can reclaim strings/byte slices, and drops oversized `cur.rows` (`cap > 4096`) instead of retaining huge buffers indefinitely.
- `writePGDataRow` uses a bounded `sync.Pool` (`pgRowBufPool`, capped at `64 KiB` on put) and direct `binary.BigEndian.PutUint32`, eliminating per-row `bytes.Buffer` allocations and `binary.Write` reflection.

---

## 3. Verification

- `TestPhase16B_MemoryBudget_PerQueryPerTenantAndRelease`
- `TestPhase16B_SQLiteQueryMemoryExhaustionAndAccounting`
- `TestPhase16B_ConcurrentMemoryBudgets` (verified with `-race`)
- `BenchmarkPhase16B_SQLiteQuery_RowArena`
