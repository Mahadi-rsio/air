# AIR Phase 16C — Selective Parallel Query Execution (`PARALLEL_EXECUTION_DESIGN.md`)

## 1. Objective

Accelerate large sequential heap scans and aggregations on multi-core nodes without hurting small/medium query latency or causing CPU oversubscription under concurrent multi-tenant load.

---

## 2. Cost-Based Selection & Guardrails ([optimizer.go](file:///workspaces/air/internal/readadapter/optimizer.go))

`QueryOptimizer.chooseParallelWorkers` and `ShouldParallelize` enforce strict gating:
1. **Scan Strategy**: Only `StrategySeqScan` is parallelized (small B-tree lookups remain serial).
2. **Minimum Size & CPU Guardrail (Evidence-Driven)**:
   - Benchmarked `BenchmarkPhase16C_SerialVsParallelHeapAggregate` on 2 vCPUs (`AMD EPYC 7763`) for 128 pages (5,120 rows):
     - `Serial_1Worker_128Pages-2`: **740,657 ns/op**
     - `Parallel_2Workers_128Pages-2`: **1,202,079 ns/op** (slower due to goroutine/channel/Pageserver lock contention)
   - Therefore, `chooseParallelWorkers` **retains serial execution** (`ParallelWorkers = 1`) whenever `GOMAXPROCS < 4` or `nblocks < 512` (~4 MiB), and only enables parallel execution on `>= 4` vCPU configurations for large scans (`>= 512` pages) where `parallelCost < serialCost * 0.80`.
5. **Anti-Oversubscription**: `ShouldParallelize` falls back to serial (`1` worker) whenever `activeGlobalQueries >= GOMAXPROCS`.

---

## 3. Bounded Parallel Execution ([parallel_scan.go](file:///workspaces/air/internal/readadapter/parallel_scan.go), [sqlite_vtab.go](file:///workspaces/air/internal/readadapter/sqlite_vtab.go))

- `parallelScanIter` uses dynamic 16-block atomic work-stealing (`nextBlock.Add(16)`) across bounded workers and a bounded channel (`make(chan parallelScanBatch, workers*2)`) for backpressure.
- `ParallelHeapAggregate` computes `COUNT(*)` and `SUM(col)` in worker-local accumulators without materializing rows.
- `Close()` cancels `pctx` and drains `outCh` so no worker goroutines can ever leak.

---

## 4. Verification

- `TestPhase16C_CostBasedParallelSelection`
- `TestPhase16C_ParallelScanAndAggregateCorrectness` (verified with `-race`)
- `BenchmarkPhase16C_SerialVsParallelHeapAggregate`
