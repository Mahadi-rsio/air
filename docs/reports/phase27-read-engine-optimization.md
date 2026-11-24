# Phase 27: Read Engine Query Execution & Optimization Report

## Summary
Phase 27 upgraded the read engine cost-based optimizer, virtual table planner integration, statistics selectivity estimation, and multi-table join ordering in `internal/readadapter/`.

## Implemented Changes
1. **Covering Index & Ordered Index Scan Costing (`internal/readadapter/optimizer.go`, `internal/readadapter/sqlite_vtab.go`)**:
   - Added `IsCovering` to `PlanCandidate` and `ChooseBestPlanWithNeededCols` to `QueryOptimizer` while preserving `ChooseBestPlan` compatibility.
   - Updated `evaluateIndexCandidate` with `isCoveringIndex` so queries whose needed columns and constraint columns are fully contained in `plan.KeyAllCols` avoid heap page fetches (`heapPages = 0`) and set `IsCovering = true`.
   - Allowed `StrategyIndexScanOrdered` candidates to be evaluated even without leading `WHERE` predicates when `orderConsumed` is true, eliminating unnecessary sort costs.
   - Updated `goAirPGBestIndexCost` in `sqlite_vtab.go` to extract `neededAllCols` from SQLite's `colUsed` bitmask and pass it to `ChooseBestPlanWithNeededCols`.

2. **Parameter-Sensitive & Join Selectivity (`internal/readadapter/stats.go`)**:
   - Added `TableStats.EstimateBoundSelectivity` for parameter-aware and unique-equality selectivity estimation.
   - Added `EstimateJoinSelectivity` using `1 / max(leftNDV, rightNDV)` with row-count fallback.

3. **Cardinality- & Index-Aware Multi-Table Join Ordering (`internal/readadapter/join_exec.go`)**:
   - Added `JoinRelationStats` and `OrderJoinInputs` to produce left-deep join orders prioritizing smallest filtered cardinality first and preferring indexed probes on inner relations.

4. **Verification (`internal/readadapter/phase27_optimizer_test.go`)**:
   - Added `TestPhase27_CoveringIndexScanAvoidsHeapFetch`, `TestPhase27_OrderByIndexScanWithoutWhere`, and `TestPhase27_BoundSelectivityAndJoinOrdering`.

## Test Results
- `go test -v -run "TestPhase15|TestPhase16|TestPhase19|TestPhase26|TestPhase27" ./internal/readadapter/...` -> `PASS` (`0.223s`; live PG17 differential tests skipped because PostgreSQL 17 server binaries are not installed).
- `go test -short ./internal/gateway/...` -> `PASS` (`5.688s`).
