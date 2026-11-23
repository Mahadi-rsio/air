# AIR Phase 20 — Correctness & Failure Testing Report

**Date:** 2026-11-23  
**Test Suite:** `go test -race -timeout 60s ./internal/cell/... ./internal/gateway/...`  
**Status:** **PASS** (0 data races, all unit and integration tests passing)

---

## 1. Test Coverage Matrix

| Requirement / Failure Mode | Test Name | File | Outcome |
|---|---|---|---|
| Write request classification & conservative unknown fallback | `TestWriteRequestClassification` | `internal/gateway/write_governor_integration_test.go` | PASS |
| Global & per-session buffer budget limits, double-release & negative accounting protection | `TestBufferBudgetGlobalAndSessionLimits` | `internal/gateway/buffer_budget_test.go` | PASS |
| Concurrent buffer budget stress (32 goroutines) with zero byte leaks | `TestBufferBudgetConcurrency` | `internal/gateway/buffer_budget_test.go` | PASS |
| Bounded write queue, per-tenant & global limits, fast `ErrWriteQueueFull` rejection | `TestWriteGovernorBoundedQueueAndLimits` | `internal/gateway/write_governor_test.go` | PASS |
| Context cancellation & queue wait timeout without slot leaks | `TestWriteGovernorCancellationAndTimeout` | `internal/gateway/write_governor_test.go` | PASS |
| Multi-tenant Deficit Round Robin (DRR) fairness (bulk vs interactive) | `TestWriteGovernorTenantFairnessDRR` | `internal/gateway/write_governor_test.go` | PASS |
| Concurrent governor stress (24 goroutines, 2 tenants) with zero leaked slots/bytes | `TestWriteGovernorConcurrentStressNoLeak` | `internal/gateway/write_governor_test.go` | PASS |
| Cell cgroup v2 (`memory.current`, `memory.high`, `memory.max`, `memory.events`, `cpu.stat`) & PSI telemetry | `TestCollectTenantTelemetryCgroupV2` | `internal/cell/telemetry_test.go` | PASS |
| Missing cgroup/PSI graceful degradation | `TestCollectTenantTelemetryMissingDegradesGracefully` | `internal/cell/telemetry_test.go` | PASS |
| Adaptive AIMD controller, hysteresis, OOM/critical halving, conservative startup, stale/missing telemetry fallback | `TestAdaptiveControllerAIMDAndHysteresis` | `internal/gateway/adaptive_controller_test.go` | PASS |
| End-to-end transaction pinning (`BEGIN`/`SAVEPOINT`/`COMMIT`), Lazy LSN (`0` extra round-trips), disconnect buffer cleanup, Prometheus `/metrics`, `/internal/v1/telemetry` | `TestPhase20Integration_TxPinningBufferBudgetAndLazyLSN` | `internal/gateway/write_governor_integration_test.go` | PASS |

---

## 2. Transaction & Durability Guarantees Verified
- **PostgreSQL Semantics Untouched:** No PostgreSQL source modifications; `fsync` and `full_page_writes` remain enabled; no cross-client transaction merging.
- **Transaction Affinity:** Explicit transactions (`st.currentBackend != nil`) are never split across backends or re-queued mid-transaction.
- **No Ambiguous Retries:** Timed-out or rejected writes return SQLSTATE `53300` before reaching PostgreSQL; in-flight writes are never automatically retried after ambiguous commit outcomes.
- **Next Phase Recommendations:**
  1. Run sustained `pgbench` + multi-tenant `COPY FROM STDIN` benchmarks against a live PostgreSQL 17 + `air_smgr` cell with real cgroup v2 `memory.high` throttling.
  2. Add a Cell Agent periodic telemetry push client (`POST /internal/v1/telemetry`) when running Gateway and Cell on separate hosts.
