# AIR Phase 17E — Crash Recovery & Failure Engineering (`FAILURE_RECOVERY_REPORT.md`)

## 1. Failure Scenarios Verified

| Scenario | Test / Verification | Outcome | Guarantees |
|---|---|---|---|
| **Pageserver / Storage Outage** | `TestPhase17E_FailureRecovery_StorageOutageAndClientDisconnect` (`failingStorageBackend`) | **PASS** | Explicit error returned; `ActiveQueries=0`, `GlobalUsedBytes=0`, `ActiveGlobal=0` after recovery |
| **Abrupt Client Disconnect** | `TestPhase17E_FailureRecovery_StorageOutageAndClientDisconnect` (TCP close mid-session) | **PASS** | Server cleans up session without hanging; subsequent clients connect and query normally |
| **Query Cancellation** | `TestPhase17E_*` + `TestPhase16F_ContextCancellationAndCleanup` + `TestPhase16F_ConcurrentCancelAndCloseRace` | **PASS** | Zero leaked goroutines, C memory, `MemoryManager` bytes, or `TenantScheduler` slots |
| **Pageserver + ReadServer Restart** | `TestPhase17B_DifferentialCorrectness_LivePG17` (full stop & restart against persisted `layerDir`) | **PASS** | All post-restart queries (`HOT+prune`, `attmissingval`, `tx`) match PostgreSQL 17.12 identically |
| **Unsupported WAL Record** | `TestPhase17_UnsupportedWALRecord_NotSilentlyIgnored` (`walredo.go`) | **PASS** | Explicit error returned (`require walredo worker`); never silently returns stale base page |
| **Unsupported PG Type / Missing Default** | `TestPhase17B_DifferentialCorrectness_LivePG17` (`numeric` column) | **PASS** | Explicit `ErrUnsupportedType` returned so gateway falls back safely to PostgreSQL |

---

## 2. Recovery Guarantees & Remaining Limitations

- **Guarantees**:
  - `defer budget.Release()` and `defer scheduler.Release()` ensure no execution memory or concurrency slots leak on panic, cancellation, client disconnect, or storage error.
  - `SQLiteReadServer` never caches failed or partial pages in L1/L2.
  - Gateway (`TenantPool.tryExecuteOnReadEngine`) falls back to PostgreSQL (`RouteReasonReadErrorFallback`) on read-engine failure for read-only queries.
- **Limitations**:
  - Mid-query `SQLiteReadServer` process crash requires the gateway (or client) to retry the read query on PostgreSQL or a restarted read engine.
  - In-flight prepared statements on a restarted `SQLiteReadServer` must be re-prepared by the client/pooler.
