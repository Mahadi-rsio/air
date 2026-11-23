# AIR Phase 20.9 — PostgreSQL 17 Correctness, Durability & Lazy LSN Report

**Date:** 2026-11-23  
**Environment:** Real PostgreSQL `17.12` via `cell.Agent` + real `air-gateway` (`PoolMode=true`, Full Phase 20 config) + real TCP connections + real LSN-counting TCP proxy.  
**Raw Data:** [correctness.log](file:///workspaces/air/benchmarks/phase20.9/results/correctness.log), [correctness.json](file:///workspaces/air/benchmarks/phase20.9/results/correctness.json).

---

## 1. Real PostgreSQL 17 Correctness & Durability Matrix (11/11 PASSED)

| # | Test Case | Result | Measured Evidence (Real PG 17.12) |
|---:|---|---|---|
| 1 | `durability_gucs` | **PASS** | `fsync=on`, `full_page_writes=on`, `synchronous_commit=on` verified via `SHOW` |
| 2 | `ddl_constraints_triggers_seq` | **PASS** | Created `SERIAL` sequences, `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, `PL/pgSQL` trigger function + `AFTER INSERT` trigger |
| 3 | `constraints_fk_trigger_upsert` | **PASS** | `RETURNING id` returned `1`, duplicate `UNIQUE` rejected, `ON CONFLICT DO UPDATE RETURNING id` returned `1`, invalid `FK` rejected, trigger inserted `audit_t` row (`count=1`) |
| 4 | `mvcc_isolation_and_tx_affinity` | **PASS** | Inside `BEGIN ... ROLLBACK`, `pg_backend_pid()` stayed identical (`15186 == 15186`), uncommitted row invisible to concurrent `conn2` (`0`), absent after `ROLLBACK` (`0`) |
| 5 | `savepoint_semantics` | **PASS** | `BEGIN` -> `INSERT` -> `SAVEPOINT s1` -> `INSERT` -> `ROLLBACK TO s1` -> `COMMIT` kept first row (`1`) and discarded rolled-back row (`0`) |
| 6 | `concurrent_updates_atomic` | **PASS** | 8 concurrent clients × 25 `UPDATE ... SET v = v + 1` produced exact `200` (zero lost updates) |
| 7 | `copy_from_stdin_streaming` | **PASS** | Streamed `10,000` rows via `COPY FROM STDIN`, `RowsAffected() == 10000`, `SELECT count(*) == 10000` |
| 8 | `prepared_statements` | **PASS** | SQL `PREPARE` / `EXECUTE` / `DEALLOCATE` + Extended Query protocol parameterized `SELECT $1` all succeeded |
| 9 | `wal_crash_recovery_sigkill` | **PASS** | Sent `SIGKILL` to live PG17 postmaster; after restart via `cell.Agent`, committed row survived (`1`) and in-flight uncommitted tx rolled back (`0`) |
| 10 | `lazy_lsn_ryow_and_count` | **PASS** | 20 pure writes: **`0` `pg_current_wal_lsn()` queries**; subsequent `SELECT` preserved RYOW (`count=1`) and executed **`1` `pg_current_wal_lsn()`**; `BEGIN/INSERT/COMMIT` executed **`0` LSN queries** and **`0` spurious read-replica LSN checks**; subsequent `SELECT` preserved RYOW (`1`) with **`1` `pg_current_wal_lsn()`** |
| 11 | `eager_lsn_baseline_count` | **PASS** | With `WriteLazyLSNEnabled=false`, 20 writes executed **`20` `pg_current_wal_lsn()` queries** (100% reduction verified for pure writes under Lazy LSN) |

---

## 2. Go Race Detector Verification

Executed after Phase 20.9 fixes (`internal/cell/telemetry_publisher.go` and `internal/gateway/pooler.go`):
- `go test -race ./internal/gateway -run "TestWrite|TestBuffer|TestAdaptive|TestPhase20"` — **PASS (1.276s, 0 races)**
- `go test -race ./internal/cell -run "TestCollectTenantTelemetry|TestTelemetryPublisher|TestCgroup"` — **PASS (4.576s, 0 races)**

---

## 3. Correctness & Lazy LSN Engineering Notes

1. **Bug Found & Fixed During Phase 20.9 (`internal/gateway/pooler.go:836`):**
   - Previously, `handleSimpleQuery` queried the read backend (`SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn())::text`) whenever `sessionWriteLSN > 0 && rlsn < sessionWriteLSN` **before** checking if the query was a read. This caused `BEGIN` and write statements to trigger wasted read-backend LSN queries.
   - Fixed by guarding the refresh with `ClassifySQLForRouting(trimmed).Target == RouteTargetReadEngine`. Verified `txReadCheckLSNs=0`.
2. **Known Lazy LSN Scope & Edge Cases:**
   - **Gateway reconnect / Cell restart:** `sessionWriteLSN` and `sessionDirtyWrite` are per-connection in-memory state in `clientSessionState`. A client reconnecting to a new session starts with `sessionWriteLSN=0` (standard session-scoped RYOW; cross-session RYOW is not guaranteed).
   - **No-op write edge case (`pooler.go:909`):** If a write produces no WAL (e.g., `UPDATE` matching 0 rows or `ROLLBACK`), `wlsn > st.sessionWriteLSN` remains false so `sessionDirtyWrite` stays conservatively `true` (routes to primary — safe for RYOW, minor read-offload inefficiency).
3. **Explicit Transaction Admission Scope (`pooler.go:444`):**
   - `acquireWritePermit` gates `BEGIN` admission, releases the permit once `BEGIN` returns, and relies on `PoolMaxBackends` while `st.currentBackend != nil` for subsequent in-transaction statements. Transaction affinity and PostgreSQL semantics are 100% preserved, and no active transaction is ever moved between backends.

---

## 4. Reproduction

```bash
./benchmarks/phase20.9/run_benchmarks.sh correctness 5
go test -race ./internal/gateway -run "TestWrite|TestBuffer|TestAdaptive|TestPhase20"
go test -race ./internal/cell -run "TestCollectTenantTelemetry|TestTelemetryPublisher|TestCgroup"
```
