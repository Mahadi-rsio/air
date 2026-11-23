# AIR Phase 21.10 — Read Engine DDL Visibility & Consistency Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (+ Phase 21.10 fixes)  
**Status:** **PASS**

---

## 1. Root Causes Confirmed

1. **Per-session LSN isolation (`internal/gateway/pooler.go`, `sql_router.go`):** `sessionWriteLSN` was tracked only per client connection. Brand-new connections (`sessionWriteLSN == 0`) bypassed `ReadEngineLSN < SessionWriteLSN` and routed `SELECT` queries to `air-read-engine` immediately after another connection executed DDL.
2. **Premature `LastLSN` advancement (`internal/pageserver/readpath.go`):** `PutPageBatch` advanced `Timeline.lastLSN`, causing `GetTenantInfo` (and thus Read Engine's `pg_last_wal_replay_lsn()`) to report LSNs ahead of `WALReceiver` catalog (`pg_class`) WAL ingestion.
3. **Unprotected Extended-Query Read Engine execution (`internal/gateway/pooler.go`):** Unlike simple queries (`tryExecuteOnReadEngine`), `handleExtendedBatch` streamed Read Engine `'E'` responses directly to the client without falling back to primary PostgreSQL.

---

## 2. Changes Implemented

- **`internal/gateway/pooler.go`:**
  - Added pool-wide `poolDDLLSN` and `poolDDLDirty` on `TenantPool`, plus `txHasDDL` on `clientSessionState`.
  - Added `syncReadEngineForQuery` so **all** connections (including `sessionWriteLSN == 0`) check `max(sessionWriteLSN, poolDDLLSN)` and verify Read Engine has a confirmed non-zero LSN before routing.
  - Added `recordPrimaryWriteState` to update `poolDDLLSN` on autocommit DDL or transaction commit after DDL.
  - Buffered Read Engine extended-query responses in `runBatchOnBackend` until `'Z'`; on any `'E'` response (when primary is not suspended), safely rolls back parses and falls back to primary PostgreSQL.
- **`internal/pageserver/readpath.go`, `server.go`:**
  - Tracked `walLastLSN` separately from `PutPageBatch` snapshots and returned `WALLastLSN()` from `GetTenantInfo`.

---

## 3. Verification & Test Results

| Test | Command / Target | Result | Evidence |
|---|---|---|---|
| Unit test: DDL barrier blocks new connection (`sessionWriteLSN == 0`) | `go test -race -v ./internal/gateway -run TestTenantPool_DDLVisibilityBarrierBlocksNewConnectionLag` | **PASS** | Verified lag fallback, post-sync Read Engine routing, `poolDDLDirty`, and `ReadEngineLSN == 0` fallback |
| Live 4-service stack: `ddl-visibility` | `go run ./benchmarks/phase21 --mode=ddl-visibility --tenant-prefix=p2110e` | **PASS** | `CREATE TABLE` + immediate `SELECT` on same & 10 brand-new connections (10/10); `CREATE INDEX` + `ALTER TABLE` visible on new connection; `DROP TABLE` returned `42P01`; concurrent reads + `air-p21-read-engine` restart (`2742 ops, 0 errors`) |
