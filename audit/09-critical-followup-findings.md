# Audit 09 — Critical Follow-up Findings Register

**Repository:** `Mahadi-rsio/air` (`main`)  
**Audit Date:** 2026-11-15  
**Scope:** Independent Principal Database Engineer follow-up audit focused on data loss, tenant isolation, distributed ownership, crash recovery, and PostgreSQL durability.

---

## 1. Production-Intended Execution Path vs. Experimental Packages

| Path | Role | Production Relevance |
| :--- | :--- | :--- |
| [control.go](file:///workspaces/air/stage1/pkg/control/control.go) | Control Plane (`NewControlPlane`, `ProvisionDatabase`, `GetTenantAuth`, `DeleteDatabase`, `ReconcileOnce`) | **Production-intended Control Plane** (only `ControlPlane` implementation in repo) |
| [gateway.go](file:///workspaces/air/stage1/pkg/gateway/gateway.go) | TLS/SNI Wire Gateway + bbolt RU WAL | **Production-intended Gateway** (only `Gateway` implementation in repo) |
| [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) | Cell Agent (`ClaimCluster`, `ActivateCluster`, `ReadoptRunningTenants`, `StartLeaseWatchdog`, `EnforceTenantQuotaOnce`) | **Production-intended Cell Agent** (supersedes `stage1/stage2/stage3` cell agents) |
| [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go) | Encrypted Backup, WAL Spool Drain, PITR, DAG GC | **Production-intended Backup/PITR Engine** |
| [r2_cas.go](file:///workspaces/air/stage4/pkg/fencing/r2_cas.go) | Cloudflare R2 Conditional `If-Match` CAS Fencing | **Production-intended Fencing Layer** |
| [virtualizer.go](file:///workspaces/air/stage4/pkg/wire/virtualizer.go) | `pgproto3` Wire Virtualizer & 2PC Drain Guard | **Production-intended Connection Virtualizer** |
| `stage0/`, `mvp/`, `stage2/pkg/*`, `stage3/pkg/*`, `*/cmd/*-runner/` | Historical PoCs and experiment runners | **Experimental / Historical** (not deployed in production path) |

---

## 2. Primary P0 & P1 Findings Summary

| ID | Severity | Classification | File & Function | Status |
| :--- | :--- | :--- | :--- | :--- |
| `FUP-P0-01` | **P0** | **CONFIRMED** | [control.go](file:///workspaces/air/stage1/pkg/control/control.go) — `NewControlPlane()` | **REMEDIATED & VERIFIED** |
| `FUP-P0-02` | **P0** | **CONFIRMED** | [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) — `ClaimCluster()` & all `tenantID` FS ops | **REMEDIATED & VERIFIED** |
| `FUP-P0-03` | **P0** | **CONFIRMED** | [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) — `ReadoptRunningTenants()` | **REMEDIATED & VERIFIED** |
| `FUP-P0-04` | **P0** | **CONFIRMED** | [control.go](file:///workspaces/air/stage1/pkg/control/control.go) — `ProvisionDatabase()` | **REMEDIATED & VERIFIED** |
| `FUP-P1-01` | **P1** | **CONFIRMED** | [control.go](file:///workspaces/air/stage1/pkg/control/control.go) — Provisioning transitions | **REMEDIATED & VERIFIED** |
| `FUP-P1-02` | **P1** | **CONFIRMED** | [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go) — `DrainAndEncryptWALSpoolToR2()` | **REMEDIATED & VERIFIED** |
| `FUP-P1-03` | **P1** | **CONFIRMED** | [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) — `isPostgresProcessAlive()`, `SignalProcessTree()` | **REMEDIATED & VERIFIED** |
| `FUP-P1-04` | **P1** | **CONFIRMED** | [virtualizer.go](file:///workspaces/air/stage4/pkg/wire/virtualizer.go) — `RequestDrain()`, `handleClient()` | **REMEDIATED & VERIFIED** |

---

## 3. Additional Risk Audit Classifications (Workstream 2)

| Risk Area | Classification | File & Symbol | Evidence & Disposition |
| :--- | :--- | :--- | :--- |
| Plaintext passwords in metadata | **CONFIRMED** | [control.go](file:///workspaces/air/stage1/pkg/control/control.go) (`clusters.app_password`, `idempotency_records.response_json`) | Previously stored `pw_<hex>` in plaintext in both tables. **Remediated:** `clusters.app_password_enc` AES-256-GCM encrypted via `EnvelopeKMS`, legacy column set to `"[ENCRYPTED]"`, `response_json` redacted (`"[REDACTED]"`). |
| Secrets in logs, JSON responses, backup manifests, crash dumps | **CONDITIONAL** | [control.go](file:///workspaces/air/stage1/pkg/control/control.go), [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) (`.init_secret.sql`), [r2_cas.go](file:///workspaces/air/stage4/pkg/fencing/r2_cas.go) | `TenantRuntime.AppPassword` uses `json:"-"`, `TenantFenceManifest.DEKBundle` stores KEK-wrapped DEK. `idempotency_records.response_json` previously leaked `AppPassword` (remediated). Transient `.init_secret.sql` is `0600` and unlinked after `psql`, but a crash during `psql` leaves it on disk until cleanup. |
| Concurrent provision/delete/wake/migrate races | **CONFIRMED** | [control.go](file:///workspaces/air/stage1/pkg/control/control.go) (`ProvisionDatabase`, `DeleteDatabase`) | Previously `ProvisionDatabase` locked only `idempotencyKey`, allowing concurrent provisions or `DeleteDatabase` for the same `tenantID` to race on `tenants/claiming/<tenantID>`. **Remediated:** added per-tenant lock (`"t:"+tenantID`) alongside per-key lock. |
| Metadata DB HA and recovery | **CONFIRMED** | [control.go](file:///workspaces/air/stage1/pkg/control/control.go) (`NewControlPlane`) | Single-node local PG17 instance in `metaDir` with no streaming replica, no WAL archiving of metadata DB itself, and no multi-node failover. **Remaining dependency:** production requires managed HA PostgreSQL / Patroni + R2 WAL-G for Control Plane metadata. |
| Backup manifest consistency | **CONFIRMED** | [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go) (`RunEncryptedBackupStep`), [r2_cas.go](file:///workspaces/air/stage4/pkg/fencing/r2_cas.go) | `RunEncryptedBackupStep` uploads backup objects to R2 and returns `BackupArtifact` in memory without atomically committing to `TenantFenceManifest` in R2 via `PutManifestCAS`. Crash before caller persists manifest orphans the R2 object. |
| Timeline and incremental backup chain validation | **CONFIRMED** | [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go) (`CombineAndReplayPITR`) | Previously passed `chain` directly to `pg_combinebackup` without checking `FULL` root or `ParentID` linkage. **Remediated:** added explicit `chain[0].Type == "FULL"` and `ParentID == BackupID` validation; timeline history validation added for WAL names. |
| Tar extraction and decompression limits | **CONFIRMED** | [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go) (`CombineAndReplayPITR`) | Previously used unbounded `io.ReadAll(obj.Body)` and `tar --zstd -xf` without `--no-same-owner --no-same-permissions`. **Remediated:** bounded `io.LimitReader` (`maxRestoreObjMiB = 2048`) and `--no-same-owner --no-same-permissions`. |
| Gateway tenant authorization and SNI binding | **CONFIRMED** | [gateway.go](file:///workspaces/air/stage1/pkg/gateway/gateway.go) (`handleConn`) | Plain TCP (`sniHost == ""`) is accepted without TLS SNI, and when `params["database"] == "postgres"` or `""`, SNI vs database mismatch check is skipped; Gateway delegates all client auth to backend SCRAM. Requires mandatory TLS SNI + control-plane token/JWT enforcement at edge in production. |
| Backend session-state leakage | **CONFIRMED** | [virtualizer.go](file:///workspaces/air/stage4/pkg/wire/virtualizer.go) (`acquireBackend`, `releaseBackend`) | Previously pooled `BackendConn` in `idlePool` without resetting backend session state across different clients (`SET work_mem`, temp tables, `LISTEN`, etc.). **Remediated:** added `lastClientID` tracking on `BackendConn` and mandatory `DISCARD ALL;` on cross-client reuse; verified by `TestRemediation_BackendSessionStateIsolation`. |
| Tenant quota enforcement and filesystem accounting | **CONFIRMED** | [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) (`MeasureTenantDiskBytes`, `EnforceTenantQuotaOnce`) | Previously walked only `tenants/active/<id>` (omitting `spool/wal/<id>`) using logical `info.Size()`, and ran only on-demand. **Remediated:** now walks both `tenants/active/<id>` and `spool/wal/<id>` using `st_blocks * 512`. Background enforcement loop remains a production integration item. |
| Unbounded in-memory maps and goroutine leaks | **CONFIRMED** | [control.go](file:///workspaces/air/stage1/pkg/control/control.go) (`keyLocks`), [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) (`PrewarmPool`), [virtualizer.go](file:///workspaces/air/stage4/pkg/wire/virtualizer.go) (`idlePool`) | `cp.keyLocks` grew without eviction; `PrewarmPool` spawned `backgroundRefillLoop` on every call; `idlePool` had no cap. **Remediated:** refcounted `keyLocks` deletion on unlock, `refillOnce` on `PrewarmPool`, and `idlePool` cap (`64`). |

---

## 4. Detailed Findings (Primary P0 & P1)

### `FUP-P0-01` — Metadata Database Destruction on Startup
- **File & Function:** [control.go](file:///workspaces/air/stage1/pkg/control/control.go) — `NewControlPlane()`
- **Evidence (Before Fix):** Line 63 unconditionally executed `_ = os.RemoveAll(metaDir)` before `initdb`.
- **Trigger Conditions:** Any Control Plane process restart, crash recovery, or rolling deploy pointing at an existing `metaDir`.
- **Failure Scenario:** Control Plane restarts; `NewControlPlane()` deletes `metaDir`, initializes a blank PostgreSQL cluster, and loses every tenant's `clusters`, `idempotency_records`, `usage_ledger`, and `audit_events` rows.
- **Customer / Data Impact:** Total loss of tenant routing, credentials, billing ledger, and provisioning state across the cell/region.
- **Severity:** **P0**
- **Root Cause:** Demo-style clean-slate initialization left in `NewControlPlane()`.
- **Proposed Fix:** Remove `os.RemoveAll(metaDir)`; detect existing `PG_VERSION`; start existing cluster safely (handling stale `postmaster.pid` only when PID is dead); fail closed with `ErrMetadataCorrupted` if corrupted; add `schema_migrations` versioning (`v1`, `v2`).
- **Regression Test:** `TestRemediation_MetadataPreservedAcrossRestartAndNoPlaintextPasswords`, `TestRemediation_CorruptedMetadataRefusesDestructiveReset`.
- **Remaining Uncertainty:** Single-node local PG17 still lacks multi-node HA replication (tracked under Metadata DB HA).

### `FUP-P0-02` — Filesystem Path Traversal & Command Injection
- **File & Function:** [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) — `ClaimCluster()`, `ActivateCluster()`, `StartOrAdoptTenant()`, `StopTenantClean()`, `StopAndPurgeTenant()`, `MeasureTenantDiskBytes()`
- **Evidence (Before Fix):** `filepath.Join(a.volDir, "tenants/claiming", tenantID)` followed immediately by `os.RemoveAll(claimDir)` (line 412) with zero validation; `StopTenantClean` interpolated `pgdata` and `mode` into `su - postgres -c` shell string (line 672).
- **Trigger Conditions:** Caller supplies `tenantID` containing `..`, `/`, or symlink inside tenant directory (reproduced with `ClaimCluster("../../victim")`).
- **Failure Scenario:** `os.RemoveAll` deletes arbitrary directories on the Cell volume (e.g., another tenant's `pgdata`, `.cell_ballast`, or `pool/ready`), or shell metacharacters execute arbitrary commands as `postgres`.
- **Customer / Data Impact:** Cross-tenant data destruction and host/container code execution.
- **Severity:** **P0**
- **Root Cause:** Missing input allowlist, canonical path containment check, symlink check, and parameterized exec.
- **Proposed Fix:** Enforce `ValidateTenantID` (`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`) + `safeTenantSubpath` (`filepath.Rel` containment + `os.Lstat` symlink rejection + `EvalSymlinks` base check) on all tenant FS paths; replace `su - postgres -c` string formatting with `runPostgresCmd`.
- **Regression Test:** `TestRemediation_PathTraversalAndSymlinkContainment`.
- **Remaining Uncertainty:** None for Go-level path handling; OS-level container/VM filesystem permissions remain defense-in-depth.

### `FUP-P0-03` — `flock` & Readoption Ownership Bypass
- **File & Function:** [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) — `ReadoptRunningTenants()`
- **Evidence (Before Fix):** Line 188 discarded lock acquisition error: `_ = a.acquireFlockLocked(tenantID, filepath.Join(tDir, "agent.lock"))`, and immediately registered the tenant in `a.tenants[tenantID]`.
- **Trigger Conditions:** Two Cell Agents restart simultaneously on the same volume, or a new Agent starts while the previous Agent still holds `agent.lock`.
- **Failure Scenario:** Second Agent fails to acquire `flock`, ignores the error, adopts the running `postmaster`, and both Agents manage/stop/backup the same PGDATA concurrently.
- **Customer / Data Impact:** Split-brain process management, corrupted `postmaster.pid`, and data corruption.
- **Severity:** **P0**
- **Root Cause:** Swallowed error return from `acquireFlockLocked` and absence of `postmaster.pid` PGDATA/identity/epoch checks.
- **Proposed Fix:** Skip/reject readoption whenever `acquireFlockLocked` fails; verify `postmaster.pid` PGDATA path, `/proc/<pid>/exe`, `/proc/<pid>/stat` `starttime`, and `SetEpochChecker` control-plane fencing epoch before registering.
- **Regression Test:** `TestRemediation_ReadoptFlockAndEpochAndPIDReuse`.
- **Remaining Uncertainty:** Requires production integration of `SetEpochChecker` against live R2/Control Plane epoch store.

### `FUP-P0-04` — Cross-Tenant Idempotency Credential Leak
- **File & Function:** [control.go](file:///workspaces/air/stage1/pkg/control/control.go) — `ProvisionDatabase()`
- **Evidence (Before Fix):** `idempotency_records` keyed solely by `idempotency_key`; line 180 queried `WHERE idempotency_key = $1` and returned `cached` (`ProvisionResponse` including `AppPassword`) without checking `tenant_id` or payload hash.
- **Trigger Conditions:** Tenant B calls `ProvisionDatabase(ctx, keyA, "tenant_b")` using Tenant A's `idempotencyKey`.
- **Failure Scenario:** Control Plane returns Tenant A's `TenantID`, `SystemIdentifier`, `Port`, and plaintext `AppPassword` to Tenant B.
- **Customer / Data Impact:** Complete cross-tenant authentication compromise.
- **Severity:** **P0**
- **Root Cause:** Unscoped idempotency key and unvalidated cache replay.
- **Proposed Fix:** Verify `recTenant == tenantID`, `recHash == payloadHash`, and `cached.TenantID == tenantID`; reject mismatches with `ErrIdempotencyConflict`; redact `AppPassword` in stored `response_json` and encrypt `app_password_enc`.
- **Regression Test:** `TestRemediation_CrossTenantIdempotencyConcurrentIsolation`.
- **Remaining Uncertainty:** None in `ProvisionDatabase`.

### `FUP-P1-01` — Provisioning Crash Consistency
- **File & Function:** [control.go](file:///workspaces/air/stage1/pkg/control/control.go) — `ProvisionDatabase()`, `RecoverProvisioningOperations()`
- **Evidence (Before Fix):** `idempotency_records` inserted `'IN_PROGRESS'` (`ON CONFLICT DO NOTHING`) and never transitioned on crash or failure; no durable operation state machine (`REQUESTED`, `CLAIMED`, `ACTIVATING`, `ACTIVE`) or startup recovery existed.
- **Trigger Conditions:** Process crash (`SIGKILL`) or transient error between `ClaimCluster`, `INSERT INTO clusters`, `ActivateCluster`, and final `UPDATE`.
- **Failure Scenario:** Orphaned `tenants/claiming/<tenantID>` directories, stuck `'CLAIMING'` rows in `clusters`, and permanently wedged `'IN_PROGRESS'` idempotency records.
- **Customer / Data Impact:** Leaked disk space and permanently unprovisionable tenant IDs.
- **Severity:** **P1**
- **Root Cause:** Non-transactional multi-step provisioning without a durable state machine or crash reconciler.
- **Proposed Fix:** Add `provisioning_ops` table (`REQUESTED -> CLAIMED -> ACTIVATING -> ACTIVE / ROLLED_BACK`), `RecoverProvisioningOperations` on startup, idempotent rollback on every error path, and `SetFaultHook` for deterministic fault injection.
- **Regression Test:** `TestRemediation_ProvisioningCrashConsistencyAndRecovery`.
- **Remaining Uncertainty:** External Fly machine deletion if crash occurs across remote VM boundaries requires control-plane sweepers.

### `FUP-P1-02` — WAL Durability Fail-Open & Unverified Deletion
- **File & Function:** [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go) — `DrainAndEncryptWALSpoolToR2()`
- **Evidence (Before Fix):** Line 184 skipped upload when `s3c == nil` (`if s3c != nil { ... }`) and unconditionally executed `_ = os.Remove(segPath)` at line 194 without `HeadObject` verification or a durable upload manifest.
- **Trigger Conditions:** Misconfigured/nil R2 client, or crash after `PutObject` before tracking, or corrupt/out-of-order files in spool.
- **Failure Scenario:** Local WAL segments are deleted without ever being uploaded to R2, creating an unrecoverable PITR gap.
- **Customer / Data Impact:** Permanent data loss / broken PITR chain.
- **Severity:** **P1**
- **Root Cause:** Missing fail-closed client guard, missing `HeadObject` verification, and missing atomic upload manifest.
- **Proposed Fix:** Return `ErrR2ClientRequired` if `s3c == nil || bucket == ""`; validate WAL segment/history names and sort in LSN order; compute SHA-256; verify R2 `HeadObject` size; commit `VERIFIED_IN_R2` to fsynced `wal_upload_manifest.json` before `os.Remove(segPath)`; support safe resume.
- **Regression Test:** `TestRemediation_DrainWAL_FailClosedAndFaultInjection`.
- **Remaining Uncertainty:** Tested against in-process mock S3 server for fault injection; live R2 path verified in Stage 4 runner.

### `FUP-P1-03` — Process Identity & Supervision Gaps
- **File & Function:** [agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go) — `isPostgresProcessAlive()`, `SignalProcessTree()`, `StartOrAdoptTenant()`
- **Evidence (Before Fix):** `isPostgresProcessAlive` checked only `/proc/<pid>/stat` state and `comm` containing `"postgres"` (vulnerable to PID reuse); `SignalProcessTree` scanned only direct children (`ppid == rootPID`), missing grandchildren/workers, and released `flock` without waiting for exit.
- **Trigger Conditions:** PID wraparound after agent crash, or PostgreSQL spawning grandchildren (e.g. `archive_command` shell, `sh -c ...`).
- **Failure Scenario:** Wrong process adopted or signaled; surviving grandchild processes continue writing to PGDATA after `flock` is released.
- **Customer / Data Impact:** Orphaned background writers, split-brain I/O, or accidental kill of unrelated PID.
- **Severity:** **P1**
- **Root Cause:** Lack of `starttime` / `exe` / `PGDATA` verification, shallow `ppid` scan, and missing cgroup/exit barrier.
- **Proposed Fix:** Record and verify `/proc/<pid>/stat` `starttime`, `/proc/<pid>/exe`, and `postmaster.pid` PGDATA in `agent.pidmeta`; add recursive descendant tree traversal (`collectProcessTree`), cgroup v2 (`air_tenant_<id>`) attachment/kill, and `waitForPIDsExit` before `releaseFlockLocked`.
- **Regression Test:** `TestRemediation_ReadoptFlockAndEpochAndPIDReuse`, `TestRemediation_ProcessSupervision_NestedGrandchildKill`.
- **Remaining Uncertainty:** Cgroup v2 write requires container/VM delegation of `/sys/fs/cgroup`; recursive `/proc` walk provides fallback when cgroupfs is read-only.

### `FUP-P1-04` — Wire Virtualizer Drain Fail-Open
- **File & Function:** [virtualizer.go](file:///workspaces/air/stage4/pkg/wire/virtualizer.go) — `RequestDrain()`, `handleClient()`
- **Evidence (Before Fix):** Line 138 (`if err == nil && unmigratable > 0`) and line 338 ignored `err != nil` from `checkBackendUnmigratableState(ctx)`, allowing drain/migration to proceed when backend state was unknown; also blocked on `drainReqCh` when 0 clients were connected.
- **Trigger Conditions:** Backend PostgreSQL unavailable, query timeout, permission denied, or connection reset during `RequestDrain`.
- **Failure Scenario:** Live migration drains and cuts over a tenant while active 2PC prepared transactions or advisory locks exist on the backend.
- **Customer / Data Impact:** Silent loss of 2PC state or advisory lock invariants during migration.
- **Severity:** **P1**
- **Root Cause:** Inverted error check (`err == nil && ...`) failing open instead of failing closed.
- **Proposed Fix:** Veto drain (`ErrDrainVetoed`) on ANY `err != nil` from `checkBackendUnmigratableState(ctx)` in both `RequestDrain` and `handleClient`; handle 0-active-client drain without channel deadlock.
- **Regression Test:** `TestRemediation_DrainVetoOnBackendErrors`.
- **Remaining Uncertainty:** None.
