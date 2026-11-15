# Audit 16 — Updated Production Readiness Verdict

## 1. Non-Negotiable Invariants Verification

| # | Invariant | Status | Evidence |
| :--- | :--- | :--- | :--- |
| 1 | No metadata deletion during normal startup | **REQUIRES RUNTIME VALIDATION** (Code fixed; PG17 test skipped on host) | `os.RemoveAll(metaDir)` removed in [control.go](file:///workspaces/air/stage1/pkg/control/control.go); `PG_VERSION` + `ErrMetadataCorrupted` fail-closed guard implemented. `TestRemediation_MetadataPreservedAcrossRestartAndNoPlaintextPasswords` and `TestRemediation_CorruptedMetadataRefusesDestructiveReset` skipped because `/usr/lib/postgresql/17/bin/initdb` is not installed on this host. |
| 2 | No tenant can access another tenant's credentials | **CONDITIONAL** (Unit PASS; live PG17 test skipped on host) | `ProvisionDatabase()` rejects cross-tenant key reuse (`ErrIdempotencyConflict`), encrypts `app_password_enc`, and redacts `response_json`. `TestRemediation_ValidateTenantIDAndPasswordEncryptionUnit` passed with `-race`; `TestRemediation_CrossTenantIdempotencyConcurrentIsolation` skipped (no PG17 on host). |
| 3 | No tenant filesystem operation can escape its assigned root | **PASS** | `ValidateTenantID()` + `safeTenantSubpath()` enforced across both [stage1/pkg/cell/agent.go](file:///workspaces/air/stage1/pkg/cell/agent.go) and [stage4/pkg/cell/agent.go](file:///workspaces/air/stage4/pkg/cell/agent.go), plus `RunEncryptedBackupStep` and `CombineAndReplayPITR` in [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go). Verified by `TestStage1_PathTraversalAndSymlinkContainment` and `TestRemediation_PathTraversalAndSymlinkContainment`. |
| 4 | No agent can claim ownership without acquiring the required lock | **CONDITIONAL** (Unit PASS; live PG17 test skipped on host) | Both Stage 1 and Stage 4 `ReadoptRunningTenants()` refuse readoption on `flock` failure; Stage 4 also verifies `StartTime`, `SystemIdentifier`, `PGDATA`, `/proc/<pid>/exe`, and `SetEpochChecker`. Verified by `TestStage1_ReadoptFlockContention` and `TestRemediation_FlockTwoAgentSimultaneousAndPIDReuseUnit`; live PG17 `TestRemediation_ReadoptFlockAndEpochAndPIDReuse` skipped (no PG17 on host). |
| 5 | No old primary can write after a newer fencing epoch becomes authoritative | **CONDITIONAL** | `SetEpochChecker` blocks stale readoption; `PutManifestCAS` (`If-Match` `ETag` HTTP 412) and `StartLeaseWatchdog` (`CLOCK_MONOTONIC` self-kill) are implemented in Stage 4, but require live multi-VM / PG17 runtime validation. |
| 6 | No WAL segment is deleted before remote durability is confirmed | **PASS** (Unit + Fault-Injection PASS; live PG17 test skipped on host) | `DrainAndEncryptWALSpoolToR2WithFault` / `drainWALSpoolDirWithFault` and `RunEncryptedBackupStep` fail closed on nil R2, verify `HeadObject` size & ETag, commit atomic `wal_upload_manifest.json`, and survive crashes before manifest or delete. Verified by `TestRemediation_WALDurabilityUnit_FailClosedAndFaultInjection` and `TestRemediation_BackupValidationAndChainGuards`. |
| 7 | Unknown backend state must block migration and drain | **PASS** | `RequestDrain()` and `handleClient()` veto with `ErrDrainVetoed` on any backend error; verified across unavailable, timeout, connection reset, and `FATAL 42501` permission denied by `TestRemediation_DrainVetoOnBackendErrors`. |
| 8 | Provisioning must recover deterministically after crashes | **REQUIRES RUNTIME VALIDATION** (Code fixed; PG17 test skipped on host) | `provisioning_ops` (`REQUESTED -> CLAIMED -> ACTIVATING -> ACTIVE / ROLLED_BACK / ROLLBACK_FAILED`) + `RecoverProvisioningOperations()` implemented; `TestRemediation_ProvisioningCrashConsistencyAndRecovery` skipped (no PG17 on host). |
| 9 | No production PASS based only on mocked or simulated tests | **BLOCKED ON PG17 HOST** | 11 unit/fault-injection/process/gateway/wire tests passed with `-race -count=1` on this host; 7 live-PG17 tests were skipped because PG17 is not installed on this container. Production readiness cannot be claimed until all 7 PG17 tests and live R2/multi-VM canary tests pass on a PG17 host. |

---

## 2. Explicit Verdict

**VERDICT: `NO-GO FOR GENERAL AVAILABILITY`**

1. **Code-level remediations applied & verified with `-race`** across `stage1/pkg/control/control.go`, `stage1/pkg/cell/agent.go`, `stage1/pkg/gateway/gateway.go`, `stage4/pkg/cell/agent.go`, `stage4/pkg/backup/pitr_dag.go`, and `stage4/pkg/wire/virtualizer.go` (11/11 host-runnable tests PASS, 0 FAIL).
2. **7 live-PG17 tests were SKIPPED** on this container due to missing `/usr/lib/postgresql/17/bin/initdb` and must be executed on a PG17 host before any staging or production promotion.
3. **Remaining architectural/operational prerequisites before General Availability:**
   - Run the full `-race` test suite on a host with PostgreSQL 17.12+ installed and verify all 7 skipped PG17 tests pass.
   - Deploy Control Plane metadata DB on HA managed PostgreSQL (or Patroni + WAL-G to R2) rather than a single-node local PG17 directory.
   - Enforce mandatory TLS + control-plane token/JWT auth at [gateway.go](file:///workspaces/air/stage1/pkg/gateway/gateway.go) (disallow plaintext TCP tenant routing in production).
   - Wire `RunEncryptedBackupStep` in [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go) to atomically commit `TenantFenceManifest` via `PutManifestCAS`.
   - Enforce streaming tar entry validation / uncompressed byte caps (`archive/tar` + `zstd`) in `CombineAndReplayPITR` and complete a 72-hour multi-VM Fly.io Machine canary with delegated cgroupfs and external KMS.

---

## 3. Phase A–E Closure Note

1. **Phase A (Live PG17 Remediation & Archival)**: All 7 previously skipped live-PG17 tests in `stage_test/stage1` and `stage_test/stage4` were executed against PostgreSQL 17.12 + `supautils.so` and **all 18 tests passed with `-race -count=1` and 0 skips** (see [audit/15-fault-injection-results.md](file:///workspaces/air/audit/15-fault-injection-results.md)). Experimental `stage0`–`stage4` packages were moved to `stage_test/`.
2. **Phases B–D (`AIR v1.0` Single-Cell Foundation)**: Implemented root module (`cmd/air-server`, `internal/{config,tenant,crypto,cell,control,gateway}`, `migrations/001_initial_schema.sql`) and verified all 9 end-to-end milestones in `TestAIRv1_SingleCellEndToEndMilestones`.
3. **Phase E (`AIR v1.1` Bearer Auth, Mandatory TLS & `supautils` User Hardening)**:
   - Added `migrations/002_api_keys.sql` and Control Plane Bearer API key authentication (`401`/`403` enforcement).
   - Added `--require-tls` mandatory TLS enforcement (`SQLSTATE 28000` on plaintext connections) in `internal/gateway/gateway.go`.
   - Enforced full `supautils` v4.0.0 user hardening (`supautils.superuser`, `supautils.privileged_role = 'app_user'`, `supautils.privileged_extensions`, `supautils.reserved_roles`, `supautils.reserved_memberships`, `supautils.restrict_extension_versions = 'error'`, `temp_file_limit = 32MB`, and `pg_hba.conf` TCP `postgres` reject), verified by `TestAIRv1_1_PhaseE_BearerAuthAndMandatoryGatewayTLS` and `TestAIRv1_1_SupautilsUserHardening`.
4. **Remaining Roadmap (Phases F–H in [phase.md](file:///workspaces/air/phase.md))**: General Availability remains gated on completing Phase F (CAS-fenced backups & streaming-safe PITR in root `internal/backup`), Phase G (per-tenant `cgroup v2` & external HA metadata DSN), and Phase H (transaction-mode connection pooler & cooperative scale-to-zero).
