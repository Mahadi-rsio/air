# Audit 16 — Updated Production Readiness Verdict

## 1. Non-Negotiable Invariants Verification

| # | Invariant | Status | Evidence |
| :--- | :--- | :--- | :--- |
| 1 | No metadata deletion during normal startup | **PASS** | `os.RemoveAll(metaDir)` removed; `TestRemediation_MetadataPreservedAcrossRestartAndNoPlaintextPasswords` & `TestRemediation_CorruptedMetadataRefusesDestructiveReset` pass with `-race` on live PG17. |
| 2 | No tenant can access another tenant's credentials | **PASS** | `ProvisionDatabase()` rejects cross-tenant key reuse (`ErrIdempotencyConflict`), encrypts `app_password_enc`, and redacts `response_json`; verified under concurrency. |
| 3 | No tenant filesystem operation can escape its assigned root | **PASS** | `ValidateTenantID()` + `safeTenantSubpath()` enforced across all `stage4/pkg/cell/agent.go` paths; verified by `TestRemediation_PathTraversalAndSymlinkContainment`. |
| 4 | No agent can claim ownership without acquiring the required lock | **PASS** | `ReadoptRunningTenants()` refuses readoption on `flock` failure, stale epoch, or PID reuse; verified by `TestRemediation_ReadoptFlockAndEpochAndPIDReuse`. |
| 5 | No old primary can write after a newer fencing epoch becomes authoritative | **PASS** | `SetEpochChecker` blocks stale readoption; `PutManifestCAS` (`If-Match` `ETag` HTTP 412) + `StartLeaseWatchdog` (`CLOCK_MONOTONIC` self-kill) verified in Stage 4 + follow-up tests. |
| 6 | No WAL segment is deleted before remote durability is confirmed | **PASS** | `DrainAndEncryptWALSpoolToR2WithFault()` fails closed on nil R2, verifies `HeadObject` size, commits atomic `wal_upload_manifest.json`, and survives crashes before manifest or delete. |
| 7 | Unknown backend state must block migration and drain | **PASS** | `RequestDrain()` and `handleClient()` veto with `ErrDrainVetoed` on any backend error; verified across unavailable, timeout, and reset conditions. |
| 8 | Provisioning must recover deterministically after crashes | **PASS** | `provisioning_ops` (`REQUESTED -> CLAIMED -> ACTIVATING -> ACTIVE / ROLLED_BACK`) + `RecoverProvisioningOperations()` verified across all 4 fault hooks and simulated `SIGKILL`. |
| 9 | No production PASS based only on mocked or simulated tests | **CONDITIONAL** | All P0/P1 remediations were tested against **live PostgreSQL 17.12** with `-race` (fault-injection WAL tests used a real HTTP S3 `PutObject`/`HeadObject` server; live R2 was tested in `stage4-runner`). Production multi-VM Fly.io canary, Gateway mandatory TLS+JWT auth, Control Plane metadata HA, and `RunEncryptedBackupStep` atomic R2 manifest commit remain external/operational prerequisites. |

---

## 2. Explicit Verdict

**VERDICT: `NO-GO FOR GENERAL AVAILABILITY` / `CONDITIONAL GO FOR CONTROLLED STAGING CANARY`**

- **All 4 P0 risks (`FUP-P0-01`..`FUP-P0-04`) and all 4 P1 risks (`FUP-P1-01`..`FUP-P1-04`) identified in this critical follow-up audit are REMEDIATED and VERIFIED with `-race` and live PostgreSQL 17.12.**
- **Remaining non-code / architectural prerequisites before General Availability:**
  1. Deploy Control Plane metadata DB on HA managed PostgreSQL (or Patroni + WAL-G to R2) rather than a single-node local PG17 directory.
  2. Enforce mandatory TLS SNI + control-plane token/JWT authorization at [gateway.go](file:///workspaces/air/stage1/pkg/gateway/gateway.go) (disallow plain TCP tenant routing).
  3. Wire `RunEncryptedBackupStep` in [pitr_dag.go](file:///workspaces/air/stage4/pkg/backup/pitr_dag.go) to atomically commit `TenantFenceManifest` via `PutManifestCAS` after `pg_basebackup` upload.
  4. Complete the 72-hour multi-VM Fly.io Machine canary (`iad`/`ord`) with delegated cgroupfs and external KMS.
