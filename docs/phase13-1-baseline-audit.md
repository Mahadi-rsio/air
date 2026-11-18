# AIR Phase 13.1 — Repository Baseline Audit

**Date:** 2026-11-18  
**Repository:** `https://github.com/Mahadi-rsio/air.git`  
**Default Branch:** `main`  
**Baseline Commit SHA:** `d54bdc2a68b92e2628c7c77abd93a92c2afe6a38`  
**Last Stable Phase 12 Commit SHA:** `9ce153c4602ef66050c01fa45f83ba144b542dc5` (with Phase 12 benchmark artifacts recorded in `7c53a8ba90944c3d3f9c62f5a989b527db1f07aa`)

---

## 1. Repository State & Recent Commit History

A clean `git status` was verified on `main` at commit `d54bdc2a68b92e2628c7c77abd93a92c2afe6a38` before any edits were made.

### Recent Commit History

| Commit SHA | Summary | Phase |
| :--- | :--- | :--- |
| `d54bdc2` | `feat(phase13): backup hardening, WAL archival, PITR, restore, and verification` | Phase 13 (Baseline HEAD) |
| `7c53a8b` | `bench(phase12): record Phase 12 architecture hardening benchmark report` | Phase 12 |
| `9ce153c` | `Phase 12: Architecture Hardening, Cold-Offload Correctness & Hot-Path Optimization` | Phase 12 |
| `de43f6a` | `bench: record Phase 11 storage engine perfection report` | Phase 11 |
| `848f18c` | `Phase 11: Storage Engine Perfection, Compaction & End-to-End Durability` | Phase 11 |

### Diff Since Last Stable Phase 12 Commit (`9ce153c..d54bdc2`)

Files added or modified between `9ce153c` and `d54bdc2`:

- `benchmarks/REPORT.md` (modified in `7c53a8b`)
- `benchmarks/phase12_verify.sh` (added in `7c53a8b`)
- `benchmarks/results/phase12/phase12_verify.log` (added in `7c53a8b`)
- `docs/phase13-architecture.md` (added in `d54bdc2`, 144 lines)
- `internal/backup/lsn.go` (added in `d54bdc2`, 117 lines)
- `internal/backup/manifest.go` (added in `d54bdc2`, 243 lines)
- `internal/backup/pitr_dag.go` (modified in `d54bdc2`, +24/-2 lines)
- `internal/backup/recovery.go` (added in `d54bdc2`, 149 lines)
- `internal/backup/store.go` (added in `d54bdc2`, 219 lines)
- `internal/backup/tar_safe.go` (added in `d54bdc2`, 188 lines)
- `internal/backup/verify.go` (added in `d54bdc2`, 159 lines)
- `internal/control/control.go` (modified in `d54bdc2`, +5/-9 lines — introduced syntax/build corruption)

---

## 2. Existing Architecture & Package Structure

### High-Level Architecture

AIR is a multi-tenant, scale-to-zero PostgreSQL 17 platform with decoupled compute and storage:

1. **Data-Plane Gateway (`gateway/`)**: Standalone C++20/libpqxx PostgreSQL wire-protocol proxy that terminates client TLS/GSS/SCRAM connections, queries the Go Control Plane via gRPC (`GetRoute`) to wake suspended tenants on demand, and pools backend connections.
2. **Control Plane (`internal/control/`)**: Go service managing tenant lifecycle (`CREATING`, `ACTIVE`, `IDLE`, `SUSPENDING`, `SUSPENDED`, `DELETED`), idempotent provisioning, cold offload (`local`, `offloading`, `offloaded`, `restoring_local`, `failed`), R2 CAS fencing, and backup/PITR/restore orchestration.
3. **Cell Agent (`internal/cell/`)**: Local PostgreSQL 17 process supervisor managing `initdb`, `pg_ctl`, Unix-domain socket readiness, cgroup v2 memory limits, warm pool pre-initialization, and storage layout (`base/`, `pg_wal/`, `spool/wal/`).
4. **Disaggregated Page Server (`internal/pageserver/`)**: Custom binary-framed TCP storage server (`GetPage@LSN`, `GetRelSize`, `GetSlruSegment`) backed by `InMemoryLayer`, immutable local `ImageLayer`/`DeltaLayer` files with 64-byte headers, CRC32C checksums, sorted 20-byte index entries, 64KB Content-Addressable Units (CAUs) in Cloudflare R2, and background compaction.
5. **PostgreSQL Storage Manager Extension (`extensions/air_smgr/`)**: C extension hooking PostgreSQL 17 `f_smgr` to fetch missing 8KB pages on demand from the Page Server while maintaining a local disk cache.
6. **Backup, WAL Archival & PITR Engine (`internal/backup/`)**: Incremental/full tar+zstd+AES-256-GCM envelope-encrypted backups, WAL spool archiver, immutable backup manifests, S3/R2 verification, and PostgreSQL 17 `recovery.signal` PITR replay.
7. **Fencing & Envelope Crypto (`internal/fencing/`, `internal/crypto/`, `internal/tenant/`)**: HTTP `If-Match` ETag CAS fencing on R2 manifests, HKDF-SHA256 + AES-256-GCM per-tenant DEK envelope encryption, and strict tenant identifier/generation validation.

### Current Package Structure

| Path | Language | Role |
| :--- | :--- | :--- |
| `cmd/air-server/` | Go | Unified entrypoint starting Control Plane HTTP/gRPC, Cell Agent, Page Server, and WAL Receiver |
| `internal/config/` | Go | Environment variable configuration loading and validation |
| `internal/tenant/` | Go | Tenant ID and generation validation (`ValidateTenantID`, `ValidateGeneration`) |
| `internal/crypto/` | Go | Master KEK loading, HKDF-SHA256 derivation, AES-256-GCM envelope encryption (`AIR1` format) |
| `internal/fencing/` | Go | Cloudflare R2 `If-Match` CAS manifest fencing, epoch management, backup/WAL metadata |
| `internal/backup/` | Go | Base/incremental tar+zstd+AES-GCM backups, WAL spool draining, `BackupManifest`, `Store`, `VerifyBackup`, PITR staging & replay |
| `internal/cell/` | Go | PostgreSQL 17 process supervision, warm pool, cgroup limits, `air_smgr` GUC wiring |
| `internal/pageserver/` | Go | Page Server TCP protocol, WAL receiver (`pglogrepl`), `InMemoryLayer`, `ImageLayer`, `DeltaLayer`, CAU bundler, compactor, R2 hydration |
| `internal/control/` | Go | Control Plane state machine, SQLite/Postgres metadata store, HTTP REST API, gRPC routing server, SSE broadcaster, cold offload worker |
| `internal/pb/` | Go/Protobuf | gRPC `ControlPlaneService` (`GetRoute`) protobuf definitions |
| `extensions/air_smgr/` | C (PG17) | Custom PostgreSQL 17 storage manager (`f_smgr`) communicating with `internal/pageserver` |
| `gateway/` | C++20 | Standalone PostgreSQL wire-protocol proxy & connection pooler |
| `tests/e2e/` | Go | End-to-end integration tests across Phase A through Phase 12 |

---

## 3. Baseline Build & Test Status (at `d54bdc2`)

### Compilation (`go build ./...`)

**Status:** **FAILS AT BASELINE (`d54bdc2`)**

```text
# github.com/Mahadi-rsio/air/internal/backup
internal/backup/pitr_dag.go:224:16: undefined: backup
internal/backup/pitr_dag.go:225:2: undefined: r2Key
```

And in `internal/control/control.go`:

```text
# github.com/Mahadi-rsio/air/internal/control
internal/control/control.go:35:24: syntax error: unexpected =, expecting )
internal/control/control.go:1188:2: syntax error: unexpected gen at end of statement
internal/control/control.go:1192:3: syntax error: non-declaration statement outside function body
```

### Per-Package Test Status at Baseline (`d54bdc2`)

| Package | Build Status | Test Status | Notes |
| :--- | :--- | :--- | :--- |
| `internal/config` | PASS | PASS (`0.002s`) | No test files |
| `internal/crypto` | PASS | PASS (`0.002s`) | No test files |
| `internal/tenant` | PASS | PASS (`0.002s`) | Tested transitively via `pageserver` and `tests/e2e` |
| `internal/fencing` | PASS | PASS (`0.025s`) | 5 unit tests pass |
| `internal/cell` | PASS | PASS (`0.003s`) | Tested via integration tests |
| `internal/pageserver` | PASS | PASS (`15.266s`) | All 15 unit/property/compaction/CAU tests pass |
| `internal/backup` | **FAIL** | **FAIL (build error)** | `pitr_dag.go:224-225` undefined `backup` & `r2Key` |
| `internal/control` | **FAIL** | **FAIL (build error)** | `control.go:34` & `1188` syntax errors |
| `cmd/air-server` | **FAIL** | **FAIL (build error)** | Depends on `internal/control` and `internal/backup` |
| `tests/e2e` | **FAIL** | **FAIL (build error)** | `e2e_test.go` and `phase_h_test.go` import removed `internal/gateway` package |

---

## 4. Validated Known Issues at Baseline (`d54bdc2`)

Every issue below was directly verified against the source code at `d54bdc2a68b92e2628c7c77abd93a92c2afe6a38`:

### P0 — Build-Breaking & Correctness Defects

1. **P0-1 (`internal/backup/pitr_dag.go:224-225`) — Self-package qualifier and undeclared variable**:
   - Inside `package backup`, `RunEncryptedBackupStepGen` calls `backup.BackupBaseObjectKey(tenantID, generation, bkpID)` and assigns `r2Key = baseObjKey` without declaring `r2Key`.
2. **P0-2 (`internal/control/control.go:34-39`) — Corrupted top-level `var (` declaration**:
   - A one-line `type restoreDatabaseReq struct` definition was pasted over `var (`, removing `ErrIdempotencyConflict` and `ErrNotFound` and causing a syntax error on the remaining sentinel errors.
3. **P0-3 (`internal/control/control.go:1185-1190`) — Syntax corruption and duplicate backup step in `backupTenantToR2`**:
   - Missing closing brace `}`, stray character `t`, calls `RunEncryptedBackupStep` (without generation) followed immediately by `RunEncryptedBackupStepGen` (with generation), and drops the error check on the second call.
4. **P0-4 (`internal/backup/` & `internal/control/`) — Immutable `BackupManifest` is never published during backups**:
   - Although `internal/backup/manifest.go` and `internal/backup/store.go` define `BackupManifest`, `Seal()`, and `Store.PublishManifest()`, `backupTenantToR2` never constructs or publishes `tenants/<tenant>/<gen>/backups/<backup_id>/manifest.json`. Furthermore, `RunEncryptedBackupStepGen` updates the mutable `TenantFenceManifest` *before* required WAL segments are drained and uploaded.
5. **P0-5 (`internal/control/control.go:1165, 1225, 1278`) — Generation propagation mismatch in WAL archival, incremental parent lookup, and restore**:
   - `drainWALSpoolAndUpdateManifestGen` calls `DrainStoppedWALSpoolToR2Gen` (with `generation`) when `stopped == true`, but calls `DrainAndEncryptWALSpoolToR2` (with `generation = ""`) when `stopped == false` (active tenant), writing active WAL to non-generation-scoped keys.
   - `backupTenantToR2` (for `INCR` parent lookup) and `RestoreDatabase` call `fencing.ReadManifestWithETag` (without `generation`), while `RunEncryptedBackupStepGen` writes to the generation-scoped manifest via `ReadManifestWithETagGen`.
6. **P0-6 (`internal/control/control.go:1252`) — `RestoreDatabase` is a validation-only temporary restore and ignores `target_time` and `new_tenant_id`**:
   - `RestoreDatabase` stages recovery in a temporary directory that is immediately deleted (`defer cleanup()`), does not wire `target_time` or `new_tenant_id`, and does not provision a new tenant generation or persist backup/restore catalog state (the v5/v6 migrations mentioned in `d54bdc2`'s commit message were never written to `internal/control/control.go`).

### P1 — Data Integrity, Verification & Test Suite Defects

7. **P1-1 (`tests/e2e/e2e_test.go`, `tests/e2e/phase_h_test.go`) — Stale import of removed `internal/gateway` package**:
   - Commit `97de54d` removed the Go `internal/gateway` package when introducing the C++ `air-gateway`, leaving `tests/e2e` unable to compile.
8. **P1-2 (`internal/backup/store.go:154`, `pitr_dag.go:509`, `verify.go:94`) — Plaintext vs. ciphertext SHA-256 confusion in `Store.Head` / `VerifyObject`**:
   - `drainWALSpoolDirWithFaultGen` sets S3 metadata `"air-sha256": plainSHA` and `"air-cipher-sha256": cipherSHA`.
   - When an S3/R2 provider does not return native `ChecksumSHA256`, `Store.Head()` falls back to `out.Metadata["air-sha256"]` (plaintext SHA-256), while `VerifyBackup` passes `ref.SHA256 = w.CipherSHA256` (ciphertext SHA-256) to `VerifyObject()`. With `StreamHash: false`, `VerifyObject` compares plaintext SHA-256 against ciphertext SHA-256 and fails with `ErrObjectChecksum`.
9. **P1-3 (`internal/backup/lsn.go:109-112`) — `ValidateWALContinuity` last segment end LSN bug**:
   - `lastCovered` was computed as `segs[len(segs)-1].start` instead of `segs[len(segs)-1].start + WALSegmentSize`. Because a WAL segment covers `[start, start + 16MB)`, any `endLSN > lastSeg.start` (including single-segment backups where `endLSN` is inside the segment or at `start + 16MB`) erroneously fails with `ErrMissingWALSegment`.
10. **P1-4 (`internal/backup/pitr_dag.go:60, 703`, `lsn.go:88`) — Non-segment WAL files (`.history`, `.backup`) break `ValidateWALContinuity` and `StagePITRRestore`**:
    - `isValidWALFilename` allows `.history` and `.backup` files to be archived as `WALSegmentArtifact` entries, which then fail `WALSegmentStartLSN` parsing during continuity validation and restore staging.

---

## 5. Deferred Gateway Runtime Issue (Scope Restriction)

Per the Phase 13.1 scope restrictions:

- **Issue:** In containerized deployments (`Dockerfile`), the standalone C++ `air-gateway` binary is compiled in a `ubuntu:24.04` builder stage against custom-built gRPC `v1.83.0-dev` shared libraries (`/usr/local/lib/libgrpc++.so.1.83`), which are not copied into the final runtime stage (`COPY --from=cpp-builder /build/gateway/air-gateway /usr/local/bin/air-gateway`). Starting `air-gateway` in the container without those shared libraries fails with:
  `error while loading shared libraries: libgrpc++.so.1.83: cannot open shared object file: No such file or directory`
- **Phase 13.1 Disposition:** **Intentionally deferred.** `Dockerfile`, `gateway/*`, and `scripts/air-entrypoint.sh` are **not modified** in Phase 13.1. The issue is tracked in [known-issues.md](file:///workspaces/air/docs/known-issues.md) and [phase13-1-final-report.md](file:///workspaces/air/docs/phase13-1-final-report.md).

---

## 6. Phase 13 Implementation Status at Baseline (`d54bdc2`)

| Component / Capability | Baseline Status at `d54bdc2` | Audit Notes |
| :--- | :--- | :--- |
| LSN & WAL segment math (`internal/backup/lsn.go`) | Partial / Buggy | Implemented, but `ValidateWALContinuity` miscalculates `lastCovered` LSN and rejects `.history` files |
| Immutable `BackupManifest` (`internal/backup/manifest.go`) | Unwired | Struct, `Seal()`, and `Validate()` exist, but never called by backup or restore orchestration |
| Backup object store (`internal/backup/store.go`) | Partial / Buggy | Implemented, but `Head()` conflates `air-sha256` (plaintext) with ciphertext SHA-256 |
| Safe tar extraction (`internal/backup/tar_safe.go`) | Implemented | Prevents path traversal, symlink/hardlink/device escapes, and zip-bombs |
| PG17 recovery config (`internal/backup/recovery.go`) | Implemented | Writes `postgresql.auto.conf` + `recovery.signal` for `target_lsn`, `target_time`, or `latest` |
| Backup verification (`internal/backup/verify.go`) | Unwired / Buggy | `VerifyBackup` exists, but is unwired to Control Plane/HTTP and affected by `Head()` checksum bug |
| Generation-scoped WAL archival (`internal/backup/pitr_dag.go`) | Partial | Stopped WAL uses `generation`; active WAL drain in `control.go` omits `generation` |
| Control Plane Backup/Restore Catalog (v5/v6 schema) | Missing | Mentioned in commit message of `d54bdc2`, but absent from `internal/control/control.go` |
| Restore to new tenant (`new_tenant_id`) & `target_time` | Unwired | `RestoreDatabase` only runs temporary validation restore and ignores `new_tenant_id` / `target_time` |
