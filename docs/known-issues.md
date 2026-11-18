# AIR Known Issues & Deferred Engineering Items

This document tracks known operational and packaging issues in the AIR repository, including items intentionally deferred from **Phase 13.1 (Code Quality, Architecture Refactoring & Remaining Engineering Audit)**.

---

## 1. Deferred Issue: `air-gateway` Container Runtime Shared Library Dependency (`libgrpc++.so.1.83`)

- **Status**: Intentionally Deferred in Phase 13.1 (Scheduled for Gateway Packaging Phase)
- **Severity**: High (Container Deployment / Fly.io Runtime)
- **Affected Components**:
  - `air-gateway` C++20 binary inside the multi-stage container image built by [Dockerfile](file:///workspaces/air/Dockerfile)
  - Container startup script [scripts/air-entrypoint.sh](file:///workspaces/air/scripts/air-entrypoint.sh)
  - Public PostgreSQL wire protocol listener (`:5432`) and Gateway Admin/Metrics HTTP server (`:8081`) in container deployments

### 1.1 Exact Symptom and Error Message

When starting the production container image, `air-gateway` immediately exits at dynamic link time with:

```text
/usr/local/bin/air-gateway: error while loading shared libraries: libgrpc++.so.1.83: cannot open shared object file: No such file or directory
```

Because [scripts/air-entrypoint.sh](file:///workspaces/air/scripts/air-entrypoint.sh#L56-L70) launches `air-gateway` as an unmonitored background job (`air-gateway ... &`) before `exec air-server`, the container stays running (`air-server` serves `/healthz` on `:8080` and gRPC on `:9090`), while ports `:5432` and `:8081` remain closed.

### 1.2 Root Cause

1. **Missing Runtime Shared Libraries in Final Image Stage**:
   - In [Dockerfile](file:///workspaces/air/Dockerfile#L6-L24), Stage 1 (`builder`) installs `libgrpc++-dev`, `libprotobuf-dev`, and `protobuf-compiler-grpc` and compiles [gateway/Makefile](file:///workspaces/air/gateway/Makefile) dynamically against `libgrpc++`, `libgrpc`, `libprotobuf`, `libabsl_*`, `libpq`, `libssl`, and `libcrypto`.
   - In [Dockerfile](file:///workspaces/air/Dockerfile#L79-L106), Stage 2 (`FROM debian:bookworm-slim`) copies `/usr/local/bin/air-gateway` from `builder` (`COPY --from=builder /src/gateway/air-gateway /usr/local/bin/air-gateway`), but only installs `ca-certificates`, `curl`, `gnupg`, `iproute2`, `jq`, `libpq5`, `procps`, `zstd`, and `postgresql-17`. The C++ gRPC and Protobuf shared libraries (`libgrpc++.so*`, `libgrpc.so*`, `libprotobuf.so*`, `libabsl_*.so*`, `libre2.so*`, `libc-ares.so*`, `libupb.so*`) are not installed in the runtime image.
2. **Silent Background Exit in Entrypoint**:
   - [scripts/air-entrypoint.sh](file:///workspaces/air/scripts/air-entrypoint.sh) starts `air-gateway` in the background without `wait -n` or readiness verification on `:5432` / `:8081`.

### 1.3 Why Deferred in Phase 13.1

Per Phase 13.1 Scope Restrictions:
- Phase 13.1 is dedicated to Go codebase modularity, concurrency safety, developer documentation, and end-to-end verification of Phase 13 Backup, WAL Archival, Checksum Verification, PITR, and Restore workflows.
- Modifying `gateway/*`, [Dockerfile](file:///workspaces/air/Dockerfile), or [scripts/air-entrypoint.sh](file:///workspaces/air/scripts/air-entrypoint.sh) was explicitly prohibited in Phase 13.1 to isolate Go control-plane/storage changes from container packaging changes.
- Note: Building and running `air-gateway` directly in the development workspace (`make -C gateway all test`) works when the development host has `libgrpc++-dev` installed.

### 1.4 Recommended Fix for the Future Gateway Packaging Phase

1. **Runtime Library Packaging in [Dockerfile](file:///workspaces/air/Dockerfile)**:
   - Install matching Debian Bookworm runtime packages (`libgrpc++1.51`, `libgrpc29`, `libprotobuf32`, `libc-ares2`, `libre2-9`) in the runtime stage, **or** copy all non-glibc shared libraries resolved by `ldd /src/gateway/air-gateway` from the `builder` stage and run `ldconfig`.
   - Add a build-time verification step in the final stage of [Dockerfile](file:///workspaces/air/Dockerfile):
     ```dockerfile
     RUN ldd /usr/local/bin/air-gateway | (! grep -q "not found") && /usr/local/bin/air-gateway --help >/dev/null
     ```
2. **Fail-Fast Supervision in [scripts/air-entrypoint.sh](file:///workspaces/air/scripts/air-entrypoint.sh)**:
   - Supervise both `air-server` and `air-gateway` using `wait -n` (with `trap` forwarding `SIGTERM`/`SIGINT` to both PIDs) so the container exits immediately if either process crashes.

---

## 2. Current Architectural & Operational Limitations

### 2.1 Non-Destructive Restore Model (`new_tenant_id` vs. Validation Restore)
- **Behavior**: `POST /v1/databases/{id}/restore` never overwrites an existing live tenant's `PGDATA` in place.
  - When `new_tenant_id` is omitted, AIR performs a **validation PITR restore**: it downloads and decrypts the backup chain and WAL segments into an isolated staging directory, replays WAL up to the requested `target_lsn`, `target_time`, or `latest`, runs `VerifyQuery` and `pg_amcheck`, records the result in `restore_ops`, and cleans up the staging directory.
  - When `new_tenant_id` is provided, AIR provisions a **new isolated tenant database** from the restored `PGDATA`, generates a fresh KMS DEK (`epoch = 1`) and `app_user` password, rewrites `air_smgr` and PostgreSQL port/socket configuration, starts the restored PostgreSQL 17 instance, and registers the new tenant in `ACTIVE` state.
- **Rationale**: In-place destructive restore risks irreversible data loss if a user selects an unintended recovery target LSN/timestamp. Restoring into a new tenant allows side-by-side verification before switching application traffic.

### 2.2 Co-Located Cell Agent and Pageserver Socket
- **Behavior**: Each `air-server` node manages local PostgreSQL 17 instances via `cell.Agent` and communicates with `air_smgr.so` over a local Unix domain socket (`--pageserver-socket`, default `/tmp/air-pageserver.sock`).
- **Implication**: Tenant compute (`postgres` process) and the `pageserver.Server` cache reside on the same host volume (`--data-dir`), while Cloudflare R2 serves as the shared durable object store for CAU layers, fencing manifests, base/incremental backups, and archived WAL segments.

### 2.3 Timestamp PITR Boundary Checking
- **Behavior**: While `target_lsn` (`RecoveryTargetLSN`) is strictly validated against the manifest's `[ConsistencyLSN, WALEndLSN]` range before starting PostgreSQL recovery, `target_time` (`RecoveryTargetTimestamp`) can only be coarsely pre-checked against backup creation time because individual WAL record timestamps inside compressed segments are only decoded by PostgreSQL during WAL replay. If a `target_time` precedes the base backup consistency point, PostgreSQL recovery halts before promotion and `StagePITRRestore` returns an error.
