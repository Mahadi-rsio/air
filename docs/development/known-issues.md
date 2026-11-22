# AIR Known Issues & Deferred Engineering Items

This document tracks known operational and packaging issues in the AIR repository, including items intentionally deferred from **Phase 13.1 (Code Quality, Architecture Refactoring & Remaining Engineering Audit)**.

---

## 1. ~~Deferred Issue~~ Resolved (Phase 14): `air-gateway` Container Runtime Shared Library Dependency (`libgrpc++.so.1.83`)

- **Status**: **Resolved in Phase 14** (see [Phase 14 Gateway & Integration Audit](file:///workspaces/air/docs/phase14-gateway-audit.md))
- **Severity**: High (Container Deployment / Fly.io Runtime)
- **Affected Components**:
  - `air-gateway` C++20 binary inside the multi-stage container image built by [Dockerfile](file:///workspaces/air/Dockerfile)
  - [gateway/Makefile](file:///workspaces/air/gateway/Makefile)
  - Container startup script [scripts/air-entrypoint.sh](file:///workspaces/air/scripts/air-entrypoint.sh) (already updated to supervise both `air-server` and `air-gateway` via `wait -n`)

### 1.1 Exact Symptom and Root Causes Found in Phase 14

Previously, container builds/runtimes suffered from three packaging/linking issues:
1. **Unconditional conda RPATH in [gateway/Makefile](file:///workspaces/air/gateway/Makefile)**: `RPATH_FLAGS := -Wl,-rpath,/opt/conda/lib` linked local builds against `/opt/conda/lib/libgrpc++.so.1.83` and `/opt/conda/lib/libssl.so.3` (requiring `OPENSSL_3.3.0`), causing binaries built on conda-equipped hosts to fail on standard Debian/Ubuntu systems or inside containers.
2. **Missing gRPC/Protobuf dev packages & stale pre-generated `pb` headers in [Dockerfile](file:///workspaces/air/Dockerfile) `cpp-builder`**: Stage 1b (`cpp-builder`) lacked `libgrpc++-dev`, `libprotobuf-dev`, `protobuf-compiler`, and `protobuf-compiler-grpc`, and `gateway/src/pb/airdb.pb.h` had been pre-generated against conda's newer protobuf (`google/protobuf/runtime_version.h`).
3. **Missing gRPC/Protobuf runtime shared libraries in [Dockerfile](file:///workspaces/air/Dockerfile) Stage 3**: The final `debian:bookworm-slim` runtime stage lacked `libgrpc++1.51` and `libprotobuf32` and had no `ldd` verification.

### 1.2 Resolution Applied in Phase 14

1. **[gateway/Makefile](file:///workspaces/air/gateway/Makefile)**: Guarded `RPATH_FLAGS` with `ifneq ($(wildcard /opt/conda/lib),)` so `-Wl,-rpath,/opt/conda/lib` is only added when `/opt/conda/lib` exists, and added `src/admin_grpc.hpp` to `HEADERS`.
2. **[Dockerfile](file:///workspaces/air/Dockerfile)**:
   - Added `libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc` to `cpp-builder` and regenerated `src/pb/*` from `proto/airdb.proto` during the Docker build.
   - Added `libgrpc++1.51 libprotobuf32` to the final `debian:bookworm-slim` runtime stage and added `RUN ldd /usr/local/bin/air-gateway && (! ldd /usr/local/bin/air-gateway | grep -q "not found")`.
3. **Verification**: Verified `cpp-builder` (`make all` + `make test`: `All C++ air-gateway tests passed!`) and clean `debian:bookworm-slim` runtime container (`ldd`, `air-gateway` startup on `:5432`/`:8081`, `GET /healthz` -> `{"status":"ok"}`, and clean `SIGTERM` exit code `0`).

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
