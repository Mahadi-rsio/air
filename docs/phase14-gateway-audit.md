# AIR Phase 14 — Gateway Engineering, Runtime Reliability & Integration Audit

## 1. Executive Summary

Phase 14 investigated and resolved three operational and integration issues in AIR:
1. **`air-gateway` Container Packaging & Shared Library Linking (`libgrpc++.so.1.83`)**: Fixed [Dockerfile](file:///workspaces/air/Dockerfile) and [gateway/Makefile](file:///workspaces/air/gateway/Makefile) so `air-gateway` builds cleanly on `debian:bookworm-slim` and starts reliably in a clean runtime container without conda or missing gRPC/Protobuf shared libraries.
2. **Gateway Architecture & Concurrency Audit**: Audited all modules under `gateway/src/` (`main.cpp`, `config.hpp`, `pg_wire.hpp`, `discovery.hpp`, `pooler.hpp`, `admin_http.hpp`, `admin_grpc.hpp`, `metrics.hpp`) and fixed two concrete runtime bugs:
   - Replaced the 1-byte-at-a-time TLS read and shared-buffer race in `proxy_bidirectional` ([gateway/src/main.cpp](file:///workspaces/air/gateway/src/main.cpp)) with `Stream::read_some` ([gateway/src/pg_wire.hpp](file:///workspaces/air/gateway/src/pg_wire.hpp)) and per-direction stack buffers.
   - Added active client tracking (`active_clients`, `clients_mu`, `clients_cv`) in `main()` ([gateway/src/main.cpp](file:///workspaces/air/gateway/src/main.cpp)) so `SIGTERM` shutdown waits up to 5 seconds for detached client threads to drain before destroying `pools`, `discovery`, `metrics`, `tls_ctx`, and `cfg`.
3. **Existing E2E Test Failures**: Investigated and resolved both pre-existing E2E failures (`TestPageserverCAU_LivePG17_LifecycleAndRestart` and `TestPhaseH_CooperativeScaleToZero_DrainVetoesAndWakeStorm`), both of which were test bugs rather than server/Pageserver regressions.

---

## 2. Gateway Runtime Packaging Investigation & Fix

### 2.1 Root Cause Analysis

1. **Unconditional Conda RPATH in [gateway/Makefile](file:///workspaces/air/gateway/Makefile)**:
   - `RPATH_FLAGS := -Wl,-rpath,/opt/conda/lib` was hardcoded unconditionally, causing binaries built on conda-equipped development hosts to embed `/opt/conda/lib/libgrpc++.so.1.83` and `/opt/conda/lib/libssl.so.3` (requiring `OPENSSL_3.3.0`).
2. **Missing gRPC/Protobuf Build Dependencies & Stale Generated Proto Sources in `cpp-builder`**:
   - [Dockerfile](file:///workspaces/air/Dockerfile) Stage 1b (`cpp-builder`) installed `g++ make pkg-config libpq-dev libcurl4-openssl-dev libssl-dev`, omitting `libgrpc++-dev`, `libprotobuf-dev`, `protobuf-compiler`, and `protobuf-compiler-grpc`.
   - In addition, `gateway/src/pb/airdb.pb.h` was pre-generated against conda's newer protobuf (`google/protobuf/runtime_version.h`), which fails to compile against Debian Bookworm's `libprotobuf-dev` (3.21).
3. **Missing Runtime Shared Libraries in Stage 3**:
   - [Dockerfile](file:///workspaces/air/Dockerfile) Stage 3 (`debian:bookworm-slim`) omitted `libgrpc++1.51` and `libprotobuf32` and lacked an `ldd` sanity check on `/usr/local/bin/air-gateway`.
4. **Entrypoint Supervision ([scripts/air-entrypoint.sh](file:///workspaces/air/scripts/air-entrypoint.sh))**:
   - Inspected and confirmed already fixed: `scripts/air-entrypoint.sh` starts `air-server` and `air-gateway` with `trap` forwarding `TERM INT` and supervises both via `wait -n "$SERVER_PID" "$GATEWAY_PID"`.

### 2.2 Fixes Implemented

- **[gateway/Makefile](file:///workspaces/air/gateway/Makefile)**:
  - Guarded `RPATH_FLAGS` with `ifneq ($(wildcard /opt/conda/lib),)` so `-Wl,-rpath,/opt/conda/lib` is only used when `/opt/conda/lib` exists.
  - Added `src/admin_grpc.hpp` to `HEADERS`.
- **[Dockerfile](file:///workspaces/air/Dockerfile)**:
  - Added `libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc` to `cpp-builder`, copied `proto/airdb.proto`, and regenerated `src/pb/*` before `make all`.
  - Added `libgrpc++1.51 libprotobuf32` to Stage 3 and added `RUN ldd /usr/local/bin/air-gateway && (! ldd /usr/local/bin/air-gateway | grep -q "not found")`.

### 2.3 Clean Container Verification

- `docker build --target cpp-builder -t air-gw-build-test .` succeeded cleanly.
- `make test` inside `air-gw-build-test` passed (`All C++ air-gateway tests passed!`).
- `ldd /out-air-gateway` inside `air-gw-build-test` resolved all shared libraries (`libpq.so.5`, `libgrpc++.so.1.51`, `libgrpc.so.29`, `libprotobuf.so.32`, `libssl.so.3`, `libcrypto.so.3`, `libabsl_*.so.20220623`) with zero `not found`.
- Clean `debian:bookworm-slim` runtime container (`air-gw-runtime-test`) started `/usr/local/bin/air-gateway`, listened on `127.0.0.1:5432` and `127.0.0.1:8081`, served `GET /healthz` -> `{"status":"ok"}`, and exited cleanly with code `0` on `SIGTERM`.

---

## 3. Gateway Architecture Audit (`gateway/src/`)

| Area | Current Design | Findings & Recommendations |
| :--- | :--- | :--- |
| **PG Wire Protocol** ([pg_wire.hpp](file:///workspaces/air/gateway/src/pg_wire.hpp)) | Custom framing (`read_startup`, `read_packet`, `read_some`, `build_error_response`, OpenSSL `TLSContext`). | Clean framing helpers. Fixed 1-byte TLS read + shared buffer race in `proxy_bidirectional` ([main.cpp](file:///workspaces/air/gateway/src/main.cpp)). |
| **Client Lifecycle & Shutdown** ([main.cpp](file:///workspaces/air/gateway/src/main.cpp)) | `accept()` loop spawns `std::thread(...).detach()` per client; `SIGTERM` closes `g_listen_fd`. | **Fixed in Phase 14**: `main()` now waits up to 5s via `active_clients` + `clients_cv` before destroying `pools`/`discovery`/`metrics`. `AdminHTTPServer` still detaches short-lived HTTP handler threads. |
| **Discovery & Routing** ([discovery.hpp](file:///workspaces/air/gateway/src/discovery.hpp)) | gRPC `ListDatabases` + `WatchTenants` stream + fallback metadata PG polling + `TouchTenant` batching + `WakeTenant`. | Good multi-path resilience. Uses a shared `std::mutex` and separate background threads; keep `touch_loop` and `watch_tenant_stream` bounded on shutdown. |
| **Authentication & Isolation** ([pooler.hpp](file:///workspaces/air/gateway/src/pooler.hpp)) | Client auth uses `AuthenticationCleartextPassword` (`build_auth_cleartext`); backend auth verifies credentials against tenant metadata/role and sets `TenantPool` per tenant. | **Recommendation**: Upgrade client-side auth to `SCRAM-SHA-256` when `require_tls` is disabled so credentials are never sent in cleartext over non-TLS links. |
| **Connection Pooling & SQL Rewriting** ([pooler.hpp](file:///workspaces/air/gateway/src/pooler.hpp)) | Per-tenant transaction/session pool (`TenantPool`) with `PoolManager` registry; rewrites `SET ROLE`/`CREATE ROLE` etc. using `std::regex_match` in `sqlparse`. | **Scalability/Correctness Note**: `std::regex_match` runs on every `Q` packet and does not tokenize multi-statement `Q` packets; consider replacing with a lightweight SQL lexer/prefix scanner in a future phase. |
| **Metrics & Concurrency** ([metrics.hpp](file:///workspaces/air/gateway/src/metrics.hpp)) | `MetricsRegistry` tracks global/tenant/user connection counts and `LatencyHistogram` under a single `std::mutex mu_`. | **Scalability Note**: Global `mu_` is acquired on every query and connection open/close; shard per tenant or use atomics for hot counters under high QPS. |
| **Build Layout** ([Makefile](file:///workspaces/air/gateway/Makefile)) | Header-only `.hpp` modules compiled into a single translation unit (`src/main.cpp`). | Works reliably now; splitting `.hpp` implementations into `.cpp` units would speed up incremental C++ builds as the gateway grows. |

---

## 4. E2E Failure Investigations & Fixes

### 4.1 `TestPageserverCAU_LivePG17_LifecycleAndRestart` ([tests/e2e/pageserver_cau_test.go](file:///workspaces/air/tests/e2e/pageserver_cau_test.go))
- **Classification**: Test bug (stale Phase 12 path expectation).
- **Root Cause**: Phase 12 introduced generation-scoped Pageserver R2 keys (`pageserver.LayerManifestR2KeyGen(tenantID, generation, epoch)`), but the test still checked the legacy un-generationed `pageserver.LayerManifestR2Key(tenantID, resp.FencingEpoch)` and called `ps2.EnsureTenant` without `ps2.SetTenantGeneration(tenantID, resp.Generation)`.
- **Fix**: Updated the R2 manifest key check to `pageserver.LayerManifestR2KeyGen(tenantID, resp.Generation, resp.FencingEpoch)` and added `ps2.SetTenantGeneration(tenantID, resp.Generation)` before restarting `ps2`.

### 4.2 `TestPhaseH_CooperativeScaleToZero_DrainVetoesAndWakeStorm` ([tests/e2e/phase_h_test.go](file:///workspaces/air/tests/e2e/phase_h_test.go))
- **Classification**: Test bug (session left `idle` instead of `idle in transaction`).
- **Root Cause**: `cell.Agent.CheckTenantDrainSafety` ([internal/cell/agent.go](file:///workspaces/air/internal/cell/agent.go)) vetoes drain only when `pg_stat_activity` shows `state <> 'idle'` (active queries or open transactions). In the test, connection `c1` committed `CREATE TABLE` + `INSERT` and remained `idle`, so `SuspendTenant` correctly succeeded instead of returning `409 Conflict`.
- **Fix**: Opened a transaction (`BEGIN`) on `c1` before `cp.SuspendTenant` (making `c1` `idle in transaction`) and closed it with `ROLLBACK` before `c1.Close(ctx)`.

---

## 5. Verification Summary

- **Gateway Docker Build (`cpp-builder`)**: `PASS`
- **Gateway C++ Unit Tests (`make test` in `air-gw-build-test`)**: `PASS` (`All C++ air-gateway tests passed!`)
- **Gateway Clean Runtime Container (`air-gw-runtime-test`)**: `PASS` (`ldd` clean, `:5432`/`:8081` bound, `GET /healthz` -> `{"status":"ok"}`, `SIGTERM` exit `0`)
- **Go E2E Test Suite (`go test -v -count=1 ./tests/e2e/...`)**: `PASS` (`58.660s` — 11/11 non-skipped tests passed; `TestPageserverCAU_RealR2_LivePG17_CreateDBAndInsert` skipped because R2 credentials are not set)
