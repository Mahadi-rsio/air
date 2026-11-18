# AIR Phase 14A — Gateway Completion, Protocol Correctness & Runtime Reliability Report

## 1. Scope & Status Summary

| Area | Status | Notes |
| --- | --- | --- |
| PG Wire Protocol (Startup, SSL/GSS, Cancel, Simple/Extended Query, Flush, COPY IN) | **IMPLEMENTED AND VERIFIED** | Verified via C++ unit tests (`gateway_test`) and live `psql` + `pgx v5` E2E tests |
| Auth, Constant-Time Verification & Fail-Closed | **IMPLEMENTED AND VERIFIED** | Empty/missing `app_password` rejected with `FATAL 28000`; `CRYPTO_memcmp` comparison |
| Tenant Routing, Isolation & gRPC Reliability | **IMPLEMENTED AND VERIFIED** | Cross-tenant isolation, `NOT_FOUND` -> `3D000`, `UNAVAILABLE` -> `57P03`, keepalive + reconnect |
| Connection Pooling, Idle Reaper & Timeouts | **IMPLEMENTED AND VERIFIED** | `auth_timeout_ms`, `client_idle_timeout_ms`, `pool_idle_timeout_sec`, `CancelRegistry` |
| Performance (`TCP_NODELAY`) | **IMPLEMENTED AND VERIFIED** | Warm connect+SELECT p50 improved from 123.02ms to 961.37µs; 16-worker QPS 371.9 -> 5164.7 |
| Docker Runtime & `ldd` Dependency Verification | **IMPLEMENTED AND VERIFIED** | Clean `docker build`, zero missing libs (`libgrpc++1.51`), `psql` & `SIGTERM` verified |
| Observability (`/internal/v1/metrics`) | **IMPLEMENTED AND VERIFIED** | Added `auth_failures`, `protocol_errors`, `rpc_errors`, `timeouts`, `pool_exhausted` |

---

## 2. Defects Identified & Fixes Implemented

### 2.1 PostgreSQL Wire Protocol (`gateway/src/pg_wire.hpp`, `gateway/src/pooler.hpp`, `gateway/src/main.cpp`)
1. **Startup & Pre-Startup Negotiation**:
   - Added handling for `GSSENCRequest` (`80877104` -> `'N'`) alongside `SSLRequest` (`80877103` -> `'S'`/`'N'`).
   - Implemented `CancelRequest` (`80877102`) routing via `CancelRegistry` (`(gw_pid, gw_secret) -> CancelTarget`) forwarding real PostgreSQL cancel packets to the active backend.
   - Enforced `kPGProtocolVersion3` (`196608`) with `FATAL 0A000` on unsupported protocol versions.
   - Enforced `kMaxPGPacketBytes` (32 MiB) packet size limit to prevent unbounded client-controlled allocations.
2. **Authentication & Fail-Closed**:
   - Fixed `serve_client` to fail closed (`FATAL 28000`) when `expected_pass` is empty rather than bypassing password verification.
   - Replaced string equality with `constant_time_equals` (`CRYPTO_memcmp`).
   - Enforced `req_user == "app_user"` in `pool_mode` (rejecting superuser `postgres` attempts).
   - Fixed `parse_sni_tenant` to ignore single-label hostnames (`localhost`) and IPv4 literals so TLS connections to `localhost` with `dbname=<tenant>` succeed.
3. **Simple & Extended Query Protocol**:
   - Fixed Simple Query `DEALLOCATE` to forward to the backend (returning real `26000` on missing statements and preserving transaction state) instead of faking success; added `RESET` session variable tracking.
   - Added Extended Query `Flush ('H')` support (`relay_flush_batch`) so pipelined `Parse` + `Describe` + `Flush` sequences do not deadlock waiting for `Sync`.
   - Added `COPY FROM STDIN` (`'G'` CopyInResponse -> `'d'`/`'c'`/`'f'` relay) support (`relay_copy_in`), verified via `psql \copy` / `COPY ... FROM STDIN`.
   - Added SQL `PREPARE` / `EXECUTE` / `DEALLOCATE` rewriting in Extended Query `Parse ('P')` (required by `pgx v5`, which sends SQL `PREPARE` via Extended Query `Parse`).
   - Bounded Extended Query batching (`kMaxExtBatchMsgs = 4096`, `kMaxExtBatchBytes = 32 MiB`), added `undo_parses` rollback on backend error, and rejected unknown frontend message types with `ERROR 08P01` + `ReadyForQuery`.
   - Tracked `backend_in_flight` so abrupt client disconnects mid-query/mid-transaction discard dirty backend connections rather than returning them to the pool.

### 2.2 Tenant Routing & gRPC Reliability (`gateway/src/discovery.hpp`, `gateway/src/main.cpp`)
- Added gRPC HTTP/2 keepalive channel arguments (`GRPC_ARG_KEEPALIVE_TIME_MS=30000`, `GRPC_ARG_KEEPALIVE_TIMEOUT_MS=5000`).
- Propagated `grpc::StatusCode` to PostgreSQL SQLSTATE mapping: `NOT_FOUND` -> `3D000` (with stale cache eviction) and `UNAVAILABLE` / `DEADLINE_EXCEEDED` -> `57P03`.
- Fixed startup race when `air-gateway` boots a few seconds before `air-server`: `resolve_tenant` now performs a 1-shot `channel_->WaitForConnected(500ms)` + retry when `GetTenantRoute` fails with `UNAVAILABLE` due to `TRANSIENT_FAILURE` backoff.

### 2.3 Timeouts, Pool Reaper & Admin HTTP (`gateway/src/config.hpp`, `gateway/src/pooler.hpp`, `gateway/src/admin_http.hpp`)
- Added `AIR_AUTH_TIMEOUT_MS` (default `5000`), `AIR_CLIENT_IDLE_TIMEOUT_MS` (default `0`), and `AIR_POOL_IDLE_TIMEOUT_SEC` (default `300`) with a 30s background idle-backend reaper cleanly joined on shutdown.
- Transitioned client read timeout from `auth_timeout_ms` to `client_idle_timeout_ms` after authentication completes (`ReadyForQuery('I')`).
- Capped concurrent `admin_http` worker threads at 64 (returning `503` when saturated) and added graceful drain (`http_cv_.wait_for` 3s) in `stop()`.
- Fixed `proxy_bidirectional` `poll()` error/`POLLNVAL` handling and cache eviction on dial failure in non-pool mode.

### 2.4 Performance (`TCP_NODELAY`)
- Baseline E2E measurement showed `query_p50 = 41.01ms` on localhost due to Linux 40ms delayed-ACK / Nagle stalls across client and backend TCP sockets.
- Added `set_tcp_nodelay(int fd)` (`IPPROTO_TCP`, `TCP_NODELAY`) in `gateway/src/pg_wire.hpp` and applied it to accepted client sockets (`main.cpp`) and dialed backend TCP sockets (`dial_backend_socket`):
  - **Warm connect + `SELECT 1` + close p50**: `123.02ms` -> **`961.37µs`** (p95: `124.98ms` -> `2.61ms`)
  - **16-worker concurrent `SELECT 1` throughput**: `371.9 QPS` -> **`5164.7 QPS`** (query p50: `41.01ms` -> `2.38ms`, p95: `42.09ms` -> `7.14ms`)
  - **Gateway footprint under load**: `fds = 20`, `VmRSS = 29224 kB`

### 2.5 Docker & Runtime Packaging (`Dockerfile`)
- Inspected the runtime image: `Dockerfile` links against Debian Bookworm `libgrpc++1.51` (`libgrpc++.so.1.51`), not conda's `1.83`. `ldd /usr/local/bin/air-gateway` inside the final image confirmed `x86_64` with zero `not found` shared libraries.
- Fixed a Docker build failure in `pg-builder` where stale host `.o` files (`extensions/air_smgr/communicator.o` built with GCC 13 LTO 13.1) caused an LTO version mismatch against Debian Bookworm's GCC 12 (`lto1: fatal error: bytecode stream in file 'communicator.o' generated with LTO version 13.1 instead of the expected 12.0`) by running `make clean` in `Dockerfile` before building `air_smgr`.

---

## 3. Verification Results

1. **C++ Unit & Protocol Regression Tests**:
   - `make -C gateway all test` -> `All C++ air-gateway tests passed!` (exit code 0)
2. **C++ ASan + UBSan Sanitizer Run**:
   - Compiled `gateway/tests/gateway_test.cpp` with `-fsanitize=address,undefined -O1 -g -DNDEBUG` and ran with `ASAN_OPTIONS=detect_leaks=0` -> `All C++ air-gateway tests passed!` (exit code 0, zero ASan/UBSan errors)
3. **Live Gateway E2E Protocol, Isolation & Baseline Test**:
   - `go test -v -count=1 -run TestGateway_Phase14A_EndToEndProtocolAndIsolation ./tests/e2e/...` -> `PASS` (`3.95s`)
   - Covers `psql` (plain + `sslmode=require` + `COPY FROM STDIN`), `pgx v5` (CRUD, rollback, `PREPARE`/`EXECUTE`/`DEALLOCATE`), 16-goroutine concurrent multi-tenant isolation, invalid/cross-tenant credentials (`28P01`), superuser rejection, unknown tenant (`3D000`), abrupt disconnect mid-transaction, suspended tenant auto-wake, `/internal/v1/metrics`, Control Plane unavailability, and `SIGTERM` shutdown.
4. **Full E2E Suite**:
   - `go test -count=1 ./tests/e2e/...` -> `ok github.com/Mahadi-rsio/air/tests/e2e 57.408s` (exit code 0)
5. **Docker Build & Runtime Entrypoint Verification**:
   - `docker build -t air-phase14a-test .` -> succeeded (`40/40`)
   - `docker run --rm --entrypoint sh air-phase14a-test -c 'uname -m && ldd /usr/local/bin/air-gateway'` -> `x86_64`, `libgrpc++.so.1.51 => /lib/x86_64-linux-gnu/libgrpc++.so.1.51`, zero missing libraries
   - Full container run with `/usr/local/bin/air-entrypoint.sh`: `air-server:8080/healthz` & `air-gateway:8081/healthz` ready in 6s -> provisioned `docker_tenant` -> `psql -h 127.0.0.1 -p 5432 -U app_user -d docker_tenant -tAc "SELECT current_database(), 1+1;"` returned `docker_tenant|2` -> `/internal/v1/metrics` recorded `query_count: 1` -> `docker stop -t 15` logged `[air-gateway] shutting down...` cleanly.

---

## 4. Known Limitations
- **gRPC TLS**: Gateway-to-Control-Plane gRPC currently uses insecure loopback/VPC channel (`grpc::InsecureChannelCredentials()`).
- **Non-pool mode**: `pool_mode=false` uses a direct bidirectional TCP splice without wire-level statement inspection.
- **R2 cold-offload E2E**: R2 credentialed tests are skipped in environments without R2 credentials.
