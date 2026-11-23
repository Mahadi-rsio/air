# AIR Phase 21.2 — Cell Agent & Pageserver Configuration Integration Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Modified Files:**
- `cmd/air-server/main.go`
- `cmd/air-server/pageserver_test.go` (new)
- `internal/control/pageserver_client.go`
- `internal/control/pageserver_grpc_test.go`

---

## 1. Root Cause Identified in Audit

1. `cmd/air-server/main.go` never called `ag.SetPageserverConfig(enabled, socketPath, lfcMaxMB)`, even when `AIR_PAGESERVER_ENABLED=true` or `AIR_PAGESERVER_GRPC_ADDR` was configured.
2. `air_smgr.so` communicates exclusively over a local Unix domain socket (`air_smgr.pageserver_socket`) using the binary `AIRP` wire protocol, whereas distributed deployments (`docker-compose.yml`, `deploy/fly/`) run `air-pageserver` as a separate service over gRPC (`AIR_PAGESERVER_GRPC_ADDR`).
3. `GRPCPageserverClient` lacked startup health/auth validation (`WaitForHealthy`), transient gRPC retry handling, and `pageserver.Service` methods needed to bridge a local `pageserver.UnixServer` to a remote `air-pageserver`.

---

## 2. Implemented Configuration & Lifecycle Flow

### 2.1 `cmd/air-server/main.go` (`configurePageserver`)
- **Remote Pageserver mode (`cfg.PageserverGRPCAddr != ""`):**
  - Dials `GRPCPageserverClient(cfg.PageserverGRPCAddr, cfg.InternalToken)`.
  - Calls `WaitForHealthy(ctx, 15*time.Second)` at startup. Authentication/config errors (`Unauthenticated`, `PermissionDenied`, `InvalidArgument`) fail immediately without waiting for timeout; unreachable Pageserver fails after timeout rather than silently ignoring the error.
  - Registers `cp.SetPageserverClient(psClient)`.
  - When `cfg.PageserverEnabled` (`AIR_PAGESERVER_ENABLED=true`) is set:
    - Validates `cfg.PageserverSocket != ""` and `ag.HasAirSmgr()` (`air_smgr.so` present alongside `pg_bin_dir`), rolling back and failing fast if missing.
    - Calls `ag.SetPageserverConfig(true, cfg.PageserverSocket, cfg.PageserverCacheMB)`.
    - Starts `pageserver.NewUnixServer(cfg.PageserverSocket, psClient)` to bridge local `air_smgr.so` `AIRP` Unix-socket RPCs to the remote `air-pageserver` over gRPC.
- **In-process Pageserver mode (`cfg.PageserverGRPCAddr == "" && cfg.PageserverEnabled`):**
  - Validates `cfg.PageserverSocket != ""` and `ag.HasAirSmgr()`.
  - Instantiates and starts `pageserver.Pageserver` listening on `cfg.PageserverSocket`, wires `cp.SetPageserver(ps)`, and calls `ag.SetPageserverConfig(true, cfg.PageserverSocket, cfg.PageserverCacheMB)`.
- **Graceful shutdown:**
  - `cp.ShutdownActiveTenants(shutCtx)` stops active PostgreSQL 17 instances first, then closes the Unix socket bridge (`unixSrv.Close()`), flushes Pageserver (`FlushAll`), and closes the client/server cleanly.

### 2.2 `internal/control/pageserver_client.go`
- Added `Health(ctx)` and `WaitForHealthy(ctx, timeout)` with fast-fail on auth errors and exponential backoff on transient `Unavailable` errors.
- Added `retryPageserverRPC` for transient gRPC errors (`Unavailable`, `ResourceExhausted`, `Aborted`).
- Implemented `pageserver.Service` (`GetPageAtLSN`, `GetPageBatch`, `GetRelSize`, `GetRelExists`, `GetDBSize`, `PutPageBatch`, `UnlinkRel`) on `GRPCPageserverClient`.

---

## 3. Verification Results

Executed `go test -race -v ./cmd/air-server/...` and `go test -race -v ./internal/control -run TestGRPCPageserverClient`:

| Test | Scenario Covered | Result |
|---|---|---|
| `TestConfigurePageserver_FailureModes` | Unavailable Pageserver at startup, empty socket path, missing `air_smgr.so`, wrong `AIR_INTERNAL_TOKEN` fast-fail | **PASS** (0.32s) |
| `TestConfigurePageserver_RemoteBridgeWiresAgent` | Remote gRPC Pageserver + `SetPageserverConfig` + `UnixServer` bridge socket creation | **PASS** (0.02s) |
| `TestGRPCPageserverClient_LifecycleAndWAL` | Tenant ensure, WAL ingestion, flush, purge over gRPC with token auth | **PASS** (0.07s) |
| `TestGRPCPageserverClient_SkeletonRoundTrip` | Metadata skeleton save/restore over gRPC | **PASS** (0.03s) |
| `TestGRPCPageserverClient_ServiceBridgeAndFailureRecovery` | Auth failure, `PutPageBatch`/`GetPageBatch`/`GetPageAtLSN`/`GetRelSize`/`GetRelExists`/`UnlinkRel` over gRPC, Pageserver stop/restart, `WaitForHealthy` recovery, post-restart read | **PASS** (0.04s) |

---

## 4. Status

**Phase 21.2 Verdict: PASSED (Real Measurements).**
Cell Agent Pageserver configuration wiring, startup validation, authentication enforcement, Unix-socket-to-gRPC bridging, graceful shutdown, and restart recovery are implemented and verified.
