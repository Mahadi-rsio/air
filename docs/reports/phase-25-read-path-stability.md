# Phase 25 — Read Path Stability, Observability & Gateway Connection Lifecycle Report

**Date**: 2026-11-25  
**Target Cluster**: Fly.io (`sin` region) — `air-pg-gateway`, `air-pg-read-engine`, `air-pg-pageserver` (all `shared-cpu-1x`, `256 MB` RAM)  
**Final Decision**: **PASS WITH LIMITATIONS**

---

## 1. Executive Summary

Prior to Phase 25, the 128-worker stress test failed at minute 3 (`sec=122`): throughput collapsed from `317.5 QPS` to `35.5 QPS` (`-89%`), P95 rose to `10.3–12.8s`, `29` `SELECT` queries hit the 15s `CTX_TIMEOUT`, and `80` replacement connections were rejected with `SQLSTATE 53300` (tenant connection limit exceeded).

Phase 25 delivered three major changes:
1. **Prometheus `/metrics` Observability** across `air-pg-gateway`, `air-pg-read-engine`, and `air-pg-pageserver`, documented in [docs/metrics.md](../metrics.md).
2. **Gateway Client-Disconnect & Connection Accounting Fix**: safe `MSG_PEEK | MSG_DONTWAIT` socket disconnect polling (`25ms` interval, zero pipelined byte consumption), context cancellation propagation, dirty backend discard, and `sync.Once`-guarded `ReleaseTenantUser()`.
3. **Root Cause Diagnosis & Read-Path CPU/RPC Optimization**:
   - **Confirmed Root Cause**: The minute-3 stall was **not** caused by Pageserver I/O, layer lookup, or WAL redo. Live 2s `/metrics` scraping and `/proc/stat` proved that `air-pg-read-engine` and `air-pg-gateway` (`shared-cpu-1x`) exhausted their hypervisor CPU burst credits around `t=110s–118s`, triggering severe CPU steal (`122.82s` steal on Read Engine in the baseline diagnostic run) and ~10x execution slowdown.
   - **Software Drivers Fixed**:
     - Disabled `AIR_READ_ENGINE_DEBUG` and `AIR_PAGESERVER_DEBUG` in Fly configs (`deploy/fly/fly.read-engine.toml`, `deploy/fly/fly.pageserver.toml`), eliminating ~1,500+ `log.Printf` lines/sec on the hot path.
     - Added `singleflight.Group` coalescing in `PageserverGRPCBackend.GetTenantInfo` (`internal/readadapter/pageserver_grpc_backend.go`) with detached request timeout, `x-air-internal-token` metadata forwarding, and zero post-RPC caching.
     - Reduced `watchClientDisconnect` poll overhead in `internal/gateway/disconnect.go` from `10ms` to `25ms` (`2.5x` fewer `recvfrom` syscalls while preserving `< 100ms` slot release).

**Validation Outcome**:
- Across the 4-minute 128-worker validation (`71,325` queries) and three 15-minute benchmarks at `64`, `96`, and `128` workers (`225,006` queries over 45 minutes) — **`296,331` total queries** — the cluster achieved **`0` errors (`0.000%`)**, **`0` `SQLSTATE 53300` rejections**, **`0` Read Engine fallbacks**, **`0` circuit-breaker trips**, and **`0` Pageserver RPC errors**.
- **Limitation**: On Fly.io `shared-cpu-1x` VMs, once the ~2-minute CPU burst credits drain under sustained saturation, hypervisor CPU throttling caps steady-state throughput at `~75–91` avg QPS (`p99 = 3.15s–5.82s` over 15m), exceeding the `< 1.5s` P99 target on `shared-cpu-1x`. All software bugs, timeouts, and connection leaks are resolved.

---

## 2. Minute-3 Stall Diagnosis (2s `/metrics` & `/proc/stat` Evidence)

### Baseline Diagnostic Run (`128` workers, 4m, pre-optimization)

| Metric | Fast Phase (`20s–106s`) | Stalled Phase (`120s–240s`) | Ratio |
|---|---:|---:|---:|
| Gateway `read_engine_latency_seconds` avg | `452.70 ms` (`n=19,728`) | `6,869.44 ms` (`n=2,194`) | `15.2x` |
| Read Engine `scheduler_wait_seconds` avg | `410.63 ms` (`n=19,722`) | `6,414.53 ms` (`n=2,190`) | `15.6x` |
| Read Engine `pageserver_rpc_latency_seconds` avg | `3.345 ms` (`n=89,814`) | `35.608 ms` (`n=11,669`) | `10.6x` |
| Read Engine `cache_sync_latency_seconds` avg | `2.399 ms` (`n=3,676`) | `30.965 ms` (`n=510`) | `12.9x` |
| Pageserver `getpage_total_seconds` avg | `0.885 ms` (`n=36,160`) | `1.352 ms` (`n=4,712`) | `1.5x` |
| Pageserver `getpage_storage_io_seconds` avg | `0.126 ms` (`n=544`) | `0.080 ms` (`n=30`) | `0.6x` |
| Pageserver `read_limiter_wait_seconds` avg | `0.000 ms` (`n=0`) | `0.000 ms` (`n=0`) | `—` |
| Pageserver `index_cache` / `decoded_chunk_cache` hit | `99.99%` / `99.98%` | `99.99%` / `99.98%` | `—` |

**Timeline of Collapse (`t=108s–120s`)**:
- `t=108s`: `gw_req=22,628`, `re_q=99`, `re_act=4`, `rpc=102,082`, `ps_gp=40,836`.
- `t=110s`: Read Engine `shared-cpu-1x` burst credits exhausted (`/proc/stat` recorded `steal = 122.82s` vs `user+sys = 57.60s`).
- `t=112s–130s`: Read Engine throughput dropped ~10x (`~200 QPS` → `~18 QPS`), `re_q` pinned at `115–122`, and Gateway `read_engine_in_flight` pinned at `125–127/128`.

### Post-Optimization Comparison (`128` workers, 4m)

| Window | Baseline Diag | Post-Opt (`opt3`) | Change |
|---|---:|---:|---:|
| Minute 1 QPS | `423.4` | `589.5` | `+39.2%` |
| Minute 2 QPS | `271.7` | `378.1` | `+39.2%` |
| Minute 3 QPS | `36.4` | `121.9` | `3.35x` |
| Minute 4 QPS | `34.7` | `97.1` | `2.80x` |
| Overall Avg QPS | `191.5` | `295.71` | `+54.4%` |
| Pageserver RPCs / Query (`FAST`) | `4.55` | `2.58` | `-43.3%` |
| Total Errors | `0` (post-disconnect fix) | `0` (`0.000%`) | `0` |

In `opt3`, `/proc/stat` still showed hypervisor CPU throttling after burst exhaustion (`steal=81.77s` on `air-pg-read-engine`, `+79.88s` on `air-pg-gateway`, `19.69s` on `air-pg-pageserver`), confirming `shared-cpu-1x` baseline CPU is the remaining hardware ceiling.

---

## 3. Gateway Client-Disconnect & Connection Accounting Fix

**Root Cause of the 80 `SQLSTATE 53300` Rejections**:  
When clients timed out (`15s` `CTX_TIMEOUT`) and disconnected while Gateway was blocked waiting on Read Engine, Gateway continued waiting for the backend query to finish, holding the tenant slot in `TenantActiveConnections` while `pgxpool` opened replacement connections. Old and new connections overlapped, exhausting the tenant connection limit.

**Implemented Fix**:
- `internal/gateway/disconnect.go`: `watchClientDisconnect` polls `MSG_PEEK | MSG_DONTWAIT` on the raw socket every `25ms`, cancelling the query context immediately on `EOF`/`ECONNRESET` without consuming pipelined PG wire bytes, and stops cleanly via `doneCh` (zero goroutine leaks).
- `internal/gateway/client_session.go` & `internal/gateway/pooler.go`: `ReleaseTenantUser()` is guarded by `sync.Once` to prevent double-release/underflow; dirty backend connections are discarded on cancellation across Simple and Extended Query paths.

**Regression Tests** (`internal/gateway/disconnect_test.go`):
- `TestPhase25_ClientDisconnect_ReleasesTenantSlotWithin100ms` (`MaxTenantClientConns = 2`, stalled mock Read Engine, client disconnect → replacement connection succeeds within `< 100ms`, `0` `53300` errors): **PASS (`0.07s`)**.
- `TestPhase25_PipelinedBytesNotConsumedByDisconnectWatcher`: **PASS (`0.00s`)**.

---

## 4. 15-Minute Validation Benchmarks (`shared-cpu-1x`)

| Benchmark | Duration | Total OK | Errors | Avg QPS | Peak 2s QPS | p50 | p95 | p99 | `53300` Rej | Fallbacks / Trips |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **64 workers** | `15m1.14s` | `75,043` | `0 (0.000%)` | `83.28` | `801.50` | `138.39ms` | `2.556s` | `3.146s` | `0` | `0` / `0` |
| **96 workers** | `15m0.89s` | `68,317` | `0 (0.000%)` | `75.83` | `324.50` | `640.52ms` | `4.082s` | `4.745s` | `0` | `0` / `0` |
| **128 workers** | `15m1.74s` | `81,646` | `0 (0.000%)` | `90.54` | `337.00` | `878.60ms` | `4.787s` | `5.824s` | `0` | `0` / `0` |

**Scraped Internal Metrics Across 15-Minute Runs**:
- **64 workers**: `39,007` Read Engine requests, `92,540` Pageserver RPCs (`0` errors, avg `19.58ms`), `44,801` `GetPage` (`2.21ms` avg), `0` disconnects/cancellations, Read Engine heap `56.6 MB`.
- **96 workers**: `35,764` Read Engine requests, `85,038` Pageserver RPCs (`0` errors, avg `22.02ms`), `44,211` `GetPage` (`2.00ms` avg), `0` disconnects/cancellations, Read Engine heap `34.8 MB`.
- **128 workers**: `43,622` Read Engine requests, `76,085` Pageserver RPCs (`0` errors, avg `21.30ms`), `36,113` `GetPage` (`3.27ms` avg), `0` disconnects/cancellations, Read Engine heap `51.9 MB`.

---

## 5. Pass Criteria Evaluation

| Criterion | Target | Observed | Status |
|---|---|---|---|
| `53300` connection rejections | `0` | `0` across all runs | **PASS** |
| Disconnect slot leaks | `0` | `0` (`< 100ms` release verified) | **PASS** |
| Error rate | `< 0.05%` | `0.000%` (`0` errors in `296,331` queries) | **PASS** |
| Bounded queues & memory | No unbounded growth | Heap `34.8–56.6 MB`, `0` limiter waits | **PASS** |
| No >20% throughput collapse | `<= 20%` drop | `shared-cpu-1x` burst drain causes drop after ~2m | **LIMITATION (VM tier)** |
| P99 latency | `< 1.5s` | `3.15s–5.82s` under `shared-cpu-1x` throttling | **LIMITATION (VM tier)** |

**Recommendation**: For sustained >300 QPS workloads without hypervisor burst-credit throttling, deploy `air-pg-read-engine` and `air-pg-gateway` on dedicated CPU (`performance-1x` or `shared-cpu-2x+`) rather than `shared-cpu-1x`.
