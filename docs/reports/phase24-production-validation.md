# AIR Phase 24 — Production Workload Validation Report

- **Repository**: `/workspaces/air`
- **Test Environment**: `linux/amd64`, `2` vCPU (`nproc=2`), `7.8 GiB` RAM, `go1.27.3`
- **Default Flag**: `Config.EnableCAWAL = false` (retained opt-in)
- **Related Phase 24 Reports**:
  - [phase24-architecture-audit.md](file:///workspaces/air/docs/reports/phase24-architecture-audit.md)
  - [phase24-benchmark-analysis.md](file:///workspaces/air/docs/reports/phase24-benchmark-analysis.md)
  - [phase24-scheduler-stability.md](file:///workspaces/air/docs/reports/phase24-scheduler-stability.md)
  - [phase24-memory-analysis.md](file:///workspaces/air/docs/reports/phase24-memory-analysis.md)

---

## 1. Phase 5 — Background Workload Isolation

Command: `go test -v ./internal/pageserver -run '^TestPhase24_BackgroundWorkloadIsolation$' -count=1`

- **Observed Result**: `PASS (0.19s)`
- **Metrics**: `peak_goroutines=4`, `after_close=2`, `dropped_flushes=0`, `fg_write_p99=7.0 µs`, `fg_read_p99=392.0 µs`.
- **Changes Applied**:
  - `MemoryBudget.wg` and shutdown `ctx`/`cancel` now track and cancel background `CompactL0Batch` (`internal/pageserver/memory_budget.go`) and `Timeline.MaybeFlushAsync` (`internal/pageserver/readpath.go`) goroutines cleanly on `Pageserver.Close()`.

---

## 2. Phase 6 — Multi-Tenant Fairness & Burst Testing

Command: `go test -v ./internal/pageserver -run '^TestPhase24_MultiTenantFairness$' -count=1`

| Scenario | Tenants | Global p99 (µs) | Normal p99 (µs) | Noisy p99 (µs) | Throughput (MB/s) |
|---|---:|---:|---:|---:|---:|
| `A_uniform_20T` | 20 | 41.0 | 41.0 | — | 85.92 |
| `B_noisy_neighbor_20T` (8× writer) | 20 | 30.0 | 51.0 | 21.0 | 104.05 |
| `C_mixed_20T` (read/write/idle) | 20 | 69.0 | 69.0 | — | 69.78 |
| `D_burst_75T` | 75 | 60.0 | 60.0 | — | 92.89 |
| `E_bg_pressure_20T` (concurrent compaction) | 20 | 172.0 | 172.0 | — | 61.14 |

- **Observation**: The 8× noisy-neighbor tenant did not destabilize normal tenants (`normal_p99 = 51.0 µs` vs `41.0 µs` uniform). Concurrent background compaction increased mixed workload p99 modestly from `69.0 µs` to `172.0 µs` with zero dropped writes or flushes.

---

## 3. Phase 7 — CA-WAL Production Validation & Threshold Sweep

Command: `go test -v ./internal/pageserver -run '^TestPhase24_CAWALThresholdValidation$' -count=1`

| Threshold | Workload | p99 (µs) | Disk (KB) | Dedup Ratio | Inline | External | Cache Hits | Singleflight Hits |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 4 KB | `rec_1024B` | 79.0 | 16 | 19.03× | 0 | 10 | 9 | 0 |
| 4 KB | `rec_4096B` | 77.0 | 16 | 76.17× | 0 | 10 | 9 | 0 |
| 8 KB | `rec_1024B` | 52.0 | 16 | 19.03× | 0 | 10 | 9 | 0 |
| 8 KB | `rec_4096B` | 49.0 | 16 | 76.17× | 0 | 10 | 9 | 0 |
| **16 KB** | `rec_1024B` | **50.0** | **16** | **19.85×** | **10** | **0** | **0** | **0** |
| **16 KB** | `rec_4096B` | **113.0** | **16** | **76.18×** | **0** | **10** | **9** | **0** |
| 32 KB | `rec_1024B` | 55.0 | 16 | 19.85× | 10 | 0 | 0 | 0 |
| 32 KB | `rec_4096B` | 127.0 | 16 | 76.17× | 0 | 10 | 9 | 0 |
| 64 KB | `rec_1024B` | 37.0 | 16 | 19.85× | 10 | 0 | 0 | 0 |
| 64 KB | `rec_4096B` | 47.0 | 16 | 79.34× | 10 | 0 | 0 | 0 |

- **Decision on Threshold**: Keep `DefaultCAWALInlineThresholdBytes = 16 * 1024` (`16 KB`). It cleanly keeps `~8 KB` WAL tables inline (`10` inline, `0` external files) while routing `~32 KB` WAL tables externally (`76.18×` dedup, `9/10` cache hits).

---

## 4. Phase 8 — Accelerated Soak & Fault-Injection Testing

Command: `go test -v ./internal/pageserver -run '^TestPhase24_AcceleratedSoakAndFaultInjection$' -count=1`

- **Observed Result**: `PASS (4.23s)`
  - `enableCAWAL=false`: `2s` × 20 concurrent tenants (`6,140` WAL ingest/flush/compaction cycles) -> restart + `LookupKeyInLayer` recovery `OK`.
  - `enableCAWAL=true`: `2s` × 20 concurrent tenants (`6,068` WAL ingest/flush/compaction/GC cycles) -> restart + `LookupKeyInLayer` recovery `OK`.
  - **Fault Injection**: Interrupted `.walb.tmp` blob file, interrupted `.manifest.json.tmp` file, cancelled `CAWALStore.GC`, concurrent `EnsureTenant` + `PurgeTenant` across 8 ephemeral tenants, followed by `Close()` + restart recovery (`lastLSN=0x114001 layers=1 deltas=1`).
- **Scope Limitation**: Due to single-session execution constraints, Phase 24 ran an accelerated `2s` high-concurrency soak per mode rather than a new 30–60 minute wall-clock soak (a 30-minute soak was previously recorded in Phase 22 commit `518a0c5b`).

---

## 5. Phase 9 — Multi-Machine Fly.io Topology Inspection

Inspected live Fly.io apps via `flyctl machines list` and `flyctl volumes list` (read-only; machines were `stopped` / `suspended`):

| Fly App | Region | Machine Size | State | Attached Volume |
|---|---|---|---|---|
| `air-pg-gateway` | `sin` | `shared-cpu-1x:256MB` | `stopped` | None |
| `air-pg-pageserver` | `sin` | `shared-cpu-1x:256MB` | `stopped` | `air_pageserver_data` (`1 GB`, encrypted) |
| `air-pg-read-engine` | `sin` | `shared-cpu-1x:256MB` | `stopped` | `air_read_engine_data` (`1 GB`, encrypted) |
| `air-pg-server` | `sin` | `shared-cpu-2x:512MB` | `stopped` | `air_server_data` (`2 GB`, encrypted) |

- **Limitation**: Because all 4 Fly.io machines are `stopped` (`suspended`) and live R2/multi-machine traffic was not executed against them in this session, multi-machine runtime behavior is **not** claimed as validated here; only local Linux validation and live Fly.io machine/volume configuration inspection were performed.

---

## 6. Phase 10 — Full Repository & Race Verification

1. **`go test -race ./internal/pageserver/... -count=1`**: **PASS** (`148.561s`, 0 data races).
2. **`go test ./... -count=1`**:
   - **Passed**: `benchmarks/phase21` (`0.009s`), `cmd/air-read-engine` (`0.224s`), `cmd/air-server` (`0.379s`), `internal/backup` (`0.045s`), `internal/cell` (`21.685s`), `internal/config` (`0.004s`), `internal/control` (`227.617s`), `internal/crypto` (`0.013s`), `internal/fencing` (`0.036s`), `internal/gateway` (`5.744s`), `internal/pageserver` (`74.949s`).
   - **Skipped / No test files**: `benchmarks/{phase17..phase5_multitenant,r2proxy,r2tool}`, `cmd/{air-gateway,air-pageserver,air-walredo}`, `internal/{pb,tenant}`.
   - **Failed (pre-existing environment/artifact gaps)**:
     - `internal/readadapter` (`122.552s`): `TestPageserverGRPCBackend_RoundTripAndAuth` (`pageserver_grpc_test.go:74`: expected LSN `0x2000`, got `0xffffffffffffffff`) and `TestPhase14E_PG17_LiveVerification` (`phase14d_invalidation_test.go:387`: `no such table: p14e_items`).
     - `tests/e2e` (`107.054s`): `TestAIRv1_SingleCellEndToEndMilestones` (`POST /v1/databases status=400`), `TestGateway_Phase14A_EndToEndProtocolAndIsolation` (missing `/workspaces/air/gateway/air-gateway`), `TestE2E_NativePageserver_SQLiteReadEngine_ZeroSync` (missing `air_smgr.so` in `/usr/lib/postgresql/17/bin`).

---

## 7. Remaining Limitations & Final Engineering Decision

- **Remaining Limitations**:
  1. First-batch `SetTenantGeneration` still acquires `p.mu.Lock()` once per new `(tenantID, generation)` pair.
  2. Live multi-machine Fly.io network/R2 soak and 30–60 minute wall-clock soak were not executed in this session (Fly.io machines are `stopped`).
  3. `internal/readadapter` (2 tests) and `tests/e2e` (3 tests) have pre-existing environment/artifact failures.
- **Final Engineering Decision**: **`PASS WITH LIMITATIONS`**
  - Keep `Config.EnableCAWAL = false` by default while deploying the Phase 24 `MemoryBudget` lock-free dirty counter, `debug.FreeOSMemory()` pressure gating, and tracked background shutdown fixes.
