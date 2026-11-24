# AIR Phase 24 — Phase 1: Repository & Architecture Audit

- **Repository**: `/workspaces/air`
- **Base Commit**: `f47a6dfc` (`perf(pageserver): Phase 23 adaptive CA-WAL, singleflight, and RLock fast paths`)
- **Audit Scope**: Gateway (`internal/gateway/`), Pageserver (`internal/pageserver/`), Read Server (`internal/readadapter/`), and Fly.io / Docker deployment topology (`fly.toml`, `docker-compose.yml`, `deploy/fly/*`).

---

## 1. Executive Summary of Audit Findings

The Phase 22/23 architecture successfully introduced bounded caches, per-hash singleflight, 16-way sharded pins, `MemoryBudget`, `WriteGovernor`, `TenantScheduler`, and `RLock` tenant fast paths. However, auditing the current source code uncovered **five high-impact contention/lifecycle issues** that directly threaten p99 latency predictability and scheduler stability under 50–100 tenant bursts on 256–512 MB / 1–2 vCPU machines:

1. **O(N_tenants) Lock Storm on Every Write (`server.go:474-484`, `memory_budget.go:295, 323`)**: `enforceBeforeWrite()` calls `effectiveTenantLimits()`, which calls `b.ps.dirtyTenantCount()`. That acquires `p.mu.RLock()` and loops over **every** tenant acquiring `tl.mu.RLock()`. Since `IngestWALRecords` calls `EnforceInMemBudget` before, every 32 records, and after each batch, 50–100 concurrent tenants generate thousands of cross-tenant lock acquisitions per second and starve `p.mu.Lock()` (`EnsureTenant`, `SetTenantGeneration`, `PurgeTenant`).
2. **Aggressive `runtime.ReadMemStats` + `debug.FreeOSMemory()` in 200ms Loop (`memory_budget.go:440, 488-500`)**: `runMaintenanceStep()` triggers a stop-the-world `runtime.ReadMemStats` every `200ms` and invokes `debug.FreeOSMemory()` (synchronous full GC + OS scavenge) up to every `500ms` whenever `HeapIdle - HeapReleased > 12 MiB` — even when not under `highPressure`. During concurrent bursts, this injects periodic STW/GC stalls directly into foreground p99 latency.
3. **Untracked Background Goroutines Using `context.Background()` (`readpath.go:354`, `memory_budget.go:475`)**: `Timeline.MaybeFlushAsync()` and `MemoryBudget.runMaintenanceStep()` compaction spawn goroutines that are not tracked in a `WaitGroup` and use `context.Background()`, so `Pageserver.Close()` (`server.go:267-279`) cannot cancel or wait for them cleanly.
4. **Uncapped / Unbounded Per-Response & Parser Buffers (`pooler.go:1236`, `walparser.go:392`, `readpath.go:55, 61-64`, `cache.go:703`)**: `handleExtendedBatch()` buffers entire read-backend responses into `outBuf []byte`, `WALStreamParser.contBuf` never shrinks after large continuation records, `Timeline.relSizes`/`slruPages` are not evicted under continuous writes, and `TieredCacheBackend.GetPageBatch()` allocates `nblocks * 8192` bytes without `MemoryManager` reservation.
5. **Unmatched `DiskCache.EvictPrefix` on Tenant Purge (`server.go:390`, `cache.go:288-297`)**: `PurgeTenant` calls `EvictPrefix(prefix)`, but `DiskCache` keys items by `cacheKeyHash(key)` (32-char SHA-256 hex), so `strings.HasPrefix(item.key, prefix)` never matches.

---

## 2. Component-by-Component Audit

### 2.1 Gateway (`internal/gateway/`)

| Area | Mechanism & Source Location | Status / Finding |
|---|---|---|
| **Background Goroutines** | `AdaptiveController.Start()` (`adaptive_controller.go:127-145`) | Bounded: 1 ticker goroutine (`1s` default, `config.go:89`), joined in `Stop()` (`lines 147-153`). `WriteGovernor` and `BufferBudget` spawn 0 goroutines. |
| **Write Admission** | `WriteGovernor.Acquire()` (`write_governor.go:278-356`) | Bounded queue (`queueMax=256`, `queueMaxBytes=64 MiB`, `queueTimeout=5s`), Deficit Round Robin in `dispatchLocked()` (`lines 428-500`), race-safe `abortWaiter()` (`lines 358-404`). |
| **Cancellation Gap** | `TenantPool.acquireWritePermit()` (`pooler.go:446-466`) | Calls `gov.Acquire(context.Background(), ...)` (`line 454`); disconnected clients remain queued up to `queueTimeout` (`5s`). |
| **Adaptive Controller** | `AdaptiveController.Step()` (`adaptive_controller.go:157-270`) | AIMD (`hysteresisUp=3`, `hysteresisDown=2`), `15s` stale telemetry fallback (`lines 176-188`). `WriteAdaptiveEnabled` defaults to `false` (`config.go:92`). |
| **Buffer Budgets** | `BufferBudget` (`buffer_budget.go:17-182`), `ServeClient()` (`pooler.go:1464-1477`) | Global cap `64 MiB` (`config.go:90`), session cap `8 MiB` (`config.go:91`, fallback `32 MiB` at `pooler.go:1399`), `4096` messages (`pooler.go:156`). **Gap**: if `p.bufBudget == nil`, `ServeClient()` creates `NewBufferBudget(0)` (no global cap, `pooler.go:1407-1409`). |
| **Extended Batch Read Buffer** | `handleExtendedBatch()` (`pooler.go:1211-1252`) | Accumulates full read-backend response into `outBuf []byte` (`line 1236`) for primary fallback on `'E'`; large read results are unbounded in RAM. |
| **Lock Contention & Map Growth** | `MetricsRegistry.mu` (`metrics.go:194`), `WriteGovernor.tenants` (`write_governor.go:145`) | `MetricsRegistry.mu` is held across JSON/Prometheus serialization; `tenants`/`users` maps in `MetricsRegistry` and `WriteGovernor` are never pruned. |

### 2.2 Pageserver (`internal/pageserver/`)

| Area | Mechanism & Source Location | Status / Finding |
|---|---|---|
| **Goroutine Lifecycle** | `MaybeFlushAsync` (`readpath.go:354`), `runMaintenanceStep` (`memory_budget.go:475`), `UnixServer.Start` (`protocol.go:310`) | Flushers (`flushSem`, max 2) and compactors (`compactSem`, max 1) use `context.Background()` and are not tracked by `WaitGroup` in `Pageserver.Close()` (`server.go:267-279`). `UnixServer` has no max-connection cap. |
| **Global Lock Contention** | `Pageserver.dirtyTenantCount()` (`server.go:474-484`) -> `effectiveTenantLimits()` (`memory_budget.go:295`) | Called on every `enforceBeforeWrite()` (`memory_budget.go:323, 384`) and checkpoint/batch ingestion (`grpc.go:126, 158, 173`): acquires `p.mu.RLock()` + `tl.mu.RLock()` for **every** tenant. |
| **Global Cache Locks** | `DecodedChunkCache.mu`, `boundedIndexCache.mu`, `DiskCache.mu`, `ReadLimiter.mu` | Each uses a single global `sync.Mutex` across all tenants. |
| **Per-Tenant Locks** | `Timeline.mu`, `flushMu`, `compactMu`, `manifestMu` (`readpath.go:37-41`) | `Timeline.mu` is held exclusively across `ApplyWALRecord` (`readpath.go:425`), `PutPageBatch` (`readpath.go:1064`), and `SetRelSizes` (`grpc.go:64`). |
| **CA-WAL Bounded Structures** | `CAWALStore` (`cawal.go:134-142, 178-184, 373-383`) | `verified` bounded to `4096` entries; `flights` cleaned on completion; `pinShards` is 16-way sharded; `ioSem` bounded to `[4, 16]` (`GOMAXPROCS*2`). **Note**: `ioSem` only gates `putBlobDiskLeader`, not `GetBlobVerified`, `layers.go` L0 writes, or `CAWALStore.GC`. |
| **WAL & Layer Buffers** | `walparser.go:392`, `layer_v2.go:209-210, 366`, `readpath.go:55, 61-64` | `contBuf` never shrinks capacity; `v2StreamWriter` holds unique WAL payloads until `Finish()`; `relSizes` and `slruPages` are unbounded under continuous writes; `Timeline.layers` only auto-compacts when `InMemBytes() == 0` (`memory_budget.go:473`). |
| **Maintenance & GC Interference** | `MemoryBudget.runMaintenanceStep` (`memory_budget.go:410-502`) | Runs every `200ms`: calls `runtime.ReadMemStats` (STW) and `debug.FreeOSMemory()` up to every `500ms` whenever `HeapIdle - HeapReleased > 12 MiB`. |
| **Checkpoint & Background I/O** | `grpc.go:168`, `walreceiver.go:175`, `compactor.go:70, 425, 479` | Checkpoint records trigger synchronous `FlushInMemory(ctx)` stalling ingestion; `CompactL0Batch` and `GetRelSize` bypass `ReadLimiter`. |
| **Cache Purge Bug** | `PurgeTenant` (`server.go:390`) -> `DiskCache.EvictPrefix` (`cache.go:288-297`) | `item.key` is a SHA-256 hex digest (`cache.go:148, 186`), so raw `prefix` never matches. |

### 2.3 Read Server (`internal/readadapter/`)

| Area | Mechanism & Source Location | Status / Finding |
|---|---|---|
| **Scheduler** | `TenantScheduler` (`scheduler.go:85-329`) | Bounded concurrency (`MaxGlobalConcurrency = max(2, GOMAXPROCS)*4`, `MaxQueueDepth = 128`), priority-based fair dispatch (`PriorityInteractive > Normal > Batch`, fewest active per tenant, FIFO), memory soft-limit rejection (`0.90`). Zero background goroutines. |
| **Memory Manager** | `MemoryManager` (`memory_budget.go:12-14, 84-310`) | Bounded global (`256 MiB`), per-tenant (`64 MiB`), and per-query (`16 MiB`) execution budgets via atomic CAS with idempotent `Release()`. |
| **Noisy Neighbor** | `NoisyNeighborController` (`noisy_neighbor.go:38-141`) | Hard timeouts, plan-cost rejection/demotion, and tumbling-window (`1s`) per-tenant CPU quota. `tenants` map is never pruned. |
| **Tiered Cache** | `TieredCacheBackend` (`cache.go:51-64, 177-1358`) | Strict LRU for L1 (`16 MiB` in 512MB profile, `64 MiB` default) and L2 (`64 MiB` / `4096` entries in 512MB profile), `metaMap` capped at `4096`, `sfGroup` singleflight for L3, cgroup PSI-aware `O_DIRECT`/L2 bypass. **Gap**: `GetPageBatch()` (`cache.go:703, 758`) allocates `nblocks * 8192` bytes outside `MemoryManager` and `sfGroup`. |

### 2.4 Deployment Topology (`fly.toml`, `deploy/fly/*`, `docker-compose.yml`)

| Service | Fly App (`deploy/fly/`) | CPU / RAM | Volume | Ports / Internal Routing |
|---|---|---|---|---|
| **Gateway** | `air-pg-gateway` (`sin`) | `shared`, `1` vCPU, `256mb` | None | `:5432` PG, `:8081` HTTP, `:9093` gRPC (`auto_stop=off`, `min_machines=1`) |
| **Pageserver** | `air-pg-pageserver` (`sin`) | `shared`, `1` vCPU, `256mb` | `air_pageserver_data` (`1gb` -> `/var/lib/air-pageserver`) | `:9091` gRPC, `:8083` HTTP (`AIR_PAGESERVER_CACHE_MB=64`, R2 `air-pg-backups`) |
| **Read Engine** | `air-pg-read-engine` (`sin`) | `shared`, `1` vCPU, `256mb` | `air_read_engine_data` (`1gb` -> `/var/lib/air-read-engine`) | `:5433` PG, `:8082` HTTP, `:9092` gRPC (`air-pg-pageserver.internal:9091`) |
| **AIR PG Server** | `air-pg-server` (`sin`) | `shared`, `2` vCPU, `512mb` | `air_server_data` (`2gb` -> `/var/lib/air`) | `:8080` HTTP, `:9090` gRPC (`AIR_PAGESERVER_CACHE_MB=128`, `AIR_IDLE_TIMEOUT=30s`) |

---

## 3. Confirmed Priorities for Phase 24 Investigation & Targeted Fixes

1. **Quantify Latency Variance (Phase 2 & 3)**: Measure 10–20 repeated runs across 1–100 tenants and `GOMAXPROCS=1,2,4` to separate `dirtyTenantCount()` lock storms, `debug.FreeOSMemory()` STW pauses, and Go scheduler preemption.
2. **Eliminate Confirmed Contention Bottlenecks (only where benchmarked)**:
   - Replace `dirtyTenantCount()` `O(N_tenants)` lock scan with an `atomic.Int64` dirty-tenant counter maintained on transitions (`0 <-> >0`), eliminating `p.mu.RLock()` + `tl.mu.RLock()` storms on every write.
   - Gate `debug.FreeOSMemory()` in `runMaintenanceStep()` behind actual memory pressure or a higher idle threshold/cooldown so it does not trigger STW full-GC loops every `500ms` during bursts.
   - Track background flushes/compactions with a `WaitGroup` and shutdown `context.Context` for clean `Pageserver.Close()`.
3. **Validate 256 MB / 512 MB Memory, Multi-Tenant Fairness, Soak/Fault-Injection, and Fly.io Topology (Phases 4–10)**.
