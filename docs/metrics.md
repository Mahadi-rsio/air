# AIR Prometheus Metrics Reference

AIR exposes lightweight, concurrency-safe Prometheus text-exposition `/metrics` endpoints on all three read-path services. Hot-path histograms use lock-free atomic bucket counters (`internal/prom`) with bounded label cardinality so scraping never blocks query execution.

## Endpoints & Configuration

| Service | Default Address | Flag / Env Var | Auth |
| --- | --- | --- | --- |
| `air-gateway` | `[::]:8081` | `-admin-addr` / `AIR_GATEWAY_ADMIN_ADDR` | Optional `Authorization: Bearer $AIR_INTERNAL_TOKEN` |
| `air-read-engine` | `[::]:9102` | `-metrics-addr` / `AIR_READ_ENGINE_METRICS_ADDR` | Optional `Authorization: Bearer $AIR_INTERNAL_TOKEN` |
| `air-pageserver` | `[::]:9101` | `-metrics-addr` / `AIR_PAGESERVER_METRICS_ADDR` | Optional `Authorization: Bearer $AIR_INTERNAL_TOKEN` |

Setting `-metrics-addr ""` (or `off` / `disabled`) disables the dedicated metrics HTTP listener on `air-read-engine` or `air-pageserver` in resource-constrained environments.

---

## 1. Gateway (`air-pg-gateway`)

### Read-Path & Circuit Breaker

| Metric | Type | Meaning |
| --- | --- | --- |
| `read_engine_requests_total` | Counter | Total read queries dispatched from Gateway to Read Engine. |
| `read_engine_in_flight` | Gauge | Current number of in-flight Read Engine requests. |
| `read_engine_latency_seconds` | Histogram | Read Engine execution latency (isolated from total client query latency). |
| `read_engine_fallbacks_total` | Counter | Total Read Engine requests that fell back to PostgreSQL primary. |
| `read_breaker_open` | Gauge | Number of tenant pools whose Read Engine circuit breaker is currently open. |
| `read_breaker_trips_total` | Counter | Total times a tenant pool's Read Engine circuit breaker has tripped open. |

### Connection Lifecycle & Cancellation

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `tenant_active_connections` | Gauge | `tenant` | Active client connections currently accounted for per tenant. |
| `tenant_connection_rejections_total` | Counter | `tenant` | Total client connections rejected (e.g., `SQLSTATE 53300`) per tenant. |
| `client_disconnects_total` | Counter | — | Total client TCP disconnects detected while a backend query was in flight. |
| `query_cancellations_total` | Counter | — | Total query cancellations propagated to Read Engine or PostgreSQL primary. |

*(Existing Gateway metrics such as `air_gateway_connections_active`, `air_gateway_queries_total`, `air_gateway_pool_backends_active`, `air_gateway_timeouts_total`, and `air_gateway_pool_exhausted_total` remain exported on the same endpoint.)*

---

## 2. Read Engine (`air-pg-read-engine`)

All Phase 25 Read Engine metrics use aggregate or max-across-tenants gauges (no per-tenant Prometheus labels) to keep cardinality strictly bounded.

### TenantScheduler

| Metric | Type | Meaning |
| --- | --- | --- |
| `read_engine_scheduler_active_global` | Gauge | Currently executing queries across all tenants in `TenantScheduler`. |
| `read_engine_scheduler_queued_global` | Gauge | Currently queued requests waiting for scheduler admission. |
| `read_engine_scheduler_max_global` | Gauge | Configured maximum global concurrency in `TenantScheduler`. |
| `read_engine_scheduler_active_tenants` | Gauge | Number of tenants with at least one active query. |
| `read_engine_scheduler_tenant_active_max` | Gauge | Highest active query count among any single tenant. |
| `read_engine_scheduler_tenant_queued_max` | Gauge | Highest queued request count among any single tenant. |
| `read_engine_scheduler_admitted_total` | Counter | Total requests admitted by `TenantScheduler`. |
| `read_engine_scheduler_backpressure_total` | Counter | Total requests rejected due to scheduler backpressure. |
| `read_engine_scheduler_cancelled_waiters_total` | Counter | Total queued waiters cancelled before admission. |
| `read_engine_scheduler_wait_seconds` | Histogram | Time spent waiting in `TenantScheduler` before admission. |

### SQLite Reader Pool

| Metric | Type | Meaning |
| --- | --- | --- |
| `read_engine_reader_pool_active` | Gauge | Active SQLite reader slots checked out across tenants. |
| `read_engine_reader_pool_available` | Gauge | Idle SQLite reader slots currently available across tenants. |
| `read_engine_reader_pool_timeouts_total` | Counter | Total reader pool acquisition timeouts. |
| `read_engine_reader_pool_wait_seconds` | Histogram | Time spent waiting to acquire a SQLite reader slot. |

### Pageserver RPC Client

| Metric | Type | Meaning |
| --- | --- | --- |
| `read_engine_pageserver_rpc_in_flight` | Gauge | Current in-flight Pageserver RPC calls. |
| `read_engine_pageserver_rpc_requests_total` | Counter | Total Pageserver RPC calls issued. |
| `read_engine_pageserver_rpc_errors_total` | Counter | Total Pageserver RPC failures. |
| `read_engine_pageserver_rpc_timeouts_total` | Counter | Total Pageserver RPC deadline/timeout failures. |
| `read_engine_pageserver_rpc_latency_seconds` | Histogram | Pageserver gRPC call latency. |

### Tiered Cache

| Metric | Type | Meaning |
| --- | --- | --- |
| `read_engine_cache_l1_hits_total` | Counter | TieredCache L1 (memory) hits. |
| `read_engine_cache_l1_misses_total` | Counter | TieredCache L1 misses. |
| `read_engine_cache_l1_evictions_total` | Counter | TieredCache L1 evictions. |
| `read_engine_cache_l1_bytes` | Gauge | Current L1 cache memory usage in bytes. |
| `read_engine_cache_l2_hits_total` | Counter | TieredCache L2 (disk/mmap) hits. |
| `read_engine_cache_l2_misses_total` | Counter | TieredCache L2 misses. |
| `read_engine_cache_l2_evictions_total` | Counter | TieredCache L2 evictions. |
| `read_engine_cache_l2_bytes` | Gauge | Current L2 cache usage in bytes. |
| `read_engine_cache_l3_hits_total` | Counter | TieredCache L3 (Pageserver) fetch successes. |
| `read_engine_cache_l3_misses_total` | Counter | TieredCache L3 fetch failures/misses. |
| `read_engine_cache_l3_fetches_total` | Counter | Total L3 fetch requests issued to Pageserver. |
| `read_engine_cache_tenant_invalidations_total` | Counter | Full tenant cache invalidations. |
| `read_engine_cache_reset_required_total` | Counter | `ResetRequired` events received from Pageserver. |
| `read_engine_cache_sync_latency_seconds` | Histogram | Cache synchronization latency against WAL/LSN updates. |

### Query Totals

| Metric | Type | Meaning |
| --- | --- | --- |
| `read_engine_active_queries` | Gauge | Active queries currently executing in `SQLiteReadServer`. |
| `read_engine_queries_allowed_total` | Counter | Total read queries admitted for execution. |
| `read_engine_queries_failed_total` | Counter | Total read queries that returned an error. |
| `read_engine_queries_cancelled_total` | Counter | Total read queries cancelled via context or client disconnect. |

---

## 3. Pageserver (`air-pg-pageserver`)

### ReadLimiter

| Metric | Type | Meaning |
| --- | --- | --- |
| `pageserver_read_limiter_active` | Gauge | Active concurrent page reads admitted by `ReadLimiter`. |
| `pageserver_read_limiter_pending` | Gauge | Pending page reads waiting in `ReadLimiter`. |
| `pageserver_read_limiter_admitted_total` | Counter | Total reads admitted by `ReadLimiter`. |
| `pageserver_read_limiter_rejected_total` | Counter | Total reads rejected due to a full `ReadLimiter` queue. |
| `pageserver_read_limiter_cancelled_total` | Counter | Total reads cancelled while waiting in `ReadLimiter`. |
| `pageserver_read_limiter_wait_seconds` | Histogram | Queue wait duration inside `ReadLimiter`. |

### `GetPageAtLSN`, Storage I/O & Layer Lookup

| Metric | Type | Meaning |
| --- | --- | --- |
| `pageserver_getpage_layer_lookup_seconds` | Histogram | Time spent locating candidate layers/keys during `GetPageAtLSN`. |
| `pageserver_getpage_storage_io_seconds` | Histogram | Storage `ReadRange` I/O latency. |
| `pageserver_getpage_wal_redo_seconds` | Histogram | WAL redo page reconstruction latency during `GetPageAtLSN`. |
| `pageserver_getpage_total_seconds` | Histogram | Total end-to-end `GetPageAtLSN` reconstruction latency. |
| `pageserver_layer_lookup_seconds` | Histogram | Per-layer `LookupKeyInLayer` latency. |
| `pageserver_layer_scans_per_request` | Histogram | Distribution of how many layers each `GetPageAtLSN` scanned. |

### Layer Management & Compaction

| Metric | Type | Meaning |
| --- | --- | --- |
| `pageserver_timelines_active` | Gauge | Number of active timelines loaded in Pageserver. |
| `pageserver_layers_active_total` | Gauge | Total active immutable layers across all timelines. |
| `pageserver_layers_active_max_per_timeline` | Gauge | Maximum active layers on any single timeline. |
| `pageserver_layers_l0` | Gauge | Total active L0 delta layers across timelines. |
| `pageserver_layers_l1` | Gauge | Total active L1 delta layers across timelines. |
| `pageserver_layers_img` | Gauge | Total active Image layers across timelines. |
| `pageserver_compactions_active` | Gauge | Currently running L0-to-L1 compactions. |
| `pageserver_compactions_total` | Counter | Total L0-to-L1 compactions started. |

### WAL Redo

| Metric | Type | Meaning |
| --- | --- | --- |
| `pageserver_walredo_active_workers` | Gauge | Active WAL redo worker processes. |
| `pageserver_walredo_queue_depth` | Gauge | Pending WAL redo jobs in queue. |
| `pageserver_walredo_queue_wait_seconds` | Histogram | Time spent waiting in the WAL redo queue. |
| `pageserver_walredo_exec_seconds` | Histogram | WAL redo worker execution duration. |
| `pageserver_walredo_timeouts_total` | Counter | Total WAL redo timeout errors. |
| `pageserver_walredo_failures_total` | Counter | Total failed WAL redo requests. |

### Caches & Memory

| Metric | Type | Meaning |
| --- | --- | --- |
| `pageserver_index_cache_hits_total` | Counter | Layer B-tree index cache hits. |
| `pageserver_index_cache_misses_total` | Counter | Layer B-tree index cache misses. |
| `pageserver_index_cache_evictions_total` | Counter | Layer B-tree index cache evictions. |
| `pageserver_decoded_chunk_cache_hits_total` | Counter | `DecodedChunkCache` hits. |
| `pageserver_decoded_chunk_cache_misses_total` | Counter | `DecodedChunkCache` misses. |
| `pageserver_decoded_chunk_cache_evictions_total` | Counter | `DecodedChunkCache` evictions. |
| `pageserver_decoded_chunk_cache_bytes` | Gauge | Current `DecodedChunkCache` memory usage in bytes. |
| `pageserver_memory_inmem_bytes` | Gauge | Total active `InMemoryLayer` bytes across timelines. |
| `pageserver_memory_pending_flush_bytes` | Gauge | Total frozen `InMemoryLayer` bytes waiting to flush to disk. |
| `pageserver_memory_pressure_events_total` | Counter | Total memory budget backpressure waits and rejections. |
