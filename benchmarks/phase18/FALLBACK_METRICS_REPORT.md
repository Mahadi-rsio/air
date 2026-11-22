# Phase 18F — PostgreSQL Fallback Reduction & Observability Report

**Date**: 2026-11-22  
**Component**: `internal/readadapter/sqlite_server.go` (`PrometheusMetrics`, `recordQueryError`)

---

## 1. Implemented Prometheus Metrics

`SQLiteReadServer.PrometheusMetrics()` now exports all required low-cardinality Phase 18 metrics (verified in `TestPhase18F_PrometheusMetrics`):

- `air_read_queries_total{status="succeeded|failed|cancelled"}`
- `air_native_read_queries_total`
- `air_postgres_fallback_queries_total`
- `air_postgres_fallback_total{reason="toast|multixact|unsupported_type|unsupported_operator|unsupported_function|unsupported_syntax|other"}`
- `air_cache_l1_hits_total`
- `air_cache_l2_hits_total`
- `air_cache_misses_total`
- `air_cache_evictions_total`
- `air_l2_cache_bytes`
- `air_l2_cache_entries`
- `air_tenant_memory_bytes`
- `air_cgroup_memory_bytes`

---

## 2. Measured Fallback Reduction (Extended-Type Workload)

Evaluated on the 22-query Phase 18 live PostgreSQL 17.12 differential suite (`TestPhase18_TypesDifferential_LivePG17`), comprising 6 `numeric` queries, 5 `inet`/`cidr` queries, 4 scalar-array queries, and 7 unsupported-operator queries (`SUM(numeric)`, `numeric + 1`, `inet << cidr`, `host(inet)`, `arr[1]`, `arr @> ...`, `ORDER BY arr`):

| Workload Segment | Queries | Phase 17 Fallback Rate | Phase 18 Fallback Rate | Reduction |
| :--- | ---: | ---: | ---: | ---: |
| `numeric` (select, filter, `ORDER BY`, `GROUP BY`, `MIN`/`MAX`, `JOIN`) | 6 | 100.0% (6/6) | **0.0%** (0/6) | **-100.0%** |
| `inet` / `cidr` (select, equality, range, `ORDER BY`) | 5 | 100.0% (5/5) | **0.0%** (0/5) | **-100.0%** |
| Scalar arrays (`int2[]`, `int4[]`, `int8[]`, `text[]`, `bool[]`, `float4[]`, `float8[]`) | 4 | 100.0% (4/4) | **0.0%** (0/4) | **-100.0%** |
| Unsupported operators (`SUM`, `+`, `<<`, `host()`, `[1]`, `@>`, `ORDER BY arr`) | 7 | 100.0% (7/7) | 100.0% (7/7) | Preserved (safe PG fallback) |
| **Combined 22-Query Extended Workload** | **22** | **100.0% (22/22)** | **31.8% (7/22)** | **-68.2 percentage points** |
