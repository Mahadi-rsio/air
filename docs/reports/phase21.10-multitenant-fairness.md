# AIR Phase 21.10 — Multi-Tenant Fairness Under Real Contention Report

**Date:** 2026-11-23  
**Repository:** `github.com/Mahadi-rsio/air`  
**Base Commit:** `803c0fd9f0cab7c2c5b0b85c3898f901fd264d4f` (+ Phase 21.10 fixes)  
**Configuration:** Static Write Governor (`WriteMaxGlobal=16`, `WriteMaxPerTenant=8`, `AIR_WRITE_ADAPTIVE_ENABLED=false`)  
**Status:** **PARTIAL** (4-tenant 32w & 64w overload **PASS**; 8-tenant simultaneous active bootstrap on `256 MB` Pageserver **FAIL** with Pageserver OOM)

---

## 1. 4-Tenant Contention (`32` Concurrent Writers > `WriteMaxGlobal=16`, `60s`) — **PASS**

- **Command:** `go run ./benchmarks/phase21 --mode=mt-overload --duration=60s --concurrency=32 --tenants=4 --tenant-prefix=p2110_4t32w`
- **Write Governor Stats:** `CompletedTotal=24,042`, `ErrorsTotal=0`, `RejectedTotal=0`, `TimeoutTotal=0`, `QueueDepth=15`, `QueueWaitP50Us=500`, `QueueWaitP95Us=250000`, `QueueWaitP99Us=500000`.

| Tenant | Writers | Ops (60s) | TPS | Errors | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| `t0` (bulk) | 25 | 8,137 | 135.6 | 0 | 142.22 | 419.00 | 767.77 |
| `t1` (interactive) | 2 | 7,551 | 125.8 | 0 | **10.39** | **41.77** | **69.44** |
| `t2` (mixed R/W) | 4 | 9,551 | 159.2 | 0 | 11.74 | 78.24 | 198.45 |
| `t3` (intermittent) | 1 | 358 | 6.0 | 0 | 161.76 | 192.63 | 226.96 |

---

## 2. 4-Tenant Contention (`64` Concurrent Writers > `WriteMaxGlobal=16`, `60s`) — **PASS**

- **Command:** `go run ./benchmarks/phase21 --mode=mt-overload --duration=60s --concurrency=64 --tenants=4 --tenant-prefix=p2110_4t64w`
- **Write Governor Stats:** `CompletedTotal=22,317`, `ErrorsTotal=0`, `RejectedTotal=0`, `TimeoutTotal=0`, `QueueDepth=16`, `QueueWaitP50Us=500`, `QueueWaitP95Us=250000`, `QueueWaitP99Us=500000`.

| Tenant | Writers | Ops (60s) | TPS | Errors | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| `t0` (bulk) | 57 | 7,779 | 129.7 | 0 | 351.55 | 1010.25 | 1342.52 |
| `t1` (interactive) | 2 | 6,947 | 115.8 | 0 | **11.60** | **45.78** | **72.90** |
| `t2` (mixed R/W) | 4 | 8,695 | 144.9 | 0 | 13.18 | 87.50 | 244.57 |
| `t3` (intermittent) | 1 | 353 | 5.9 | 0 | 163.63 | 194.15 | 219.14 |

**Fairness Verdict:** Under `4x` global write overload (`57` bulk writers on `t0`), DRR prevented starvation and kept interactive `t1` at **`11.60 ms` p50** and **`45.78 ms` p95** with `0` errors.

---

## 3. 8-Tenant Simultaneous Active Test on `256 MB` Pageserver — **FAIL (Pageserver OOM)**

- **Command:** `go run ./benchmarks/phase21 --mode=mt-overload --duration=60s --concurrency=32 --tenant-prefix=p2110b_32w`
- **Result:** Provisioning 8 concurrent active tenants (plus 2 prior active tenants) caused `air-p21-pageserver` (`256 MB` limit) to OOMKill (`ExitCode=137`) during `BootstrapTenantFromPGData` of the 10th tenant.
- **Implication:** A `256 MB` Pageserver comfortably hosts 4 active tenants under 64-writer overload, but hosting 8+ simultaneous un-evicted active tenants requires either a larger Pageserver memory tier (`512 MB+`) or lower per-tenant `InMemoryLayer` budgets (`defaultMaxInMemBytes`).
