# AIR Phase 20.9 — Multi-Tenant Fairness & Isolation Report

**Date:** 2026-11-23  
**Environment:** 4 simultaneous real PostgreSQL `17.12` tenant clusters (`tenant_a`, `tenant_b`, `tenant_c`, `tenant_d`) managed by `cell.Agent`, each in its own 512 MB cgroup v2 (`air_tenant_<id>`), behind a single `air-gateway` instance (`WriteMaxGlobal=12`, `WriteMinGlobal=4`, `WriteMaxPerTenant=4`, `PoolMaxBackends=16`, 8s per mode).  
**Raw Data:** [multitenant.log](file:///workspaces/air/benchmarks/phase20.9/results/multitenant.log), [multitenant.json](file:///workspaces/air/benchmarks/phase20.9/results/multitenant.json).

---

## 1. Simultaneous 4-Tenant Workload Design

- **`tenant_a` (`heavy_bulk_16c`):** 16 concurrent clients inserting 2 KB rows continuously (noisy neighbor).
- **`tenant_b` (`interactive_small_2c`):** 2 concurrent clients issuing small interactive INSERTs (latency-sensitive).
- **`tenant_c` (`mixed_rw_4c`):** 4 concurrent clients executing 70% `SELECT` / 30% `UPDATE`.
- **`tenant_d` (`intermittent_2c`):** 2 concurrent clients writing every 50 ms.

---

## 2. Measured Per-Tenant Throughput & Latency (Real PostgreSQL 17.12)

| Mode | Tenant | Role | TPS | p50 (ms) | p95 (ms) | p99 (ms) | Errors |
|---|---|---|---:|---:|---:|---:|---:|
| **A: No Governor** | `tenant_a` | Heavy bulk (16c) | 2,275.7 | 6.38 | 11.73 | 14.62 | 0 |
| **A: No Governor** | `tenant_b` | Interactive small (2c) | 507.1 | 3.64 | 7.49 | 10.12 | 0 |
| **A: No Governor** | `tenant_c` | Mixed R/W (4c) | 993.8 | 3.66 | 7.68 | 10.52 | 0 |
| **A: No Governor** | `tenant_d` | Intermittent (2c) | 35.7 | 54.84 | 59.98 | 64.75 | 0 |
| **B: Static Governor (DRR)** | `tenant_a` | Heavy bulk (16c) | 1,593.3 | 9.46 | 14.05 | 21.60 | 0 |
| **B: Static Governor (DRR)** | `tenant_b` | Interactive small (2c) | **705.5 (+39.1%)** | **2.62** | **4.91 (-34.4%)** | **7.71 (-23.8%)** | 0 |
| **B: Static Governor (DRR)** | `tenant_c` | Mixed R/W (4c) | **1,402.7 (+41.1%)** | **2.62** | **5.09 (-33.7%)** | **8.29 (-21.2%)** | 0 |
| **B: Static Governor (DRR)** | `tenant_d` | Intermittent (2c) | **36.7 (+2.8%)** | **53.33** | **57.97 (-3.4%)** | 70.61 | 0 |
| **C: Adaptive Governor** | `tenant_a` | Heavy bulk (16c) | 1,259.3 | 12.21 | 16.81 | 26.81 | 0 |
| **C: Adaptive Governor** | `tenant_b` | Interactive small (2c) | **656.7 (+29.5%)** | **2.82** | **4.98 (-33.5%)** | **7.12 (-29.6%)** | 0 |
| **C: Adaptive Governor** | `tenant_c` | Mixed R/W (4c) | **1,705.8 (+71.6%)** | **2.15** | **4.33 (-43.6%)** | **6.11 (-41.9%)** | 0 |
| **C: Adaptive Governor** | `tenant_d` | Intermittent (2c) | **36.9 (+3.4%)** | **53.23** | **56.25 (-6.2%)** | **62.28 (-3.8%)** | 0 |

---

## 3. Fairness & Isolation Assessment

1. **Noisy-Neighbor Protection Verified on Real PG17:** Without the Write Governor, `tenant_a` consumed 60% of total cluster write capacity (`2,275.7 TPS`), pushing `tenant_b` (interactive) p95 latency to `7.49 ms` and `tenant_c` (mixed) to `7.68 ms`.
2. **DRR + Per-Tenant Limits Deliver Major Wins:** With **Static Governor (`B_static_governor`)**:
   - Interactive `tenant_b` throughput rose **+39.1%** and p95 latency dropped **-34.4%** (`7.49ms -> 4.91ms`).
   - Mixed R/W `tenant_c` throughput rose **+41.1%** and p95 latency dropped **-33.7%** (`7.68ms -> 5.09ms`).
   - Aggregate cluster throughput remained virtually unchanged (`3,812.3 -> 3,738.2 TPS`, `-1.9%`), proving fairness was achieved without wasting host capacity.
3. **Zero Cross-Tenant Errors or Starvation:** All 4 tenants operated with `0` errors and independent per-tenant cgroup v2 isolation.

### Reproduction
```bash
./benchmarks/phase20.9/run_benchmarks.sh multitenant 8
```
