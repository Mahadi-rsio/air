# Phase 18G — Tenant Efficiency & Isolation Report

**Date**: 2026-11-22

---

## 1. Per-Tenant Memory & Efficiency Scaling

Measured from the cgroup v2 capacity matrix (`cap_harness`):

| Tenants | Profile | Process RSS / Tenant | Post-GC RSS / Tenant | Cgroup Peak / Tenant |
| ---: | :--- | ---: | ---: | ---: |
| 50 (`mixed`) | `micro-512mb` | 1.51 MB | 0.89 MB | 2.60 MB |
| 100 (`idle`) | `micro-512mb` | 0.62 MB | 0.31 MB | 0.54 MB |
| 150 (`mixed`) | `small-1gb` | 0.83 MB | 0.49 MB | 1.58 MB |
| 300 (`mixed`) | `medium-2gb` | 0.72 MB | 0.42 MB | 1.47 MB |
| 500 (`mixed`) | `prod-4gb` | 0.76 MB | 0.43 MB | 1.51 MB |

---

## 2. Abusive Tenant Isolation (`micro-512mb`, 50 Tenants)

4 of 8 workers hammered `t_0000` continuously while 4 workers served the remaining 49 tenants:
- **Normal tenants**: `p50 = 1.81 ms`, `p95 = 7.75 ms`, `p99 = 16.36 ms`.
- **Abusive tenant (`t_0000`)**: `p50 = 349.84 ms` (throttled by per-tenant scheduler/semaphore fairness).
- **Failures**: `0` failed queries; `cgroup_peak = 59.53 MB`; post-workload RSS recovered to `40.27 MB`.
