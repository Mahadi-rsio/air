# Phase 18G/18H — Memory & Tenant Capacity Report (Linux Cgroup v2)

**Date**: 2026-11-22  
**Enforcement**: Real Linux cgroup v2 (`memory.max`, `memory.swap.max = 0`), bounded L2 (`fadvise`, `L2BypassUnderPressure = true`, `L2MaxConcurrentIO = 4`).

---

## 1. Multi-Profile Capacity Matrix

| Cgroup Profile | `memory.max` | Tenants | Workload | QPS | p50 (ms) | p99 (ms) | Process RSS (MB) | Cgroup Peak (MB) | Cgroup Anon (MB) | Cgroup File (MB) | Post-GC RSS (MB) | Failed |
| :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `micro-512mb` | 512 MB | 50 | `mixed` | 356.6 | 17.04 | 71.97 | 75.53 | **130.13** | 63.70 | 48.54 | 44.61 | 0 |
| `micro-512mb` | 512 MB | 100 | `idle` (10% active) | 3238.0 | 1.97 | 9.43 | 62.00 | **53.59** | 49.93 | 1.46 | 31.03 | 0 |
| `micro-512mb` | 512 MB | 50 | `abusive` | 1064.5 | 1.82 | 198.47 | 52.64 | **59.53** | 40.82 | 7.09 | 40.27 | 0 |
| `small-1gb` | 1024 MB | 150 | `mixed` | 408.9 | 14.99 | 50.78 | 124.07 | **237.00** | 112.27 | 97.02 | 73.10 | 0 |
| `medium-2gb` | 2048 MB | 300 | `mixed` | 394.5 | 14.92 | 48.51 | 215.90 | **442.13** | 204.00 | 193.98 | 125.64 | 0 |
| `prod-4gb` | 4096 MB | 500 | `mixed` | 401.5 | 13.85 | 44.29 | 377.60 | **754.87** | 365.79 | 322.77 | 217.40 | 0 |

---

## 2. Key Capacity Conclusions

1. **Zero OOMs across all profiles (50 to 500 tenants)** with `memory.swap.max = 0`.
2. **512MB node headroom**: 50 active mixed tenants peak at **130.13 MB** cgroup memory (25.4% of 512MB, down from 381 MB in Phase 17); 100 mostly-idle tenants peak at **53.59 MB**.
3. **Linear Scaling**: At 500 tenants (`prod-4gb`), process RSS is **377.60 MB** (~0.75 MB/tenant) and cgroup peak is **754.87 MB** (18.4% of 4GB), recovering to **217.40 MB** RSS after GC.
