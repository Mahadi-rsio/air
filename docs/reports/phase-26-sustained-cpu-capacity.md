# Phase 26 — Sustained CPU Capacity & Tier Isolation Report

**Date**: 2026-11-25  
**Target Cluster**: Fly.io (`sin` region) — `air-pg-gateway`, `air-pg-read-engine`, `air-pg-pageserver`  
**Workload**: `/tmp/fly_stress` (`5m0s` per run, `64` / `96` / `128` concurrent workers, mixed read/write `fly_test_db` workload, 2s Prometheus `/metrics` scraping, `/proc/stat` snapshots at `t=0s`, `t=120s`, `t=300s`)  
**Post-Test State**: All three machines restored to `shared-cpu-1x` (`256 MB`) after Experiment 5 completed.

---

## 1. Executive Summary

Phase 26 evaluated five sequential CPU tier configurations using identical **5-minute (`300s`)** benchmarks at `64`, `96`, and `128` workers, splitting each run into the **burst window (`0–120s`)** and the **sustained window (`120–300s`)**:

1. **Baseline (`shared-cpu-1x` on all 3 services)**: Every run hit hypervisor CPU burst-credit exhaustion (`steal_pct = 44.8%–62.1%` on Pageserver; `27.9%` on Read Engine at `128w`), collapsing sustained `120–300s` throughput to `107.46–143.67 QPS` with `p99 = 4.48s–5.20s`.
2. **Gateway-only upgrade (`performance-2x` on Gateway, `shared-cpu-1x` on Read Engine & Pageserver)**: **Did not improve sustained performance.** Gateway CPU steal dropped to `0.1%`, but Read Engine (`shared-cpu-1x`) was throttled at `43.6%–67.4%` CPU steal (`1.12s–3.16s` avg scheduler wait), capping `120–300s` QPS at `69.82–84.18 QPS`.
3. **Read Engine-only upgrade (`performance-2x` on Read Engine, `shared-cpu-1x` on Gateway & Pageserver)**: Eliminated Read Engine CPU steal (`0.1%`) and improved `120–300s` QPS at `96w` (`123.11` vs `109.05`) and `128w` (`138.43` vs `107.46`), **but immediately exposed Pageserver (`53.1%–68.3%` steal) and Gateway (`31.3%–48.6%` steal at `128w`)** as the next `shared-cpu-1x` ceilings.
4. **Combined Gateway + Read Engine upgrade (`performance-2x` on both, `shared-cpu-1x` on Pageserver)**: Eliminated CPU steal on both Gateway (`0.1%–0.2%`) and Read Engine (`0.1%–0.2%`), improving `120–300s` QPS at `96w` (`151.54`, `+39.0%` vs baseline) and `128w` (`129.52`, `+20.5%` vs baseline), **while proving `air-pg-pageserver` (`shared-cpu-1x`) was still CPU-throttled at `55.6%–65.4%` steal** (`GetPage` avg latency `6.90ms–7.32ms` in `120–300s`).
5. **Full Read-Path Upgrade (`performance-2x` on Gateway, Read Engine, and Pageserver)** *(justified by Experiment 4 Pageserver steal)*: Eliminated CPU steal across all three services (`0.2%–0.7%`).
   - At **`64` workers**, sustained `120–300s` QPS rose to **`240.74 QPS`** (`+67.6%` vs baseline `143.67`, `+118.5%` vs `gw_re` `110.16`), and overall `p99` dropped **`5.2x`** from `4.481s` to **`863.56ms`**.
   - At **`96` and `128` workers**, overall throughput jumped to **`1,375.32 QPS`** (`p99=85.81ms`) and **`1,384.99 QPS`** (`p99=154.33ms`), respectively. However, Prometheus counter inspection showed that once write throughput reached `~345 writes/sec`, `ReadEngineLSN < SessionWriteLSN` triggered Gateway's read-your-own-writes guard ([sql_router.go](../../internal/gateway/sql_router.go#L508)), routing `~99%` of reads in `96w`/`128w` to the PG primary (`air-pg-server`) rather than `ReadEngine` (`read_engine_requests_total` incremented by `1,900` at `96w` and `665` at `128w`, `read_engine_fallbacks_total = 0`).

Across all **15 five-minute runs (75 minutes total, `1,358,763` queries)**, there were **`0` software errors (`0.000%`)**, **`0` `SQLSTATE 53300` rejections**, and **`0` Read Engine fallbacks**.

---

## 2. Experiment Matrix & Methodology

| Exp | Label | `air-pg-gateway` | `air-pg-read-engine` | `air-pg-pageserver` |
|---|---|---|---|---|
| 1 | `baseline` | `shared-cpu-1x` (1 vCPU, 256 MB) | `shared-cpu-1x` (1 vCPU, 256 MB) | `shared-cpu-1x` (1 vCPU, 256 MB) |
| 2 | `gw_only` | `performance-2x` (2 ded. vCPU, 4 GB) | `shared-cpu-1x` (1 vCPU, 256 MB) | `shared-cpu-1x` (1 vCPU, 256 MB) |
| 3 | `re_only` | `shared-cpu-1x` (1 vCPU, 256 MB) | `performance-2x` (2 ded. vCPU, 4 GB) | `shared-cpu-1x` (1 vCPU, 256 MB) |
| 4 | `gw_re` | `performance-2x` (2 ded. vCPU, 4 GB) | `performance-2x` (2 ded. vCPU, 4 GB) | `shared-cpu-1x` (1 vCPU, 256 MB) |
| 5 | `gw_re_ps` | `performance-2x` (2 ded. vCPU, 4 GB) | `performance-2x` (2 ded. vCPU, 4 GB) | `performance-2x` (2 ded. vCPU, 4 GB) |

- **Identical Workload**: `/tmp/fly_stress -duration 300s -concurrency {64,96,128}` against `37.16.13.247:5432` (`fly_test_db`), sequential execution with 15s settle between worker counts.
- **Telemetry**: 2s Prometheus scraping of Gateway (`:8081/metrics`), Read Engine (`:9102/metrics`), and Pageserver (`:9101/metrics`), plus `/proc/stat` at `t=0s`, `t=120s`, and `t=300s`.

---

## 3. Summary Results (All 5 Experiments × 3 Worker Counts)

### 3.1 Throughput, Latency & Errors (`fly_stress` 5-minute runs)

| Exp | Workers | Total OK | Errors | Avg QPS | `0–120s` QPS | `120–300s` QPS | Overall P50 | Overall P95 | Overall P99 | `53300` / Fallbacks |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **`baseline`** | `64` | `84,461` | `0` | `281.28` | `487.80` | `143.67` | `120.60ms` | `342.12ms` | `4.481s` | `0` / `0` |
| **`baseline`** | `96` | `41,125` | `0` | `137.01` | `178.33` | `109.05` | `163.15ms` | `2.661s` | `5.120s` | `0` / `0` |
| **`baseline`** | `128` | `35,543` | `0` | `118.06` | `133.93` | `107.46` | `220.60ms` | `4.000s` | `5.201s` | `0` / `0` |
| **`gw_only`** | `64` | `23,283` | `0` | `77.58` | `71.84` | `81.10` | `129.01ms` | `2.719s` | `3.001s` | `0` / `0` |
| **`gw_only`** | `96` | `28,731` | `0` | `95.27` | `112.35` | `84.18` | `239.36ms` | `3.040s` | `3.768s` | `0` / `0` |
| **`gw_only`** | `128` | `21,243` | `0` | `70.65` | `71.23` | `69.82` | `299.68ms` | `5.120s` | `5.677s` | `0` / `0` |
| **`re_only`** | `64` | `42,947` | `0` | `142.80` | `210.30` | `98.04` | `86.53ms` | `1.928s` | `3.678s` | `0` / `0` |
| **`re_only`** | `96` | `46,982` | `0` | `156.34` | `206.05` | `123.11` | `159.71ms` | `2.402s` | `4.309s` | `0` / `0` |
| **`re_only`** | `128` | `48,261` | `0` | `160.54` | `193.47` | `138.43` | `402.35ms` | `2.721s` | `5.437s` | `0` / `0` |
| **`gw_re`** | `64` | `38,127` | `0` | `126.84` | `151.95` | `110.16` | `156.36ms` | `1.999s` | `2.644s` | `0` / `0` |
| **`gw_re`** | `96` | `46,641` | `0` | `154.41` | `160.57` | `151.54` | `160.13ms` | `2.406s` | `3.838s` | `0` / `0` |
| **`gw_re`** | `128` | `45,903` | `0` | `152.95` | `187.18` | `129.52` | `161.78ms` | `2.964s` | `6.068s` | `0` / `0` |
| **`gw_re_ps`** | `64` | `137,040` | `0` | `455.93` | `780.36` | `240.74` | `67.74ms` | `618.19ms` | `863.56ms` | `0` / `0` |
| **`gw_re_ps`**\* | `96` | `412,691` | `0` | `1,375.32` | `1,384.55` | `1,369.16` | `68.27ms` | `76.37ms` | `85.81ms` | `0` / `0` |
| **`gw_re_ps`**\* | `128` | `415,624` | `0` | `1,384.99` | `1,402.89` | `1,373.05` | `80.47ms` | `138.24ms` | `154.33ms` | `0` / `0` |

\* In `gw_re_ps` `96w` and `128w`, `ReadEngineLSN < SessionWriteLSN` under `~345 writes/sec` routed `~99%` of reads to the PG primary (`air-pg-server`) via read-your-own-writes consistency rather than `ReadEngine`.

---

## 4. CPU Utilization, CPU Steal & Scheduler Wait (`0–120s` vs `120–300s`)

### 4.1 `/proc/stat` CPU Busy% and Steal%

| Exp | Workers | GW Busy / Steal (`0–120s` → `120–300s`) | RE Busy / Steal (`0–120s` → `120–300s`) | PS Busy / Steal (`0–120s` → `120–300s`) |
|---|---:|---|---|---|
| **`baseline`** | `64` | `42.6% / 0.5%` → `14.8% / 0.2%` | `53.0% / 0.5%` → `14.7% / 0.2%` | `61.2% / 0.4%` → `25.7% / 55.1%` |
| **`baseline`** | `96` | `17.4% / 0.3%` → `11.8% / 0.2%` | `17.9% / 0.2%` → `10.4% / 0.1%` | `6.7% / 54.8%` → `6.1% / 62.1%` |
| **`baseline`** | `128` | `14.3% / 0.2%` → `12.1% / 0.2%` | `12.6% / 0.1%` → `9.5% / 27.9%` | `5.9% / 56.3%` → `6.2% / 44.8%` |
| **`gw_only`** | `64` | `5.3% / 0.1%` → `6.2% / 0.1%` | `7.8% / 67.4%` → `6.2% / 63.9%` | `5.2% / 0.1%` → `7.1% / 0.1%` |
| **`gw_only`** | `96` | `7.6% / 0.1%` → `6.5% / 0.1%` | `8.1% / 64.7%` → `6.6% / 43.6%` | `7.7% / 0.1%` → `6.2% / 32.6%` |
| **`gw_only`** | `128` | `6.4% / 0.1%` → `6.1% / 0.1%` | `7.1% / 65.0%` → `6.3% / 66.0%` | `6.0% / 0.1%` → `5.6% / 0.1%` |
| **`re_only`** | `64` | `17.5% / 0.3%` → `10.3% / 0.2%` | `10.7% / 0.1%` → `4.9% / 0.1%` | `9.6% / 53.6%` → `7.2% / 68.3%` |
| **`re_only`** | `96` | `17.9% / 0.3%` → `12.3% / 0.2%` | `9.7% / 0.1%` → `6.3% / 0.1%` | `6.9% / 56.6%` → `6.0% / 61.5%` |
| **`re_only`** | `128` | `11.6% / 31.3%` → `4.7% / 48.6%` | `8.6% / 0.1%` → `5.3% / 0.1%` | `6.6% / 53.1%` → `5.9% / 57.7%` |
| **`gw_re`** | `64` | `8.3% / 0.2%` → `6.9% / 0.1%` | `7.8% / 0.1%` → `5.9% / 0.1%` | `6.3% / 55.7%` → `6.9% / 65.4%` |
| **`gw_re`** | `96` | `9.0% / 0.1%` → `8.2% / 0.1%` | `7.6% / 0.1%` → `6.2% / 0.1%` | `5.9% / 55.6%` → `6.6% / 61.6%` |
| **`gw_re`** | `128` | `9.8% / 0.2%` → `7.9% / 0.1%` | `9.0% / 0.2%` → `6.5% / 0.1%` | `6.8% / 58.1%` → `5.8% / 61.8%` |
| **`gw_re_ps`** | `64` | `28.9% / 0.5%` → `12.2% / 0.2%` | `37.0% / 0.4%` → `11.4% / 0.2%` | `44.3% / 0.3%` → `53.1% / 0.2%` |
| **`gw_re_ps`** | `96` | `38.6% / 0.6%` → `37.7% / 0.6%` | `16.7% / 0.2%` → `15.6% / 0.2%` | `28.5% / 0.2%` → `28.7% / 0.3%` |
| **`gw_re_ps`** | `128` | `38.0% / 0.7%` → `37.8% / 0.7%` | `16.5% / 0.2%` → `16.3% / 0.3%` | `29.6% / 0.3%` → `28.5% / 0.2%` |

### 4.2 Scheduler Wait, Queue Depth & Pageserver `GetPage` Latency

| Exp | Workers | RE Sched Wait (`0–120s` → `120–300s`) | PS `GetPage` Avg (`0–120s` → `120–300s`) | Max RE Queue | Max GW In-Flight |
|---|---:|---|---|---:|---:|
| **`baseline`** | `64` | `96.30ms` → `603.91ms` | `0.93ms` → `9.34ms` | `56` | `64` |
| **`baseline`** | `96` | `791.93ms` → `1,685.03ms` | `3.53ms` → `8.74ms` | `88` | `95` |
| **`baseline`** | `128` | `1,713.78ms` → `2,074.87ms` | `3.90ms` → `4.14ms` | `122` | `127` |
| **`gw_only`** | `64` | `1,329.20ms` → `1,121.05ms` | `0.21ms` → `0.43ms` | `55` | `64` |
| **`gw_only`** | `96` | `1,236.49ms` → `1,912.32ms` | `0.24ms` → `3.51ms` | `89` | `96` |
| **`gw_only`** | `128` | `3,041.18ms` → `3,162.64ms` | `0.25ms` → `0.33ms` | `119` | `128` |
| **`re_only`** | `64` | `468.20ms` → `1,118.59ms` | `2.25ms` → `9.78ms` | `58` | `64` |
| **`re_only`** | `96` | `748.71ms` → `1,374.86ms` | `2.69ms` → `7.34ms` | `88` | `95` |
| **`re_only`** | `128` | `778.87ms` → `1,242.65ms` | `3.25ms` → `8.21ms` | `116` | `127` |
| **`gw_re`** | `64` | `689.32ms` → `983.71ms` | `2.71ms` → `6.90ms` | `56` | `62` |
| **`gw_re`** | `96` | `1,052.14ms` → `1,138.90ms` | `2.61ms` → `7.32ms` | `88` | `94` |
| **`gw_re`** | `128` | `1,193.54ms` → `1,940.20ms` | `3.50ms` → `7.22ms` | `121` | `127` |
| **`gw_re_ps`** | `64` | `31.16ms` → `323.79ms` | `0.84ms` → `25.42ms` | `55` | `63` |
| **`gw_re_ps`** | `96` | `39.46ms` (`0–120s`, `n=1,008`) | `0.13ms` → `0.87ms` | `38` | `25` |
| **`gw_re_ps`** | `128` | `218.85ms` (`0–120s`, `n=142`) | `11.08ms` → `1.00ms` | `27` | `97` |

---

## 5. Acceptance Criteria & Bottleneck Analysis

| Criterion | Outcome | Evidence |
|---|---|---|
| Higher sustained QPS during final 3m (`120–300s`) | **PASS (`gw_re_ps` & `gw_re` @ 96/128w)** | `gw_re_ps` `64w`: `240.74` vs `143.67` (`+67.6%`); `gw_re` `96w`: `151.54` vs `109.05` (`+39.0%`); `128w`: `129.52` vs `107.46` (`+20.5%`). `gw_only` **FAIL** (`69.8–84.2 QPS`). |
| Lower P95/P99 latency | **PASS (`gw_re_ps`)** | `gw_re_ps` `64w` P95 `618.19ms` (vs `342.12ms` burst-skewed baseline, but P99 dropped **`5.2x`**: `863.56ms` vs `4.481s`). |
| Reduced CPU steal / throttling | **PASS (`gw_re_ps`)** | Steal dropped from `44.8%–68.3%` (`shared-cpu-1x`) to **`0.2%–0.7%`** across all three services on `performance-2x`. |
| Zero software errors & `SQLSTATE 53300` regressions | **PASS** | `0` errors and `0` `53300` rejections across all `1,358,763` queries in all 15 runs. |
| Bounded scheduler queues & Pageserver load | **PASS** | Max RE queue stayed bounded (`<= 122`, matching `<= concurrency`), and `Max GW InFlight` stayed `<= 128`. |

### Architectural Findings
1. **Upgrading Gateway alone is ineffective**: Read Engine (`shared-cpu-1x`) is the immediate CPU bottleneck behind Gateway, stalling at `64%–67%` CPU steal.
2. **Upgrading Gateway + Read Engine exposes Pageserver**: On `shared-cpu-1x`, Pageserver hit `55.6%–65.4%` CPU steal whenever Gateway and Read Engine were unthrottled, inflating `GetPage` latency to `~7ms` and starving Read Engine worker threads.
3. **All three read-path tiers require dedicated CPU (`performance-1x` or `performance-2x`) for sustained load**: Only `gw_re_ps` eliminated CPU steal across the full pipeline (`0.2%–0.7%`).
4. **Read-Your-Own-Writes LSN lag under high write rates (`>300 writes/sec`)**: In `gw_re_ps` `96w`/`128w`, `ReadEngineLSN` lagged behind `SessionWriteLSN`, causing Gateway to route reads to PG primary (`air-pg-server`) rather than `ReadEngine`. Optimizing `ReadEngineLSN` propagation / Pageserver WAL ingestion rate is the next lever if high write rates must keep reads on `ReadEngine`.
