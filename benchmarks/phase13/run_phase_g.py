#!/usr/bin/env python3
"""
AIR Phase 13 — Phase G: Resource & Cost Analysis (512 MB, 1 GB, 2 GB RAM budgets).
"""

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_phase13_bench import (
    PG_BIN, AIR_BIN, free_port, wait_http, get_json,
    read_proc_stats, find_postgres_pids, run_pgbench, write_scripts
)

OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phase_g_results.json")

TIERS = [
    {
        "name": "512MB",
        "budget_mb": 512,
        "srv_gomem": "180MiB",
        "re_gomem": "80MiB",
        "gw_gomem": "60MiB",
        "ps_cache_mb": 64,
        "re_shared_buffers": "32MB",
    },
    {
        "name": "1GB",
        "budget_mb": 1024,
        "srv_gomem": "400MiB",
        "re_gomem": "160MiB",
        "gw_gomem": "120MiB",
        "ps_cache_mb": 128,
        "re_shared_buffers": "64MB",
    },
    {
        "name": "2GB",
        "budget_mb": 2048,
        "srv_gomem": "800MiB",
        "re_gomem": "320MiB",
        "gw_gomem": "200MiB",
        "ps_cache_mb": 256,
        "re_shared_buffers": "128MB",
    },
]


def dir_size_mb(path):
    total = 0
    for root, _, files in os.walk(path):
        for fn in files:
            try:
                total += os.path.getsize(os.path.join(root, fn))
            except OSError:
                pass
    return round(total / (1024.0 * 1024.0), 2)


def run_tier(tier, scripts, work_root):
    tier_dir = os.path.join(work_root, tier["name"])
    shutil.rmtree(tier_dir, ignore_errors=True)
    srv_data = os.path.join(tier_dir, "air_s")
    re_data = os.path.join(tier_dir, "air_r")
    re_cache = os.path.join(tier_dir, "air_c")
    ps_sock = os.path.join(srv_data, "ps.sock")
    for d in (srv_data, re_data, re_cache):
        os.makedirs(d, exist_ok=True)

    meta_p, http_p, grpc_p, gw_adm_grpc_p = free_port(), free_port(), free_port(), free_port()
    re_http_p, re_pg_p, gw_pg_p, gw_adm_p = free_port(), free_port(), free_port(), free_port()

    procs = []
    try:
        env_srv = os.environ.copy()
        env_srv["GOMEMLIMIT"] = tier["srv_gomem"]
        p_srv = subprocess.Popen([
            os.path.join(AIR_BIN, "air-server"),
            "-data-dir", srv_data, "-meta-dir", os.path.join(srv_data, "meta"),
            "-master-key-path", os.path.join(srv_data, "master.key"),
            "-pg-bin-dir", PG_BIN, "-meta-port", str(meta_p),
            "-http-addr", f"127.0.0.1:{http_p}",
            "-grpc-addr", f"127.0.0.1:{grpc_p}",
            "-gateway-admin-grpc-addr", f"127.0.0.1:{gw_adm_grpc_p}",
            "-pageserver", "-pageserver-socket", ps_sock,
            "-pageserver-cache-mb", str(tier["ps_cache_mb"]),
            "-bootstrap-account-id", "acct_pg",
            "-bootstrap-api-key", "air_pg_secret_key",
            "-internal-token", "air_pg_internal",
        ], env=env_srv, stdout=open(os.path.join(tier_dir, "srv.log"), "w"), stderr=subprocess.STDOUT)
        procs.append(p_srv)
        wait_http(f"http://127.0.0.1:{http_p}/healthz")

        env_re = os.environ.copy()
        env_re["GOMEMLIMIT"] = tier["re_gomem"]
        p_re = subprocess.Popen([
            os.path.join(AIR_BIN, "air-read-engine"),
            "-data-dir", re_data, "-http-addr", f"127.0.0.1:{re_http_p}",
            "-listen-host", "127.0.0.1", "-pg-port", str(re_pg_p),
            "-pageserver-socket", ps_sock, "-cache-dir", re_cache,
            "-shared-buffers", tier["re_shared_buffers"],
        ], env=env_re, stdout=open(os.path.join(tier_dir, "re.log"), "w"), stderr=subprocess.STDOUT)
        procs.append(p_re)
        wait_http(f"http://127.0.0.1:{re_http_p}/healthz")

        env_gw = os.environ.copy()
        env_gw["GOMEMLIMIT"] = tier["gw_gomem"]
        p_gw = subprocess.Popen([
            os.path.join(AIR_BIN, "air-gateway"),
            "--gateway-addr", f"127.0.0.1:{gw_pg_p}",
            "--gateway-admin-addr", f"127.0.0.1:{gw_adm_p}",
            "--gateway-admin-grpc-addr", f"127.0.0.1:{gw_adm_grpc_p}",
            "--db-grpc-addr", f"127.0.0.1:{grpc_p}",
            "--control-plane-url", f"http://127.0.0.1:{http_p}",
            "--internal-token", "air_pg_internal",
            "--backend-host", "127.0.0.1", "--pool-mode", "--pool-max-backends", "32",
            "--max-client-conns", "1024", "--max-tenant-client-conns", "600",
            "--read-engine-host", "127.0.0.1", "--read-engine-port", str(re_pg_p),
        ], env=env_gw, stdout=open(os.path.join(tier_dir, "gw.log"), "w"), stderr=subprocess.STDOUT)
        procs.append(p_gw)
        wait_http(f"http://127.0.0.1:{gw_adm_p}/healthz")

        tenants = {}
        for idx, tid in enumerate(["pg_t0", "pg_t1", "pg_t2", "pg_t3"]):
            req = urllib.request.Request(
                f"http://127.0.0.1:{http_p}/v1/databases",
                data=json.dumps({"tenant_id": tid}).encode(),
                headers={
                    "Authorization": "Bearer air_pg_secret_key",
                    "Idempotency-Key": f"idem-{tid}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                info = json.loads(r.read().decode())
            tenants[tid] = info
            pg_sock_dir = os.path.join(srv_data, "tenants", "active", tid, "sock")
            subprocess.check_call([
                os.path.join(PG_BIN, "psql"),
                f"host={pg_sock_dir} port={info['direct_port']} user=postgres dbname={tid}",
                "-c", "GRANT ALL ON SCHEMA public TO app_user;"
            ], stdout=subprocess.DEVNULL)
            sql = (
                "CREATE TABLE IF NOT EXISTS items (id int4 PRIMARY KEY, cat int4, val text);"
                f"INSERT INTO items SELECT g, g % 10, 't{idx}_item_' || g FROM generate_series(1, 1000) g ON CONFLICT DO NOTHING;"
            )
            env_u = os.environ.copy()
            env_u["PGPASSWORD"] = info["password"]
            subprocess.check_call([
                os.path.join(PG_BIN, "psql"),
                f"host=127.0.0.1 port={gw_pg_p} user={info['username']} dbname={tid} sslmode=disable",
                "-c", sql,
            ], env=env_u, stdout=subprocess.DEVNULL)

        # Warmup
        t0_info = tenants["pg_t0"]
        run_pgbench("127.0.0.1", gw_pg_p, t0_info["username"], t0_info["password"], "pg_t0", 10, 2, script_path=scripts["ro_items"])

        # Measure 50-client single tenant + 4x25=100-client multi-tenant
        pg_pids = find_postgres_pids(srv_data)
        all_pids = [p_srv.pid, p_re.pid, p_gw.pid] + pg_pids
        s0 = read_proc_stats(all_pids)
        m0 = get_json(f"http://127.0.0.1:{re_http_p}/metrics")
        t_start = time.time()
        res_50 = run_pgbench("127.0.0.1", gw_pg_p, t0_info["username"], t0_info["password"], "pg_t0", 50, 8, script_path=scripts["ro_items"])
        dt = time.time() - t_start
        s1 = read_proc_stats(all_pids)
        m1 = get_json(f"http://127.0.0.1:{re_http_p}/metrics")

        cpu_pct = sum(s1.get(p, {}).get("cpu_sec", 0) - s0.get(p, {}).get("cpu_sec", 0) for p in all_pids) / max(0.1, dt) * 100.0
        srv_rss = round(s1.get(p_srv.pid, {}).get("rss_kb", 0) / 1024.0, 1)
        re_rss = round(s1.get(p_re.pid, {}).get("rss_kb", 0) / 1024.0, 1)
        gw_rss = round(s1.get(p_gw.pid, {}).get("rss_kb", 0) / 1024.0, 1)
        pg_rss = round(sum(s1.get(p, {}).get("rss_kb", 0) for p in pg_pids) / 1024.0, 1)
        total_rss = round(srv_rss + re_rss + gw_rss + pg_rss, 1)

        hits = m1.get("cache_hits", 0) - m0.get("cache_hits", 0)
        misses = m1.get("cache_misses", 0) - m0.get("cache_misses", 0)
        hit_ratio = round(hits / max(1, hits + misses), 4)

        # Multi-tenant 4x25 = 100 clients
        mt_res = {}
        def worker(tid, info):
            mt_res[tid] = run_pgbench("127.0.0.1", gw_pg_p, info["username"], info["password"], tid, 25, 6, script_path=scripts["ro_items"])

        threads = [threading.Thread(target=worker, args=(tid, tenants[tid])) for tid in tenants]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        mt_total_tps = round(sum(mt_res[t]["tps"] for t in mt_res), 2)
        mt_max_err = max(mt_res[t]["error_rate"] for t in mt_res)

        nvme_srv_mb = dir_size_mb(srv_data)
        nvme_cache_mb = dir_size_mb(re_cache)

        return {
            "tier": tier["name"],
            "budget_mb": tier["budget_mb"],
            "single_tenant_50c": res_50,
            "cpu_pct": round(cpu_pct, 1),
            "rss_mb": {
                "air_server": srv_rss,
                "air_read_engine": re_rss,
                "air_gateway": gw_rss,
                "postgres_all_tenants": pg_rss,
                "total": total_rss,
            },
            "cache_hit_ratio": hit_ratio,
            "nvme_usage_mb": {
                "pageserver_and_pgdata": nvme_srv_mb,
                "read_engine_l2_cache": nvme_cache_mb,
            },
            "multitenant_4x25c": {
                "total_tps": mt_total_tps,
                "max_error_rate": mt_max_err,
                "per_tenant_tps": {t: mt_res[t]["tps"] for t in sorted(mt_res)},
                "per_tenant_p99_ms": {t: mt_res[t]["p99_ms"] for t in sorted(mt_res)},
            },
        }
    finally:
        for p in reversed(procs):
            try:
                p.terminate()
                p.wait(timeout=5)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
        shutil.rmtree(tier_dir, ignore_errors=True)


def main():
    work_root = "/tmp/p13_phase_g"
    shutil.rmtree(work_root, ignore_errors=True)
    os.makedirs(work_root, exist_ok=True)
    scripts = write_scripts(os.path.join(work_root, "scripts"))
    out = {"tiers": []}
    for tier in TIERS:
        print(f"=== Phase G Tier: {tier['name']} ===", flush=True)
        res = run_tier(tier, scripts, work_root)
        out["tiers"].append(res)
        print(
            f"  50c tps={res['single_tenant_50c']['tps']} p50={res['single_tenant_50c']['p50_ms']}ms "
            f"p99={res['single_tenant_50c']['p99_ms']}ms rss={res['rss_mb']['total']}MB "
            f"(srv={res['rss_mb']['air_server']} re={res['rss_mb']['air_read_engine']} "
            f"gw={res['rss_mb']['air_gateway']} pg={res['rss_mb']['postgres_all_tenants']}) "
            f"mt_100c_tps={res['multitenant_4x25c']['total_tps']}",
            flush=True,
        )
    with open(OUT_PATH, "w") as f:
        json.dump(out, f, indent=2)
    print(f"Saved Phase G results to {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
