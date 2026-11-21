#!/usr/bin/env python3
"""
AIR Phase 13 — End-to-End Load Testing & Resource Benchmark Harness.
Compares:
  A: PostgreSQL 15 primary-only
  B: PostgreSQL 15 primary + PostgreSQL 15 physical streaming read replica
  C: AIR PostgreSQL 15 write engine + Pageserver + SQLite Read Server + Go Gateway
"""

import glob
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request

PG_BIN = "/usr/lib/postgresql/15/bin"
AIR_BIN = "/tmp/air-bin"
RESULTS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_http(url, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.15)
    raise RuntimeError(f"timeout waiting for {url}")


def get_json(url, token=None):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode())


def read_proc_stats(pids):
    """Return {pid: {rss_kb, cpu_ticks, read_bytes, write_bytes, syscr, syscw}}."""
    out = {}
    clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    for pid in pids:
        if pid is None:
            continue
        try:
            with open(f"/proc/{pid}/stat") as f:
                parts = f.read().split()
            utime = int(parts[13])
            stime = int(parts[14])
            cpu_sec = (utime + stime) / clk_tck
            rss_kb = 0
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1])
                        break
            rbytes = wbytes = syscr = syscw = 0
            try:
                with open(f"/proc/{pid}/io") as f:
                    for line in f:
                        k, v = line.split(":")
                        v = int(v.strip())
                        if k == "read_bytes":
                            rbytes = v
                        elif k == "write_bytes":
                            wbytes = v
                        elif k == "syscr":
                            syscr = v
                        elif k == "syscw":
                            syscw = v
            except Exception:
                pass
            out[pid] = {
                "rss_kb": rss_kb,
                "cpu_sec": cpu_sec,
                "read_bytes": rbytes,
                "write_bytes": wbytes,
                "syscr": syscr,
                "syscw": syscw,
            }
        except Exception:
            pass
    return out


def find_postgres_pids(data_dir_substr):
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\x00", b" ").decode(errors="ignore")
            if "postgres" in cmd and data_dir_substr in cmd:
                pids.append(int(entry))
        except Exception:
            pass
    return pids


def run_pgbench(host, port, user, password, db, clients, duration_s, script_path=None, builtin_flag=None, log_dir="/tmp/pgb_logs"):
    shutil.rmtree(log_dir, ignore_errors=True)
    os.makedirs(log_dir, exist_ok=True)
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    threads = min(clients, 4)
    cmd = [
        os.path.join(PG_BIN, "pgbench"),
        "-h", host, "-p", str(port), "-U", user,
        "-c", str(clients), "-j", str(threads), "-T", str(duration_s),
        "-n", "-l", "--log-prefix", os.path.join(log_dir, "log"),
    ]
    if builtin_flag:
        cmd.append(builtin_flag)
    if script_path:
        cmd.extend(["-f", script_path])
    cmd.append(db)

    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    out = proc.stdout + "\n" + proc.stderr

    tps = 0.0
    failed = 0
    processed = 0
    m = re.search(r"tps = ([\d.]+)", out)
    if m:
        tps = float(m.group(1))
    m = re.search(r"number of transactions actually processed:\s*(\d+)", out)
    if m:
        processed = int(m.group(1))
    m = re.search(r"number of failed transactions:\s*(\d+)", out)
    if m:
        failed = int(m.group(1))

    latencies_ms = []
    for lf in glob.glob(os.path.join(log_dir, "log*")):
        with open(lf) as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        latencies_ms.append(int(parts[2]) / 1000.0)
                    except ValueError:
                        pass
    latencies_ms.sort()

    def pct(p):
        if not latencies_ms:
            return 0.0
        idx = min(len(latencies_ms) - 1, int((len(latencies_ms) - 1) * p / 100.0))
        return round(latencies_ms[idx], 3)

    return {
        "rc": proc.returncode,
        "tps": round(tps, 2),
        "processed": processed,
        "failed": failed,
        "error_rate": round(failed / max(1, processed + failed), 6),
        "p50_ms": pct(50),
        "p95_ms": pct(95),
        "p99_ms": pct(99),
        "max_ms": round(latencies_ms[-1], 3) if latencies_ms else 0.0,
    }


def setup_schema(host, port, user, password, db):
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    # 1. Standard pgbench scale 1 (100k rows)
    subprocess.check_call([
        os.path.join(PG_BIN, "pgbench"), "-i", "-n", "-s", "1", "-q",
        "-h", host, "-p", str(port), "-U", user, db
    ], env=env)
    # 2. Additional workload tables: items (1000 rows), users (500 rows), orders (2000 rows)
    sql = """
    CREATE TABLE IF NOT EXISTS items (id int4 PRIMARY KEY, cat int4, val text);
    INSERT INTO items SELECT g, g % 10, 'item_' || g FROM generate_series(1, 1000) g ON CONFLICT DO NOTHING;
    CREATE TABLE IF NOT EXISTS users (id int4 PRIMARY KEY, name text, bal int4);
    INSERT INTO users SELECT g, 'user_' || g, g * 10 FROM generate_series(1, 500) g ON CONFLICT DO NOTHING;
    CREATE TABLE IF NOT EXISTS orders (id int4 PRIMARY KEY, user_id int4, amount int4);
    INSERT INTO orders SELECT g, (g % 500) + 1, (g % 100) + 10 FROM generate_series(1, 2000) g ON CONFLICT DO NOTHING;
    """
    subprocess.check_call([
        os.path.join(PG_BIN, "psql"),
        f"host={host} port={port} user={user} dbname={db} sslmode=disable",
        "-c", sql
    ], env=env, stdout=subprocess.DEVNULL)


def write_scripts(script_dir):
    os.makedirs(script_dir, exist_ok=True)
    scripts = {
        "ro_items": "\\set id random(1, 1000)\nSELECT id, val FROM items WHERE id = :id;\n",
        "ro_pgbench": "\\set aid random(1, 100000)\nSELECT abalance FROM pgbench_accounts WHERE aid = :aid;\n",
        "read_heavy_90_10": (
            "\\set r random(1, 10)\n"
            "\\set id random(1, 1000)\n"
            "\\if :r <= 9\n"
            "SELECT id, val FROM items WHERE id = :id;\n"
            "\\else\n"
            "UPDATE items SET cat = cat + 1 WHERE id = :id;\n"
            "\\endif\n"
        ),
        "rw_50_50": (
            "\\set r random(1, 2)\n"
            "\\set id random(1, 1000)\n"
            "\\if :r = 1\n"
            "SELECT id, val FROM items WHERE id = :id;\n"
            "\\else\n"
            "UPDATE items SET cat = cat + 1 WHERE id = :id;\n"
            "\\endif\n"
        ),
        "write_heavy_20_80": (
            "\\set r random(1, 5)\n"
            "\\set id random(1, 1000)\n"
            "\\if :r = 1\n"
            "SELECT id, val FROM items WHERE id = :id;\n"
            "\\else\n"
            "UPDATE items SET cat = cat + 1 WHERE id = :id;\n"
            "\\endif\n"
        ),
        "join_heavy": (
            "\\set uid random(1, 500)\n"
            "SELECT u.id, u.name, o.id, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = :uid;\n"
        ),
        "range_scan": (
            "\\set lo random(1, 900)\n"
            "SELECT id, cat, val FROM items WHERE id >= :lo AND id < :lo + 50;\n"
        ),
    }
    paths = {}
    for name, body in scripts.items():
        p = os.path.join(script_dir, f"{name}.sql")
        with open(p, "w") as f:
            f.write(body)
        paths[name] = p
    return paths


def main():
    work_root = "/tmp/p13_bench"
    shutil.rmtree(work_root, ignore_errors=True)
    os.makedirs(work_root, exist_ok=True)
    scripts = write_scripts(os.path.join(work_root, "scripts"))

    results = {
        "env": {
            "cpus": os.cpu_count(),
            "pg_version": "15.19",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "concurrency_sweep": [],
        "workload_matrix": [],
        "multitenant": [],
    }

    # =========================================================================
    # TOPOLOGY A: PostgreSQL 15 Primary-Only
    # =========================================================================
    print("=== Topology A: PostgreSQL 15 Primary-Only ===", flush=True)
    pg_a_dir = os.path.join(work_root, "pg_a")
    os.makedirs(pg_a_dir, exist_ok=True)
    subprocess.check_call([os.path.join(PG_BIN, "initdb"), "-D", pg_a_dir, "-U", "postgres", "-A", "trust", "--no-sync"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    with open(os.path.join(pg_a_dir, "postgresql.conf"), "a") as f:
        f.write("\nmax_connections = 650\nshared_buffers = 64MB\nwal_level = replica\nmax_wal_senders = 6\n")
    with open(os.path.join(pg_a_dir, "pg_hba.conf"), "a") as f:
        f.write("\nhost all all 127.0.0.1/32 trust\nhost replication all 127.0.0.1/32 trust\n")
    pg_a_port = free_port()
    p_pg_a = subprocess.Popen([os.path.join(PG_BIN, "postgres"), "-D", pg_a_dir, "-k", pg_a_dir, "-h", "127.0.0.1", "-p", str(pg_a_port)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2.0)
    try:
        setup_schema("127.0.0.1", pg_a_port, "postgres", "", "postgres")
        for c in [1, 10, 50, 100, 250, 500]:
            pids = find_postgres_pids(pg_a_dir)
            s0 = read_proc_stats(pids)
            t0 = time.time()
            res = run_pgbench("127.0.0.1", pg_a_port, "postgres", "", "postgres", c, 6, script_path=scripts["ro_items"])
            dt = time.time() - t0
            s1 = read_proc_stats(pids)
            cpu_pct = sum(s1.get(p, {}).get("cpu_sec", 0) - s0.get(p, {}).get("cpu_sec", 0) for p in pids) / max(0.1, dt) * 100.0
            rss_mb = sum(s1.get(p, {}).get("rss_kb", 0) for p in pids) / 1024.0
            entry = {"topology": "A_pg_primary", "workload": "ro_items", "clients": c, **res, "cpu_pct": round(cpu_pct, 1), "rss_mb": round(rss_mb, 1)}
            results["concurrency_sweep"].append(entry)
            print(f"  [A] ro_items c={c}: tps={res['tps']} p50={res['p50_ms']}ms p99={res['p99_ms']}ms cpu={cpu_pct:.1f}% rss={rss_mb:.1f}MB err={res['error_rate']}", flush=True)

        for wname in ["ro_pgbench", "read_heavy_90_10", "rw_50_50", "write_heavy_20_80", "join_heavy", "range_scan"]:
            pids = find_postgres_pids(pg_a_dir)
            s0 = read_proc_stats(pids)
            t0 = time.time()
            res = run_pgbench("127.0.0.1", pg_a_port, "postgres", "", "postgres", 50, 6, script_path=scripts[wname])
            dt = time.time() - t0
            s1 = read_proc_stats(pids)
            cpu_pct = sum(s1.get(p, {}).get("cpu_sec", 0) - s0.get(p, {}).get("cpu_sec", 0) for p in pids) / max(0.1, dt) * 100.0
            rss_mb = sum(s1.get(p, {}).get("rss_kb", 0) for p in pids) / 1024.0
            entry = {"topology": "A_pg_primary", "workload": wname, "clients": 50, **res, "cpu_pct": round(cpu_pct, 1), "rss_mb": round(rss_mb, 1)}
            results["workload_matrix"].append(entry)
            print(f"  [A] {wname} c=50: tps={res['tps']} p50={res['p50_ms']}ms p99={res['p99_ms']}ms", flush=True)

        # =====================================================================
        # TOPOLOGY B: PostgreSQL 15 Primary + Streaming Read Replica
        # =====================================================================
        print("=== Topology B: PG15 Primary + PG15 Read Replica ===", flush=True)
        pg_b_dir = os.path.join(work_root, "pg_b_replica")
        pg_b_port = free_port()
        subprocess.check_call([
            os.path.join(PG_BIN, "pg_basebackup"),
            "-h", "127.0.0.1", "-p", str(pg_a_port), "-U", "postgres",
            "-D", pg_b_dir, "-Fp", "-Xs", "-R", "-c", "fast"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(os.path.join(pg_b_dir, "postgresql.conf"), "a") as f:
            f.write(f"\nhot_standby = on\nport = {pg_b_port}\nmax_connections = 650\nshared_buffers = 64MB\n")
        p_pg_b = subprocess.Popen([os.path.join(PG_BIN, "postgres"), "-D", pg_b_dir, "-k", pg_b_dir, "-h", "127.0.0.1", "-p", str(pg_b_port)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2.0)
        try:
            for c in [1, 10, 50, 100, 250, 500]:
                pids_a = find_postgres_pids(pg_a_dir)
                pids_b = find_postgres_pids(pg_b_dir)
                pids = pids_a + pids_b
                s0 = read_proc_stats(pids)
                t0 = time.time()
                res = run_pgbench("127.0.0.1", pg_b_port, "postgres", "", "postgres", c, 6, script_path=scripts["ro_items"])
                dt = time.time() - t0
                s1 = read_proc_stats(pids)
                cpu_pct = sum(s1.get(p, {}).get("cpu_sec", 0) - s0.get(p, {}).get("cpu_sec", 0) for p in pids) / max(0.1, dt) * 100.0
                rss_mb = sum(s1.get(p, {}).get("rss_kb", 0) for p in pids) / 1024.0
                entry = {"topology": "B_pg_replica", "workload": "ro_items", "clients": c, **res, "cpu_pct": round(cpu_pct, 1), "rss_mb": round(rss_mb, 1)}
                results["concurrency_sweep"].append(entry)
                print(f"  [B] ro_items c={c}: tps={res['tps']} p50={res['p50_ms']}ms p99={res['p99_ms']}ms cpu={cpu_pct:.1f}% rss={rss_mb:.1f}MB", flush=True)

            for wname in ["ro_pgbench", "join_heavy", "range_scan"]:
                pids = find_postgres_pids(pg_a_dir) + find_postgres_pids(pg_b_dir)
                s0 = read_proc_stats(pids)
                t0 = time.time()
                res = run_pgbench("127.0.0.1", pg_b_port, "postgres", "", "postgres", 50, 6, script_path=scripts[wname])
                dt = time.time() - t0
                s1 = read_proc_stats(pids)
                cpu_pct = sum(s1.get(p, {}).get("cpu_sec", 0) - s0.get(p, {}).get("cpu_sec", 0) for p in pids) / max(0.1, dt) * 100.0
                rss_mb = sum(s1.get(p, {}).get("rss_kb", 0) for p in pids) / 1024.0
                entry = {"topology": "B_pg_replica", "workload": wname, "clients": 50, **res, "cpu_pct": round(cpu_pct, 1), "rss_mb": round(rss_mb, 1)}
                results["workload_matrix"].append(entry)
                print(f"  [B] {wname} c=50: tps={res['tps']} p50={res['p50_ms']}ms p99={res['p99_ms']}ms", flush=True)
        finally:
            p_pg_b.terminate()
            p_pg_b.wait(timeout=5)
    finally:
        p_pg_a.terminate()
        p_pg_a.wait(timeout=5)

    # =========================================================================
    # TOPOLOGY C: AIR (PG15 Write Engine + Pageserver + SQLite Read Server + Gateway)
    # =========================================================================
    print("=== Topology C: AIR Full Architecture ===", flush=True)
    srv_data = os.path.join(work_root, "air_s")
    re_data = os.path.join(work_root, "air_r")
    re_cache = os.path.join(work_root, "air_c")
    ps_sock = os.path.join(srv_data, "ps.sock")
    for d in (srv_data, re_data, re_cache):
        os.makedirs(d, exist_ok=True)

    meta_p, http_p, grpc_p, gw_adm_grpc_p = free_port(), free_port(), free_port(), free_port()
    re_http_p, re_pg_p, gw_pg_p, gw_adm_p = free_port(), free_port(), free_port(), free_port()

    air_procs = []
    try:
        srv_log = open(os.path.join(work_root, "air_srv.log"), "w")
        p_srv = subprocess.Popen([
            os.path.join(AIR_BIN, "air-server"),
            "-data-dir", srv_data, "-meta-dir", os.path.join(srv_data, "meta"), "-master-key-path", os.path.join(srv_data, "master.key"),
            "-pg-bin-dir", PG_BIN, "-meta-port", str(meta_p), "-http-addr", f"127.0.0.1:{http_p}",
            "-grpc-addr", f"127.0.0.1:{grpc_p}", "-gateway-admin-grpc-addr", f"127.0.0.1:{gw_adm_grpc_p}", "-pageserver",
            "-pageserver-socket", ps_sock, "-pageserver-cache-mb", "128", "-bootstrap-account-id", "acct_p13",
            "-bootstrap-api-key", "air_p13_secret_key", "-internal-token", "air_p13_internal",
        ], stdout=srv_log, stderr=subprocess.STDOUT)
        air_procs.append(p_srv)
        wait_http(f"http://127.0.0.1:{http_p}/healthz")

        re_log = open(os.path.join(work_root, "air_re.log"), "w")
        p_re = subprocess.Popen([
            os.path.join(AIR_BIN, "air-read-engine"),
            "-data-dir", re_data, "-http-addr", f"127.0.0.1:{re_http_p}", "-listen-host", "127.0.0.1",
            "-pg-port", str(re_pg_p), "-pageserver-socket", ps_sock, "-cache-dir", re_cache, "-shared-buffers", "64MB",
        ], stdout=re_log, stderr=subprocess.STDOUT)
        air_procs.append(p_re)
        wait_http(f"http://127.0.0.1:{re_http_p}/healthz")

        gw_log = open(os.path.join(work_root, "air_gw.log"), "w")
        p_gw = subprocess.Popen([
            os.path.join(AIR_BIN, "air-gateway"),
            "--gateway-addr", f"127.0.0.1:{gw_pg_p}", "--gateway-admin-addr", f"127.0.0.1:{gw_adm_p}",
            "--gateway-admin-grpc-addr", f"127.0.0.1:{gw_adm_grpc_p}", "--db-grpc-addr", f"127.0.0.1:{grpc_p}",
            "--control-plane-url", f"http://127.0.0.1:{http_p}", "--internal-token", "air_p13_internal",
            "--backend-host", "127.0.0.1", "--pool-mode", "--pool-max-backends", "32",
            "--max-client-conns", "1024", "--max-tenant-client-conns", "600",
            "--read-engine-host", "127.0.0.1", "--read-engine-port", str(re_pg_p),
        ], stdout=gw_log, stderr=subprocess.STDOUT)
        air_procs.append(p_gw)
        wait_http(f"http://127.0.0.1:{gw_adm_p}/healthz")

        # Provision primary bench tenant + 3 extra tenants for multi-tenant test
        tenants = {}
        for idx, tid in enumerate(["bench_t0", "bench_t1", "bench_t2", "bench_t3"]):
            req = urllib.request.Request(
                f"http://127.0.0.1:{http_p}/v1/databases",
                data=json.dumps({"tenant_id": tid}).encode(),
                headers={"Authorization": "Bearer air_p13_secret_key", "Idempotency-Key": f"idem-{tid}", "Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                prov = json.loads(r.read().decode())
            sock_dir = os.path.join(srv_data, "tenants", "active", tid, "sock")
            subprocess.check_call([
                os.path.join(PG_BIN, "psql"),
                f"host={sock_dir} port={prov['direct_port']} user=postgres dbname={tid}",
                "-c", "GRANT ALL ON SCHEMA public TO app_user;"
            ], stdout=subprocess.DEVNULL)
            tenants[tid] = prov
            if idx == 0:
                setup_schema("127.0.0.1", gw_pg_p, prov["username"], prov["password"], tid)
            else:
                env = os.environ.copy()
                env["PGPASSWORD"] = prov["password"]
                subprocess.check_call([
                    os.path.join(PG_BIN, "psql"),
                    f"host=127.0.0.1 port={gw_pg_p} user={prov['username']} dbname={tid} sslmode=disable",
                    "-c", "CREATE TABLE items (id int4 PRIMARY KEY, cat int4, val text); INSERT INTO items SELECT g, g%10, 't_'||g FROM generate_series(1,1000) g;"
                ], env=env, stdout=subprocess.DEVNULL)
        time.sleep(0.5)

        t0_prov = tenants["bench_t0"]
        all_air_pids = lambda: [p_srv.pid, p_re.pid, p_gw.pid] + find_postgres_pids(srv_data)

        for c in [1, 10, 50, 100, 250, 500]:
            pids = all_air_pids()
            s0 = read_proc_stats(pids)
            m0 = get_json(f"http://127.0.0.1:{re_http_p}/metrics")
            t0 = time.time()
            res = run_pgbench("127.0.0.1", gw_pg_p, t0_prov["username"], t0_prov["password"], "bench_t0", c, 6, script_path=scripts["ro_items"])
            dt = time.time() - t0
            m1 = get_json(f"http://127.0.0.1:{re_http_p}/metrics")
            s1 = read_proc_stats(pids)

            cpu_pct = sum(s1.get(p, {}).get("cpu_sec", 0) - s0.get(p, {}).get("cpu_sec", 0) for p in pids) / max(0.1, dt) * 100.0
            rss_mb = sum(s1.get(p, {}).get("rss_kb", 0) for p in pids) / 1024.0
            l1_hits = m1["cache"]["L1Hits"] - m0["cache"]["L1Hits"]
            l1_miss = m1["cache"]["L1Misses"] - m0["cache"]["L1Misses"]
            l2_hits = m1["cache"]["L2Hits"] - m0["cache"]["L2Hits"]
            ps_reqs = m1["pageserver"]["RequestsTotal"] - m0["pageserver"]["RequestsTotal"]
            q_tot = m1["queries_total"] - m0["queries_total"]
            q_fb = m1["total_fallbacks"] - m0["total_fallbacks"]
            hit_ratio = round((l1_hits + l2_hits) / max(1, l1_hits + l1_miss), 4)
            fb_ratio = round(q_fb / max(1, res["processed"]), 4)

            entry = {
                "topology": "C_air", "workload": "ro_items", "clients": c, **res,
                "cpu_pct": round(cpu_pct, 1), "rss_mb": round(rss_mb, 1),
                "re_queries": q_tot, "re_fallbacks": q_fb, "fallback_ratio": fb_ratio,
                "cache_hit_ratio": hit_ratio, "ps_req_per_sec": round(ps_reqs / max(0.1, dt), 1),
                "re_rss_mb": round(s1.get(p_re.pid, {}).get("rss_kb", 0) / 1024.0, 1),
                "gw_rss_mb": round(s1.get(p_gw.pid, {}).get("rss_kb", 0) / 1024.0, 1),
                "srv_rss_mb": round(s1.get(p_srv.pid, {}).get("rss_kb", 0) / 1024.0, 1),
            }
            results["concurrency_sweep"].append(entry)
            print(f"  [C] ro_items c={c}: tps={res['tps']} p50={res['p50_ms']}ms p99={res['p99_ms']}ms cpu={cpu_pct:.1f}% rss={rss_mb:.1f}MB hit={hit_ratio} fb={fb_ratio}", flush=True)

        for wname in ["ro_pgbench", "read_heavy_90_10", "rw_50_50", "write_heavy_20_80", "join_heavy", "range_scan"]:
            pids = all_air_pids()
            s0 = read_proc_stats(pids)
            m0 = get_json(f"http://127.0.0.1:{re_http_p}/metrics")
            t0 = time.time()
            res = run_pgbench("127.0.0.1", gw_pg_p, t0_prov["username"], t0_prov["password"], "bench_t0", 50, 6, script_path=scripts[wname])
            dt = time.time() - t0
            m1 = get_json(f"http://127.0.0.1:{re_http_p}/metrics")
            s1 = read_proc_stats(pids)
            cpu_pct = sum(s1.get(p, {}).get("cpu_sec", 0) - s0.get(p, {}).get("cpu_sec", 0) for p in pids) / max(0.1, dt) * 100.0
            rss_mb = sum(s1.get(p, {}).get("rss_kb", 0) for p in pids) / 1024.0
            q_tot = m1["queries_total"] - m0["queries_total"]
            q_fb = m1["total_fallbacks"] - m0["total_fallbacks"]
            l1_hits = m1["cache"]["L1Hits"] - m0["cache"]["L1Hits"]
            l1_miss = m1["cache"]["L1Misses"] - m0["cache"]["L1Misses"]
            entry = {
                "topology": "C_air", "workload": wname, "clients": 50, **res,
                "cpu_pct": round(cpu_pct, 1), "rss_mb": round(rss_mb, 1),
                "re_queries": q_tot, "re_fallbacks": q_fb,
                "fallback_ratio": round(q_fb / max(1, res["processed"]), 4),
                "cache_hit_ratio": round(l1_hits / max(1, l1_hits + l1_miss), 4),
            }
            results["workload_matrix"].append(entry)
            print(f"  [C] {wname} c=50: tps={res['tps']} p50={res['p50_ms']}ms p99={res['p99_ms']}ms re_q={q_tot} fb={q_fb}", flush=True)

        # Multi-tenant concurrent test: 4 tenants x 25 clients = 100 concurrent clients
        print("  [C] Running 4-tenant concurrent workload (4 x 25 = 100 clients)...", flush=True)
        mt_procs = []
        t0 = time.time()
        for tid, prov in tenants.items():
            env = os.environ.copy()
            env["PGPASSWORD"] = prov["password"]
            mt_procs.append((tid, subprocess.Popen([
                os.path.join(PG_BIN, "pgbench"), "-h", "127.0.0.1", "-p", str(gw_pg_p),
                "-U", prov["username"], "-c", "25", "-j", "1", "-T", "6", "-n",
                "-f", scripts["ro_items"], tid
            ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)))
        mt_total_tps = 0.0
        for tid, mp in mt_procs:
            out, _ = mp.communicate()
            m = re.search(r"tps = ([\d.]+)", out)
            tps = float(m.group(1)) if m else 0.0
            mt_total_tps += tps
            results["multitenant"].append({"tenant": tid, "clients": 25, "tps": round(tps, 2)})
        print(f"  [C] Multi-tenant total TPS (100 clients across 4 tenants): {mt_total_tps:.2f}", flush=True)

    finally:
        for p in reversed(air_procs):
            p.terminate()
        for p in reversed(air_procs):
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved results to {RESULTS_PATH}", flush=True)


if __name__ == "__main__":
    main()
