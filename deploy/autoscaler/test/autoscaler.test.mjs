import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { GatewayAutoscalerDO } from "../src/durable_object.mjs";
import worker from "../src/index.mjs";

function makeSQLiteStorage() {
  const db = new DatabaseSync(":memory:");
  return {
    sql: {
      exec(query, ...params) {
        if (params.length === 0 && query.includes(";")) {
          db.exec(query);
          return { toArray: () => [] };
        }
        const stmt = db.prepare(query);
        const upper = query.trim().toUpperCase();
        if (upper.startsWith("SELECT")) {
          const rows = stmt.all(...params);
          return { toArray: () => rows };
        }
        stmt.run(...params);
        return { toArray: () => [] };
      },
    },
  };
}

function makeMockHarness(overrides = {}) {
  const state = {
    machines: overrides.machines || [
      {
        id: "m_baseline",
        region: "sin",
        state: "started",
        private_ip: "fdaa:0:1::1",
        probe_url: "http://probe-m_baseline",
        created_at: "2026-11-01T00:00:00Z",
        config: {
          image: "registry.fly.io/air-pg-gateway:latest",
          guest: { cpu_kind: "shared", cpus: 2, memory_mb: 256 },
          env: { AIR_REQUIRE_TLS: "true" },
          services: [{ protocol: "tcp", internal_port: 5432, autostop: "off", autostart: false }],
          mounts: [{ volume: "vol_base", path: "/var/lib/air-gateway" }],
        },
      },
    ],
    volumes: overrides.volumes || [
      { id: "vol_base", name: "air_gateway_metering", region: "sin", size_gb: 1, attached_machine_id: "m_baseline" },
    ],
    prom: overrides.prom || { activeConns: 900, maxConns: 1024, cpuPct: 82 },
    probeHandlers: overrides.probeHandlers || {},
    flyCalls: [],
    nextVolSeq: 1,
    nextMachSeq: 1,
  };

  const mockFetch = async (url, init = {}) => {
    const u = new URL(url);
    const method = (init.method || "GET").toUpperCase();

    // Prometheus
    if (u.hostname === "prom.local") {
      const q = u.searchParams.get("query") || "";
      let val = 0;
      if (q.includes("connections_active")) val = state.prom.activeConns;
      else if (q.includes("connections_max")) val = state.prom.maxConns;
      else if (q.includes("cpu_utilization")) val = state.prom.cpuPct;
      return new Response(JSON.stringify({ status: "success", data: { result: [{ value: [0, String(val)] }] } }), { status: 200 });
    }

    // PG TLS Probe
    if (u.hostname === "pgprobe.local") {
      const body = JSON.parse(init.body || "{}");
      if (overrides.pgProbeFail) {
        return new Response(JSON.stringify({ ok: false, tls_verified: false }), { status: 200 });
      }
      return new Response(JSON.stringify({ ok: true, tls_verified: true, machine_id: body.machine_id }), { status: 200 });
    }

    // Machine private probes
    if (u.hostname.startsWith("probe-")) {
      const mId = u.hostname.replace("probe-", "");
      if (state.probeHandlers[mId]) {
        return state.probeHandlers[mId](u.pathname, init);
      }
      if (u.pathname === "/readyz") {
        return new Response(JSON.stringify({
          ready: true,
          checks: { tls: { ok: true }, discovery: { ok: true }, backends: { ok: true }, metering: { ok: true } },
        }), { status: 200 });
      }
      if (u.pathname === "/internal/v1/cordon" || u.pathname === "/internal/v1/uncordon") {
        return new Response(JSON.stringify({ status: "ok" }), { status: 200 });
      }
      if (u.pathname === "/internal/v1/drain") {
        const body = JSON.parse(init.body || "{}");
        return new Response(JSON.stringify({
          op_id: body.op_id,
          gateway_id: mId,
          drained: true,
          metering_confirmed: true,
          pending_batches: 0,
          quarantined_count: 0,
          legacy_pending_count: 0,
          blocked: false,
        }), { status: 200 });
      }
    }

    // Fly Machines API
    if (u.hostname === "api.machines.dev") {
      state.flyCalls.push({ method, path: u.pathname, body: init.body ? JSON.parse(init.body) : null });
      if (u.pathname.endsWith("/volumes") && method === "GET") {
        return new Response(JSON.stringify(state.volumes), { status: 200 });
      }
      if (u.pathname.endsWith("/volumes") && method === "POST") {
        if (overrides.failVolumeCreate) {
          return new Response("timeout", { status: 504 });
        }
        const req = JSON.parse(init.body);
        const vol = { id: `vol_burst_${state.nextVolSeq++}`, name: req.name, region: req.region, size_gb: req.size_gb, attached_machine_id: null };
        state.volumes.push(vol);
        return new Response(JSON.stringify(vol), { status: 200 });
      }
      if (u.pathname.endsWith("/machines") && method === "GET") {
        return new Response(JSON.stringify(state.machines), { status: 200 });
      }
      if (u.pathname.endsWith("/machines") && method === "POST") {
        if (overrides.failMachineCreate) {
          return new Response("ambiguous timeout", { status: 504 });
        }
        const req = JSON.parse(init.body);
        const id = `m_burst_${state.nextMachSeq++}`;
        const m = {
          id,
          region: req.region,
          state: "started",
          private_ip: `fdaa:0:1::${10 + state.nextMachSeq}`,
          probe_url: `http://probe-${id}`,
          created_at: new Date().toISOString(),
          config: req.config,
        };
        const volId = req.config?.mounts?.[0]?.volume;
        const v = state.volumes.find((x) => x.id === volId);
        if (v) v.attached_machine_id = id;
        state.machines.push(m);
        return new Response(JSON.stringify(m), { status: 200 });
      }
      const mMatch = u.pathname.match(/\/machines\/([^/]+)(\/stop)?$/);
      if (mMatch) {
        const id = mMatch[1];
        const isStop = Boolean(mMatch[2]);
        const idx = state.machines.findIndex((m) => m.id === id);
        if (idx < 0) return new Response("not found", { status: 404 });
        if (isStop && method === "POST") {
          state.machines[idx].state = "stopped";
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
        if (method === "GET") {
          return new Response(JSON.stringify(state.machines[idx]), { status: 200 });
        }
        if (method === "POST") {
          const req = JSON.parse(init.body);
          state.machines[idx].config = req.config;
          return new Response(JSON.stringify(state.machines[idx]), { status: 200 });
        }
        if (method === "DELETE") {
          const volId = state.machines[idx].config?.mounts?.[0]?.volume;
          const v = state.volumes.find((x) => x.id === volId);
          if (v) v.attached_machine_id = null;
          state.machines.splice(idx, 1);
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
      }
    }
    return new Response("unexpected fetch " + url, { status: 500 });
  };

  const storage = makeSQLiteStorage();
  const env = {
    FLY_APP_NAME: "air-pg-gateway",
    FLY_API_TOKEN: "test-fly-token",
    AIR_INTERNAL_TOKEN: "test-internal-token",
    PROMETHEUS_URL: "http://prom.local",
    PG_PROBE_URL: "http://pgprobe.local/probe",
    AUTOSCALER_MODE: "decision_only",
    AUTO_ADD_ENABLED: "true",
    AUTO_REMOVE_ENABLED: "false",
    SUSTAINED_HIGH_TICKS: "3",
    SUSTAINED_LOW_TICKS: "3",
    SCALE_OUT_COOLDOWN_SEC: "60",
    SCALE_IN_COOLDOWN_SEC: "60",
    __FETCH: mockFetch,
    ...(overrides.env || {}),
  };

  const doInstance = new GatewayAutoscalerDO({ storage }, env);
  return { doInstance, state, env };
}

test("decision_only mode never mutates Fly Machines or Volumes under sustained load", async () => {
  const { doInstance, state } = makeMockHarness();
  for (let i = 0; i < 4; i++) {
    const res = await doInstance.handleTick(1000 + i * 60000);
    if (i >= 2) {
      assert.equal(res.action, "decision_scale_out");
      assert.equal(res.mode, "decision_only");
    }
  }
  assert.equal(state.machines.length, 1);
  assert.equal(state.flyCalls.filter((c) => c.method === "POST" || c.method === "DELETE").length, 0);
});

test("auto-add provisions dedicated volume + cordoned burst Machine, verifies readiness + TLS PG, then admits", async () => {
  const { doInstance, state } = makeMockHarness({
    env: { AUTOSCALER_MODE: "enforce" },
  });
  let res;
  for (let i = 0; i < 3; i++) {
    res = await doInstance.handleTick(10000 + i * 60000);
  }
  assert.equal(res.action, "scale_out_admitted");
  assert.equal(state.machines.length, 2);
  assert.equal(state.volumes.length, 2);

  const burst = state.machines.find((m) => m.id !== "m_baseline");
  assert.equal(burst.config.guest.cpus, 4);
  assert.equal(burst.config.guest.memory_mb, 1024);
  assert.equal(burst.config.skip_service_registration, false);

  // Baseline size was never mutated.
  const base = state.machines.find((m) => m.id === "m_baseline");
  assert.equal(base.config.guest.cpus, 2);
  assert.equal(base.config.guest.memory_mb, 256);
});

test("ambiguous Machine creation timeout marks unresolved and blocks duplicate resource creation", async () => {
  const { doInstance, state } = makeMockHarness({
    env: { AUTOSCALER_MODE: "enforce", SUSTAINED_HIGH_TICKS: "1" },
    failMachineCreate: true,
  });
  await assert.rejects(() => doInstance.handleTick(10000));
  assert.equal(state.volumes.length, 2);
  assert.equal(state.machines.length, 1);

  // Next tick reconciles volume_ready -> unresolved without creating a second volume or machine!
  const res2 = await doInstance.handleTick(70000);
  assert.equal(res2.action, "scale_out_unresolved");
  const res3 = await doInstance.handleTick(130000);
  assert.equal(res3.action, "blocked_unresolved_op");
  assert.equal(state.volumes.length, 2, "must not create duplicate volume on retry");
  assert.equal(state.machines.length, 1);
});

test("failed readiness leaves baseline serving and does not admit burst Machine", async () => {
  const { doInstance, state } = makeMockHarness({
    env: { AUTOSCALER_MODE: "enforce", SUSTAINED_HIGH_TICKS: "1" },
    pgProbeFail: true,
  });
  const res = await doInstance.handleTick(10000);
  assert.equal(res.action, "scale_out_failed_readiness");
  const burst = state.machines.find((m) => m.id !== "m_baseline");
  assert.equal(burst.config.skip_service_registration, true, "burst must remain cordoned on failed readiness");
});

test("auto-remove drains burst Machine, verifies operation-bound ack, deletes Machine, and retains volume", async () => {
  const { doInstance, state } = makeMockHarness({
    env: { AUTOSCALER_MODE: "enforce", AUTO_REMOVE_ENABLED: "true", SUSTAINED_LOW_TICKS: "2" },
    prom: { activeConns: 20, maxConns: 1024, cpuPct: 10 },
    machines: [
      {
        id: "m_baseline",
        region: "sin",
        state: "started",
        private_ip: "fdaa:0:1::1",
        probe_url: "http://probe-m_baseline",
        created_at: "2026-11-01T00:00:00Z",
        config: { guest: { cpu_kind: "shared", cpus: 2, memory_mb: 256 }, mounts: [{ volume: "vol_base" }] },
      },
      {
        id: "m_burst_1",
        region: "sin",
        state: "started",
        private_ip: "fdaa:0:1::11",
        probe_url: "http://probe-m_burst_1",
        created_at: "2026-11-02T00:00:00Z",
        config: {
          guest: { cpu_kind: "shared", cpus: 4, memory_mb: 1024 },
          mounts: [{ volume: "vol_burst_1" }],
          metadata: { air_role: "burst" },
        },
      },
    ],
    volumes: [
      { id: "vol_base", name: "air_gateway_metering", region: "sin", size_gb: 1, attached_machine_id: "m_baseline" },
      { id: "vol_burst_1", name: "air_gw_burst_1", region: "sin", size_gb: 1, attached_machine_id: "m_burst_1" },
    ],
  });

  await doInstance.handleTick(1000000);
  const res = await doInstance.handleTick(1060000);
  assert.equal(res.action, "scale_in_deleted_machine_retained_volume");
  assert.equal(state.machines.length, 1);
  assert.equal(state.machines[0].id, "m_baseline");
  // Volume MUST be retained!
  assert.equal(state.volumes.length, 2);
  assert.equal(state.volumes.find((v) => v.id === "vol_burst_1").attached_machine_id, null);
});

test("drain timeout or unconfirmed metering restores admission and never deletes Machine or volume", async () => {
  const { doInstance, state } = makeMockHarness({
    env: { AUTOSCALER_MODE: "enforce", AUTO_REMOVE_ENABLED: "true", SUSTAINED_LOW_TICKS: "1" },
    prom: { activeConns: 20, maxConns: 1024, cpuPct: 10 },
    machines: [
      {
        id: "m_baseline",
        region: "sin",
        state: "started",
        private_ip: "fdaa:0:1::1",
        probe_url: "http://probe-m_baseline",
        created_at: "2026-11-01T00:00:00Z",
        config: { guest: { cpu_kind: "shared", cpus: 2, memory_mb: 256 }, mounts: [{ volume: "vol_base" }] },
      },
      {
        id: "m_burst_1",
        region: "sin",
        state: "started",
        private_ip: "fdaa:0:1::11",
        probe_url: "http://probe-m_burst_1",
        created_at: "2026-11-02T00:00:00Z",
        config: {
          guest: { cpu_kind: "shared", cpus: 4, memory_mb: 1024 },
          mounts: [{ volume: "vol_burst_1" }],
          metadata: { air_role: "burst" },
        },
      },
    ],
    volumes: [
      { id: "vol_base", name: "air_gateway_metering", region: "sin", size_gb: 1, attached_machine_id: "m_baseline" },
      { id: "vol_burst_1", name: "air_gw_burst_1", region: "sin", size_gb: 1, attached_machine_id: "m_burst_1" },
    ],
    probeHandlers: {
      m_burst_1: (path) => {
        if (path === "/internal/v1/drain") {
          return new Response(JSON.stringify({
            op_id: "wrong_or_incomplete",
            drained: false,
            metering_confirmed: false,
            pending_batches: 2,
            error: "drain timeout with active_clients=1",
          }), { status: 409 });
        }
        return new Response(JSON.stringify({ status: "ok" }), { status: 200 });
      },
    },
  });

  const res = await doInstance.handleTick(1000000);
  assert.equal(res.action, "scale_in_deferred");
  assert.equal(state.machines.length, 2, "must not delete Machine on unconfirmed drain");
  assert.equal(state.volumes.length, 2, "must not delete volume");
  const burst = state.machines.find((m) => m.id === "m_burst_1");
  assert.equal(burst.config.skip_service_registration, false, "must restore admission on deferred drain");
});

test("worker fetch requires X-Air-Internal-Token except for /healthz", async () => {
  const resHealth = await worker.fetch(new Request("https://worker.local/healthz"), { AUTOSCALER_MODE: "decision_only" });
  assert.equal(resHealth.status, 200);
  const resUnauth = await worker.fetch(new Request("https://worker.local/status"), { AIR_INTERNAL_TOKEN: "secret" });
  assert.equal(resUnauth.status, 401);
});
