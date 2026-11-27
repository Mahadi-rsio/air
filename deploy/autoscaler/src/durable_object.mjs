/**
 * SQLite-backed Durable Object for metering-safe Gateway horizontal autoscaling.
 *
 * Persists:
 *  - operations: operation intent, state machine, machine_id, volume_id, errors
 *  - machine_volumes: Machine-to-volume mappings & roles (baseline vs burst)
 *  - autoscaler_state: cooldowns, sustained load counters, baseline machine ID
 */
export class GatewayAutoscalerDO {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.sql = ctx.storage.sql;
    this.initialized = false;
  }

  ensureSchema() {
    if (this.initialized) return;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS operations (
        op_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        state TEXT NOT NULL,
        machine_id TEXT NOT NULL DEFAULT '',
        volume_id TEXT NOT NULL DEFAULT '',
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        last_error TEXT NOT NULL DEFAULT '',
        details_json TEXT NOT NULL DEFAULT '{}'
      );
      CREATE TABLE IF NOT EXISTS machine_volumes (
        machine_id TEXT PRIMARY KEY,
        volume_id TEXT NOT NULL,
        role TEXT NOT NULL,
        region TEXT NOT NULL,
        size_gb INTEGER NOT NULL,
        vm_size TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS autoscaler_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );
    `);
    this.initialized = true;
  }

  getState(key, fallback = null) {
    const rows = this.sql.exec(`SELECT value FROM autoscaler_state WHERE key = ?`, key).toArray();
    return rows.length > 0 ? rows[0].value : fallback;
  }

  setState(key, value, now = Date.now()) {
    this.sql.exec(
      `INSERT INTO autoscaler_state (key, value, updated_at) VALUES (?, ?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
      key,
      String(value),
      now
    );
  }

  getIntState(key, fallback = 0) {
    const v = this.getState(key, null);
    if (v === null) return fallback;
    const n = Number.parseInt(v, 10);
    return Number.isFinite(n) ? n : fallback;
  }

  createOperation(opId, kind, state, now = Date.now(), details = {}) {
    this.sql.exec(
      `INSERT INTO operations (op_id, kind, state, created_at, updated_at, details_json)
       VALUES (?, ?, ?, ?, ?, ?)`,
      opId,
      kind,
      state,
      now,
      now,
      JSON.stringify(details)
    );
  }

  updateOperation(opId, patch, now = Date.now()) {
    const existing = this.sql.exec(`SELECT * FROM operations WHERE op_id = ?`, opId).toArray()[0];
    if (!existing) return;
    const state = patch.state ?? existing.state;
    const machineId = patch.machine_id ?? existing.machine_id;
    const volumeId = patch.volume_id ?? existing.volume_id;
    const lastError = patch.last_error ?? existing.last_error;
    const detailsJson = patch.details ? JSON.stringify(patch.details) : existing.details_json;
    this.sql.exec(
      `UPDATE operations SET state = ?, machine_id = ?, volume_id = ?, last_error = ?, details_json = ?, updated_at = ?
       WHERE op_id = ?`,
      state,
      machineId,
      volumeId,
      lastError,
      detailsJson,
      now,
      opId
    );
  }

  getPendingScaleOutOps() {
    return this.sql.exec(
      `SELECT * FROM operations WHERE kind = 'scale_out' AND state IN ('intent', 'volume_ready', 'machine_created', 'ready_verified', 'unresolved')
       ORDER BY created_at ASC`
    ).toArray();
  }

  upsertMachineVolume(row, now = Date.now()) {
    this.sql.exec(
      `INSERT INTO machine_volumes (machine_id, volume_id, role, region, size_gb, vm_size, state, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(machine_id) DO UPDATE SET
         volume_id = excluded.volume_id,
         role = excluded.role,
         region = excluded.region,
         size_gb = excluded.size_gb,
         vm_size = excluded.vm_size,
         state = excluded.state,
         updated_at = excluded.updated_at`,
      row.machine_id,
      row.volume_id || "",
      row.role,
      row.region || "",
      row.size_gb || 1,
      row.vm_size || "",
      row.state,
      now,
      now
    );
  }

  cfg() {
    const env = this.env || {};
    return {
      appName: env.FLY_APP_NAME || "air-pg-gateway",
      flyBase: (env.FLY_API_BASE || "https://api.machines.dev/v1").replace(/\/+$/, ""),
      region: env.FLY_REGION || "sin",
      mode: env.AUTOSCALER_MODE || "decision_only",
      autoAddEnabled: (env.AUTO_ADD_ENABLED ?? "true") === "true",
      autoRemoveEnabled: (env.AUTO_REMOVE_ENABLED ?? "false") === "true",
      maxReplicas: Number.parseInt(env.MAX_GATEWAY_REPLICAS || "2", 10),
      sustainedHighTicks: Number.parseInt(env.SUSTAINED_HIGH_TICKS || "3", 10),
      sustainedLowTicks: Number.parseInt(env.SUSTAINED_LOW_TICKS || "5", 10),
      scaleOutCooldownMs: Number.parseInt(env.SCALE_OUT_COOLDOWN_SEC || "300", 10) * 1000,
      scaleInCooldownMs: Number.parseInt(env.SCALE_IN_COOLDOWN_SEC || "600", 10) * 1000,
      highConnUtilPct: Number.parseFloat(env.HIGH_CONN_UTIL_PCT || "75"),
      lowConnUtilPct: Number.parseFloat(env.LOW_CONN_UTIL_PCT || "25"),
      minSpareConns: Number.parseInt(env.MIN_SPARE_CONNS || "64", 10),
      burstCpuKind: env.BURST_CPU_KIND || "shared",
      burstCpus: Number.parseInt(env.BURST_CPUS || "4", 10),
      burstMemoryMb: Number.parseInt(env.BURST_MEMORY_MB || "1024", 10),
      burstVolumeSizeGb: Number.parseInt(env.BURST_VOLUME_SIZE_GB || "1", 10),
      drainTimeoutMs: Number.parseInt(env.DRAIN_TIMEOUT_MS || "30000", 10),
      readinessMaxAttempts: Number.parseInt(env.READINESS_MAX_ATTEMPTS || "8", 10),
      readinessRetryMs: Number.parseInt(env.READINESS_RETRY_MS || "1500", 10),
      baselineMachineId: env.BASELINE_MACHINE_ID || "",
      flyToken: env.FLY_API_TOKEN || "",
      internalToken: env.AIR_INTERNAL_TOKEN || "",
      promUrl: (env.PROMETHEUS_URL || "").replace(/\/+$/, ""),
      pgProbeUrl: (env.PG_PROBE_URL || "").replace(/\/+$/, ""),
    };
  }

  async fetch(request) {
    this.ensureSchema();
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/status") {
      return Response.json({
        operations: this.sql.exec(`SELECT * FROM operations ORDER BY created_at DESC LIMIT 50`).toArray(),
        machine_volumes: this.sql.exec(`SELECT * FROM machine_volumes ORDER BY created_at ASC`).toArray(),
        state: this.sql.exec(`SELECT * FROM autoscaler_state`).toArray(),
      });
    }
    if (request.method === "POST" && url.pathname === "/tick") {
      const body = await request.json().catch(() => ({}));
      const result = await this.handleTick(body.nowMs || Date.now());
      return Response.json(result);
    }
    return new Response("not found", { status: 404 });
  }

  async flyRequest(path, init = {}) {
    const c = this.cfg();
    const headers = new Headers(init.headers || {});
    if (c.flyToken) headers.set("Authorization", `Bearer ${c.flyToken}`);
    if (init.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
    const fetchFn = this.env.__FETCH || fetch;
    const res = await fetchFn(`${c.flyBase}/apps/${encodeURIComponent(c.appName)}${path}`, {
      ...init,
      headers,
    });
    const text = await res.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw: text };
    }
    if (!res.ok) {
      const err = new Error(`Fly API ${init.method || "GET"} ${path} failed (${res.status}): ${text}`);
      err.status = res.status;
      throw err;
    }
    return data;
  }

  async probeMachinePrivate(machine, path, init = {}) {
    const c = this.cfg();
    const fetchFn = this.env.__FETCH || fetch;
    const headers = new Headers(init.headers || {});
    if (c.internalToken) headers.set("X-Air-Internal-Token", c.internalToken);
    if (init.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");

    // In Fly 6PN, each Machine has a private_ip (IPv6).
    const host = machine.private_ip.includes(":") && !machine.private_ip.startsWith("[")
      ? `[${machine.private_ip}]`
      : machine.private_ip;
    const probeBase = machine.probe_url || `http://${host}:8082`;
    const res = await fetchFn(`${probeBase}${path}`, { ...init, headers });
    const text = await res.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw: text };
    }
    return { ok: res.ok, status: res.status, data };
  }

  async verifyTLSPostgresProbe(machine) {
    const c = this.cfg();
    if (!c.pgProbeUrl) {
      // Fall back to private /readyz TLS check when no external PG probe service is configured.
      const r = await this.probeMachinePrivate(machine, "/readyz");
      return Boolean(r.ok && r.data?.ready && r.data?.checks?.tls?.ok);
    }
    const fetchFn = this.env.__FETCH || fetch;
    const headers = { "Content-Type": "application/json" };
    if (c.internalToken) headers["X-Air-Internal-Token"] = c.internalToken;
    const res = await fetchFn(c.pgProbeUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({ machine_id: machine.id, private_ip: machine.private_ip, port: 5432, require_tls: true }),
    });
    if (!res.ok) return false;
    const body = await res.json().catch(() => ({}));
    return Boolean(body.ok && body.tls_verified);
  }

  async queryPrometheus() {
    const c = this.cfg();
    if (!c.promUrl) return { activeConns: 0, maxConns: 1024, cpuPct: 0, ok: false };
    const fetchFn = this.env.__FETCH || fetch;
    const headers = {};
    if (c.internalToken) headers["Authorization"] = `Bearer ${c.internalToken}`;
    const q = async (expr) => {
      const res = await fetchFn(`${c.promUrl}/api/v1/query?query=${encodeURIComponent(expr)}`, { headers });
      if (!res.ok) throw new Error(`Prometheus query failed (${res.status})`);
      const json = await res.json();
      const val = json?.data?.result?.[0]?.value?.[1];
      return val !== undefined ? Number.parseFloat(val) : 0;
    };
    const [activeConns, maxConns, cpuPct] = await Promise.all([
      q(`sum(air_gateway_connections_active{app="${c.appName}"})`),
      q(`sum(air_gateway_connections_max{app="${c.appName}"})`),
      q(`avg(air_gateway_cpu_utilization_pct{app="${c.appName}"})`),
    ]);
    return {
      activeConns: Number.isFinite(activeConns) ? activeConns : 0,
      maxConns: Number.isFinite(maxConns) && maxConns > 0 ? maxConns : 1024,
      cpuPct: Number.isFinite(cpuPct) ? cpuPct : 0,
      ok: true,
    };
  }

  identifyBaseline(machines) {
    const c = this.cfg();
    if (c.baselineMachineId) {
      const m = machines.find((x) => x.id === c.baselineMachineId);
      if (m) return m.id;
    }
    const stored = this.getState("baseline_machine_id", "");
    if (stored && machines.some((x) => x.id === stored)) {
      return stored;
    }
    // Select oldest non-burst Machine as baseline and persist it.
    const candidates = machines
      .filter((m) => m.config?.metadata?.air_role !== "burst")
      .sort((a, b) => (a.created_at || "").localeCompare(b.created_at || ""));
    const baseline = candidates[0] || machines[0];
    if (baseline) {
      this.setState("baseline_machine_id", baseline.id);
      return baseline.id;
    }
    return "";
  }

  async reconcileInventory(machines, volumes, now) {
    const c = this.cfg();
    const pending = this.getPendingScaleOutOps();
    for (const op of pending) {
      const matchingMachine = machines.find((m) => m.config?.metadata?.air_op_id === op.op_id || m.id === op.machine_id);
      const matchingVol = volumes.find((v) => v.name === `air_gw_burst_${op.op_id}` || v.id === op.volume_id);

      if (matchingMachine && matchingVol) {
        this.upsertMachineVolume({
          machine_id: matchingMachine.id,
          volume_id: matchingVol.id,
          role: "burst",
          region: matchingMachine.region || c.region,
          size_gb: matchingVol.size_gb || c.burstVolumeSizeGb,
          vm_size: `${matchingMachine.config?.guest?.cpu_kind || c.burstCpuKind}-${matchingMachine.config?.guest?.cpus || c.burstCpus}`,
          state: matchingMachine.state || "started",
        }, now);
        if (op.state === "intent" || op.state === "volume_ready" || op.state === "unresolved") {
          this.updateOperation(op.op_id, { state: "machine_created", machine_id: matchingMachine.id, volume_id: matchingVol.id }, now);
        }
      } else if (!matchingMachine && matchingVol) {
        if (op.state !== "unresolved") {
          this.updateOperation(op.op_id, { state: "volume_ready", volume_id: matchingVol.id }, now);
        }
      } else if (op.state === "intent") {
        // Ambiguous timeout during creation: mark unresolved so we do NOT blindly create duplicates.
        this.updateOperation(op.op_id, { state: "unresolved", last_error: "reconcile found intent without confirmed inventory" }, now);
      }
    }

    const baselineId = this.identifyBaseline(machines);
    for (const m of machines) {
      const mountVolId = m.config?.mounts?.[0]?.volume || "";
      const role = m.id === baselineId ? "baseline" : (m.config?.metadata?.air_role || "burst");
      this.upsertMachineVolume({
        machine_id: m.id,
        volume_id: mountVolId,
        role,
        region: m.region || c.region,
        size_gb: 1,
        vm_size: `${m.config?.guest?.cpu_kind || "shared"}-${m.config?.guest?.cpus || 2}`,
        state: m.state || "unknown",
      }, now);
    }
    return baselineId;
  }

  async handleTick(now = Date.now()) {
    this.ensureSchema();
    const c = this.cfg();
    // Single-flight tick guard inside DO.
    if (this.inTick) {
      return { action: "skipped_concurrent_tick" };
    }
    this.inTick = true;
    try {
      const [machines, volumes, metrics] = await Promise.all([
        this.flyRequest("/machines"),
        this.flyRequest("/volumes"),
        this.queryPrometheus(),
      ]);

      const baselineId = await this.reconcileInventory(machines, volumes, now);
      const activeMachines = machines.filter((m) => m.state === "started" || m.state === "starting");
      const burstMachines = activeMachines.filter((m) => m.id !== baselineId);

      // Block new creates if any scale_out operation is unresolved.
      const unresolved = this.sql.exec(
        `SELECT op_id, state FROM operations WHERE kind = 'scale_out' AND state IN ('intent', 'volume_ready', 'machine_created', 'unresolved')`
      ).toArray();

      const utilPct = metrics.maxConns > 0 ? (metrics.activeConns / metrics.maxConns) * 100 : 0;
      const spareConns = Math.max(0, metrics.maxConns - metrics.activeConns);
      const isHighLoad = metrics.ok && (utilPct >= c.highConnUtilPct || spareConns < c.minSpareConns || metrics.cpuPct >= 75);
      const isLowLoad = metrics.ok && utilPct <= c.lowConnUtilPct && spareConns >= c.minSpareConns * 2 && metrics.cpuPct <= 35;

      let highTicks = this.getIntState("sustained_high_ticks", 0);
      let lowTicks = this.getIntState("sustained_low_ticks", 0);
      highTicks = isHighLoad ? highTicks + 1 : 0;
      lowTicks = isLowLoad ? lowTicks + 1 : 0;
      this.setState("sustained_high_ticks", highTicks, now);
      this.setState("sustained_low_ticks", lowTicks, now);

      const lastOut = this.getIntState("last_scale_out_at", 0);
      const lastIn = this.getIntState("last_scale_in_at", 0);

      // 1. Resume or evaluate Scale-Out (auto-add)
      if (unresolved.length > 0) {
        if (c.mode !== "enforce") {
          return { action: "blocked_unresolved_op", mode: c.mode, unresolved };
        }
        const op = unresolved[0];
        if (op.state === "unresolved") {
          return { action: "blocked_unresolved_op", mode: c.mode, op_id: op.op_id };
        }
        return await this.continueScaleOut(op, machines, volumes, now);
      }

      if (
        c.autoAddEnabled &&
        highTicks >= c.sustainedHighTicks &&
        activeMachines.length < c.maxReplicas &&
        (lastOut === 0 || now - lastOut >= c.scaleOutCooldownMs)
      ) {
        if (c.mode !== "enforce") {
          return { action: "decision_scale_out", mode: "decision_only", utilPct, spareConns, highTicks };
        }
        return await this.executeScaleOut(machines, volumes, now);
      }

      // 2. Evaluate Scale-In (auto-remove, behind separate feature flag)
      if (
        c.autoRemoveEnabled &&
        burstMachines.length > 0 &&
        lowTicks >= c.sustainedLowTicks &&
        (lastIn === 0 || now - lastIn >= c.scaleInCooldownMs)
      ) {
        const targetBurst = burstMachines[0];
        const baseline = activeMachines.find((m) => m.id === baselineId);
        if (!baseline) {
          return { action: "defer_scale_in_no_healthy_baseline" };
        }
        if (c.mode !== "enforce") {
          return { action: "decision_scale_in", mode: "decision_only", target: targetBurst.id, utilPct, lowTicks };
        }
        return await this.executeScaleIn(targetBurst, baseline, now);
      }

      return {
        action: "noop",
        mode: c.mode,
        activeMachines: activeMachines.length,
        baselineId,
        utilPct,
        spareConns,
        highTicks,
        lowTicks,
      };
    } finally {
      this.inTick = false;
    }
  }

  async executeScaleOut(machines, volumes, now) {
    const c = this.cfg();
    const opId = `out_${now}_${Math.random().toString(16).slice(2, 8)}`;
    // Persist intent BEFORE creating volume or Machine.
    this.createOperation(opId, "scale_out", "intent", now, { region: c.region, cpus: c.burstCpus, memory_mb: c.burstMemoryMb });

    // Select an existing retained detached burst volume in region, or provision a new dedicated volume.
    let vol = volumes.find(
      (v) => v.region === c.region && !v.attached_machine_id && v.name?.startsWith("air_gw_burst_")
    );
    if (!vol) {
      vol = await this.flyRequest("/volumes", {
        method: "POST",
        body: JSON.stringify({
          name: `air_gw_burst_${opId}`,
          region: c.region,
          size_gb: c.burstVolumeSizeGb,
          encrypted: true,
        }),
      });
    }
    if (!vol?.id) {
      this.updateOperation(opId, { state: "unresolved", last_error: "volume creation returned no id" }, now);
      return { action: "scale_out_unresolved", op_id: opId };
    }
    this.updateOperation(opId, { state: "volume_ready", volume_id: vol.id }, now);

    // Clone baseline config shape without mutating baseline; create cordoned with skip_service_registration.
    const baseline = machines.find((m) => m.id === this.identifyBaseline(machines));
    const baseEnv = { ...(baseline?.config?.env || {}) };
    const burstEnv = {
      ...baseEnv,
      AIR_BILLING_ENABLED: "true",
      AIR_METERING_LOG_DIR: "/var/lib/air-gateway/metering",
      AIR_GATEWAY_CORDONED: "true",
      AIR_GATEWAY_DISCOVERY_HOST: `${c.appName}.internal`,
      AIR_GATEWAY_PEER_ADMIN_PORT: "9093",
      AIR_GATEWAY_PRIVATE_HTTP_ADDR: "[::]:8082",
    };

    const created = await this.flyRequest("/machines", {
      method: "POST",
      body: JSON.stringify({
        region: c.region,
        skip_launch: false,
        config: {
          image: baseline?.config?.image || "registry.fly.io/air-pg-gateway:latest",
          env: burstEnv,
          guest: { cpu_kind: c.burstCpuKind, cpus: c.burstCpus, memory_mb: c.burstMemoryMb },
          mounts: [{ volume: vol.id, path: "/var/lib/air-gateway" }],
          services: (baseline?.config?.services || []).map((s) => ({ ...s, autostop: "off", autostart: false })),
          files: baseline?.config?.files || [],
          metadata: {
            air_role: "burst",
            air_op_id: opId,
          },
          skip_service_registration: true,
        },
      }),
    });
    if (!created?.id) {
      this.updateOperation(opId, { state: "unresolved", last_error: "machine create returned no id" }, now);
      return { action: "scale_out_unresolved", op_id: opId };
    }
    this.updateOperation(opId, { state: "machine_created", machine_id: created.id, volume_id: vol.id }, now);
    return await this.verifyAndAdmitBurst(opId, created, vol, now);
  }

  async continueScaleOut(op, machines, volumes, now) {
    const machine = machines.find((m) => m.id === op.machine_id);
    const vol = volumes.find((v) => v.id === op.volume_id);
    if (!machine || !vol) {
      this.updateOperation(op.op_id, { state: "unresolved", last_error: "missing machine or volume during resume" }, now);
      return { action: "scale_out_unresolved", op_id: op.op_id };
    }
    return await this.verifyAndAdmitBurst(op.op_id, machine, vol, now);
  }

  async verifyAndAdmitBurst(opId, machine, vol, now) {
    const c = this.cfg();
    // Poll readiness until the Gateway process finishes booting and passes subsystem checks.
    let subsystemsOK = false;
    for (let attempt = 0; attempt < c.readinessMaxAttempts; attempt++) {
      const ready = await this.probeMachinePrivate(machine, "/readyz").catch(() => ({ ok: false }));
      // Note: machine starts cordoned, so check individual subsystems rather than overall ready=false from cordon.
      const checks = ready?.data?.checks;
      subsystemsOK = Boolean(
        checks && checks.tls?.ok && checks.discovery?.ok && checks.backends?.ok && checks.metering?.ok
      );
      if (subsystemsOK) break;
      if (attempt + 1 < c.readinessMaxAttempts && c.readinessRetryMs > 0) {
        await new Promise((r) => setTimeout(r, c.readinessRetryMs));
      }
    }
    const tlsPGOK = subsystemsOK ? await this.verifyTLSPostgresProbe(machine).catch(() => false) : false;

    if (!subsystemsOK || !tlsPGOK) {
      this.updateOperation(opId, { state: "failed", last_error: "burst readiness or TLS PG probe failed" }, now);
      return { action: "scale_out_failed_readiness", op_id: opId, machine_id: machine.id };
    }
    this.updateOperation(opId, { state: "ready_verified" }, now);

    const uncordon = await this.probeMachinePrivate(machine, "/internal/v1/uncordon", { method: "POST" });
    if (!uncordon.ok) {
      this.updateOperation(opId, { state: "failed", last_error: "uncordon failed" }, now);
      return { action: "scale_out_failed_uncordon", op_id: opId, machine_id: machine.id };
    }

    await this.flyRequest(`/machines/${encodeURIComponent(machine.id)}`, {
      method: "POST",
      body: JSON.stringify({
        config: {
          ...machine.config,
          env: { ...(machine.config?.env || {}), AIR_GATEWAY_CORDONED: "false" },
          skip_service_registration: false,
        },
      }),
    });

    this.updateOperation(opId, { state: "admitted" }, now);
    this.setState("last_scale_out_at", now, now);
    this.setState("sustained_high_ticks", 0, now);
    this.upsertMachineVolume({
      machine_id: machine.id,
      volume_id: vol.id,
      role: "burst",
      region: machine.region || this.cfg().region,
      size_gb: vol.size_gb || this.cfg().burstVolumeSizeGb,
      vm_size: `${this.cfg().burstCpuKind}-${this.cfg().burstCpus}`,
      state: "admitted",
    }, now);

    return { action: "scale_out_admitted", op_id: opId, machine_id: machine.id, volume_id: vol.id };
  }

  async executeScaleIn(burstMachine, baselineMachine, now) {
    const c = this.cfg();
    // Double-check baseline protection.
    if (burstMachine.id === this.identifyBaseline([burstMachine, baselineMachine])) {
      return { action: "refused_baseline_removal", machine_id: burstMachine.id };
    }
    // Verify baseline is healthy before draining burst.
    const baseReady = await this.probeMachinePrivate(baselineMachine, "/readyz").catch(() => ({ ok: false }));
    if (!baseReady.ok || !baseReady.data?.ready) {
      return { action: "defer_scale_in_baseline_unhealthy" };
    }

    const opId = `in_${now}_${Math.random().toString(16).slice(2, 8)}`;
    const volId = burstMachine.config?.mounts?.[0]?.volume || "";
    this.createOperation(opId, "scale_in", "draining", now, { machine_id: burstMachine.id, volume_id: volId });
    this.updateOperation(opId, { machine_id: burstMachine.id, volume_id: volId }, now);

    // 1. Cordon via Fly skip_service_registration + Gateway /internal/v1/cordon.
    await this.flyRequest(`/machines/${encodeURIComponent(burstMachine.id)}`, {
      method: "POST",
      body: JSON.stringify({
        config: { ...burstMachine.config, skip_service_registration: true },
      }),
    });
    await this.probeMachinePrivate(burstMachine, "/internal/v1/cordon", { method: "POST" });

    // 2. Drain sessions & seal/flush spool, requiring fresh operation-bound acknowledgement.
    const drainRes = await this.probeMachinePrivate(burstMachine, "/internal/v1/drain", {
      method: "POST",
      body: JSON.stringify({ op_id: opId, timeout_ms: c.drainTimeoutMs }),
    }).catch((err) => ({ ok: false, data: { error: err.message } }));

    const ack = drainRes.data || {};
    const ackValid =
      drainRes.ok &&
      ack.op_id === opId &&
      ack.drained === true &&
      ack.metering_confirmed === true &&
      ack.pending_batches === 0 &&
      ack.quarantined_count === 0 &&
      ack.legacy_pending_count === 0 &&
      ack.blocked !== true;

    if (!ackValid) {
      // Drain failed or timed out or metering unconfirmed -> restore admission if healthy, NEVER force disconnect.
      const unc = await this.probeMachinePrivate(burstMachine, "/internal/v1/uncordon", { method: "POST" }).catch(() => ({ ok: false }));
      if (unc.ok) {
        await this.flyRequest(`/machines/${encodeURIComponent(burstMachine.id)}`, {
          method: "POST",
          body: JSON.stringify({
            config: { ...burstMachine.config, skip_service_registration: false },
          }),
        }).catch(() => {});
      }
      this.updateOperation(opId, {
        state: "deferred",
        last_error: ack.error || "drain or metering confirmation failed",
        details: ack,
      }, now);
      return { action: "scale_in_deferred", op_id: opId, machine_id: burstMachine.id, reason: ack.error || "unconfirmed" };
    }

    this.updateOperation(opId, { state: "flushed", details: ack }, now);

    // 3. Stop Machine and verify stopped before deletion.
    await this.flyRequest(`/machines/${encodeURIComponent(burstMachine.id)}/stop`, { method: "POST" });
    const inspected = await this.flyRequest(`/machines/${encodeURIComponent(burstMachine.id)}`);
    if (inspected?.state !== "stopped") {
      this.updateOperation(opId, { state: "deferred", last_error: `machine state ${inspected?.state} != stopped` }, now);
      return { action: "scale_in_deferred_not_stopped", op_id: opId, machine_id: burstMachine.id };
    }
    this.updateOperation(opId, { state: "stopped" }, now);

    // 4. Delete stopped Machine, RETAINING detached volume in initial release.
    await this.flyRequest(`/machines/${encodeURIComponent(burstMachine.id)}`, { method: "DELETE" });
    this.updateOperation(opId, { state: "deleted" }, now);
    this.setState("last_scale_in_at", now, now);
    this.setState("sustained_low_ticks", 0, now);
    if (volId) {
      this.upsertMachineVolume({
        machine_id: burstMachine.id,
        volume_id: volId,
        role: "burst",
        region: burstMachine.region || c.region,
        size_gb: c.burstVolumeSizeGb,
        vm_size: `${c.burstCpuKind}-${c.burstCpus}`,
        state: "retained_detached",
      }, now);
    }

    return {
      action: "scale_in_deleted_machine_retained_volume",
      op_id: opId,
      machine_id: burstMachine.id,
      retained_volume_id: volId,
    };
  }
}
