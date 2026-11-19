import axios, { AxiosError } from "axios";

const AIR_SERVER_URL = (process.env.AIR_SERVER_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const AIR_SERVER_API_KEY = process.env.AIR_SERVER_API_KEY || "";
const AIR_INTERNAL_TOKEN = process.env.AIR_INTERNAL_TOKEN || "";
const AIR_GATEWAY_ADMIN_URL = (process.env.AIR_GATEWAY_ADMIN_URL || "http://127.0.0.1:8081").replace(/\/$/, "");

export interface AirProvisionResponse {
  account_id: string;
  tenant_id: string;
  generation?: string;
  database: string;
  username: string;
  password?: string;
  direct_port: number;
  system_identifier: string;
  fencing_epoch: number;
  state: string;
}

export interface AirDatabaseInfo {
  account_id: string;
  tenant_id: string;
  generation?: string;
  direct_port: number;
  system_identifier: string;
  fencing_epoch: number;
  state: string;
}

export interface AirBackupManifest {
  backup_id: string;
  tenant_id: string;
  type: string;
  status: string;
  start_lsn?: string;
  end_lsn?: string;
  cipher_bytes?: number;
  wal_count?: number;
  created_at?: string;
  verified?: boolean;
}

export interface AirOffloadStatus {
  op_id?: string;
  tenant_id: string;
  generation?: string;
  state: string;
  uploaded_objects?: number;
  failed_objects?: number;
  retry_count?: number;
  last_error?: string;
  updated_at?: string;
}

export interface AirTenantRoute {
  tenant_id: string;
  state: string;
  host: string;
  port: number;
  sock_dir?: string;
  app_password?: string;
}

export interface AirGatewayMetrics {
  tenants?: Record<
    string,
    {
      active_client_conns?: number;
      active_backends?: number;
      query_count?: number;
      error_count?: number;
      latency_us?: {
        min?: number;
        avg?: number;
        max?: number;
        p50?: number;
        p95?: number;
        p99?: number;
      };
      bytes_in?: number;
      bytes_out?: number;
      rows_returned?: number;
    }
  >;
  global?: {
    active_client_conns?: number;
    total_queries?: number;
    total_errors?: number;
  };
}

function controlHeaders(extra?: Record<string, string>) {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...extra,
  };
  if (AIR_SERVER_API_KEY) {
    headers["Authorization"] = `Bearer ${AIR_SERVER_API_KEY}`;
  }
  return headers;
}

function internalHeaders() {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (AIR_INTERNAL_TOKEN) {
    headers["X-Air-Internal-Token"] = AIR_INTERNAL_TOKEN;
  }
  return headers;
}

export class AirBackendUnavailableError extends Error {
  constructor(
    message: string,
    public statusCode = 503
  ) {
    super(message);
  }
}

function wrapAxiosError(err: unknown, context: string): never {
  if (err instanceof AxiosError) {
    if (err.code === "ECONNREFUSED" || err.code === "ETIMEDOUT" || !err.response) {
      throw new AirBackendUnavailableError(
        `AIR backend unreachable (${context}): ${err.message}`,
        503
      );
    }
    const msg =
      (err.response.data as { error?: string } | undefined)?.error ||
      err.response.statusText ||
      err.message;
    throw new AirBackendUnavailableError(msg, err.response.status);
  }
  throw err;
}

export const airServerClient = {
  async healthz(): Promise<{ ok: boolean; error?: string }> {
    try {
      const res = await axios.get(`${AIR_SERVER_URL}/healthz`, { timeout: 2500 });
      return { ok: res.data?.status === "ok" };
    } catch (err) {
      return {
        ok: false,
        error: err instanceof Error ? err.message : "air-server unreachable",
      };
    }
  },

  async provisionDatabase(
    tenantId: string,
    idempotencyKey: string
  ): Promise<AirProvisionResponse> {
    try {
      const res = await axios.post<AirProvisionResponse>(
        `${AIR_SERVER_URL}/v1/databases`,
        { tenant_id: tenantId },
        {
          headers: controlHeaders({ "Idempotency-Key": idempotencyKey }),
          timeout: 30000,
        }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, "POST /v1/databases");
    }
  },

  async getDatabase(tenantId: string): Promise<AirDatabaseInfo> {
    try {
      const res = await axios.get<AirDatabaseInfo>(
        `${AIR_SERVER_URL}/v1/databases/${encodeURIComponent(tenantId)}`,
        { headers: controlHeaders(), timeout: 5000 }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, `GET /v1/databases/${tenantId}`);
    }
  },

  async deleteDatabase(tenantId: string): Promise<void> {
    try {
      await axios.delete(
        `${AIR_SERVER_URL}/v1/databases/${encodeURIComponent(tenantId)}`,
        { headers: controlHeaders(), timeout: 15000 }
      );
    } catch (err) {
      wrapAxiosError(err, `DELETE /v1/databases/${tenantId}`);
    }
  },

  async listBackups(tenantId: string): Promise<AirBackupManifest[]> {
    try {
      const res = await axios.get<{ backups: AirBackupManifest[] }>(
        `${AIR_SERVER_URL}/v1/databases/${encodeURIComponent(tenantId)}/backups`,
        { headers: controlHeaders(), timeout: 10000 }
      );
      return res.data.backups || [];
    } catch (err) {
      wrapAxiosError(err, `GET /v1/databases/${tenantId}/backups`);
    }
  },

  async triggerBackup(
    tenantId: string,
    type: "FULL" | "INCR" | "WAL"
  ): Promise<AirBackupManifest> {
    try {
      const res = await axios.post<AirBackupManifest>(
        `${AIR_SERVER_URL}/v1/databases/${encodeURIComponent(tenantId)}/backups`,
        { type },
        { headers: controlHeaders(), timeout: 60000 }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, `POST /v1/databases/${tenantId}/backups`);
    }
  },

  async verifyBackup(tenantId: string, backupId: string): Promise<unknown> {
    try {
      const res = await axios.post(
        `${AIR_SERVER_URL}/v1/databases/${encodeURIComponent(tenantId)}/backups/${encodeURIComponent(backupId)}/verify`,
        { stream_hash: true, deep_restore: false },
        { headers: controlHeaders(), timeout: 30000 }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, `POST /v1/databases/${tenantId}/backups/${backupId}/verify`);
    }
  },

  async restoreDatabase(
    tenantId: string,
    payload: {
      backup_id?: string;
      target_lsn?: string;
      target_time?: string;
      new_tenant_id: string;
    }
  ): Promise<unknown> {
    try {
      const res = await axios.post(
        `${AIR_SERVER_URL}/v1/databases/${encodeURIComponent(tenantId)}/restore`,
        payload,
        { headers: controlHeaders(), timeout: 60000 }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, `POST /v1/databases/${tenantId}/restore`);
    }
  },

  async getOffloadStatus(tenantId: string): Promise<AirOffloadStatus> {
    try {
      const res = await axios.get<AirOffloadStatus>(
        `${AIR_SERVER_URL}/v1/databases/${encodeURIComponent(tenantId)}/offload`,
        { headers: controlHeaders(), timeout: 5000 }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, `GET /v1/databases/${tenantId}/offload`);
    }
  },

  async getTenantRoute(tenantId: string): Promise<AirTenantRoute> {
    try {
      const res = await axios.get<AirTenantRoute>(
        `${AIR_SERVER_URL}/internal/v1/tenants/${encodeURIComponent(tenantId)}/route`,
        { headers: internalHeaders(), timeout: 5000 }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, `GET /internal/v1/tenants/${tenantId}/route`);
    }
  },

  async lifecycleAction(
    tenantId: string,
    action: "suspend" | "start" | "lock" | "unlock"
  ): Promise<unknown> {
    try {
      const res = await axios.post(
        `${AIR_SERVER_URL}/internal/v1/databases/${encodeURIComponent(tenantId)}/${action}`,
        {},
        { headers: internalHeaders(), timeout: 15000 }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, `POST /internal/v1/databases/${tenantId}/${action}`);
    }
  },

  async getGatewayMetrics(): Promise<AirGatewayMetrics> {
    try {
      const res = await axios.get<AirGatewayMetrics>(
        `${AIR_GATEWAY_ADMIN_URL}/internal/v1/metrics`,
        { headers: internalHeaders(), timeout: 3000 }
      );
      return res.data;
    } catch (err) {
      wrapAxiosError(err, "GET /internal/v1/metrics");
    }
  },
};
