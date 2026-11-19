import { Client, QueryResult } from "pg";
import { decryptSecret } from "./crypto";
import { airServerClient, AirBackendUnavailableError } from "./air-client";

export interface ProjectConnectionTarget {
  tenantId: string;
  databaseName: string;
  username: string;
  passwordEnc: string;
  directPort: number;
}

export async function withProjectPgClient<T>(
  project: ProjectConnectionTarget,
  fn: (client: Client) => Promise<T>
): Promise<T> {
  let host = process.env.AIR_GATEWAY_HOST || "127.0.0.1";
  let port = project.directPort || Number(process.env.AIR_GATEWAY_PG_PORT || 5432);
  let password = decryptSecret(project.passwordEnc);

  // If internal route is available from air-server, prefer live route
  if (process.env.AIR_INTERNAL_TOKEN) {
    try {
      const route = await airServerClient.getTenantRoute(project.tenantId);
      if (route.host) host = route.host;
      if (route.port > 0) port = route.port;
      if (route.app_password) password = route.app_password;
    } catch {
      // Fallback to stored directPort & encrypted password if internal route isn't reachable
    }
  }

  if (!port || !password) {
    throw new AirBackendUnavailableError(
      "Database connection endpoint or credentials are not provisioned yet. Start air-server and provision the tenant database.",
      503
    );
  }

  const client = new Client({
    host,
    port,
    database: project.databaseName || "postgres",
    user: project.username || "app_user",
    password,
    connectionTimeoutMillis: 5000,
    query_timeout: 30000,
    statement_timeout: 30000,
  });

  try {
    await client.connect();
    return await fn(client);
  } catch (err) {
    if (
      err instanceof Error &&
      ("code" in err && (err.code === "ECONNREFUSED" || err.code === "ETIMEDOUT"))
    ) {
      throw new AirBackendUnavailableError(
        `PostgreSQL tenant instance unreachable at ${host}:${port}`,
        503
      );
    }
    throw err;
  } finally {
    await client.end().catch(() => {});
  }
}

export function serializeQueryResult(result: QueryResult) {
  return {
    command: result.command,
    rowCount: result.rowCount ?? result.rows.length,
    fields: result.fields.map((f) => ({
      name: f.name,
      dataTypeID: f.dataTypeID,
    })),
    rows: result.rows,
  };
}
