import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import * as schema from "./schema";
export type { DBInstance };

type DBInstance = ReturnType<typeof drizzle<typeof schema>>;

function bootstrapLocalSchema(sqlite: Database.Database) {
  sqlite.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS user (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      email_verified INTEGER NOT NULL DEFAULT 0,
      image TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS session (
      id TEXT PRIMARY KEY,
      expires_at INTEGER NOT NULL,
      token TEXT NOT NULL UNIQUE,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      ip_address TEXT,
      user_agent TEXT,
      user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS account (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      provider_id TEXT NOT NULL,
      user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
      access_token TEXT,
      refresh_token TEXT,
      id_token TEXT,
      access_token_expires_at INTEGER,
      refresh_token_expires_at INTEGER,
      scope TEXT,
      password TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS verification (
      id TEXT PRIMARY KEY,
      identifier TEXT NOT NULL,
      value TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      created_at INTEGER,
      updated_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS jwks (
      id TEXT PRIMARY KEY,
      public_key TEXT NOT NULL,
      private_key TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      expires_at INTEGER,
      alg TEXT,
      crv TEXT
    );

    CREATE TABLE IF NOT EXISTS workspaces (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      slug TEXT NOT NULL UNIQUE,
      owner_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS workspace_members (
      id TEXT PRIMARY KEY,
      workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
      role TEXT NOT NULL DEFAULT 'member',
      created_at INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_member_unique ON workspace_members(workspace_id, user_id);

    CREATE TABLE IF NOT EXISTS projects (
      id TEXT PRIMARY KEY,
      workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      description TEXT NOT NULL DEFAULT '',
      tenant_id TEXT NOT NULL UNIQUE,
      region TEXT NOT NULL DEFAULT 'us-east-1',
      pg_version TEXT NOT NULL DEFAULT '17',
      compute_size TEXT NOT NULL DEFAULT '0.25-1cu',
      autoscaling_min TEXT NOT NULL DEFAULT '0.25',
      autoscaling_max TEXT NOT NULL DEFAULT '1',
      status TEXT NOT NULL DEFAULT 'PROVISIONING',
      database_name TEXT NOT NULL DEFAULT 'postgres',
      username TEXT NOT NULL DEFAULT 'app_user',
      password_enc TEXT NOT NULL DEFAULT '',
      direct_port INTEGER NOT NULL DEFAULT 0,
      system_identifier TEXT NOT NULL DEFAULT '',
      fencing_epoch INTEGER NOT NULL DEFAULT 1,
      created_by TEXT NOT NULL REFERENCES user(id),
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_projects_workspace ON projects(workspace_id);

    CREATE TABLE IF NOT EXISTS branches (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      parent_branch_id TEXT,
      tenant_id TEXT,
      status TEXT NOT NULL DEFAULT 'ACTIVE',
      is_default INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_branches_project ON branches(project_id);

    CREATE TABLE IF NOT EXISTS saved_queries (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      branch_id TEXT,
      user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      sql TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_saved_queries_project ON saved_queries(project_id);

    CREATE TABLE IF NOT EXISTS query_history (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      branch_id TEXT,
      user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
      sql TEXT NOT NULL,
      duration_ms INTEGER NOT NULL DEFAULT 0,
      row_count INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'success',
      error_message TEXT,
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_query_history_project ON query_history(project_id);

    CREATE TABLE IF NOT EXISTS api_keys (
      id TEXT PRIMARY KEY,
      workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      prefix TEXT NOT NULL,
      key_hash TEXT NOT NULL UNIQUE,
      scopes TEXT NOT NULL DEFAULT '[]',
      expires_at INTEGER,
      last_used_at INTEGER,
      revoked_at INTEGER,
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_api_keys_workspace ON api_keys(workspace_id);

    CREATE TABLE IF NOT EXISTS audit_logs (
      id TEXT PRIMARY KEY,
      workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      project_id TEXT,
      user_id TEXT NOT NULL,
      action TEXT NOT NULL,
      resource_type TEXT NOT NULL,
      resource_id TEXT NOT NULL,
      metadata TEXT NOT NULL DEFAULT '{}',
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_audit_logs_workspace ON audit_logs(workspace_id);
  `);
}

export function createDriverDb(): DBInstance {
  const dbPath = process.env.AIR_CONSOLE_DB_PATH || path.join(process.cwd(), ".data", "air-console.db");
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  const sqlite = new Database(dbPath);
  bootstrapLocalSchema(sqlite);
  return drizzle(sqlite, { schema });
}
