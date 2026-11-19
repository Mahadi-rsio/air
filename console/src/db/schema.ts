import { sqliteTable, text, integer, uniqueIndex, index } from "drizzle-orm/sqlite-core";

// ============================================================================
// Better Auth Core Tables + JWT Plugin JWKS Table
// ============================================================================

export const user = sqliteTable("user", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  email: text("email").notNull().unique(),
  emailVerified: integer("email_verified", { mode: "boolean" }).notNull().default(false),
  image: text("image"),
  createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
});

export const session = sqliteTable("session", {
  id: text("id").primaryKey(),
  expiresAt: integer("expires_at", { mode: "timestamp" }).notNull(),
  token: text("token").notNull().unique(),
  createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
  ipAddress: text("ip_address"),
  userAgent: text("user_agent"),
  userId: text("user_id")
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
});

export const account = sqliteTable("account", {
  id: text("id").primaryKey(),
  accountId: text("account_id").notNull(),
  providerId: text("provider_id").notNull(),
  userId: text("user_id")
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
  accessToken: text("access_token"),
  refreshToken: text("refresh_token"),
  idToken: text("id_token"),
  accessTokenExpiresAt: integer("access_token_expires_at", { mode: "timestamp" }),
  refreshTokenExpiresAt: integer("refresh_token_expires_at", { mode: "timestamp" }),
  scope: text("scope"),
  password: text("password"),
  createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
});

export const verification = sqliteTable("verification", {
  id: text("id").primaryKey(),
  identifier: text("identifier").notNull(),
  value: text("value").notNull(),
  expiresAt: integer("expires_at", { mode: "timestamp" }).notNull(),
  createdAt: integer("created_at", { mode: "timestamp" }),
  updatedAt: integer("updated_at", { mode: "timestamp" }),
});

export const jwks = sqliteTable("jwks", {
  id: text("id").primaryKey(),
  publicKey: text("public_key").notNull(),
  privateKey: text("private_key").notNull(),
  createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  expiresAt: integer("expires_at", { mode: "timestamp" }),
  alg: text("alg"),
  crv: text("crv"),
});

// ============================================================================
// AIR Console Control-Plane Metadata (D1 / SQLite)
// ============================================================================

export const workspaces = sqliteTable("workspaces", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  slug: text("slug").notNull().unique(),
  ownerId: text("owner_id")
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
  createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
});

export const workspaceMembers = sqliteTable(
  "workspace_members",
  {
    id: text("id").primaryKey(),
    workspaceId: text("workspace_id")
      .notNull()
      .references(() => workspaces.id, { onDelete: "cascade" }),
    userId: text("user_id")
      .notNull()
      .references(() => user.id, { onDelete: "cascade" }),
    role: text("role").notNull().default("member"), // owner | admin | member
    createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  },
  (table) => [
    uniqueIndex("idx_workspace_member_unique").on(table.workspaceId, table.userId),
  ]
);

export const projects = sqliteTable(
  "projects",
  {
    id: text("id").primaryKey(),
    workspaceId: text("workspace_id")
      .notNull()
      .references(() => workspaces.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    description: text("description").notNull().default(""),
    tenantId: text("tenant_id").notNull().unique(),
    region: text("region").notNull().default("us-east-1"),
    pgVersion: text("pg_version").notNull().default("17"),
    computeSize: text("compute_size").notNull().default("0.25-1cu"),
    autoscalingMin: text("autoscaling_min").notNull().default("0.25"),
    autoscalingMax: text("autoscaling_max").notNull().default("1"),
    status: text("status").notNull().default("PROVISIONING"),
    databaseName: text("database_name").notNull().default("postgres"),
    username: text("username").notNull().default("app_user"),
    passwordEnc: text("password_enc").notNull().default(""),
    directPort: integer("direct_port").notNull().default(0),
    systemIdentifier: text("system_identifier").notNull().default(""),
    fencingEpoch: integer("fencing_epoch").notNull().default(1),
    createdBy: text("created_by")
      .notNull()
      .references(() => user.id),
    createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
    updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
  },
  (table) => [index("idx_projects_workspace").on(table.workspaceId)]
);

export const branches = sqliteTable(
  "branches",
  {
    id: text("id").primaryKey(),
    projectId: text("project_id")
      .notNull()
      .references(() => projects.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    parentBranchId: text("parent_branch_id"),
    tenantId: text("tenant_id"),
    status: text("status").notNull().default("ACTIVE"),
    isDefault: integer("is_default", { mode: "boolean" }).notNull().default(false),
    createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
    updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
  },
  (table) => [index("idx_branches_project").on(table.projectId)]
);

export const savedQueries = sqliteTable(
  "saved_queries",
  {
    id: text("id").primaryKey(),
    projectId: text("project_id")
      .notNull()
      .references(() => projects.id, { onDelete: "cascade" }),
    branchId: text("branch_id"),
    userId: text("user_id")
      .notNull()
      .references(() => user.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    sql: text("sql").notNull(),
    createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
    updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
  },
  (table) => [index("idx_saved_queries_project").on(table.projectId)]
);

export const queryHistory = sqliteTable(
  "query_history",
  {
    id: text("id").primaryKey(),
    projectId: text("project_id")
      .notNull()
      .references(() => projects.id, { onDelete: "cascade" }),
    branchId: text("branch_id"),
    userId: text("user_id")
      .notNull()
      .references(() => user.id, { onDelete: "cascade" }),
    sql: text("sql").notNull(),
    durationMs: integer("duration_ms").notNull().default(0),
    rowCount: integer("row_count").notNull().default(0),
    status: text("status").notNull().default("success"), // success | error
    errorMessage: text("error_message"),
    createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  },
  (table) => [index("idx_query_history_project").on(table.projectId)]
);

export const apiKeys = sqliteTable(
  "api_keys",
  {
    id: text("id").primaryKey(),
    workspaceId: text("workspace_id")
      .notNull()
      .references(() => workspaces.id, { onDelete: "cascade" }),
    userId: text("user_id")
      .notNull()
      .references(() => user.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    prefix: text("prefix").notNull(),
    keyHash: text("key_hash").notNull().unique(),
    scopes: text("scopes").notNull().default("[]"), // JSON array of permission scopes
    expiresAt: integer("expires_at", { mode: "timestamp" }),
    lastUsedAt: integer("last_used_at", { mode: "timestamp" }),
    revokedAt: integer("revoked_at", { mode: "timestamp" }),
    createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  },
  (table) => [index("idx_api_keys_workspace").on(table.workspaceId)]
);

export const auditLogs = sqliteTable(
  "audit_logs",
  {
    id: text("id").primaryKey(),
    workspaceId: text("workspace_id")
      .notNull()
      .references(() => workspaces.id, { onDelete: "cascade" }),
    projectId: text("project_id"),
    userId: text("user_id").notNull(),
    action: text("action").notNull(),
    resourceType: text("resource_type").notNull(),
    resourceId: text("resource_id").notNull(),
    metadata: text("metadata").notNull().default("{}"),
    createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
  },
  (table) => [index("idx_audit_logs_workspace").on(table.workspaceId)]
);
