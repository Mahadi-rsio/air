import { z } from "zod";

export const createWorkspaceSchema = z.object({
  name: z.string().min(2, "Workspace name must be at least 2 characters").max(64),
});

export const createProjectSchema = z.object({
  workspaceId: z.string().min(1),
  name: z.string().min(2, "Project name must be at least 2 characters").max(64),
  tenantId: z
    .string()
    .min(3)
    .max(48)
    .regex(/^[a-z][a-z0-9_]{2,47}$/, "Lowercase alphanumeric & underscores only, starting with a letter"),
  region: z.enum(["us-east-1", "us-west-2", "eu-central-1", "ap-southeast-1"]),
  pgVersion: z.enum(["17", "16"]),
  computeSize: z.enum(["0.25-1cu", "0.5-2cu", "1-4cu", "2-8cu"]),
  databaseName: z.string().min(1).max(64).default("postgres"),
  username: z.string().min(1).max(64).default("app_user"),
  description: z.string().max(280).optional().default(""),
});

export const executeSqlSchema = z.object({
  sql: z.string().min(1, "SQL query cannot be empty").max(100_000),
  branchId: z.string().optional(),
});

export const saveQuerySchema = z.object({
  name: z.string().min(1).max(100),
  sql: z.string().min(1).max(100_000),
  branchId: z.string().optional(),
});

export const createApiKeySchema = z.object({
  workspaceId: z.string().min(1),
  name: z.string().min(2).max(64),
  scopes: z.array(z.string()).min(1, "Select at least one permission scope"),
  expiresInDays: z.number().int().min(1).max(365).nullable().optional(),
});

export const createBranchSchema = z.object({
  name: z
    .string()
    .min(2)
    .max(48)
    .regex(/^[a-z0-9][a-z0-9-_]{1,47}$/, "Lowercase letters, numbers, hyphens, underscores"),
  parentBranchId: z.string().min(1),
});

export const createBackupSchema = z.object({
  type: z.enum(["FULL", "INCR", "WAL"]).default("FULL"),
});

export const restoreBackupSchema = z.object({
  backupId: z.string().optional(),
  targetLsn: z.string().optional(),
  targetTime: z.string().optional(),
  newTenantId: z
    .string()
    .min(3)
    .max(48)
    .regex(/^[a-z][a-z0-9_]{2,47}$/),
});

export const tableRowInsertSchema = z.object({
  schema: z.string().default("public"),
  table: z.string().min(1),
  values: z.record(z.unknown()),
});

export const tableRowUpdateSchema = z.object({
  schema: z.string().default("public"),
  table: z.string().min(1),
  where: z.record(z.unknown()),
  values: z.record(z.unknown()),
});

export const tableRowDeleteSchema = z.object({
  schema: z.string().default("public"),
  table: z.string().min(1),
  where: z.record(z.unknown()),
});
