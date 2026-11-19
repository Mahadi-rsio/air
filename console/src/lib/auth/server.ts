import { NextRequest, NextResponse } from "next/server";
import { and, eq, isNull } from "drizzle-orm";
import { auth } from "./index";
import { getDb, schema } from "@/db";
import { generateId, sha256Hex } from "@/lib/crypto";

export type ApiKeyScope =
  | "projects:read"
  | "projects:write"
  | "databases:query"
  | "databases:write"
  | "backups:read"
  | "backups:write"
  | "api_keys:manage"
  | "usage:read";

export interface AuthContext {
  userId: string;
  email?: string;
  name?: string;
  authType: "session" | "jwt" | "api_key";
  apiKeyWorkspaceId?: string;
  scopes?: string[];
}

export class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
    public code?: string
  ) {
    super(message);
  }
}

export async function authenticateRequest(
  req: NextRequest,
  requiredScope?: ApiKeyScope
): Promise<AuthContext> {
  const authHeader = req.headers.get("authorization");
  if (authHeader?.startsWith("Bearer air_key_")) {
    const rawKey = authHeader.slice("Bearer ".length).trim();
    const keyHash = sha256Hex(rawKey);
    const db = getDb();

    const [keyRow] = await db
      .select()
      .from(schema.apiKeys)
      .where(and(eq(schema.apiKeys.keyHash, keyHash), isNull(schema.apiKeys.revokedAt)))
      .limit(1);

    if (!keyRow) {
      throw new HttpError(401, "Invalid or revoked API key", "INVALID_API_KEY");
    }

    if (keyRow.expiresAt && keyRow.expiresAt.getTime() < Date.now()) {
      throw new HttpError(401, "API key has expired", "EXPIRED_API_KEY");
    }

    const scopes: string[] = JSON.parse(keyRow.scopes || "[]");
    if (requiredScope && !scopes.includes(requiredScope) && !scopes.includes("*")) {
      throw new HttpError(
        403,
        `API key missing required scope: ${requiredScope}`,
        "INSUFFICIENT_SCOPE"
      );
    }

    await db
      .update(schema.apiKeys)
      .set({ lastUsedAt: new Date() })
      .where(eq(schema.apiKeys.id, keyRow.id));

    return {
      userId: keyRow.userId,
      authType: "api_key",
      apiKeyWorkspaceId: keyRow.workspaceId,
      scopes,
    };
  }

  const sessionData = await auth.api.getSession({
    headers: req.headers,
  });

  if (!sessionData?.user) {
    throw new HttpError(401, "Authentication required", "UNAUTHORIZED");
  }

  return {
    userId: sessionData.user.id,
    email: sessionData.user.email,
    name: sessionData.user.name,
    authType: authHeader?.startsWith("Bearer ") ? "jwt" : "session",
  };
}

export async function requireWorkspaceAccess(
  ctx: AuthContext,
  workspaceId: string,
  allowedRoles?: Array<"owner" | "admin" | "member">
) {
  if (ctx.authType === "api_key" && ctx.apiKeyWorkspaceId !== workspaceId) {
    throw new HttpError(403, "API key does not belong to this workspace", "FORBIDDEN");
  }

  const db = getDb();
  const [membership] = await db
    .select()
    .from(schema.workspaceMembers)
    .where(
      and(
        eq(schema.workspaceMembers.workspaceId, workspaceId),
        eq(schema.workspaceMembers.userId, ctx.userId)
      )
    )
    .limit(1);

  if (!membership) {
    throw new HttpError(403, "Access denied to workspace", "FORBIDDEN");
  }

  if (allowedRoles && !allowedRoles.includes(membership.role as "owner" | "admin" | "member")) {
    throw new HttpError(403, "Insufficient workspace role permissions", "FORBIDDEN");
  }

  return membership;
}

export async function requireProjectAccess(
  ctx: AuthContext,
  projectId: string,
  allowedRoles?: Array<"owner" | "admin" | "member">
) {
  const db = getDb();
  const [project] = await db
    .select()
    .from(schema.projects)
    .where(eq(schema.projects.id, projectId))
    .limit(1);

  if (!project) {
    throw new HttpError(404, "Project not found", "NOT_FOUND");
  }

  const membership = await requireWorkspaceAccess(ctx, project.workspaceId, allowedRoles);
  return { project, membership };
}

export async function recordAuditLog(params: {
  workspaceId: string;
  projectId?: string | null;
  userId: string;
  action: string;
  resourceType: string;
  resourceId: string;
  metadata?: Record<string, unknown>;
}) {
  const db = getDb();
  await db.insert(schema.auditLogs).values({
    id: generateId("aud"),
    workspaceId: params.workspaceId,
    projectId: params.projectId ?? null,
    userId: params.userId,
    action: params.action,
    resourceType: params.resourceType,
    resourceId: params.resourceId,
    metadata: JSON.stringify(params.metadata ?? {}),
    createdAt: new Date(),
  });
}

export function handleApiError(err: unknown) {
  if (err instanceof HttpError) {
    return NextResponse.json(
      { error: err.message, code: err.code },
      { status: err.status }
    );
  }
  console.error("[AIR Console API Error]", err);
  return NextResponse.json(
    { error: err instanceof Error ? err.message : "Internal server error" },
    { status: 500 }
  );
}
