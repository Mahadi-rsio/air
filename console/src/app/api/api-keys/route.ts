import { NextRequest, NextResponse } from "next/server";
import { and, desc, eq } from "drizzle-orm";
import { getDb, schema } from "@/db";
import {
  authenticateRequest,
  handleApiError,
  recordAuditLog,
  requireWorkspaceAccess,
} from "@/lib/auth/server";
import { generateApiKeyToken, generateId } from "@/lib/crypto";
import { createApiKeySchema } from "@/schemas";

export async function GET(req: NextRequest) {
  try {
    const ctx = await authenticateRequest(req, "api_keys:manage");
    const workspaceId = req.nextUrl.searchParams.get("workspaceId");
    if (!workspaceId) {
      return NextResponse.json({ error: "workspaceId is required" }, { status: 400 });
    }

    await requireWorkspaceAccess(ctx, workspaceId);
    const db = getDb();

    const rows = await db
      .select({
        id: schema.apiKeys.id,
        workspaceId: schema.apiKeys.workspaceId,
        name: schema.apiKeys.name,
        prefix: schema.apiKeys.prefix,
        scopes: schema.apiKeys.scopes,
        expiresAt: schema.apiKeys.expiresAt,
        lastUsedAt: schema.apiKeys.lastUsedAt,
        revokedAt: schema.apiKeys.revokedAt,
        createdAt: schema.apiKeys.createdAt,
      })
      .from(schema.apiKeys)
      .where(eq(schema.apiKeys.workspaceId, workspaceId))
      .orderBy(desc(schema.apiKeys.createdAt));

    return NextResponse.json({
      apiKeys: rows.map((r) => ({
        ...r,
        scopes: JSON.parse(r.scopes || "[]"),
      })),
    });
  } catch (err) {
    return handleApiError(err);
  }
}

export async function POST(req: NextRequest) {
  try {
    const ctx = await authenticateRequest(req, "api_keys:manage");
    const body = await req.json();
    const parsed = createApiKeySchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues[0]?.message || "Invalid API key payload" },
        { status: 400 }
      );
    }

    const { workspaceId, name, scopes, expiresInDays } = parsed.data;
    await requireWorkspaceAccess(ctx, workspaceId, ["owner", "admin"]);

    const { token, prefix, hash } = generateApiKeyToken();
    const id = generateId("key");
    const now = new Date();
    const expiresAt = expiresInDays
      ? new Date(now.getTime() + expiresInDays * 86400 * 1000)
      : null;

    const db = getDb();
    await db.insert(schema.apiKeys).values({
      id,
      workspaceId,
      userId: ctx.userId,
      name: name.trim(),
      prefix,
      keyHash: hash,
      scopes: JSON.stringify(scopes),
      expiresAt,
      lastUsedAt: null,
      revokedAt: null,
      createdAt: now,
    });

    await recordAuditLog({
      workspaceId,
      userId: ctx.userId,
      action: "api_key.created",
      resourceType: "api_key",
      resourceId: id,
      metadata: { name: name.trim(), prefix, scopes },
    });

    // Return plaintext token ONLY once on creation
    return NextResponse.json(
      {
        apiKey: {
          id,
          name: name.trim(),
          prefix,
          scopes,
          expiresAt,
          createdAt: now,
          secret: token,
        },
      },
      { status: 201 }
    );
  } catch (err) {
    return handleApiError(err);
  }
}

export async function DELETE(req: NextRequest) {
  try {
    const ctx = await authenticateRequest(req, "api_keys:manage");
    const workspaceId = req.nextUrl.searchParams.get("workspaceId");
    const keyId = req.nextUrl.searchParams.get("id");
    const mode = req.nextUrl.searchParams.get("mode") || "revoke";

    if (!workspaceId || !keyId) {
      return NextResponse.json(
        { error: "workspaceId and id are required" },
        { status: 400 }
      );
    }

    await requireWorkspaceAccess(ctx, workspaceId, ["owner", "admin"]);
    const db = getDb();

    if (mode === "delete") {
      await db
        .delete(schema.apiKeys)
        .where(
          and(
            eq(schema.apiKeys.id, keyId),
            eq(schema.apiKeys.workspaceId, workspaceId)
          )
        );
    } else {
      await db
        .update(schema.apiKeys)
        .set({ revokedAt: new Date() })
        .where(
          and(
            eq(schema.apiKeys.id, keyId),
            eq(schema.apiKeys.workspaceId, workspaceId)
          )
        );
    }

    await recordAuditLog({
      workspaceId,
      userId: ctx.userId,
      action: mode === "delete" ? "api_key.deleted" : "api_key.revoked",
      resourceType: "api_key",
      resourceId: keyId,
    });

    return NextResponse.json({ ok: true });
  } catch (err) {
    return handleApiError(err);
  }
}
