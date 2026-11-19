import { NextRequest, NextResponse } from "next/server";
import { eq } from "drizzle-orm";
import { getDb, schema } from "@/db";
import {
  authenticateRequest,
  handleApiError,
  recordAuditLog,
} from "@/lib/auth/server";
import { generateId } from "@/lib/crypto";
import { createWorkspaceSchema } from "@/schemas";

export async function GET(req: NextRequest) {
  try {
    const ctx = await authenticateRequest(req);
    const db = getDb();

    const memberships = await db
      .select({
        workspace: schema.workspaces,
        role: schema.workspaceMembers.role,
      })
      .from(schema.workspaceMembers)
      .innerJoin(
        schema.workspaces,
        eq(schema.workspaceMembers.workspaceId, schema.workspaces.id)
      )
      .where(eq(schema.workspaceMembers.userId, ctx.userId));

    return NextResponse.json({
      workspaces: memberships.map((m) => ({
        ...m.workspace,
        role: m.role,
      })),
    });
  } catch (err) {
    return handleApiError(err);
  }
}

export async function POST(req: NextRequest) {
  try {
    const ctx = await authenticateRequest(req);
    const body = await req.json();
    const parsed = createWorkspaceSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues[0]?.message || "Invalid workspace input" },
        { status: 400 }
      );
    }

    const db = getDb();
    const now = new Date();
    const workspaceId = generateId("ws");
    const baseSlug = parsed.data.name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "") || "workspace";
    const slug = `${baseSlug}-${workspaceId.slice(-6)}`;

    await db.insert(schema.workspaces).values({
      id: workspaceId,
      name: parsed.data.name.trim(),
      slug,
      ownerId: ctx.userId,
      createdAt: now,
      updatedAt: now,
    });

    await db.insert(schema.workspaceMembers).values({
      id: generateId("wsm"),
      workspaceId,
      userId: ctx.userId,
      role: "owner",
      createdAt: now,
    });

    await recordAuditLog({
      workspaceId,
      userId: ctx.userId,
      action: "workspace.created",
      resourceType: "workspace",
      resourceId: workspaceId,
      metadata: { name: parsed.data.name.trim() },
    });

    const [created] = await db
      .select()
      .from(schema.workspaces)
      .where(eq(schema.workspaces.id, workspaceId))
      .limit(1);

    return NextResponse.json(
      { workspace: { ...created, role: "owner" } },
      { status: 201 }
    );
  } catch (err) {
    return handleApiError(err);
  }
}
