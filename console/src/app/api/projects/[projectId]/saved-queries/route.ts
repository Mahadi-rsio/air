import { NextRequest, NextResponse } from "next/server";
import { and, eq } from "drizzle-orm";
import { getDb, schema } from "@/db";
import {
  authenticateRequest,
  handleApiError,
  requireProjectAccess,
} from "@/lib/auth/server";
import { generateId } from "@/lib/crypto";
import { saveQuerySchema } from "@/schemas";

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:query");
    const { projectId } = await params;
    await requireProjectAccess(ctx, projectId);

    const body = await req.json();
    const parsed = saveQuerySchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues[0]?.message || "Invalid query payload" },
        { status: 400 }
      );
    }

    const db = getDb();
    const now = new Date();
    const id = generateId("sq");
    await db.insert(schema.savedQueries).values({
      id,
      projectId,
      branchId: parsed.data.branchId || null,
      userId: ctx.userId,
      name: parsed.data.name.trim(),
      sql: parsed.data.sql,
      createdAt: now,
      updatedAt: now,
    });

    return NextResponse.json(
      {
        query: {
          id,
          projectId,
          name: parsed.data.name.trim(),
          sql: parsed.data.sql,
          createdAt: now,
        },
      },
      { status: 201 }
    );
  } catch (err) {
    return handleApiError(err);
  }
}

export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:query");
    const { projectId } = await params;
    await requireProjectAccess(ctx, projectId);

    const id = req.nextUrl.searchParams.get("id");
    if (!id) {
      return NextResponse.json({ error: "id is required" }, { status: 400 });
    }

    const db = getDb();
    await db
      .delete(schema.savedQueries)
      .where(
        and(
          eq(schema.savedQueries.id, id),
          eq(schema.savedQueries.projectId, projectId)
        )
      );

    return NextResponse.json({ ok: true });
  } catch (err) {
    return handleApiError(err);
  }
}
