import { NextRequest, NextResponse } from "next/server";
import { desc, eq } from "drizzle-orm";
import { getDb, schema } from "@/db";
import {
  authenticateRequest,
  handleApiError,
  requireProjectAccess,
} from "@/lib/auth/server";
import { AirBackendUnavailableError } from "@/lib/air-client";
import { serializeQueryResult, withProjectPgClient } from "@/lib/pg-client";
import { generateId } from "@/lib/crypto";
import { executeSqlSchema } from "@/schemas";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:query");
    const { projectId } = await params;
    await requireProjectAccess(ctx, projectId);

    const db = getDb();
    const history = await db
      .select()
      .from(schema.queryHistory)
      .where(eq(schema.queryHistory.projectId, projectId))
      .orderBy(desc(schema.queryHistory.createdAt))
      .limit(50);

    const saved = await db
      .select()
      .from(schema.savedQueries)
      .where(eq(schema.savedQueries.projectId, projectId))
      .orderBy(desc(schema.savedQueries.updatedAt));

    return NextResponse.json({ history, savedQueries: saved });
  } catch (err) {
    return handleApiError(err);
  }
}

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:query");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const body = await req.json();
    const parsed = executeSqlSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues[0]?.message || "Invalid SQL payload" },
        { status: 400 }
      );
    }

    const { sql, branchId } = parsed.data;
    const db = getDb();
    const start = performance.now();

    try {
      const result = await withProjectPgClient(project, async (client) => {
        const res = await client.query(sql);
        const finalRes = Array.isArray(res) ? res[res.length - 1] : res;
        return serializeQueryResult(finalRes);
      });

      const durationMs = Math.round(performance.now() - start);
      await db.insert(schema.queryHistory).values({
        id: generateId("qh"),
        projectId,
        branchId: branchId || null,
        userId: ctx.userId,
        sql: sql.slice(0, 10000),
        durationMs,
        rowCount: result.rowCount,
        status: "success",
        errorMessage: null,
        createdAt: new Date(),
      });

      return NextResponse.json({ ...result, durationMs });
    } catch (err) {
      const durationMs = Math.round(performance.now() - start);
      if (err instanceof AirBackendUnavailableError) {
        return NextResponse.json(
          { error: err.message, code: "AIR_BACKEND_UNAVAILABLE" },
          { status: err.statusCode }
        );
      }

      const pgErr = err as { message?: string; code?: string; detail?: string; position?: string };
      const errMsg = pgErr.message || "SQL execution failed";

      await db.insert(schema.queryHistory).values({
        id: generateId("qh"),
        projectId,
        branchId: branchId || null,
        userId: ctx.userId,
        sql: sql.slice(0, 10000),
        durationMs,
        rowCount: 0,
        status: "error",
        errorMessage: errMsg,
        createdAt: new Date(),
      });

      return NextResponse.json(
        {
          error: errMsg,
          pgCode: pgErr.code,
          detail: pgErr.detail,
          position: pgErr.position,
          durationMs,
        },
        { status: 422 }
      );
    }
  } catch (err) {
    return handleApiError(err);
  }
}
