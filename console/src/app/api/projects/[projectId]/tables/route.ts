import { NextRequest, NextResponse } from "next/server";
import {
  authenticateRequest,
  handleApiError,
  requireProjectAccess,
} from "@/lib/auth/server";
import { AirBackendUnavailableError } from "@/lib/air-client";
import { withProjectPgClient } from "@/lib/pg-client";
import {
  tableRowDeleteSchema,
  tableRowInsertSchema,
  tableRowUpdateSchema,
} from "@/schemas";

function quoteIdent(ident: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_$]*$/.test(ident)) {
    throw new Error(`Invalid SQL identifier: ${ident}`);
  }
  return `"${ident}"`;
}

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:query");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const schemaName = req.nextUrl.searchParams.get("schema") || "public";
    const tableName = req.nextUrl.searchParams.get("table");
    const limit = Math.min(Math.max(Number(req.nextUrl.searchParams.get("limit") || 50), 1), 200);
    const offset = Math.max(Number(req.nextUrl.searchParams.get("offset") || 0), 0);
    const sortCol = req.nextUrl.searchParams.get("sortCol");
    const sortDir = req.nextUrl.searchParams.get("sortDir") === "desc" ? "DESC" : "ASC";

    if (!tableName) {
      return NextResponse.json({ error: "table parameter is required" }, { status: 400 });
    }

    const qSchema = quoteIdent(schemaName);
    const qTable = quoteIdent(tableName);
    const orderClause = sortCol ? `ORDER BY ${quoteIdent(sortCol)} ${sortDir}` : "";

    const data = await withProjectPgClient(project, async (client) => {
      const countRes = await client.query(`SELECT COUNT(*)::int AS total FROM ${qSchema}.${qTable}`);
      const rowsRes = await client.query(
        `SELECT * FROM ${qSchema}.${qTable} ${orderClause} LIMIT $1 OFFSET $2`,
        [limit, offset]
      );
      const colsRes = await client.query(
        `
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        `,
        [schemaName, tableName]
      );
      return {
        total: countRes.rows[0]?.total ?? 0,
        columns: colsRes.rows,
        rows: rowsRes.rows,
      };
    });

    return NextResponse.json(data);
  } catch (err) {
    if (err instanceof AirBackendUnavailableError) {
      return NextResponse.json(
        { error: err.message, code: "AIR_BACKEND_UNAVAILABLE" },
        { status: err.statusCode }
      );
    }
    return handleApiError(err);
  }
}

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const body = await req.json();
    const parsed = tableRowInsertSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json({ error: "Invalid insert payload" }, { status: 400 });
    }

    const { schema, table, values } = parsed.data;
    const keys = Object.keys(values);
    if (keys.length === 0) {
      return NextResponse.json({ error: "No column values provided" }, { status: 400 });
    }

    const qCols = keys.map(quoteIdent).join(", ");
    const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
    const paramsArr = keys.map((k) => values[k]);

    const inserted = await withProjectPgClient(project, async (client) => {
      const res = await client.query(
        `INSERT INTO ${quoteIdent(schema)}.${quoteIdent(table)} (${qCols}) VALUES (${placeholders}) RETURNING *`,
        paramsArr
      );
      return res.rows[0];
    });

    return NextResponse.json({ row: inserted }, { status: 201 });
  } catch (err) {
    if (err instanceof AirBackendUnavailableError) {
      return NextResponse.json(
        { error: err.message, code: "AIR_BACKEND_UNAVAILABLE" },
        { status: err.statusCode }
      );
    }
    return handleApiError(err);
  }
}

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const body = await req.json();
    const parsed = tableRowUpdateSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json({ error: "Invalid update payload" }, { status: 400 });
    }

    const { schema, table, where, values } = parsed.data;
    const setKeys = Object.keys(values);
    const whereKeys = Object.keys(where);
    if (setKeys.length === 0 || whereKeys.length === 0) {
      return NextResponse.json({ error: "Both values and where clause are required" }, { status: 400 });
    }

    const setClauses = setKeys.map((k, i) => `${quoteIdent(k)} = $${i + 1}`);
    const whereClauses = whereKeys.map(
      (k, i) => `${quoteIdent(k)} = $${setKeys.length + i + 1}`
    );
    const paramsArr = [...setKeys.map((k) => values[k]), ...whereKeys.map((k) => where[k])];

    const updated = await withProjectPgClient(project, async (client) => {
      const res = await client.query(
        `UPDATE ${quoteIdent(schema)}.${quoteIdent(table)} SET ${setClauses.join(", ")} WHERE ${whereClauses.join(" AND ")} RETURNING *`,
        paramsArr
      );
      return res.rows;
    });

    return NextResponse.json({ rows: updated });
  } catch (err) {
    if (err instanceof AirBackendUnavailableError) {
      return NextResponse.json(
        { error: err.message, code: "AIR_BACKEND_UNAVAILABLE" },
        { status: err.statusCode }
      );
    }
    return handleApiError(err);
  }
}

export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "databases:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const body = await req.json();
    const parsed = tableRowDeleteSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json({ error: "Invalid delete payload" }, { status: 400 });
    }

    const { schema, table, where } = parsed.data;
    const whereKeys = Object.keys(where);
    if (whereKeys.length === 0) {
      return NextResponse.json({ error: "where clause is required" }, { status: 400 });
    }

    const whereClauses = whereKeys.map((k, i) => `${quoteIdent(k)} = $${i + 1}`);
    const paramsArr = whereKeys.map((k) => where[k]);

    const rowCount = await withProjectPgClient(project, async (client) => {
      const res = await client.query(
        `DELETE FROM ${quoteIdent(schema)}.${quoteIdent(table)} WHERE ${whereClauses.join(" AND ")}`,
        paramsArr
      );
      return res.rowCount ?? 0;
    });

    return NextResponse.json({ deleted: rowCount });
  } catch (err) {
    if (err instanceof AirBackendUnavailableError) {
      return NextResponse.json(
        { error: err.message, code: "AIR_BACKEND_UNAVAILABLE" },
        { status: err.statusCode }
      );
    }
    return handleApiError(err);
  }
}
