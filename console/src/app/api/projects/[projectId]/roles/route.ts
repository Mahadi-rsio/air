import { NextRequest, NextResponse } from "next/server";
import {
  authenticateRequest,
  handleApiError,
  requireProjectAccess,
} from "@/lib/auth/server";
import { AirBackendUnavailableError } from "@/lib/air-client";
import { withProjectPgClient } from "@/lib/pg-client";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:read");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const roles = await withProjectPgClient(project, async (client) => {
      const res = await client.query(`
        SELECT
          rolname AS name,
          rolsuper AS is_superuser,
          rolcanlogin AS can_login,
          rolcreatedb AS can_create_db,
          rolcreaterole AS can_create_role,
          rolbypassrls AS bypass_rls,
          rolconnlimit AS conn_limit
        FROM pg_roles
        WHERE rolname NOT LIKE 'pg_%'
        ORDER BY rolname;
      `);
      return res.rows;
    });

    return NextResponse.json({ roles });
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
    const ctx = await authenticateRequest(req, "projects:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId, ["owner", "admin"]);

    const body = await req.json();
    const roleName = String(body?.name || "").trim();
    const password = String(body?.password || "");
    if (!/^[a-z_][a-z0-9_]{1,47}$/.test(roleName)) {
      return NextResponse.json(
        { error: "Role name must be lowercase alphanumeric/underscore" },
        { status: 400 }
      );
    }
    if (password.length < 8) {
      return NextResponse.json(
        { error: "Password must be at least 8 characters" },
        { status: 400 }
      );
    }

    await withProjectPgClient(project, async (client) => {
      const escapedPass = password.replace(/'/g, "''");
      await client.query(
        `CREATE ROLE "${roleName}" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '${escapedPass}';`
      );
    });

    return NextResponse.json({ ok: true, name: roleName }, { status: 201 });
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
