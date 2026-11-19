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

    const extensions = await withProjectPgClient(project, async (client) => {
      const res = await client.query(`
        SELECT
          name,
          default_version,
          installed_version,
          comment
        FROM pg_available_extensions
        ORDER BY (installed_version IS NOT NULL) DESC, name ASC;
      `);
      return res.rows;
    });

    return NextResponse.json({ extensions });
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
    const { project } = await requireProjectAccess(ctx, projectId, ["owner", "admin"]);

    const body = await req.json();
    const name = String(body?.name || "").trim();
    const action = body?.action === "disable" ? "disable" : "enable";
    if (!/^[a-z0-9_-]{1,64}$/i.test(name)) {
      return NextResponse.json({ error: "Invalid extension name" }, { status: 400 });
    }

    await withProjectPgClient(project, async (client) => {
      if (action === "enable") {
        await client.query(`CREATE EXTENSION IF NOT EXISTS "${name}";`);
      } else {
        await client.query(`DROP EXTENSION IF EXISTS "${name}";`);
      }
    });

    return NextResponse.json({ ok: true, name, action });
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
