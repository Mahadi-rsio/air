import { NextRequest, NextResponse } from "next/server";
import { eq } from "drizzle-orm";
import { getDb, schema } from "@/db";
import {
  authenticateRequest,
  handleApiError,
  recordAuditLog,
  requireProjectAccess,
} from "@/lib/auth/server";
import { airServerClient, AirBackendUnavailableError } from "@/lib/air-client";

const VALID_ACTIONS = new Set(["suspend", "start", "lock", "unlock"]);

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId, ["owner", "admin"]);

    const body = await req.json();
    const action = String(body?.action || "").toLowerCase();
    if (!VALID_ACTIONS.has(action)) {
      return NextResponse.json(
        { error: "Invalid lifecycle action. Expected suspend, start, lock, or unlock." },
        { status: 400 }
      );
    }

    const result = await airServerClient.lifecycleAction(
      project.tenantId,
      action as "suspend" | "start" | "lock" | "unlock"
    );

    const statusMap: Record<string, string> = {
      suspend: "SUSPENDED",
      start: "ACTIVE",
      lock: "LOCKED",
      unlock: "SUSPENDED",
    };

    const db = getDb();
    await db
      .update(schema.projects)
      .set({ status: statusMap[action], updatedAt: new Date() })
      .where(eq(schema.projects.id, project.id));

    await recordAuditLog({
      workspaceId: project.workspaceId,
      projectId: project.id,
      userId: ctx.userId,
      action: `project.${action}`,
      resourceType: "project",
      resourceId: project.id,
    });

    return NextResponse.json({ ok: true, status: statusMap[action], result });
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
