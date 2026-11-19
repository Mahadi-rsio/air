import { NextRequest, NextResponse } from "next/server";
import {
  authenticateRequest,
  handleApiError,
  recordAuditLog,
  requireProjectAccess,
} from "@/lib/auth/server";
import { airServerClient, AirBackendUnavailableError } from "@/lib/air-client";
import { createBackupSchema } from "@/schemas";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "backups:read");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const backups = await airServerClient.listBackups(project.tenantId);
    return NextResponse.json({ backups });
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
    const ctx = await authenticateRequest(req, "backups:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId, ["owner", "admin"]);

    const body = await req.json().catch(() => ({}));
    const parsed = createBackupSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json({ error: "Invalid backup type" }, { status: 400 });
    }

    const backup = await airServerClient.triggerBackup(
      project.tenantId,
      parsed.data.type
    );

    await recordAuditLog({
      workspaceId: project.workspaceId,
      projectId: project.id,
      userId: ctx.userId,
      action: "backup.triggered",
      resourceType: "backup",
      resourceId: backup.backup_id || project.tenantId,
      metadata: { type: parsed.data.type },
    });

    return NextResponse.json({ backup }, { status: 201 });
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
