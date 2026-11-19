import { NextRequest, NextResponse } from "next/server";
import {
  authenticateRequest,
  handleApiError,
  recordAuditLog,
  requireProjectAccess,
} from "@/lib/auth/server";
import { airServerClient, AirBackendUnavailableError } from "@/lib/air-client";
import { restoreBackupSchema } from "@/schemas";

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "backups:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId, ["owner", "admin"]);

    const body = await req.json();
    const parsed = restoreBackupSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues[0]?.message || "Invalid restore parameters" },
        { status: 400 }
      );
    }

    const result = await airServerClient.restoreDatabase(project.tenantId, {
      backup_id: parsed.data.backupId,
      target_lsn: parsed.data.targetLsn,
      target_time: parsed.data.targetTime,
      new_tenant_id: parsed.data.newTenantId,
    });

    await recordAuditLog({
      workspaceId: project.workspaceId,
      projectId: project.id,
      userId: ctx.userId,
      action: "backup.restored",
      resourceType: "project",
      resourceId: project.id,
      metadata: { newTenantId: parsed.data.newTenantId },
    });

    return NextResponse.json({ result });
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
