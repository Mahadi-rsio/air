import { NextRequest, NextResponse } from "next/server";
import {
  authenticateRequest,
  handleApiError,
  requireProjectAccess,
} from "@/lib/auth/server";
import { airServerClient, AirBackendUnavailableError } from "@/lib/air-client";

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string; backupId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "backups:read");
    const { projectId, backupId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const report = await airServerClient.verifyBackup(project.tenantId, backupId);
    return NextResponse.json({ report });
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
