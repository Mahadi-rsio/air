import { NextRequest, NextResponse } from "next/server";
import {
  authenticateRequest,
  handleApiError,
  recordAuditLog,
  requireProjectAccess,
} from "@/lib/auth/server";
import { decryptSecret } from "@/lib/crypto";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:read");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    const host = process.env.AIR_GATEWAY_HOST || "127.0.0.1";
    const port = project.directPort || Number(process.env.AIR_GATEWAY_PG_PORT || 5432);
    const hasPassword = Boolean(project.passwordEnc);

    return NextResponse.json({
      connection: {
        host,
        port,
        database: project.databaseName,
        username: project.username,
        sslMode: "require",
        hasPassword,
        maskedUri: `postgres://${project.username}:••••••••@${host}:${port}/${project.databaseName}?sslmode=require`,
      },
    });
  } catch (err) {
    return handleApiError(err);
  }
}

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:read");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId, ["owner", "admin", "member"]);

    const password = decryptSecret(project.passwordEnc);
    const host = process.env.AIR_GATEWAY_HOST || "127.0.0.1";
    const port = project.directPort || Number(process.env.AIR_GATEWAY_PG_PORT || 5432);

    await recordAuditLog({
      workspaceId: project.workspaceId,
      projectId: project.id,
      userId: ctx.userId,
      action: "connection.password_revealed",
      resourceType: "project",
      resourceId: project.id,
    });

    return NextResponse.json({
      password,
      uri: `postgres://${project.username}:${encodeURIComponent(password)}@${host}:${port}/${project.databaseName}?sslmode=require`,
    });
  } catch (err) {
    return handleApiError(err);
  }
}
