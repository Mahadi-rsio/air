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

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:read");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    let liveState = project.status;
    let livePort = project.directPort;
    let liveEpoch = project.fencingEpoch;
    let backendSynced = false;

    try {
      const remote = await airServerClient.getDatabase(project.tenantId);
      liveState = remote.state || liveState;
      livePort = remote.direct_port || livePort;
      liveEpoch = remote.fencing_epoch || liveEpoch;
      backendSynced = true;

      if (liveState !== project.status || livePort !== project.directPort) {
        const db = getDb();
        await db
          .update(schema.projects)
          .set({
            status: liveState,
            directPort: livePort,
            fencingEpoch: liveEpoch,
            updatedAt: new Date(),
          })
          .where(eq(schema.projects.id, project.id));
      }
    } catch {
      // air-server may be unreachable; report backendSynced: false honestly
    }

    const db = getDb();
    const branchRows = await db
      .select()
      .from(schema.branches)
      .where(eq(schema.branches.projectId, project.id));

    return NextResponse.json({
      project: {
        id: project.id,
        workspaceId: project.workspaceId,
        name: project.name,
        description: project.description,
        tenantId: project.tenantId,
        region: project.region,
        pgVersion: project.pgVersion,
        computeSize: project.computeSize,
        autoscalingMin: project.autoscalingMin,
        autoscalingMax: project.autoscalingMax,
        status: liveState,
        databaseName: project.databaseName,
        username: project.username,
        directPort: livePort,
        systemIdentifier: project.systemIdentifier,
        fencingEpoch: liveEpoch,
        createdAt: project.createdAt,
        updatedAt: project.updatedAt,
        backendSynced,
      },
      branches: branchRows,
    });
  } catch (err) {
    return handleApiError(err);
  }
}

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId, ["owner", "admin"]);

    const body = await req.json();
    const updates: Partial<typeof schema.projects.$inferInsert> = {
      updatedAt: new Date(),
    };
    if (typeof body.name === "string" && body.name.trim().length >= 2) {
      updates.name = body.name.trim();
    }
    if (typeof body.description === "string") {
      updates.description = body.description.slice(0, 280);
    }

    const db = getDb();
    await db
      .update(schema.projects)
      .set(updates)
      .where(eq(schema.projects.id, project.id));

    await recordAuditLog({
      workspaceId: project.workspaceId,
      projectId: project.id,
      userId: ctx.userId,
      action: "project.updated",
      resourceType: "project",
      resourceId: project.id,
      metadata: { fields: Object.keys(updates) },
    });

    return NextResponse.json({ ok: true });
  } catch (err) {
    return handleApiError(err);
  }
}

export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:write");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId, ["owner", "admin"]);

    try {
      await airServerClient.deleteDatabase(project.tenantId);
    } catch (err) {
      if (err instanceof AirBackendUnavailableError && err.statusCode !== 404) {
        return NextResponse.json(
          { error: err.message, code: "AIR_BACKEND_UNAVAILABLE" },
          { status: err.statusCode }
        );
      }
    }

    const db = getDb();
    await db.delete(schema.projects).where(eq(schema.projects.id, project.id));

    await recordAuditLog({
      workspaceId: project.workspaceId,
      projectId: null,
      userId: ctx.userId,
      action: "project.deleted",
      resourceType: "project",
      resourceId: project.id,
      metadata: { name: project.name, tenantId: project.tenantId },
    });

    return NextResponse.json({ status: "deleted", projectId });
  } catch (err) {
    return handleApiError(err);
  }
}
