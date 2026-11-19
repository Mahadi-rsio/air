import { NextRequest, NextResponse } from "next/server";
import { desc, eq } from "drizzle-orm";
import { getDb, schema } from "@/db";
import {
  authenticateRequest,
  handleApiError,
  recordAuditLog,
  requireWorkspaceAccess,
} from "@/lib/auth/server";
import { airServerClient, AirBackendUnavailableError } from "@/lib/air-client";
import { encryptSecret, generateId } from "@/lib/crypto";
import { createProjectSchema } from "@/schemas";

export async function GET(req: NextRequest) {
  try {
    const ctx = await authenticateRequest(req, "projects:read");
    const workspaceId = req.nextUrl.searchParams.get("workspaceId");
    if (!workspaceId) {
      return NextResponse.json(
        { error: "workspaceId query parameter is required" },
        { status: 400 }
      );
    }

    await requireWorkspaceAccess(ctx, workspaceId);
    const db = getDb();

    const rows = await db
      .select()
      .from(schema.projects)
      .where(eq(schema.projects.workspaceId, workspaceId))
      .orderBy(desc(schema.projects.createdAt));

    return NextResponse.json({
      projects: rows.map((p) => ({
        id: p.id,
        workspaceId: p.workspaceId,
        name: p.name,
        description: p.description,
        tenantId: p.tenantId,
        region: p.region,
        pgVersion: p.pgVersion,
        computeSize: p.computeSize,
        status: p.status,
        databaseName: p.databaseName,
        username: p.username,
        directPort: p.directPort,
        systemIdentifier: p.systemIdentifier,
        fencingEpoch: p.fencingEpoch,
        createdAt: p.createdAt,
        updatedAt: p.updatedAt,
      })),
    });
  } catch (err) {
    return handleApiError(err);
  }
}

export async function POST(req: NextRequest) {
  try {
    const ctx = await authenticateRequest(req, "projects:write");
    const body = await req.json();
    const parsed = createProjectSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues[0]?.message || "Invalid project payload" },
        { status: 400 }
      );
    }

    const input = parsed.data;
    await requireWorkspaceAccess(ctx, input.workspaceId, ["owner", "admin", "member"]);

    const idempotencyKey =
      req.headers.get("idempotency-key") || generateId("idem");

    // Call real air-server control plane to provision isolated PG17 tenant
    const provisioned = await airServerClient.provisionDatabase(
      input.tenantId,
      idempotencyKey
    );

    const db = getDb();
    const now = new Date();
    const projectId = generateId("prj");
    const [minCu, maxCu] = input.computeSize.replace("cu", "").split("-");

    await db.insert(schema.projects).values({
      id: projectId,
      workspaceId: input.workspaceId,
      name: input.name.trim(),
      description: input.description || "",
      tenantId: provisioned.tenant_id,
      region: input.region,
      pgVersion: input.pgVersion,
      computeSize: input.computeSize,
      autoscalingMin: minCu || "0.25",
      autoscalingMax: maxCu || "1",
      status: provisioned.state || "ACTIVE",
      databaseName: provisioned.database || input.databaseName,
      username: provisioned.username || input.username,
      passwordEnc: encryptSecret(provisioned.password || ""),
      directPort: provisioned.direct_port || 0,
      systemIdentifier: provisioned.system_identifier || "",
      fencingEpoch: provisioned.fencing_epoch || 1,
      createdBy: ctx.userId,
      createdAt: now,
      updatedAt: now,
    });

    // Create default 'main' branch record
    await db.insert(schema.branches).values({
      id: generateId("br"),
      projectId,
      name: "main",
      parentBranchId: null,
      tenantId: provisioned.tenant_id,
      status: provisioned.state || "ACTIVE",
      isDefault: true,
      createdAt: now,
      updatedAt: now,
    });

    await recordAuditLog({
      workspaceId: input.workspaceId,
      projectId,
      userId: ctx.userId,
      action: "project.created",
      resourceType: "project",
      resourceId: projectId,
      metadata: {
        tenantId: provisioned.tenant_id,
        region: input.region,
        pgVersion: input.pgVersion,
      },
    });

    return NextResponse.json(
      {
        project: {
          id: projectId,
          workspaceId: input.workspaceId,
          name: input.name.trim(),
          tenantId: provisioned.tenant_id,
          region: input.region,
          pgVersion: input.pgVersion,
          computeSize: input.computeSize,
          status: provisioned.state || "ACTIVE",
          databaseName: provisioned.database,
          username: provisioned.username,
          directPort: provisioned.direct_port,
          systemIdentifier: provisioned.system_identifier,
          fencingEpoch: provisioned.fencing_epoch,
          createdAt: now,
        },
      },
      { status: 201 }
    );
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
