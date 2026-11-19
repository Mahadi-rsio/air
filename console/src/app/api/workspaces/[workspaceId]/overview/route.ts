import { NextRequest, NextResponse } from "next/server";
import { desc, eq, inArray } from "drizzle-orm";
import { getDb, schema } from "@/db";
import {
  authenticateRequest,
  handleApiError,
  requireWorkspaceAccess,
} from "@/lib/auth/server";
import { airServerClient } from "@/lib/air-client";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ workspaceId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req);
    const { workspaceId } = await params;
    await requireWorkspaceAccess(ctx, workspaceId);

    const db = getDb();
    const projectRows = await db
      .select()
      .from(schema.projects)
      .where(eq(schema.projects.workspaceId, workspaceId))
      .orderBy(desc(schema.projects.createdAt));

    const projectIds = projectRows.map((p) => p.id);
    const branchRows =
      projectIds.length > 0
        ? await db
            .select()
            .from(schema.branches)
            .where(inArray(schema.branches.projectId, projectIds))
        : [];

    const recentAudit = await db
      .select()
      .from(schema.auditLogs)
      .where(eq(schema.auditLogs.workspaceId, workspaceId))
      .orderBy(desc(schema.auditLogs.createdAt))
      .limit(12);

    const backendHealth = await airServerClient.healthz();

    return NextResponse.json({
      stats: {
        totalProjects: projectRows.length,
        activeProjects: projectRows.filter((p) => p.status === "ACTIVE").length,
        totalDatabases: projectRows.length,
        totalBranches: branchRows.length,
        backendConnected: backendHealth.ok,
        backendError: backendHealth.error ?? null,
      },
      recentProjects: projectRows.slice(0, 6).map((p) => ({
        id: p.id,
        name: p.name,
        tenantId: p.tenantId,
        region: p.region,
        pgVersion: p.pgVersion,
        status: p.status,
        computeSize: p.computeSize,
        databaseName: p.databaseName,
        updatedAt: p.updatedAt,
      })),
      recentActivity: recentAudit.map((a) => ({
        id: a.id,
        action: a.action,
        resourceType: a.resourceType,
        resourceId: a.resourceId,
        metadata: JSON.parse(a.metadata || "{}"),
        createdAt: a.createdAt,
      })),
    });
  } catch (err) {
    return handleApiError(err);
  }
}
