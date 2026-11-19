import { NextRequest, NextResponse } from "next/server";
import { desc, eq } from "drizzle-orm";
import { getDb, schema } from "@/db";
import {
  authenticateRequest,
  handleApiError,
  requireProjectAccess,
} from "@/lib/auth/server";
import { createBranchSchema } from "@/schemas";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:read");
    const { projectId } = await params;
    await requireProjectAccess(ctx, projectId);

    const db = getDb();
    const rows = await db
      .select()
      .from(schema.branches)
      .where(eq(schema.branches.projectId, projectId))
      .orderBy(desc(schema.branches.isDefault), desc(schema.branches.createdAt));

    return NextResponse.json({
      branches: rows,
      capabilities: {
        nativeCopyOnWriteBranching: false,
        pitrCloneSupported: true,
        reason:
          "air-server v2.0 supports non-destructive PITR restore into a new tenant (POST /v1/databases/:id/restore) rather than instant CoW branching.",
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
    const ctx = await authenticateRequest(req, "projects:write");
    const { projectId } = await params;
    await requireProjectAccess(ctx, projectId, ["owner", "admin"]);

    const body = await req.json();
    const parsed = createBranchSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues[0]?.message || "Invalid branch payload" },
        { status: 400 }
      );
    }

    // Per Section 28: Never display unsupported functionality as operational.
    return NextResponse.json(
      {
        error:
          "Native copy-on-write branching is pending air-server backend integration. Use Backups & Recovery -> Restore to new tenant to clone a database.",
        code: "NOT_IMPLEMENTED_IN_BACKEND",
      },
      { status: 501 }
    );
  } catch (err) {
    return handleApiError(err);
  }
}
