import { NextRequest, NextResponse } from "next/server";
import {
  authenticateRequest,
  handleApiError,
  requireProjectAccess,
} from "@/lib/auth/server";
import { airServerClient } from "@/lib/air-client";
import { withProjectPgClient } from "@/lib/pg-client";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ projectId: string }> }
) {
  try {
    const ctx = await authenticateRequest(req, "projects:read");
    const { projectId } = await params;
    const { project } = await requireProjectAccess(ctx, projectId);

    let gatewayMetrics = null;
    try {
      const gw = await airServerClient.getGatewayMetrics();
      gatewayMetrics = gw.tenants?.[project.tenantId] ?? gw.global ?? null;
    } catch {
      // gateway metrics optional if gateway isn't running
    }

    let pgMetrics = null;
    let pgError: string | null = null;
    try {
      pgMetrics = await withProjectPgClient(project, async (client) => {
        const dbStat = await client.query(
          `
          SELECT
            numbackends AS active_connections,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            pg_database_size(current_database())::bigint AS db_size_bytes
          FROM pg_stat_database
          WHERE datname = current_database();
          `
        );

        const activityRes = await client.query(
          `
          SELECT
            COUNT(*) FILTER (WHERE state = 'active')::int AS active_queries,
            COUNT(*) FILTER (WHERE state = 'idle')::int AS idle_connections,
            COUNT(*) FILTER (WHERE state = 'idle in transaction')::int AS idle_in_tx,
            COUNT(*)::int AS total_connections
          FROM pg_stat_activity
          WHERE datname = current_database();
          `
        );

        const locksRes = await client.query(
          `
          SELECT COUNT(*)::int AS lock_count
          FROM pg_locks l
          JOIN pg_database d ON d.oid = l.database
          WHERE d.datname = current_database();
          `
        );

        const stat = dbStat.rows[0] || {};
        const blksHit = Number(stat.blks_hit || 0);
        const blksRead = Number(stat.blks_read || 0);
        const cacheHitRatio =
          blksHit + blksRead > 0
            ? Number(((blksHit / (blksHit + blksRead)) * 100).toFixed(2))
            : null;

        return {
          ...stat,
          cache_hit_ratio: cacheHitRatio,
          activity: activityRes.rows[0] || {},
          lock_count: locksRes.rows[0]?.lock_count ?? 0,
        };
      });
    } catch (err) {
      pgError = err instanceof Error ? err.message : "PostgreSQL metrics unavailable";
    }

    return NextResponse.json({
      gatewayMetrics,
      pgMetrics,
      pgError,
      collectedAt: new Date().toISOString(),
    });
  } catch (err) {
    return handleApiError(err);
  }
}
