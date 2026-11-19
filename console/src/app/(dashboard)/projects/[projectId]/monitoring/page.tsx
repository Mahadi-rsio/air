"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { Activity, RefreshCw } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { formatBytes } from "@/lib/utils";

export default function MonitoringPage() {
  const { projectId } = useParams<{ projectId: string }>();

  const { data, isFetching, refetch } = useQuery({
    queryKey: ["project-monitoring", projectId],
    queryFn: async () => {
      const res = await apiClient.get(`/projects/${projectId}/metrics`);
      return res.data;
    },
    refetchInterval: 15000,
  });

  const pg = data?.pgMetrics;
  const gw = data?.gatewayMetrics;

  return (
    <div className="space-y-5 max-w-6xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">
            Monitoring & Observability
          </h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Real-time telemetry from PostgreSQL system catalogs and C++20 AIR Gateway.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isFetching}>
          <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      {data?.pgError && (
        <div className="rounded-md border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-[var(--warning)]">
          {data.pgError}
        </div>
      )}

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-[var(--text-secondary)]">Active Queries</p>
            <p className="mt-1.5 text-2xl font-semibold font-mono">
              {pg?.activity?.active_queries ?? "—"}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-[var(--text-secondary)]">Idle Connections</p>
            <p className="mt-1.5 text-2xl font-semibold font-mono">
              {pg?.activity?.idle_connections ?? "—"}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-[var(--text-secondary)]">Cache Hit Ratio</p>
            <p className="mt-1.5 text-2xl font-semibold font-mono">
              {pg?.cache_hit_ratio != null ? `${pg.cache_hit_ratio}%` : "—"}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-[var(--text-secondary)]">Database Locks</p>
            <p className="mt-1.5 text-2xl font-semibold font-mono">
              {pg?.lock_count ?? "—"}
            </p>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-3.5 w-3.5 text-[var(--accent)]" />
              Transaction & Storage Stats
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2.5 text-xs">
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Committed Transactions</span>
              <span className="font-mono">{pg?.xact_commit ?? "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Rolled Back Transactions</span>
              <span className="font-mono">{pg?.xact_rollback ?? "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Database Size</span>
              <span className="font-mono">
                {pg?.db_size_bytes ? formatBytes(pg.db_size_bytes) : "—"}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Blocks Hit / Read</span>
              <span className="font-mono">
                {pg?.blks_hit ?? "—"} / {pg?.blks_read ?? "—"}
              </span>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Gateway Pooler & Latency</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2.5 text-xs">
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Total Queries</span>
              <span className="font-mono">{gw?.query_count ?? "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Error Count</span>
              <span className="font-mono">{gw?.error_count ?? "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">P50 / P95 / P99 Latency (µs)</span>
              <span className="font-mono">
                {gw?.latency_us
                  ? `${gw.latency_us.p50 ?? 0} / ${gw.latency_us.p95 ?? 0} / ${gw.latency_us.p99 ?? 0}`
                  : "—"}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Bytes In / Out</span>
              <span className="font-mono">
                {gw ? `${formatBytes(gw.bytes_in)} / ${formatBytes(gw.bytes_out)}` : "—"}
              </span>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
