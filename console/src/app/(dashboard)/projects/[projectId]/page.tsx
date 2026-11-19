"use client";

import * as React from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Activity,
  Database,
  Lock,
  Pause,
  Play,
  RefreshCw,
  Terminal,
  Unlock,
  Link2,
} from "lucide-react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";
import { apiClient } from "@/lib/axios";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { formatBytes } from "@/lib/utils";

interface ProjectDetail {
  id: string;
  name: string;
  description: string;
  tenantId: string;
  region: string;
  pgVersion: string;
  computeSize: string;
  status: string;
  databaseName: string;
  username: string;
  directPort: number;
  systemIdentifier: string;
  fencingEpoch: number;
  backendSynced: boolean;
}

interface MetricsResponse {
  gatewayMetrics: {
    active_client_conns?: number;
    query_count?: number;
    error_count?: number;
    latency_us?: {
      min?: number;
      avg?: number;
      max?: number;
      p50?: number;
      p95?: number;
      p99?: number;
    };
    bytes_in?: number;
    bytes_out?: number;
  } | null;
  pgMetrics: {
    active_connections?: number;
    xact_commit?: string;
    xact_rollback?: string;
    tup_returned?: string;
    tup_inserted?: string;
    tup_updated?: string;
    tup_deleted?: string;
    db_size_bytes?: string;
    cache_hit_ratio?: number | null;
    lock_count?: number;
    activity?: {
      active_queries?: number;
      idle_connections?: number;
      total_connections?: number;
    };
  } | null;
  pgError: string | null;
}

export default function ProjectOverviewPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const queryClient = useQueryClient();
  const { toast } = useToast();

  const { data, isLoading } = useQuery({
    queryKey: ["project", projectId],
    queryFn: async () => {
      const res = await apiClient.get<{ project: ProjectDetail }>(
        `/projects/${projectId}`
      );
      return res.data.project;
    },
  });

  const {
    data: metrics,
    refetch: refetchMetrics,
    isFetching: metricsFetching,
  } = useQuery({
    queryKey: ["project-metrics", projectId],
    queryFn: async () => {
      const res = await apiClient.get<MetricsResponse>(
        `/projects/${projectId}/metrics`
      );
      return res.data;
    },
  });

  const lifecycleMutation = useMutation({
    mutationFn: async (action: "suspend" | "start" | "lock" | "unlock") => {
      const res = await apiClient.post(`/projects/${projectId}/lifecycle`, {
        action,
      });
      return res.data;
    },
    onSuccess: (res) => {
      queryClient.invalidateQueries({ queryKey: ["project", projectId] });
      toast({ title: `Cluster state: ${res.status}`, variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Lifecycle action failed", description: err.message, variant: "error" });
    },
  });

  if (isLoading || !data) {
    return (
      <div className="space-y-4 max-w-7xl mx-auto animate-pulse">
        <div className="h-16 rounded-lg bg-[var(--surface)] border border-[var(--border)]" />
        <div className="grid grid-cols-4 gap-3">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-24 rounded-lg bg-[var(--surface)] border border-[var(--border)]" />
          ))}
        </div>
      </div>
    );
  }

  const tupleData = metrics?.pgMetrics
    ? [
        { name: "Returned", count: Number(metrics.pgMetrics.tup_returned || 0) },
        { name: "Inserted", count: Number(metrics.pgMetrics.tup_inserted || 0) },
        { name: "Updated", count: Number(metrics.pgMetrics.tup_updated || 0) },
        { name: "Deleted", count: Number(metrics.pgMetrics.tup_deleted || 0) },
      ]
    : [];

  const latencyData = metrics?.gatewayMetrics?.latency_us
    ? [
        { name: "min", us: metrics.gatewayMetrics.latency_us.min ?? 0 },
        { name: "p50", us: metrics.gatewayMetrics.latency_us.p50 ?? 0 },
        { name: "avg", us: metrics.gatewayMetrics.latency_us.avg ?? 0 },
        { name: "p95", us: metrics.gatewayMetrics.latency_us.p95 ?? 0 },
        { name: "p99", us: metrics.gatewayMetrics.latency_us.p99 ?? 0 },
      ]
    : [];

  return (
    <div className="space-y-5 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex flex-wrap items-start justify-between gap-4 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4">
        <div className="space-y-1">
          <div className="flex items-center gap-2.5">
            <h1 className="text-lg font-semibold tracking-tight">{data.name}</h1>
            <Badge
              variant={
                data.status === "ACTIVE"
                  ? "success"
                  : data.status === "SUSPENDED"
                  ? "warning"
                  : "danger"
              }
            >
              {data.status}
            </Badge>
            <Badge variant={data.backendSynced ? "success" : "muted"}>
              {data.backendSynced ? "Live synced" : "Cached metadata"}
            </Badge>
          </div>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-[var(--text-secondary)] font-mono">
            <span>id: {data.id}</span>
            <span>tenant: {data.tenantId}</span>
            <span>region: {data.region}</span>
            <span>pg: v{data.pgVersion}</span>
            <span>epoch: {data.fencingEpoch}</span>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Link href={`/projects/${projectId}/sql`}>
            <Button size="sm">
              <Terminal className="h-3.5 w-3.5" /> SQL Editor
            </Button>
          </Link>
          <Link href={`/projects/${projectId}/connection`}>
            <Button variant="outline" size="sm">
              <Link2 className="h-3.5 w-3.5" /> Connect
            </Button>
          </Link>
          {data.status === "ACTIVE" ? (
            <Button
              variant="outline"
              size="sm"
              disabled={lifecycleMutation.isPending}
              onClick={() => lifecycleMutation.mutate("suspend")}
            >
              <Pause className="h-3.5 w-3.5" /> Suspend
            </Button>
          ) : (
            <Button
              variant="outline"
              size="sm"
              disabled={lifecycleMutation.isPending}
              onClick={() => lifecycleMutation.mutate("start")}
            >
              <Play className="h-3.5 w-3.5" /> Start
            </Button>
          )}
          {data.status === "LOCKED" ? (
            <Button
              variant="outline"
              size="sm"
              disabled={lifecycleMutation.isPending}
              onClick={() => lifecycleMutation.mutate("unlock")}
            >
              <Unlock className="h-3.5 w-3.5" /> Unlock
            </Button>
          ) : (
            <Button
              variant="outline"
              size="sm"
              disabled={lifecycleMutation.isPending}
              onClick={() => lifecycleMutation.mutate("lock")}
            >
              <Lock className="h-3.5 w-3.5" /> Lock
            </Button>
          )}
        </div>
      </div>

      {/* Metrics Cards */}
      <div className="flex items-center justify-between">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-secondary)]">
          Live Cluster Telemetry
        </h2>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => refetchMetrics()}
          disabled={metricsFetching}
        >
          <RefreshCw className={`h-3 w-3 ${metricsFetching ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      {metrics?.pgError && (
        <div className="rounded-md border border-amber-500/30 bg-amber-500/10 px-3.5 py-2.5 text-xs text-[var(--warning)]">
          PostgreSQL Data Plane unreachable: {metrics.pgError}
        </div>
      )}

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between text-xs text-[var(--text-secondary)]">
              <span>Active Connections</span>
              <Activity className="h-3.5 w-3.5 text-[var(--accent)]" />
            </div>
            <p className="mt-2 text-2xl font-semibold font-mono">
              {metrics?.pgMetrics?.activity?.total_connections ?? "—"}
            </p>
            <p className="mt-1 text-[11px] text-[var(--text-secondary)]">
              {metrics?.pgMetrics?.activity?.active_queries ?? 0} running queries
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between text-xs text-[var(--text-secondary)]">
              <span>Database Size</span>
              <Database className="h-3.5 w-3.5" />
            </div>
            <p className="mt-2 text-2xl font-semibold font-mono">
              {metrics?.pgMetrics?.db_size_bytes
                ? formatBytes(metrics.pgMetrics.db_size_bytes)
                : "—"}
            </p>
            <p className="mt-1 text-[11px] text-[var(--text-secondary)]">
              Compute: {data.computeSize}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between text-xs text-[var(--text-secondary)]">
              <span>Buffer Cache Hit Ratio</span>
              <Activity className="h-3.5 w-3.5" />
            </div>
            <p className="mt-2 text-2xl font-semibold font-mono">
              {metrics?.pgMetrics?.cache_hit_ratio != null
                ? `${metrics.pgMetrics.cache_hit_ratio}%`
                : "—"}
            </p>
            <p className="mt-1 text-[11px] text-[var(--text-secondary)]">
              Locks: {metrics?.pgMetrics?.lock_count ?? "—"}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between text-xs text-[var(--text-secondary)]">
              <span>Gateway Queries</span>
              <Terminal className="h-3.5 w-3.5" />
            </div>
            <p className="mt-2 text-2xl font-semibold font-mono">
              {metrics?.gatewayMetrics?.query_count ?? "—"}
            </p>
            <p className="mt-1 text-[11px] text-[var(--text-secondary)]">
              Errors: {metrics?.gatewayMetrics?.error_count ?? "—"}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Tuple Read / Write Activity (pg_stat_database)</CardTitle>
          </CardHeader>
          <CardContent className="h-64">
            {tupleData.length === 0 ? (
              <div className="flex h-full items-center justify-center text-xs text-[var(--text-secondary)]">
                Connect PostgreSQL instance to view live tuple statistics.
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={tupleData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="name" tick={{ fontSize: 11, fill: "var(--text-secondary)" }} />
                  <YAxis tick={{ fontSize: 11, fill: "var(--text-secondary)" }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "var(--surface-elevated)",
                      borderColor: "var(--border)",
                      fontSize: 12,
                    }}
                  />
                  <Bar dataKey="count" fill="var(--accent)" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Gateway Query Latency (µs)</CardTitle>
          </CardHeader>
          <CardContent className="h-64">
            {latencyData.length === 0 ? (
              <div className="flex h-full items-center justify-center text-xs text-[var(--text-secondary)]">
                Gateway metrics unavailable (air-gateway admin endpoint not reachable).
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={latencyData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                  <XAxis dataKey="name" tick={{ fontSize: 11, fill: "var(--text-secondary)" }} />
                  <YAxis tick={{ fontSize: 11, fill: "var(--text-secondary)" }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "var(--surface-elevated)",
                      borderColor: "var(--border)",
                      fontSize: 12,
                    }}
                  />
                  <Bar dataKey="us" fill="var(--accent)" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
