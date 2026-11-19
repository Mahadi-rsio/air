"use client";

import * as React from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  Database,
  GitBranch,
  Link2,
  Plus,
  Server,
  Terminal,
  CreditCard,
} from "lucide-react";
import { apiClient } from "@/lib/axios";
import { useConsoleStore } from "@/stores/console-store";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatRelativeTime } from "@/lib/utils";

interface OverviewResponse {
  stats: {
    totalProjects: number;
    activeProjects: number;
    totalDatabases: number;
    totalBranches: number;
    backendConnected: boolean;
    backendError: string | null;
  };
  recentProjects: Array<{
    id: string;
    name: string;
    tenantId: string;
    region: string;
    pgVersion: string;
    status: string;
    computeSize: string;
    databaseName: string;
    updatedAt: string;
  }>;
  recentActivity: Array<{
    id: string;
    action: string;
    resourceType: string;
    resourceId: string;
    createdAt: string;
  }>;
}

export default function WorkspaceOverviewPage() {
  const activeWorkspaceId = useConsoleStore((s) => s.activeWorkspaceId);
  const activeProjectId = useConsoleStore((s) => s.activeProjectId);

  const { data, isLoading, error } = useQuery({
    queryKey: ["workspace-overview", activeWorkspaceId],
    queryFn: async () => {
      const res = await apiClient.get<OverviewResponse>(
        `/workspaces/${activeWorkspaceId}/overview`
      );
      return res.data;
    },
    enabled: Boolean(activeWorkspaceId),
  });

  const firstProjectId = activeProjectId || data?.recentProjects?.[0]?.id;

  return (
    <div className="space-y-5 max-w-7xl mx-auto">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">
            Workspace Overview
          </h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Control-plane summary for your serverless PostgreSQL 17 clusters.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={data?.stats.backendConnected ? "success" : "warning"}>
            <Server className="h-3 w-3" />
            {data?.stats.backendConnected
              ? "air-server connected"
              : "air-server offline"}
          </Badge>
          <Link href="/projects/new">
            <Button size="md">
              <Plus className="h-3.5 w-3.5" /> New Project
            </Button>
          </Link>
        </div>
      </div>

      {error && (
        <div className="rounded-md border border-red-500/30 bg-red-500/10 p-3 text-xs text-[var(--danger)]">
          {(error as Error).message}
        </div>
      )}

      {/* Metrics */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between text-xs text-[var(--text-secondary)]">
              <span>Total Projects</span>
              <Database className="h-3.5 w-3.5 text-[var(--accent)]" />
            </div>
            <p className="mt-2 text-2xl font-semibold font-mono">
              {isLoading ? "—" : data?.stats.totalProjects ?? 0}
            </p>
            <p className="mt-1 text-[11px] text-[var(--text-secondary)]">
              {data?.stats.activeProjects ?? 0} active clusters
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between text-xs text-[var(--text-secondary)]">
              <span>Total Databases</span>
              <Server className="h-3.5 w-3.5" />
            </div>
            <p className="mt-2 text-2xl font-semibold font-mono">
              {isLoading ? "—" : data?.stats.totalDatabases ?? 0}
            </p>
            <p className="mt-1 text-[11px] text-[var(--text-secondary)]">
              PostgreSQL 17 isolated tenants
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between text-xs text-[var(--text-secondary)]">
              <span>Total Branches</span>
              <GitBranch className="h-3.5 w-3.5" />
            </div>
            <p className="mt-2 text-2xl font-semibold font-mono">
              {isLoading ? "—" : data?.stats.totalBranches ?? 0}
            </p>
            <p className="mt-1 text-[11px] text-[var(--text-secondary)]">
              Tracked across projects
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between text-xs text-[var(--text-secondary)]">
              <span>Monthly Est. Cost</span>
              <CreditCard className="h-3.5 w-3.5" />
            </div>
            <p className="mt-2 text-lg font-semibold text-[var(--text-secondary)]">
              Billing API Pending
            </p>
            <p className="mt-1 text-[11px] text-[var(--text-secondary)]">
              Metering integration required
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Link
          href="/projects/new"
          className="flex items-center gap-2.5 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3 text-xs font-medium hover:bg-[var(--surface-elevated)] transition-colors"
        >
          <Plus className="h-4 w-4 text-[var(--accent)]" />
          Create Project
        </Link>
        <Link
          href={firstProjectId ? `/projects/${firstProjectId}/connection` : "/projects"}
          className="flex items-center gap-2.5 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3 text-xs font-medium hover:bg-[var(--surface-elevated)] transition-colors"
        >
          <Link2 className="h-4 w-4 text-[var(--accent)]" />
          Connect to Database
        </Link>
        <Link
          href={firstProjectId ? `/projects/${firstProjectId}/sql` : "/projects"}
          className="flex items-center gap-2.5 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3 text-xs font-medium hover:bg-[var(--surface-elevated)] transition-colors"
        >
          <Terminal className="h-4 w-4 text-[var(--accent)]" />
          Open SQL Editor
        </Link>
        <Link
          href="/billing"
          className="flex items-center gap-2.5 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3 text-xs font-medium hover:bg-[var(--surface-elevated)] transition-colors"
        >
          <Activity className="h-4 w-4 text-[var(--accent)]" />
          View Usage
        </Link>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        {/* Recent Projects */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Recent Projects</CardTitle>
            <Link href="/projects" className="text-xs text-[var(--accent)] hover:underline">
              View all
            </Link>
          </CardHeader>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="border-b border-[var(--border)] text-[var(--text-secondary)]">
                <tr>
                  <th className="px-4 py-2.5 font-medium">Project</th>
                  <th className="px-4 py-2.5 font-medium">Region</th>
                  <th className="px-4 py-2.5 font-medium">PG</th>
                  <th className="px-4 py-2.5 font-medium">Status</th>
                  <th className="px-4 py-2.5 font-medium">Compute</th>
                  <th className="px-4 py-2.5 font-medium">Updated</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)]">
                {!data?.recentProjects?.length ? (
                  <tr>
                    <td
                      colSpan={6}
                      className="px-4 py-8 text-center text-[var(--text-secondary)]"
                    >
                      No projects yet. Create your first PostgreSQL 17 project to get started.
                    </td>
                  </tr>
                ) : (
                  data.recentProjects.map((p) => (
                    <tr
                      key={p.id}
                      className="hover:bg-[var(--surface-elevated)] transition-colors"
                    >
                      <td className="px-4 py-2.5 font-medium">
                        <Link
                          href={`/projects/${p.id}`}
                          className="text-[var(--text-primary)] hover:text-[var(--accent)]"
                        >
                          {p.name}
                        </Link>
                        <div className="text-[11px] font-mono text-[var(--text-secondary)]">
                          {p.tenantId}
                        </div>
                      </td>
                      <td className="px-4 py-2.5 font-mono text-[var(--text-secondary)]">
                        {p.region}
                      </td>
                      <td className="px-4 py-2.5 font-mono">v{p.pgVersion}</td>
                      <td className="px-4 py-2.5">
                        <Badge
                          variant={
                            p.status === "ACTIVE"
                              ? "success"
                              : p.status === "SUSPENDED"
                              ? "warning"
                              : "default"
                          }
                        >
                          {p.status}
                        </Badge>
                      </td>
                      <td className="px-4 py-2.5 font-mono text-[var(--text-secondary)]">
                        {p.computeSize}
                      </td>
                      <td className="px-4 py-2.5 text-[var(--text-secondary)]">
                        {formatRelativeTime(p.updatedAt)}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </Card>

        {/* Recent Activity */}
        <Card>
          <CardHeader>
            <CardTitle>Recent Activity</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {!data?.recentActivity?.length ? (
              <div className="p-6 text-center text-xs text-[var(--text-secondary)]">
                No recent audit events recorded.
              </div>
            ) : (
              <ul className="divide-y divide-[var(--border)]">
                {data.recentActivity.map((ev) => (
                  <li key={ev.id} className="px-4 py-2.5 text-xs">
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-mono font-medium text-[var(--text-primary)]">
                        {ev.action}
                      </span>
                      <span className="text-[11px] text-[var(--text-secondary)]">
                        {formatRelativeTime(ev.createdAt)}
                      </span>
                    </div>
                    <p className="mt-0.5 text-[11px] text-[var(--text-secondary)] truncate">
                      {ev.resourceType}: {ev.resourceId}
                    </p>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
