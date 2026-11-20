"use client";

import * as React from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  ArrowUpRight,
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
    <div className="space-y-6 max-w-7xl mx-auto">
      {/* Page Header */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold tracking-tight text-[var(--text-primary)]">
            Workspace Overview
          </h1>
          <p className="mt-1 text-[13px] text-[var(--text-secondary)]">
            Control-plane summary for your serverless PostgreSQL 17 clusters.
          </p>
        </div>
        <div className="flex items-center gap-2.5">
          <Badge variant={data?.stats.backendConnected ? "success" : "warning"}>
            <span
              className={`h-1.5 w-1.5 rounded-full ${
                data?.stats.backendConnected
                  ? "bg-[var(--success)]"
                  : "bg-[var(--warning)]"
              }`}
            />
            {data?.stats.backendConnected
              ? "air-server connected"
              : "air-server offline"}
          </Badge>
          <Link href="/projects/new">
            <Button size="md">
              <Plus className="h-4 w-4" /> New Project
            </Button>
          </Link>
        </div>
      </div>

      {error && (
        <div className="rounded-xl border border-[var(--danger-border)] bg-[var(--danger-soft)] p-3.5 text-xs text-[var(--danger)]">
          {(error as Error).message}
        </div>
      )}

      {/* Metrics */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardContent className="p-5">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-[var(--text-secondary)]">
                Total Projects
              </span>
              <span className="flex h-7 w-7 items-center justify-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)]">
                <Database className="h-3.5 w-3.5" />
              </span>
            </div>
            <p className="mt-3 text-2xl font-semibold font-mono tabular-nums tracking-tight">
              {isLoading ? "—" : data?.stats.totalProjects ?? 0}
            </p>
            <p className="mt-1 text-xs text-[var(--text-muted)]">
              {data?.stats.activeProjects ?? 0} active clusters
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-5">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-[var(--text-secondary)]">
                Total Databases
              </span>
              <span className="flex h-7 w-7 items-center justify-center rounded-lg bg-[var(--surface-elevated)] text-[var(--text-secondary)]">
                <Server className="h-3.5 w-3.5" />
              </span>
            </div>
            <p className="mt-3 text-2xl font-semibold font-mono tabular-nums tracking-tight">
              {isLoading ? "—" : data?.stats.totalDatabases ?? 0}
            </p>
            <p className="mt-1 text-xs text-[var(--text-muted)]">
              PostgreSQL 17 isolated tenants
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-5">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-[var(--text-secondary)]">
                Total Branches
              </span>
              <span className="flex h-7 w-7 items-center justify-center rounded-lg bg-[var(--surface-elevated)] text-[var(--text-secondary)]">
                <GitBranch className="h-3.5 w-3.5" />
              </span>
            </div>
            <p className="mt-3 text-2xl font-semibold font-mono tabular-nums tracking-tight">
              {isLoading ? "—" : data?.stats.totalBranches ?? 0}
            </p>
            <p className="mt-1 text-xs text-[var(--text-muted)]">
              Tracked across projects
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-5">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-[var(--text-secondary)]">
                Monthly Est. Cost
              </span>
              <span className="flex h-7 w-7 items-center justify-center rounded-lg bg-[var(--surface-elevated)] text-[var(--text-secondary)]">
                <CreditCard className="h-3.5 w-3.5" />
              </span>
            </div>
            <p className="mt-3 text-lg font-semibold text-[var(--text-secondary)]">
              Billing API Pending
            </p>
            <p className="mt-1 text-xs text-[var(--text-muted)]">
              Metering integration required
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {[
          {
            href: "/projects/new",
            label: "Create Project",
            desc: "Provision PG 17 cluster",
            icon: Plus,
          },
          {
            href: firstProjectId
              ? `/projects/${firstProjectId}/connection`
              : "/projects",
            label: "Connect Database",
            desc: "Pooling & direct URIs",
            icon: Link2,
          },
          {
            href: firstProjectId
              ? `/projects/${firstProjectId}/sql`
              : "/projects",
            label: "Open SQL Editor",
            desc: "Run queries & inspect",
            icon: Terminal,
          },
          {
            href: "/billing",
            label: "View Usage",
            desc: "Compute & storage",
            icon: Activity,
          },
        ].map((item) => {
          const Icon = item.icon;
          return (
            <Link
              key={item.label}
              href={item.href}
              className="group flex items-center justify-between rounded-xl border border-[var(--border)] bg-[var(--surface)] p-4 transition-all duration-150 hover:border-[var(--accent-border)] hover:bg-[var(--surface-elevated)]"
              style={{ boxShadow: "var(--card-shadow)" }}
            >
              <div className="flex items-center gap-3 min-w-0">
                <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)] transition-colors group-hover:bg-[var(--accent)] group-hover:text-[var(--accent-fg)]">
                  <Icon className="h-4 w-4" />
                </span>
                <div className="min-w-0">
                  <p className="text-[13px] font-medium text-[var(--text-primary)] truncate">
                    {item.label}
                  </p>
                  <p className="text-xs text-[var(--text-muted)] truncate">
                    {item.desc}
                  </p>
                </div>
              </div>
              <ArrowUpRight className="h-4 w-4 shrink-0 text-[var(--text-muted)] transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5 group-hover:text-[var(--accent)]" />
            </Link>
          );
        })}
      </div>

      <div className="grid grid-cols-1 gap-5 lg:grid-cols-3">
        {/* Recent Projects */}
        <Card className="lg:col-span-2 overflow-hidden">
          <CardHeader>
            <CardTitle>Recent Projects</CardTitle>
            <Link
              href="/projects"
              className="text-xs font-medium text-[var(--accent)] hover:text-[var(--accent-hover)] transition-colors"
            >
              View all →
            </Link>
          </CardHeader>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-[13px]">
              <thead className="border-b border-[var(--border)] bg-[var(--surface-subtle)] text-[11px] uppercase tracking-wider text-[var(--text-muted)]">
                <tr>
                  <th className="px-5 py-3 font-medium">Project</th>
                  <th className="px-4 py-3 font-medium">Region</th>
                  <th className="px-4 py-3 font-medium">PG</th>
                  <th className="px-4 py-3 font-medium">Status</th>
                  <th className="px-4 py-3 font-medium">Compute</th>
                  <th className="px-5 py-3 font-medium text-right">Updated</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)]">
                {!data?.recentProjects?.length ? (
                  <tr>
                    <td
                      colSpan={6}
                      className="px-5 py-10 text-center text-xs text-[var(--text-secondary)]"
                    >
                      No projects yet. Create your first PostgreSQL 17 project to get started.
                    </td>
                  </tr>
                ) : (
                  data.recentProjects.map((p) => (
                    <tr
                      key={p.id}
                      className="hover:bg-[var(--surface-elevated)]/70 transition-colors"
                    >
                      <td className="px-5 py-3 font-medium">
                        <Link
                          href={`/projects/${p.id}`}
                          className="text-[var(--text-primary)] hover:text-[var(--accent)] transition-colors"
                        >
                          {p.name}
                        </Link>
                        <div className="text-[11px] font-mono text-[var(--text-muted)]">
                          {p.tenantId}
                        </div>
                      </td>
                      <td className="px-4 py-3 font-mono text-xs text-[var(--text-secondary)]">
                        {p.region}
                      </td>
                      <td className="px-4 py-3 font-mono text-xs">v{p.pgVersion}</td>
                      <td className="px-4 py-3">
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
                      <td className="px-4 py-3 font-mono text-xs text-[var(--text-secondary)]">
                        {p.computeSize}
                      </td>
                      <td className="px-5 py-3 text-right text-xs text-[var(--text-muted)]">
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
        <Card className="overflow-hidden">
          <CardHeader>
            <CardTitle>Recent Activity</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {!data?.recentActivity?.length ? (
              <div className="p-8 text-center text-xs text-[var(--text-secondary)]">
                No recent audit events recorded.
              </div>
            ) : (
              <ul className="divide-y divide-[var(--border)]">
                {data.recentActivity.map((ev) => (
                  <li
                    key={ev.id}
                    className="px-5 py-3 text-xs hover:bg-[var(--surface-elevated)]/50 transition-colors"
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-mono font-medium text-[var(--text-primary)]">
                        {ev.action}
                      </span>
                      <span className="text-[11px] text-[var(--text-muted)] shrink-0">
                        {formatRelativeTime(ev.createdAt)}
                      </span>
                    </div>
                    <p className="mt-1 text-[11px] text-[var(--text-secondary)] truncate">
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
