"use client";

import * as React from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { GitBranch, AlertTriangle, Archive } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatRelativeTime } from "@/lib/utils";

interface BranchResponse {
  branches: Array<{
    id: string;
    name: string;
    parentBranchId: string | null;
    tenantId: string | null;
    status: string;
    isDefault: boolean;
    createdAt: string;
  }>;
  capabilities: {
    nativeCopyOnWriteBranching: boolean;
    pitrCloneSupported: boolean;
    reason: string;
  };
}

export default function BranchesPage() {
  const { projectId } = useParams<{ projectId: string }>();

  const { data, isLoading } = useQuery({
    queryKey: ["branches", projectId],
    queryFn: async () => {
      const res = await apiClient.get<BranchResponse>(
        `/projects/${projectId}/branches`
      );
      return res.data;
    },
  });

  return (
    <div className="space-y-5 max-w-5xl mx-auto">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Branches</h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Branch topology and database cloning capabilities.
          </p>
        </div>
        <Link href={`/projects/${projectId}/backups`}>
          <Button variant="outline" size="sm">
            <Archive className="h-3.5 w-3.5" /> Clone via PITR Restore
          </Button>
        </Link>
      </div>

      {data?.capabilities && !data.capabilities.nativeCopyOnWriteBranching && (
        <div className="flex items-start gap-2.5 rounded-lg border border-amber-500/30 bg-amber-500/10 p-3.5 text-xs text-[var(--warning)]">
          <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
          <div>
            <p className="font-medium">
              Native Copy-on-Write Branching Pending Infrastructure Integration
            </p>
            <p className="mt-0.5 text-[var(--text-secondary)]">
              {data.capabilities.reason}
            </p>
          </div>
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <GitBranch className="h-3.5 w-3.5 text-[var(--accent)]" />
            Branch Tree
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-[var(--border)] text-[var(--text-secondary)]">
              <tr>
                <th className="px-4 py-2.5 font-medium">Branch Name</th>
                <th className="px-4 py-2.5 font-medium">Branch ID</th>
                <th className="px-4 py-2.5 font-medium">Tenant</th>
                <th className="px-4 py-2.5 font-medium">Status</th>
                <th className="px-4 py-2.5 font-medium">Created</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {isLoading ? (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-[var(--text-secondary)]">
                    Loading branches...
                  </td>
                </tr>
              ) : (
                (data?.branches || []).map((b) => (
                  <tr key={b.id} className="hover:bg-[var(--surface-elevated)]">
                    <td className="px-4 py-2.5 font-mono font-medium flex items-center gap-2">
                      <GitBranch className="h-3.5 w-3.5 text-[var(--accent)]" />
                      {b.name}
                      {b.isDefault && <Badge variant="success">default</Badge>}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-[var(--text-secondary)]">
                      {b.id}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-[var(--text-secondary)]">
                      {b.tenantId || "—"}
                    </td>
                    <td className="px-4 py-2.5">
                      <Badge variant={b.status === "ACTIVE" ? "success" : "muted"}>
                        {b.status}
                      </Badge>
                    </td>
                    <td className="px-4 py-2.5 text-[var(--text-secondary)]">
                      {formatRelativeTime(b.createdAt)}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </CardContent>
      </Card>
    </div>
  );
}
