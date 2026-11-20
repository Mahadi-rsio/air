"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Archive, CheckCircle2, RotateCcw, Loader2 } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Dialog } from "@/components/ui/dialog";
import { useToast } from "@/components/ui/toast";
import { formatBytes, formatRelativeTime } from "@/lib/utils";

interface BackupManifest {
  backup_id: string;
  tenant_id: string;
  type: string;
  status: string;
  start_lsn?: string;
  end_lsn?: string;
  cipher_bytes?: number;
  wal_count?: number;
  created_at?: string;
}

export default function BackupsPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const [restoreTarget, setRestoreTarget] = React.useState<BackupManifest | null>(null);
  const [newTenantId, setNewTenantId] = React.useState("");

  const { data: backups = [], isLoading, error } = useQuery({
    queryKey: ["project-backups", projectId],
    queryFn: async () => {
      const res = await apiClient.get<{ backups: BackupManifest[] }>(
        `/projects/${projectId}/backups`
      );
      return res.data.backups;
    },
    retry: false,
  });

  const triggerMutation = useMutation({
    mutationFn: async (type: "FULL" | "INCR" | "WAL") => {
      await apiClient.post(`/projects/${projectId}/backups`, { type });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["project-backups", projectId] });
      toast({ title: "Encrypted backup triggered", variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Backup failed", description: err.message, variant: "error" });
    },
  });

  const verifyMutation = useMutation({
    mutationFn: async (backupId: string) => {
      await apiClient.post(`/projects/${projectId}/backups/${backupId}/verify`);
    },
    onSuccess: () => {
      toast({ title: "Backup manifest & checksums verified", variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Verification failed", description: err.message, variant: "error" });
    },
  });

  const restoreMutation = useMutation({
    mutationFn: async () => {
      await apiClient.post(`/projects/${projectId}/restore`, {
        backupId: restoreTarget?.backup_id,
        newTenantId,
      });
    },
    onSuccess: () => {
      setRestoreTarget(null);
      setNewTenantId("");
      toast({
        title: "Non-destructive PITR restore completed",
        description: `Materialized into tenant ${newTenantId}`,
        variant: "success",
      });
    },
    onError: (err: Error) => {
      toast({ title: "Restore failed", description: err.message, variant: "error" });
    },
  });

  return (
    <div className="space-y-5 max-w-6xl mx-auto">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">
            Backups & Recovery (PITR)
          </h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Zstd-compressed, AES-256-GCM encrypted backups on Cloudflare R2 with immutable manifests.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={triggerMutation.isPending}
            onClick={() => triggerMutation.mutate("INCR")}
          >
            Incremental Backup
          </Button>
          <Button
            size="sm"
            disabled={triggerMutation.isPending}
            onClick={() => triggerMutation.mutate("FULL")}
          >
            {triggerMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            Create Full Backup
          </Button>
        </div>
      </div>

      {error && (
        <div className="rounded-md border border-[var(--warning-border)] bg-[var(--warning-soft)] p-3 text-xs text-[var(--warning)]">
          {(error as Error).message}
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Archive className="h-3.5 w-3.5 text-[var(--accent)]" />
            Backup History
          </CardTitle>
        </CardHeader>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-[var(--border)] text-[var(--text-secondary)]">
              <tr>
                <th className="px-4 py-2.5 font-medium">Backup ID</th>
                <th className="px-4 py-2.5 font-medium">Type</th>
                <th className="px-4 py-2.5 font-medium">Status</th>
                <th className="px-4 py-2.5 font-medium">LSN Range</th>
                <th className="px-4 py-2.5 font-medium">Size</th>
                <th className="px-4 py-2.5 font-medium">Created</th>
                <th className="px-4 py-2.5 text-right font-medium">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {isLoading ? (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-[var(--text-secondary)]">
                    Loading backup manifests...
                  </td>
                </tr>
              ) : backups.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-[var(--text-secondary)]">
                    No backups recorded yet.
                  </td>
                </tr>
              ) : (
                backups.map((b) => (
                  <tr key={b.backup_id} className="hover:bg-[var(--surface-elevated)]">
                    <td className="px-4 py-2.5 font-mono">{b.backup_id}</td>
                    <td className="px-4 py-2.5">
                      <Badge variant="muted">{b.type}</Badge>
                    </td>
                    <td className="px-4 py-2.5">
                      <Badge variant={b.status === "SUCCEEDED" ? "success" : "default"}>
                        {b.status}
                      </Badge>
                    </td>
                    <td className="px-4 py-2.5 font-mono text-[11px] text-[var(--text-secondary)]">
                      {b.start_lsn || "—"} → {b.end_lsn || "—"}
                    </td>
                    <td className="px-4 py-2.5 font-mono">
                      {formatBytes(b.cipher_bytes)}
                    </td>
                    <td className="px-4 py-2.5 text-[var(--text-secondary)]">
                      {formatRelativeTime(b.created_at)}
                    </td>
                    <td className="px-4 py-2.5 text-right space-x-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => verifyMutation.mutate(b.backup_id)}
                      >
                        <CheckCircle2 className="h-3.5 w-3.5 text-[var(--accent)]" /> Verify
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setRestoreTarget(b)}
                      >
                        <RotateCcw className="h-3.5 w-3.5" /> Restore
                      </Button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card>

      <Dialog
        open={Boolean(restoreTarget)}
        onOpenChange={(open) => !open && setRestoreTarget(null)}
        title="Non-Destructive PITR Restore"
        description="AIR restores into a new isolated tenant ID without overwriting the active cluster."
      >
        <form
          onSubmit={(e) => {
            e.preventDefault();
            restoreMutation.mutate();
          }}
          className="space-y-4"
        >
          <div className="space-y-1.5">
            <label className="text-xs font-medium">Source Backup ID</label>
            <Input disabled value={restoreTarget?.backup_id || ""} className="font-mono" />
          </div>
          <div className="space-y-1.5">
            <label className="text-xs font-medium">Target New Tenant ID</label>
            <Input
              required
              placeholder="restored_db_01"
              value={newTenantId}
              onChange={(e) => setNewTenantId(e.target.value.toLowerCase())}
              className="font-mono"
            />
          </div>
          <div className="flex justify-end gap-2">
            <Button type="button" variant="outline" onClick={() => setRestoreTarget(null)}>
              Cancel
            </Button>
            <Button type="submit" disabled={restoreMutation.isPending}>
              {restoreMutation.isPending ? "Restoring..." : "Start Restore"}
            </Button>
          </div>
        </form>
      </Dialog>
    </div>
  );
}
