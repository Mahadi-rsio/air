"use client";

import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Copy, Key, Plus, Trash2, Ban } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { useConsoleStore } from "@/stores/console-store";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Dialog } from "@/components/ui/dialog";
import { useToast } from "@/components/ui/toast";
import { formatRelativeTime } from "@/lib/utils";

const AVAILABLE_SCOPES = [
  { id: "projects:read", label: "Read projects" },
  { id: "projects:write", label: "Manage projects" },
  { id: "databases:query", label: "Execute SQL queries" },
  { id: "databases:write", label: "Modify table data" },
  { id: "backups:read", label: "Read backups" },
  { id: "backups:write", label: "Create & restore backups" },
  { id: "api_keys:manage", label: "Manage API keys" },
];

interface ApiKeyItem {
  id: string;
  name: string;
  prefix: string;
  scopes: string[];
  expiresAt: string | null;
  lastUsedAt: string | null;
  revokedAt: string | null;
  createdAt: string;
}

export default function ApiKeysPage() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const activeWorkspaceId = useConsoleStore((s) => s.activeWorkspaceId);

  const [createOpen, setCreateOpen] = React.useState(false);
  const [name, setName] = React.useState("");
  const [scopes, setScopes] = React.useState<string[]>(["projects:read", "databases:query"]);
  const [expiresInDays, setExpiresInDays] = React.useState<string>("90");
  const [newSecret, setNewSecret] = React.useState<string | null>(null);

  const { data: keys = [], isLoading } = useQuery({
    queryKey: ["api-keys", activeWorkspaceId],
    queryFn: async () => {
      const res = await apiClient.get<{ apiKeys: ApiKeyItem[] }>(
        `/api-keys?workspaceId=${activeWorkspaceId}`
      );
      return res.data.apiKeys;
    },
    enabled: Boolean(activeWorkspaceId),
  });

  const createMutation = useMutation({
    mutationFn: async () => {
      const res = await apiClient.post("/api-keys", {
        workspaceId: activeWorkspaceId,
        name,
        scopes,
        expiresInDays: expiresInDays ? Number(expiresInDays) : null,
      });
      return res.data.apiKey as { secret: string };
    },
    onSuccess: (created) => {
      setNewSecret(created.secret);
      setName("");
      queryClient.invalidateQueries({ queryKey: ["api-keys", activeWorkspaceId] });
      toast({ title: "API key created", variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Failed to create API key", description: err.message, variant: "error" });
    },
  });

  const revokeMutation = useMutation({
    mutationFn: async ({ id, mode }: { id: string; mode: "revoke" | "delete" }) => {
      await apiClient.delete(
        `/api-keys?workspaceId=${activeWorkspaceId}&id=${id}&mode=${mode}`
      );
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["api-keys", activeWorkspaceId] });
      toast({ title: "API key updated", variant: "success" });
    },
  });

  return (
    <div className="space-y-5 max-w-5xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">API Keys</h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Control-plane API keys for programmatic access. Secrets are SHA-256 hashed and shown only once.
          </p>
        </div>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="h-3.5 w-3.5" /> Create API Key
        </Button>
      </div>

      {newSecret && (
        <div className="rounded-lg border border-emerald-500/40 bg-emerald-500/10 p-4 space-y-2">
          <p className="text-xs font-semibold text-[var(--accent)]">
            Copy your new API key now — you will not be able to see it again.
          </p>
          <div className="flex items-center gap-2">
            <code className="flex-1 rounded border border-[var(--border)] bg-[var(--bg)] px-3 py-1.5 font-mono text-xs">
              {newSecret}
            </code>
            <Button
              size="sm"
              onClick={() => {
                navigator.clipboard.writeText(newSecret);
                toast({ title: "API key copied", variant: "success" });
              }}
            >
              <Copy className="h-3.5 w-3.5" /> Copy
            </Button>
            <Button variant="ghost" size="sm" onClick={() => setNewSecret(null)}>
              Dismiss
            </Button>
          </div>
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Key className="h-3.5 w-3.5 text-[var(--accent)]" />
            Workspace API Keys
          </CardTitle>
        </CardHeader>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-[var(--border)] text-[var(--text-secondary)]">
              <tr>
                <th className="px-4 py-2.5 font-medium">Name</th>
                <th className="px-4 py-2.5 font-medium">Prefix</th>
                <th className="px-4 py-2.5 font-medium">Scopes</th>
                <th className="px-4 py-2.5 font-medium">Status</th>
                <th className="px-4 py-2.5 font-medium">Last Used</th>
                <th className="px-4 py-2.5 text-right font-medium">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {isLoading ? (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-[var(--text-secondary)]">
                    Loading API keys...
                  </td>
                </tr>
              ) : keys.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-[var(--text-secondary)]">
                    No API keys created yet.
                  </td>
                </tr>
              ) : (
                keys.map((k) => {
                  const isRevoked = Boolean(k.revokedAt);
                  const isExpired =
                    k.expiresAt && new Date(k.expiresAt).getTime() < Date.now();
                  return (
                    <tr key={k.id} className="hover:bg-[var(--surface-elevated)]">
                      <td className="px-4 py-2.5 font-medium">{k.name}</td>
                      <td className="px-4 py-2.5 font-mono text-[var(--text-secondary)]">
                        {k.prefix}••••
                      </td>
                      <td className="px-4 py-2.5">
                        <div className="flex flex-wrap gap-1">
                          {k.scopes.map((s) => (
                            <Badge key={s} variant="muted">
                              {s}
                            </Badge>
                          ))}
                        </div>
                      </td>
                      <td className="px-4 py-2.5">
                        {isRevoked ? (
                          <Badge variant="danger">Revoked</Badge>
                        ) : isExpired ? (
                          <Badge variant="warning">Expired</Badge>
                        ) : (
                          <Badge variant="success">Active</Badge>
                        )}
                      </td>
                      <td className="px-4 py-2.5 text-[var(--text-secondary)]">
                        {formatRelativeTime(k.lastUsedAt)}
                      </td>
                      <td className="px-4 py-2.5 text-right space-x-1">
                        {!isRevoked && (
                          <Button
                            variant="ghost"
                            size="icon"
                            title="Revoke key"
                            onClick={() =>
                              revokeMutation.mutate({ id: k.id, mode: "revoke" })
                            }
                          >
                            <Ban className="h-3.5 w-3.5 text-[var(--warning)]" />
                          </Button>
                        )}
                        <Button
                          variant="ghost"
                          size="icon"
                          title="Delete key"
                          onClick={() =>
                            revokeMutation.mutate({ id: k.id, mode: "delete" })
                          }
                        >
                          <Trash2 className="h-3.5 w-3.5 text-[var(--danger)]" />
                        </Button>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </Card>

      <Dialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        title="Create Control-Plane API Key"
      >
        <form
          onSubmit={(e) => {
            e.preventDefault();
            createMutation.mutate();
            setCreateOpen(false);
          }}
          className="space-y-4"
        >
          <div className="space-y-1.5">
            <label className="text-xs font-medium">Key Name</label>
            <Input
              required
              placeholder="ci-deploy-key"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-medium">Expiration (days, optional)</label>
            <Input
              type="number"
              min={1}
              max={365}
              value={expiresInDays}
              onChange={(e) => setExpiresInDays(e.target.value)}
            />
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-medium">Permission Scopes</label>
            <div className="grid grid-cols-1 gap-1.5 sm:grid-cols-2">
              {AVAILABLE_SCOPES.map((s) => (
                <label
                  key={s.id}
                  className="flex items-center gap-2 rounded border border-[var(--border)] px-2.5 py-1.5 text-xs cursor-pointer"
                >
                  <input
                    type="checkbox"
                    checked={scopes.includes(s.id)}
                    onChange={(e) => {
                      if (e.target.checked) setScopes((p) => [...p, s.id]);
                      else setScopes((p) => p.filter((x) => x !== s.id));
                    }}
                    className="accent-[var(--accent)]"
                  />
                  <span>{s.label}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="flex justify-end gap-2">
            <Button type="button" variant="outline" onClick={() => setCreateOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={createMutation.isPending || scopes.length === 0}>
              Generate Key
            </Button>
          </div>
        </form>
      </Dialog>
    </div>
  );
}
