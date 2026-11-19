"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus, Shield } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Dialog } from "@/components/ui/dialog";
import { useToast } from "@/components/ui/toast";

interface PgRole {
  name: string;
  is_superuser: boolean;
  can_login: boolean;
  can_create_db: boolean;
  can_create_role: boolean;
  bypass_rls: boolean;
  conn_limit: number;
}

export default function RolesPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const [createOpen, setCreateOpen] = React.useState(false);
  const [roleName, setRoleName] = React.useState("");
  const [password, setPassword] = React.useState("");

  const { data: roles = [], isLoading, error } = useQuery({
    queryKey: ["project-roles", projectId],
    queryFn: async () => {
      const res = await apiClient.get<{ roles: PgRole[] }>(
        `/projects/${projectId}/roles`
      );
      return res.data.roles;
    },
    retry: false,
  });

  const createMutation = useMutation({
    mutationFn: async () => {
      await apiClient.post(`/projects/${projectId}/roles`, {
        name: roleName,
        password,
      });
    },
    onSuccess: () => {
      setCreateOpen(false);
      setRoleName("");
      setPassword("");
      queryClient.invalidateQueries({ queryKey: ["project-roles", projectId] });
      toast({ title: "PostgreSQL role created", variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Failed to create role", description: err.message, variant: "error" });
    },
  });

  return (
    <div className="space-y-4 max-w-5xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Roles & Users</h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Manage PostgreSQL database roles from <code>pg_roles</code>.
          </p>
        </div>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="h-3.5 w-3.5" /> Create Role
        </Button>
      </div>

      {error && (
        <div className="rounded-md border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-[var(--warning)]">
          {(error as Error).message}
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Shield className="h-3.5 w-3.5 text-[var(--accent)]" />
            Database Roles
          </CardTitle>
        </CardHeader>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-[var(--border)] text-[var(--text-secondary)]">
              <tr>
                <th className="px-4 py-2.5 font-medium">Role Name</th>
                <th className="px-4 py-2.5 font-medium">Login</th>
                <th className="px-4 py-2.5 font-medium">Superuser</th>
                <th className="px-4 py-2.5 font-medium">Create DB</th>
                <th className="px-4 py-2.5 font-medium">Bypass RLS</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {isLoading ? (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-[var(--text-secondary)]">
                    Loading roles...
                  </td>
                </tr>
              ) : (
                roles.map((r) => (
                  <tr key={r.name} className="hover:bg-[var(--surface-elevated)]">
                    <td className="px-4 py-2.5 font-mono font-medium">{r.name}</td>
                    <td className="px-4 py-2.5">
                      <Badge variant={r.can_login ? "success" : "muted"}>
                        {r.can_login ? "LOGIN" : "NOLOGIN"}
                      </Badge>
                    </td>
                    <td className="px-4 py-2.5">
                      {r.is_superuser ? (
                        <Badge variant="warning">SUPERUSER</Badge>
                      ) : (
                        "No"
                      )}
                    </td>
                    <td className="px-4 py-2.5">{r.can_create_db ? "Yes" : "No"}</td>
                    <td className="px-4 py-2.5">{r.bypass_rls ? "Yes" : "No"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card>

      <Dialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        title="Create PostgreSQL Role"
      >
        <form
          onSubmit={(e) => {
            e.preventDefault();
            createMutation.mutate();
          }}
          className="space-y-3.5"
        >
          <div className="space-y-1.5">
            <label className="text-xs font-medium">Role Name</label>
            <Input
              required
              placeholder="readonly_user"
              value={roleName}
              onChange={(e) => setRoleName(e.target.value.toLowerCase())}
              className="font-mono"
            />
          </div>
          <div className="space-y-1.5">
            <label className="text-xs font-medium">Password</label>
            <Input
              type="password"
              required
              minLength={8}
              placeholder="Min. 8 characters"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <Button type="button" variant="outline" onClick={() => setCreateOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={createMutation.isPending}>
              {createMutation.isPending ? "Creating..." : "Create Role"}
            </Button>
          </div>
        </form>
      </Dialog>
    </div>
  );
}
