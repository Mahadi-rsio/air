"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Puzzle, Search } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";

interface PgExtension {
  name: string;
  default_version: string;
  installed_version: string | null;
  comment: string;
}

export default function ExtensionsPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const [search, setSearch] = React.useState("");

  const { data: extensions = [], isLoading, error } = useQuery({
    queryKey: ["project-extensions", projectId],
    queryFn: async () => {
      const res = await apiClient.get<{ extensions: PgExtension[] }>(
        `/projects/${projectId}/extensions`
      );
      return res.data.extensions;
    },
    retry: false,
  });

  const toggleMutation = useMutation({
    mutationFn: async ({
      name,
      action,
    }: {
      name: string;
      action: "enable" | "disable";
    }) => {
      await apiClient.post(`/projects/${projectId}/extensions`, { name, action });
    },
    onSuccess: (_, vars) => {
      queryClient.invalidateQueries({ queryKey: ["project-extensions", projectId] });
      toast({
        title: `Extension ${vars.name} ${vars.action}d`,
        variant: "success",
      });
    },
    onError: (err: Error) => {
      toast({ title: "Extension update failed", description: err.message, variant: "error" });
    },
  });

  const filtered = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return extensions;
    return extensions.filter(
      (e) =>
        e.name.toLowerCase().includes(q) ||
        (e.comment || "").toLowerCase().includes(q)
    );
  }, [extensions, search]);

  return (
    <div className="space-y-4 max-w-5xl mx-auto">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Extensions</h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Manage PostgreSQL 17 extensions from <code>pg_available_extensions</code>.
          </p>
        </div>
        <div className="relative w-64">
          <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-[var(--text-secondary)]" />
          <Input
            placeholder="Search extensions..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-8"
          />
        </div>
      </div>

      {error && (
        <div className="rounded-md border border-[var(--warning-border)] bg-[var(--warning-soft)] p-3 text-xs text-[var(--warning)]">
          {(error as Error).message}
        </div>
      )}

      <Card>
        <div className="divide-y divide-[var(--border)]">
          {isLoading ? (
            <div className="p-8 text-center text-xs text-[var(--text-secondary)]">
              Loading PostgreSQL extensions...
            </div>
          ) : (
            filtered.map((ext) => {
              const installed = Boolean(ext.installed_version);
              return (
                <div
                  key={ext.name}
                  className="flex items-center justify-between gap-4 px-4 py-3 text-xs"
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <Puzzle className="h-3.5 w-3.5 text-[var(--accent)]" />
                      <span className="font-mono font-semibold">{ext.name}</span>
                      <Badge variant={installed ? "success" : "muted"}>
                        {installed ? `v${ext.installed_version}` : `v${ext.default_version}`}
                      </Badge>
                    </div>
                    <p className="mt-1 text-[11px] text-[var(--text-secondary)] truncate">
                      {ext.comment || "PostgreSQL extension"}
                    </p>
                  </div>
                  <Button
                    variant={installed ? "outline" : "primary"}
                    size="sm"
                    disabled={toggleMutation.isPending}
                    onClick={() =>
                      toggleMutation.mutate({
                        name: ext.name,
                        action: installed ? "disable" : "enable",
                      })
                    }
                  >
                    {installed ? "Disable" : "Enable"}
                  </Button>
                </div>
              );
            })
          )}
        </div>
      </Card>
    </div>
  );
}
