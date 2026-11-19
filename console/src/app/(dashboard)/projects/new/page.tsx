"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Loader2, Server, ShieldCheck } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { useConsoleStore } from "@/stores/console-store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useToast } from "@/components/ui/toast";

export default function NewProjectPage() {
  const router = useRouter();
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const activeWorkspaceId = useConsoleStore((s) => s.activeWorkspaceId);

  const [name, setName] = React.useState("");
  const [tenantId, setTenantId] = React.useState("");
  const [region, setRegion] = React.useState<"us-east-1" | "us-west-2" | "eu-central-1" | "ap-southeast-1">("us-east-1");
  const [pgVersion, setPgVersion] = React.useState<"17" | "16">("17");
  const [computeSize, setComputeSize] = React.useState<"0.25-1cu" | "0.5-2cu" | "1-4cu" | "2-8cu">("0.25-1cu");
  const [databaseName, setDatabaseName] = React.useState("postgres");
  const [username, setUsername] = React.useState("app_user");
  const [description, setDescription] = React.useState("");

  function handleNameChange(val: string) {
    setName(val);
    if (!tenantId || tenantId === name.toLowerCase().replace(/[^a-z0-9]+/g, "_")) {
      setTenantId(
        val
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "_")
          .replace(/^[^a-z]+/, "")
          .slice(0, 40)
      );
    }
  }

  const createMutation = useMutation({
    mutationFn: async () => {
      const res = await apiClient.post("/projects", {
        workspaceId: activeWorkspaceId,
        name,
        tenantId,
        region,
        pgVersion,
        computeSize,
        databaseName,
        username,
        description,
      });
      return res.data.project;
    },
    onSuccess: (project) => {
      queryClient.invalidateQueries({ queryKey: ["projects", activeWorkspaceId] });
      toast({
        title: "PostgreSQL cluster provisioned",
        description: `Tenant ${project.tenantId} is ${project.status}`,
        variant: "success",
      });
      router.push(`/projects/${project.id}`);
    },
    onError: (err: Error) => {
      toast({
        title: "Provisioning failed",
        description: err.message,
        variant: "error",
      });
    },
  });

  return (
    <div className="max-w-4xl mx-auto space-y-5">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">
          Create PostgreSQL Project
        </h1>
        <p className="text-xs text-[var(--text-secondary)]">
          Provisions an isolated PostgreSQL cluster via AIR Control Plane (<code>POST /v1/databases</code>).
        </p>
      </div>

      <div className="grid grid-cols-1 gap-5 lg:grid-cols-3">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            createMutation.mutate();
          }}
          className="space-y-4 lg:col-span-2"
        >
          <Card>
            <CardHeader>
              <CardTitle>1. Project Identity</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3.5">
              <div className="space-y-1.5">
                <label className="text-xs font-medium">Project Name</label>
                <Input
                  required
                  placeholder="production-core"
                  value={name}
                  onChange={(e) => handleNameChange(e.target.value)}
                />
              </div>
              <div className="space-y-1.5">
                <label className="text-xs font-medium">
                  Tenant ID (AIR cluster identifier)
                </label>
                <Input
                  required
                  placeholder="production_core"
                  value={tenantId}
                  onChange={(e) => setTenantId(e.target.value.toLowerCase())}
                  className="font-mono"
                />
                <p className="text-[11px] text-[var(--text-secondary)]">
                  Lowercase letters, numbers, and underscores only. Used for SNI routing.
                </p>
              </div>
              <div className="space-y-1.5">
                <label className="text-xs font-medium">Description (optional)</label>
                <Input
                  placeholder="Primary transactional database"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                />
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>2. Engine & Compute Configuration</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 gap-3.5 sm:grid-cols-2">
              <div className="space-y-1.5">
                <label className="text-xs font-medium">Cloud Region</label>
                <select
                  value={region}
                  onChange={(e) => setRegion(e.target.value as typeof region)}
                  className="h-8 w-full rounded-md border border-[var(--border)] bg-[var(--surface)] px-2.5 text-xs"
                >
                  <option value="us-east-1">AWS us-east-1 (N. Virginia)</option>
                  <option value="us-west-2">AWS us-west-2 (Oregon)</option>
                  <option value="eu-central-1">AWS eu-central-1 (Frankfurt)</option>
                  <option value="ap-southeast-1">AWS ap-southeast-1 (Singapore)</option>
                </select>
              </div>
              <div className="space-y-1.5">
                <label className="text-xs font-medium">PostgreSQL Version</label>
                <select
                  value={pgVersion}
                  onChange={(e) => setPgVersion(e.target.value as typeof pgVersion)}
                  className="h-8 w-full rounded-md border border-[var(--border)] bg-[var(--surface)] px-2.5 text-xs"
                >
                  <option value="17">PostgreSQL 17 (Recommended)</option>
                  <option value="16">PostgreSQL 16</option>
                </select>
              </div>
              <div className="space-y-1.5 sm:col-span-2">
                <label className="text-xs font-medium">Compute Autoscaling</label>
                <select
                  value={computeSize}
                  onChange={(e) => setComputeSize(e.target.value as typeof computeSize)}
                  className="h-8 w-full rounded-md border border-[var(--border)] bg-[var(--surface)] px-2.5 text-xs"
                >
                  <option value="0.25-1cu">0.25 – 1 CU (Shared Dev / Starter)</option>
                  <option value="0.5-2cu">0.5 – 2 CU (Small Production)</option>
                  <option value="1-4cu">1 – 4 CU (Standard Production)</option>
                  <option value="2-8cu">2 – 8 CU (High Throughput)</option>
                </select>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>3. Initial Database & Credentials</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 gap-3.5 sm:grid-cols-2">
              <div className="space-y-1.5">
                <label className="text-xs font-medium">Database Name</label>
                <Input
                  value={databaseName}
                  onChange={(e) => setDatabaseName(e.target.value)}
                  className="font-mono"
                />
              </div>
              <div className="space-y-1.5">
                <label className="text-xs font-medium">Initial Role</label>
                <Input
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="font-mono"
                />
              </div>
              <div className="sm:col-span-2 flex items-center gap-2 rounded-md border border-[var(--border)] bg-[var(--surface-elevated)] px-3 py-2 text-[11px] text-[var(--text-secondary)]">
                <ShieldCheck className="h-4 w-4 text-[var(--accent)] shrink-0" />
                Password is cryptographically generated by air-server and encrypted at rest with AES-256-GCM.
              </div>
            </CardContent>
          </Card>

          <div className="flex justify-end gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={() => router.push("/projects")}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={createMutation.isPending || !activeWorkspaceId}>
              {createMutation.isPending && (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              )}
              {createMutation.isPending ? "Provisioning PG17..." : "Create Project"}
            </Button>
          </div>
        </form>

        {/* Configuration Summary */}
        <Card className="h-fit">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Server className="h-3.5 w-3.5 text-[var(--accent)]" />
              Provisioning Summary
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2.5 text-xs">
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Project</span>
              <span className="font-medium">{name || "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Tenant ID</span>
              <span className="font-mono">{tenantId || "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Region</span>
              <span className="font-mono">{region}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Engine</span>
              <span className="font-mono">PostgreSQL {pgVersion}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Compute</span>
              <span className="font-mono">{computeSize}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--text-secondary)]">Isolation</span>
              <span>Per-tenant PGDATA + DEK</span>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
