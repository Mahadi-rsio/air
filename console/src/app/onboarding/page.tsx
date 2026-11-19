"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Database, Layers, ArrowRight, Loader2 } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { useConsoleStore } from "@/stores/console-store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useToast } from "@/components/ui/toast";

export default function OnboardingPage() {
  const router = useRouter();
  const { toast } = useToast();
  const setActiveWorkspaceId = useConsoleStore((s) => s.setActiveWorkspaceId);

  const [step, setStep] = React.useState<1 | 2>(1);
  const [workspaceName, setWorkspaceName] = React.useState("Production");
  const [workspaceId, setWorkspaceId] = React.useState<string | null>(null);
  const [projectName, setProjectName] = React.useState("core-db");
  const [tenantId, setTenantId] = React.useState("core_db");
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  async function handleCreateWorkspace(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const res = await apiClient.post("/workspaces", { name: workspaceName });
      const wsId = res.data.workspace.id as string;
      setWorkspaceId(wsId);
      setActiveWorkspaceId(wsId);
      setStep(2);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create workspace");
    } finally {
      setLoading(false);
    }
  }

  async function handleCreateFirstProject(e: React.FormEvent) {
    e.preventDefault();
    if (!workspaceId) return;
    setLoading(true);
    setError(null);
    try {
      const res = await apiClient.post("/projects", {
        workspaceId,
        name: projectName,
        tenantId,
        region: "us-east-1",
        pgVersion: "17",
        computeSize: "0.25-1cu",
      });
      toast({ title: "Project provisioned", variant: "success" });
      router.push(`/projects/${res.data.project.id}`);
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Provisioning failed. Ensure air-server is running."
      );
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-[var(--bg)] px-4">
      <div className="w-full max-w-md rounded-lg border border-[var(--border)] bg-[var(--surface)] p-6">
        <div className="mb-5 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="flex h-7 w-7 items-center justify-center rounded-md bg-[var(--accent)] text-[var(--accent-fg)]">
              <Database className="h-4 w-4" />
            </div>
            <span className="text-sm font-semibold">AIR Onboarding</span>
          </div>
          <span className="text-[11px] font-mono text-[var(--text-secondary)]">
            Step {step} of 2
          </span>
        </div>

        {error && (
          <div className="mb-4 rounded-md border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-[var(--danger)]">
            {error}
          </div>
        )}

        {step === 1 ? (
          <form onSubmit={handleCreateWorkspace} className="space-y-4">
            <div>
              <h1 className="text-base font-semibold">Create your workspace</h1>
              <p className="mt-1 text-xs text-[var(--text-secondary)]">
                Workspaces isolate your PostgreSQL projects, API keys, and team members.
              </p>
            </div>
            <div className="space-y-1.5">
              <label className="text-xs font-medium">Workspace name</label>
              <Input
                required
                value={workspaceName}
                onChange={(e) => setWorkspaceName(e.target.value)}
                placeholder="Acme Engineering"
              />
            </div>
            <Button type="submit" className="w-full" disabled={loading}>
              {loading && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              Continue <ArrowRight className="h-3.5 w-3.5" />
            </Button>
          </form>
        ) : (
          <form onSubmit={handleCreateFirstProject} className="space-y-4">
            <div>
              <h1 className="text-base font-semibold flex items-center gap-2">
                <Layers className="h-4 w-4 text-[var(--accent)]" />
                Provision your first PostgreSQL 17 project
              </h1>
              <p className="mt-1 text-xs text-[var(--text-secondary)]">
                Each project provisions an isolated PostgreSQL 17 cluster via AIR Control Plane.
              </p>
            </div>
            <div className="space-y-1.5">
              <label className="text-xs font-medium">Project name</label>
              <Input
                required
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
              />
            </div>
            <div className="space-y-1.5">
              <label className="text-xs font-medium">Tenant ID (PG database identifier)</label>
              <Input
                required
                value={tenantId}
                onChange={(e) => setTenantId(e.target.value.toLowerCase())}
                className="font-mono"
              />
            </div>
            <div className="flex items-center gap-2 pt-2">
              <Button type="submit" className="flex-1" disabled={loading}>
                {loading && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                Provision project
              </Button>
              <Button
                type="button"
                variant="outline"
                onClick={() => router.push("/dashboard")}
              >
                Skip to dashboard
              </Button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}
