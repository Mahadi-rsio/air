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
    <div className="air-auth-bg flex min-h-screen items-center justify-center px-4 py-12">
      <div
        className="w-full max-w-md rounded-xl border border-[var(--border-strong)] bg-[var(--surface)]/95 backdrop-blur-md p-6"
        style={{ boxShadow: "var(--elevated-shadow)" }}
      >
        <div className="mb-6 flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-[var(--accent)] text-[var(--accent-fg)]">

              <Database className="h-4 w-4" />
            </div>
            <span className="text-sm font-semibold tracking-tight">
              AIR Onboarding
            </span>
          </div>
          <span className="rounded-full border border-[var(--accent-border)] bg-[var(--accent-soft)] px-2.5 py-0.5 text-[11px] font-mono font-medium text-[var(--accent)]">
            Step {step} / 2
          </span>
        </div>

        {error && (
          <div className="mb-4 rounded-lg border border-[var(--danger-border)] bg-[var(--danger-soft)] px-3.5 py-2.5 text-xs text-[var(--danger)]">
            {error}
          </div>
        )}

        {step === 1 ? (
          <form onSubmit={handleCreateWorkspace} className="space-y-4">
            <div>
              <h1 className="text-lg font-semibold tracking-tight">
                Create your workspace
              </h1>
              <p className="mt-1 text-[13px] text-[var(--text-secondary)]">
                Workspaces isolate your PostgreSQL projects, API keys, and team members.
              </p>
            </div>
            <div className="space-y-1.5">
              <label className="text-xs font-medium text-[var(--text-primary)]">
                Workspace name
              </label>
              <Input
                required
                value={workspaceName}
                onChange={(e) => setWorkspaceName(e.target.value)}
                placeholder="Acme Engineering"
              />
            </div>
            <Button type="submit" size="lg" className="w-full" disabled={loading}>
              {loading && <Loader2 className="h-4 w-4 animate-spin" />}
              Continue <ArrowRight className="h-4 w-4" />
            </Button>
          </form>
        ) : (
          <form onSubmit={handleCreateFirstProject} className="space-y-4">
            <div>
              <h1 className="text-lg font-semibold tracking-tight flex items-center gap-2">
                <Layers className="h-4 w-4 text-[var(--accent)]" />
                Provision your first PostgreSQL 17 project
              </h1>
              <p className="mt-1 text-[13px] text-[var(--text-secondary)]">
                Each project provisions an isolated PostgreSQL 17 cluster via AIR Control Plane.
              </p>
            </div>
            <div className="space-y-1.5">
              <label className="text-xs font-medium text-[var(--text-primary)]">
                Project name
              </label>
              <Input
                required
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
              />
            </div>
            <div className="space-y-1.5">
              <label className="text-xs font-medium text-[var(--text-primary)]">
                Tenant ID (PG database identifier)
              </label>
              <Input
                required
                value={tenantId}
                onChange={(e) => setTenantId(e.target.value.toLowerCase())}
                className="font-mono"
              />
            </div>
            <div className="flex items-center gap-2 pt-2">
              <Button type="submit" size="lg" className="flex-1" disabled={loading}>
                {loading && <Loader2 className="h-4 w-4 animate-spin" />}
                Provision project
              </Button>
              <Button
                type="button"
                variant="outline"
                size="lg"
                onClick={() => router.push("/dashboard")}
              >
                Skip
              </Button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}
