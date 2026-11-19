"use client";

import * as React from "react";
import { useParams, useRouter } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiClient } from "@/lib/axios";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Dialog } from "@/components/ui/dialog";
import { useToast } from "@/components/ui/toast";

export default function ProjectSettingsPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const router = useRouter();
  const queryClient = useQueryClient();
  const { toast } = useToast();

  const [name, setName] = React.useState("");
  const [description, setDescription] = React.useState("");
  const [confirmDelete, setConfirmDelete] = React.useState(false);

  const { data: project } = useQuery({
    queryKey: ["project", projectId],
    queryFn: async () => {
      const res = await apiClient.get(`/projects/${projectId}`);
      return res.data.project;
    },
  });

  React.useEffect(() => {
    if (project) {
      setName(project.name);
      setDescription(project.description || "");
    }
  }, [project]);

  const updateMutation = useMutation({
    mutationFn: async () => {
      await apiClient.patch(`/projects/${projectId}`, { name, description });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["project", projectId] });
      toast({ title: "Project settings updated", variant: "success" });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async () => {
      await apiClient.delete(`/projects/${projectId}`);
    },
    onSuccess: () => {
      toast({ title: "Project purged", variant: "success" });
      router.push("/projects");
    },
  });

  return (
    <div className="space-y-5 max-w-3xl mx-auto">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">Project Settings</h1>
        <p className="text-xs text-[var(--text-secondary)]">
          Manage general configuration and lifecycle controls.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>General</CardTitle>
        </CardHeader>
        <CardContent>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              updateMutation.mutate();
            }}
            className="space-y-4"
          >
            <div className="space-y-1.5">
              <label className="text-xs font-medium">Project Name</label>
              <Input value={name} onChange={(e) => setName(e.target.value)} />
            </div>
            <div className="space-y-1.5">
              <label className="text-xs font-medium">Description</label>
              <Input
                value={description}
                onChange={(e) => setDescription(e.target.value)}
              />
            </div>
            <div className="grid grid-cols-2 gap-3 text-xs">
              <div>
                <span className="text-[var(--text-secondary)]">Project ID</span>
                <p className="mt-1 font-mono">{project?.id || "—"}</p>
              </div>
              <div>
                <span className="text-[var(--text-secondary)]">Region / PG</span>
                <p className="mt-1 font-mono">
                  {project?.region} · PG {project?.pgVersion}
                </p>
              </div>
            </div>
            <Button type="submit" disabled={updateMutation.isPending}>
              Save Changes
            </Button>
          </form>
        </CardContent>
      </Card>

      <Card className="border-red-500/40">
        <CardHeader>
          <CardTitle className="text-[var(--danger)]">Danger Zone</CardTitle>
        </CardHeader>
        <CardContent className="flex items-center justify-between gap-4">
          <div>
            <p className="text-xs font-medium">Delete Project</p>
            <p className="text-[11px] text-[var(--text-secondary)]">
              Permanently stops the PostgreSQL 17 cluster, deletes PGDATA, and shreds its DEK.
            </p>
          </div>
          <Button variant="danger" onClick={() => setConfirmDelete(true)}>
            Delete Project
          </Button>
        </CardContent>
      </Card>

      <Dialog
        open={confirmDelete}
        onOpenChange={setConfirmDelete}
        title="Confirm Project Deletion"
        description="This operation cannot be undone."
      >
        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => setConfirmDelete(false)}>
            Cancel
          </Button>
          <Button
            variant="danger"
            disabled={deleteMutation.isPending}
            onClick={() => deleteMutation.mutate()}
          >
            {deleteMutation.isPending ? "Deleting..." : "Confirm Delete"}
          </Button>
        </div>
      </Dialog>
    </div>
  );
}
