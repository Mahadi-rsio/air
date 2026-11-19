"use client";

import * as React from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { apiClient } from "@/lib/axios";
import { useConsoleStore } from "@/stores/console-store";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useToast } from "@/components/ui/toast";

export default function WorkspaceSettingsPage() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const setActiveWorkspaceId = useConsoleStore((s) => s.setActiveWorkspaceId);
  const [newWorkspaceName, setNewWorkspaceName] = React.useState("");

  const createWsMutation = useMutation({
    mutationFn: async () => {
      const res = await apiClient.post("/workspaces", { name: newWorkspaceName });
      return res.data.workspace;
    },
    onSuccess: (ws) => {
      setNewWorkspaceName("");
      setActiveWorkspaceId(ws.id);
      queryClient.invalidateQueries({ queryKey: ["workspaces"] });
      toast({ title: `Workspace "${ws.name}" created`, variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Failed to create workspace", description: err.message, variant: "error" });
    },
  });

  return (
    <div className="space-y-5 max-w-3xl mx-auto">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">Workspace Settings</h1>
        <p className="text-xs text-[var(--text-secondary)]">
          Create additional workspaces or manage control-plane preferences.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Create New Workspace</CardTitle>
        </CardHeader>
        <CardContent>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              createWsMutation.mutate();
            }}
            className="flex items-center gap-2"
          >
            <Input
              required
              placeholder="Staging Engineering"
              value={newWorkspaceName}
              onChange={(e) => setNewWorkspaceName(e.target.value)}
              className="max-w-sm"
            />
            <Button type="submit" disabled={createWsMutation.isPending}>
              Create Workspace
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
