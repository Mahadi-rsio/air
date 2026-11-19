"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Copy, Eye, EyeOff, Link2 } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/components/ui/toast";

interface ConnectionData {
  host: string;
  port: number;
  database: string;
  username: string;
  sslMode: string;
  hasPassword: boolean;
  maskedUri: string;
}

export default function ConnectionDetailsPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const { toast } = useToast();
  const [format, setFormat] = React.useState<"uri" | "psql" | "node" | "python" | "go" | "rust">("uri");
  const [revealedUri, setRevealedUri] = React.useState<string | null>(null);
  const [revealedPassword, setRevealedPassword] = React.useState<string | null>(null);

  const { data } = useQuery({
    queryKey: ["project-connection", projectId],
    queryFn: async () => {
      const res = await apiClient.get<{ connection: ConnectionData }>(
        `/projects/${projectId}/connection`
      );
      return res.data.connection;
    },
  });

  const revealMutation = useMutation({
    mutationFn: async () => {
      const res = await apiClient.post<{ password: string; uri: string }>(
        `/projects/${projectId}/connection`
      );
      return res.data;
    },
    onSuccess: (res) => {
      setRevealedUri(res.uri);
      setRevealedPassword(res.password);
    },
    onError: (err: Error) => {
      toast({ title: "Failed to reveal credentials", description: err.message, variant: "error" });
    },
  });

  const uri = revealedUri || data?.maskedUri || "";

  const snippet = React.useMemo(() => {
    switch (format) {
      case "psql":
        return `psql "${uri}"`;
      case "node":
        return `import { Pool } from "pg";\nconst pool = new Pool({ connectionString: "${uri}" });`;
      case "python":
        return `import psycopg\nconn = psycopg.connect("${uri}")`;
      case "go":
        return `conn, err := pgx.Connect(ctx, "${uri}")`;
      case "rust":
        return `let (client, connection) = tokio_postgres::connect("${uri}", tokio_postgres::NoTls).await?;`;
      default:
        return uri;
    }
  }, [format, uri]);

  return (
    <div className="space-y-5 max-w-4xl mx-auto">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">Connection Details</h1>
        <p className="text-xs text-[var(--text-secondary)]">
          Connect securely to your PostgreSQL 17 cluster through AIR Gateway.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Link2 className="h-3.5 w-3.5 text-[var(--accent)]" />
            Connection Parameters
          </CardTitle>
          <Badge variant="success">SSL {data?.sslMode || "require"}</Badge>
        </CardHeader>
        <CardContent className="grid grid-cols-2 gap-4 sm:grid-cols-4 text-xs">
          <div>
            <p className="text-[var(--text-secondary)]">Host</p>
            <p className="mt-1 font-mono font-medium">{data?.host || "—"}</p>
          </div>
          <div>
            <p className="text-[var(--text-secondary)]">Port</p>
            <p className="mt-1 font-mono font-medium">{data?.port || "—"}</p>
          </div>
          <div>
            <p className="text-[var(--text-secondary)]">Database</p>
            <p className="mt-1 font-mono font-medium">{data?.database || "—"}</p>
          </div>
          <div>
            <p className="text-[var(--text-secondary)]">Username</p>
            <p className="mt-1 font-mono font-medium">{data?.username || "—"}</p>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center gap-1.5">
            {(["uri", "psql", "node", "python", "go", "rust"] as const).map((tab) => (
              <button
                key={tab}
                type="button"
                onClick={() => setFormat(tab)}
                className={`rounded px-2.5 py-1 text-xs font-medium uppercase ${
                  format === tab
                    ? "bg-[var(--surface-elevated)] text-[var(--text-primary)]"
                    : "text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
                }`}
              >
                {tab}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                if (revealedPassword) {
                  setRevealedPassword(null);
                  setRevealedUri(null);
                } else {
                  revealMutation.mutate();
                }
              }}
            >
              {revealedPassword ? (
                <>
                  <EyeOff className="h-3.5 w-3.5" /> Hide
                </>
              ) : (
                <>
                  <Eye className="h-3.5 w-3.5" /> Reveal Password
                </>
              )}
            </Button>
            <Button
              size="sm"
              onClick={() => {
                navigator.clipboard.writeText(snippet);
                toast({ title: "Copied to clipboard", variant: "success" });
              }}
            >
              <Copy className="h-3.5 w-3.5" /> Copy
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <pre className="overflow-x-auto rounded-md border border-[var(--border)] bg-[var(--bg)] p-3.5 font-mono text-xs">
            {snippet}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}
