import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function ApiReferencePage() {
  const endpoints = [
    { method: "GET", path: "/api/workspaces", desc: "List authenticated user workspaces" },
    { method: "POST", path: "/api/projects", desc: "Provision isolated PG17 project via air-server" },
    { method: "POST", path: "/api/projects/:id/sql", desc: "Execute SQL against tenant PG17 cluster" },
    { method: "GET", path: "/api/projects/:id/schema", desc: "Introspect schemas, tables, constraints, indexes" },
    { method: "GET/POST", path: "/api/projects/:id/backups", desc: "List and trigger encrypted R2 backups" },
    { method: "POST", path: "/api/projects/:id/restore", desc: "Non-destructive PITR restore into new tenant" },
    { method: "GET/POST/DELETE", path: "/api/api-keys", desc: "Manage scoped control-plane API keys" },
  ];

  return (
    <div className="space-y-5 max-w-4xl mx-auto">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">API Reference</h1>
        <p className="text-xs text-[var(--text-secondary)]">
          Authenticate with Better Auth session cookies, Bearer JWT, or <code>Authorization: Bearer air_key_...</code>.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Console REST Endpoints</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="divide-y divide-[var(--border)]">
            {endpoints.map((ep) => (
              <div key={ep.path} className="flex items-center justify-between px-4 py-2.5 text-xs">
                <div className="flex items-center gap-3">
                  <span className="font-mono font-semibold text-[var(--accent)] w-28">
                    {ep.method}
                  </span>
                  <code className="font-mono">{ep.path}</code>
                </div>
                <span className="text-[var(--text-secondary)]">{ep.desc}</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
