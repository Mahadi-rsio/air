import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function DocsPage() {
  return (
    <div className="space-y-5 max-w-4xl mx-auto">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">AIR Documentation</h1>
        <p className="text-xs text-[var(--text-secondary)]">
          Architecture & quickstart for AIR Serverless PostgreSQL 17.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Control Plane & Data Plane Separation</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-xs text-[var(--text-secondary)] leading-relaxed">
          <p>
            AIR Console stores control-plane metadata (users, workspaces, projects, API key SHA-256 hashes, audit logs) in Cloudflare D1 / Drizzle ORM, while customer PostgreSQL 17 clusters run isolated in <code>air-server</code> behind the C++20 <code>air-gateway</code> pooler.
          </p>
          <p>
            Configure <code>AIR_SERVER_URL</code>, <code>AIR_SERVER_API_KEY</code>, and <code>AIR_INTERNAL_TOKEN</code> in <code>.env.local</code> to connect the console to your running <code>air-server</code> instance.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
