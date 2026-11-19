"use client";

import * as React from "react";
import { Users } from "lucide-react";
import { useSession } from "@/lib/auth/client";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export default function MembersPage() {
  const { data: sessionData } = useSession();

  return (
    <div className="space-y-5 max-w-4xl mx-auto">
      <div>
        <h1 className="text-lg font-semibold tracking-tight">Workspace Members</h1>
        <p className="text-xs text-[var(--text-secondary)]">
          Manage access roles for this AIR workspace.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Users className="h-3.5 w-3.5 text-[var(--accent)]" />
            Active Members
          </CardTitle>
        </CardHeader>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-[var(--border)] text-[var(--text-secondary)]">
              <tr>
                <th className="px-4 py-2.5 font-medium">Name</th>
                <th className="px-4 py-2.5 font-medium">Email</th>
                <th className="px-4 py-2.5 font-medium">Role</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              <tr>
                <td className="px-4 py-2.5 font-medium">
                  {sessionData?.user?.name || "—"}
                </td>
                <td className="px-4 py-2.5 text-[var(--text-secondary)]">
                  {sessionData?.user?.email || "—"}
                </td>
                <td className="px-4 py-2.5">
                  <Badge variant="success">owner</Badge>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
