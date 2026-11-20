"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import {
  ChevronDown,
  ChevronRight,
  FolderTree,
  RefreshCw,
  Search,
  Table2,
  KeyRound,
  ListTree,
} from "lucide-react";
import { apiClient } from "@/lib/axios";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { formatBytes } from "@/lib/utils";

interface SchemaObject {
  schema_name: string;
  table_name: string;
  object_type: string;
  row_estimate: number;
  total_bytes: number;
}

interface SchemaResponse {
  schemas: string[];
  objects: SchemaObject[];
  functions: Array<{ schema_name: string; function_name: string; return_type: string }>;
  enums: Array<{ schema_name: string; enum_name: string; labels: string[] }>;
  tableDetails: {
    schema: string;
    table: string;
    columns: Array<{
      column_name: string;
      data_type: string;
      udt_name: string;
      is_nullable: string;
      column_default: string | null;
    }>;
    constraints: Array<{
      constraint_name: string;
      constraint_type: string;
      column_name: string | null;
      foreign_table_name: string | null;
      foreign_column_name: string | null;
    }>;
    indexes: Array<{ indexname: string; indexdef: string }>;
  } | null;
}

export default function DatabaseExplorerPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const [search, setSearch] = React.useState("");
  const [expandedSchemas, setExpandedSchemas] = React.useState<Record<string, boolean>>({
    public: true,
  });
  const [selected, setSelected] = React.useState<{ schema: string; table: string } | null>(
    null
  );

  const { data, isLoading, error, refetch, isFetching } = useQuery({
    queryKey: ["explorer-schema", projectId, selected?.schema, selected?.table],
    queryFn: async () => {
      const qs = selected
        ? `?schema=${encodeURIComponent(selected.schema)}&table=${encodeURIComponent(selected.table)}`
        : "";
      const res = await apiClient.get<SchemaResponse>(
        `/projects/${projectId}/schema${qs}`
      );
      return res.data;
    },
    retry: false,
  });

  const filteredObjects = React.useMemo(() => {
    const objs = data?.objects || [];
    const q = search.trim().toLowerCase();
    if (!q) return objs;
    return objs.filter(
      (o) =>
        o.table_name.toLowerCase().includes(q) ||
        o.schema_name.toLowerCase().includes(q)
    );
  }, [data?.objects, search]);

  return (
    <div className="space-y-4 max-w-7xl mx-auto">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">
            Database Explorer
          </h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Inspect PostgreSQL schemas, tables, columns, constraints, indexes, and functions.
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => refetch()}
          disabled={isFetching}
        >
          <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? "animate-spin" : ""}`} />
          Refresh Schema
        </Button>
      </div>

      {error && (
        <div className="rounded-md border border-[var(--warning-border)] bg-[var(--warning-soft)] p-3 text-xs text-[var(--warning)]">
          {(error as Error).message}
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        {/* Tree */}
        <Card className="h-[32rem] lg:h-[calc(100vh-13rem)] flex flex-col">
          <CardHeader className="py-2.5">
            <div className="relative w-full">
              <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-[var(--text-secondary)]" />
              <Input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search database objects..."
                className="pl-8 h-7"
              />
            </div>
          </CardHeader>
          <CardContent className="flex-1 overflow-y-auto p-2 space-y-1">
            {isLoading ? (
              <p className="p-3 text-xs text-[var(--text-secondary)]">
                Introspecting catalog...
              </p>
            ) : !data?.schemas?.length ? (
              <p className="p-3 text-xs text-[var(--text-secondary)]">
                No schemas available.
              </p>
            ) : (
              data.schemas.map((s) => {
                const isOpen = expandedSchemas[s] ?? true;
                const schemaObjs = filteredObjects.filter((o) => o.schema_name === s);
                return (
                  <div key={s}>
                    <button
                      type="button"
                      onClick={() =>
                        setExpandedSchemas((prev) => ({ ...prev, [s]: !isOpen }))
                      }
                      className="flex w-full items-center justify-between rounded px-2 py-1.5 text-xs font-medium hover:bg-[var(--surface-elevated)]"
                    >
                      <span className="flex items-center gap-1.5">
                        {isOpen ? (
                          <ChevronDown className="h-3.5 w-3.5" />
                        ) : (
                          <ChevronRight className="h-3.5 w-3.5" />
                        )}
                        <FolderTree className="h-3.5 w-3.5 text-[var(--accent)]" />
                        <span className="font-mono">{s}</span>
                      </span>
                      <Badge variant="muted">{schemaObjs.length}</Badge>
                    </button>

                    {isOpen && (
                      <div className="ml-5 space-y-0.5 py-0.5">
                        {schemaObjs.map((obj) => {
                          const active =
                            selected?.schema === obj.schema_name &&
                            selected?.table === obj.table_name;
                          return (
                            <button
                              key={`${obj.schema_name}.${obj.table_name}`}
                              type="button"
                              onClick={() =>
                                setSelected({
                                  schema: obj.schema_name,
                                  table: obj.table_name,
                                })
                              }
                              className={`flex w-full items-center justify-between rounded px-2 py-1 text-xs ${
                                active
                                  ? "bg-[var(--surface-elevated)] text-[var(--text-primary)] font-medium"
                                  : "text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)]"
                              }`}
                            >
                              <span className="flex items-center gap-1.5 truncate font-mono">
                                <Table2 className="h-3 w-3 shrink-0" />
                                {obj.table_name}
                              </span>
                              <span className="text-[10px] font-mono">
                                {formatBytes(obj.total_bytes)}
                              </span>
                            </button>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })
            )}
          </CardContent>
        </Card>

        {/* Details Panel */}
        <Card className="lg:col-span-2 h-[32rem] lg:h-[calc(100vh-13rem)] overflow-y-auto">
          <CardHeader>
            <CardTitle className="font-mono">
              {selected
                ? `${selected.schema}.${selected.table}`
                : "Select a table or view to inspect"}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {!data?.tableDetails ? (
              <div className="py-12 text-center text-xs text-[var(--text-secondary)]">
                Choose a table from the schema tree on the left to view columns, keys, constraints, and indexes.
              </div>
            ) : (
              <>
                <div>
                  <h4 className="mb-2 text-xs font-semibold flex items-center gap-1.5">
                    <ListTree className="h-3.5 w-3.5 text-[var(--accent)]" />
                    Columns ({data.tableDetails.columns.length})
                  </h4>
                  <div className="overflow-x-auto rounded-md border border-[var(--border)]">
                    <table className="w-full text-left text-xs">
                      <thead className="border-b border-[var(--border)] bg-[var(--sidebar)] text-[var(--text-secondary)]">
                        <tr>
                          <th className="px-3 py-2 font-medium">Column</th>
                          <th className="px-3 py-2 font-medium">Type</th>
                          <th className="px-3 py-2 font-medium">Nullable</th>
                          <th className="px-3 py-2 font-medium">Default</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-[var(--border)]">
                        {data.tableDetails.columns.map((c) => (
                          <tr key={c.column_name}>
                            <td className="px-3 py-2 font-mono font-medium">
                              {c.column_name}
                            </td>
                            <td className="px-3 py-2 font-mono text-[var(--accent)]">
                              {c.udt_name}
                            </td>
                            <td className="px-3 py-2">
                              {c.is_nullable === "YES" ? "NULL" : "NOT NULL"}
                            </td>
                            <td className="px-3 py-2 font-mono text-[var(--text-secondary)]">
                              {c.column_default || "—"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div>
                  <h4 className="mb-2 text-xs font-semibold flex items-center gap-1.5">
                    <KeyRound className="h-3.5 w-3.5 text-[var(--accent)]" />
                    Constraints ({data.tableDetails.constraints.length})
                  </h4>
                  <div className="overflow-x-auto rounded-md border border-[var(--border)]">
                    <table className="w-full text-left text-xs">
                      <thead className="border-b border-[var(--border)] bg-[var(--sidebar)] text-[var(--text-secondary)]">
                        <tr>
                          <th className="px-3 py-2 font-medium">Name</th>
                          <th className="px-3 py-2 font-medium">Type</th>
                          <th className="px-3 py-2 font-medium">Column</th>
                          <th className="px-3 py-2 font-medium">References</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-[var(--border)]">
                        {data.tableDetails.constraints.length === 0 ? (
                          <tr>
                            <td colSpan={4} className="px-3 py-4 text-center text-[var(--text-secondary)]">
                              No constraints
                            </td>
                          </tr>
                        ) : (
                          data.tableDetails.constraints.map((c, idx) => (
                            <tr key={`${c.constraint_name}-${idx}`}>
                              <td className="px-3 py-2 font-mono">{c.constraint_name}</td>
                              <td className="px-3 py-2">
                                <Badge variant="muted">{c.constraint_type}</Badge>
                              </td>
                              <td className="px-3 py-2 font-mono">{c.column_name || "—"}</td>
                              <td className="px-3 py-2 font-mono text-[var(--text-secondary)]">
                                {c.foreign_table_name
                                  ? `${c.foreign_table_name}.${c.foreign_column_name}`
                                  : "—"}
                              </td>
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div>
                  <h4 className="mb-2 text-xs font-semibold">
                    Indexes ({data.tableDetails.indexes.length})
                  </h4>
                  <div className="space-y-1.5">
                    {data.tableDetails.indexes.map((idx) => (
                      <div
                        key={idx.indexname}
                        className="rounded border border-[var(--border)] bg-[var(--bg)] p-2.5 font-mono text-[11px]"
                      >
                        <div className="font-semibold text-[var(--text-primary)]">
                          {idx.indexname}
                        </div>
                        <div className="mt-1 text-[var(--text-secondary)] break-all">
                          {idx.indexdef}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
