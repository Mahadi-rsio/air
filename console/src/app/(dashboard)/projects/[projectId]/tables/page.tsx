"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Download,
  Plus,
  RefreshCw,
  Table2,
  Trash2,
} from "lucide-react";
import { apiClient } from "@/lib/axios";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog } from "@/components/ui/dialog";
import { useToast } from "@/components/ui/toast";

interface TableObject {
  schema_name: string;
  table_name: string;
  object_type: string;
  row_estimate: number;
}

interface TableDataResponse {
  total: number;
  columns: Array<{
    column_name: string;
    data_type: string;
    is_nullable: string;
  }>;
  rows: Record<string, unknown>[];
}

export default function TablesStudioPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const queryClient = useQueryClient();
  const { toast } = useToast();

  const [selected, setSelected] = React.useState<{ schema: string; table: string } | null>(
    null
  );
  const [page, setPage] = React.useState(0);
  const [insertOpen, setInsertOpen] = React.useState(false);
  const [insertValues, setInsertValues] = React.useState<Record<string, string>>({});
  const [deleteRow, setDeleteRow] = React.useState<Record<string, unknown> | null>(null);

  const pageSize = 50;

  const { data: schemaData, error: schemaError } = useQuery({
    queryKey: ["tables-list", projectId],
    queryFn: async () => {
      const res = await apiClient.get<{ objects: TableObject[] }>(
        `/projects/${projectId}/schema`
      );
      return res.data.objects.filter((o) => o.object_type === "table");
    },
    retry: false,
  });

  React.useEffect(() => {
    if (schemaData && schemaData.length > 0 && !selected) {
      setSelected({
        schema: schemaData[0].schema_name,
        table: schemaData[0].table_name,
      });
    }
  }, [schemaData, selected]);

  const {
    data: tableData,
    isLoading: rowsLoading,
    refetch,
    isFetching,
  } = useQuery({
    queryKey: ["table-rows", projectId, selected?.schema, selected?.table, page],
    queryFn: async () => {
      if (!selected) return null;
      const res = await apiClient.get<TableDataResponse>(
        `/projects/${projectId}/tables?schema=${encodeURIComponent(
          selected.schema
        )}&table=${encodeURIComponent(selected.table)}&limit=${pageSize}&offset=${
          page * pageSize
        }`
      );
      return res.data;
    },
    enabled: Boolean(selected),
  });

  const insertMutation = useMutation({
    mutationFn: async () => {
      if (!selected) return;
      const cleanValues: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(insertValues)) {
        if (v !== "") cleanValues[k] = v;
      }
      await apiClient.post(`/projects/${projectId}/tables`, {
        schema: selected.schema,
        table: selected.table,
        values: cleanValues,
      });
    },
    onSuccess: () => {
      setInsertOpen(false);
      setInsertValues({});
      queryClient.invalidateQueries({
        queryKey: ["table-rows", projectId, selected?.schema, selected?.table],
      });
      toast({ title: "Row inserted", variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Insert failed", description: err.message, variant: "error" });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (row: Record<string, unknown>) => {
      if (!selected) return;
      await apiClient.delete(`/projects/${projectId}/tables`, {
        data: {
          schema: selected.schema,
          table: selected.table,
          where: row,
        },
      });
    },
    onSuccess: () => {
      setDeleteRow(null);
      queryClient.invalidateQueries({
        queryKey: ["table-rows", projectId, selected?.schema, selected?.table],
      });
      toast({ title: "Row deleted", variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Delete failed", description: err.message, variant: "error" });
    },
  });

  function exportCsv() {
    if (!tableData?.rows?.length) return;
    const cols = tableData.columns.map((c) => c.column_name);
    const lines = [
      cols.join(","),
      ...tableData.rows.map((r) =>
        cols.map((c) => JSON.stringify(r[c] ?? "")).join(",")
      ),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${selected?.table || "table"}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="space-y-4 max-w-7xl mx-auto">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">
            Table Management Studio
          </h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Browse records, insert rows, and inspect table contents.
          </p>
        </div>
        {selected && (
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isFetching}>
              <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? "animate-spin" : ""}`} />
              Refresh
            </Button>
            <Button variant="outline" size="sm" onClick={exportCsv}>
              <Download className="h-3.5 w-3.5" /> Export CSV
            </Button>
            <Button size="sm" onClick={() => setInsertOpen(true)}>
              <Plus className="h-3.5 w-3.5" /> Insert Row
            </Button>
          </div>
        )}
      </div>

      {schemaError && (
        <div className="rounded-md border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-[var(--warning)]">
          {(schemaError as Error).message}
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-4">
        <Card className="h-[calc(100vh-12rem)] overflow-y-auto">
          <CardHeader>
            <CardTitle>Tables ({schemaData?.length ?? 0})</CardTitle>
          </CardHeader>
          <CardContent className="p-2 space-y-0.5">
            {!schemaData?.length ? (
              <p className="p-3 text-xs text-[var(--text-secondary)]">
                No user tables found.
              </p>
            ) : (
              schemaData.map((t) => {
                const active =
                  selected?.schema === t.schema_name &&
                  selected?.table === t.table_name;
                return (
                  <button
                    key={`${t.schema_name}.${t.table_name}`}
                    type="button"
                    onClick={() => {
                      setPage(0);
                      setSelected({ schema: t.schema_name, table: t.table_name });
                    }}
                    className={`flex w-full items-center justify-between rounded px-2.5 py-1.5 text-xs ${
                      active
                        ? "bg-[var(--surface-elevated)] text-[var(--text-primary)] font-medium"
                        : "text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)]"
                    }`}
                  >
                    <span className="flex items-center gap-1.5 truncate font-mono">
                      <Table2 className="h-3.5 w-3.5 text-[var(--accent)]" />
                      {t.schema_name}.{t.table_name}
                    </span>
                    <span className="text-[10px] font-mono">~{t.row_estimate}</span>
                  </button>
                );
              })
            )}
          </CardContent>
        </Card>

        <Card className="lg:col-span-3 h-[calc(100vh-12rem)] flex flex-col overflow-hidden">
          <CardHeader className="py-2.5">
            <CardTitle className="font-mono">
              {selected ? `${selected.schema}.${selected.table}` : "Select a table"}
            </CardTitle>
            {tableData && (
              <span className="text-xs text-[var(--text-secondary)] font-mono">
                {tableData.total} total rows
              </span>
            )}
          </CardHeader>

          <div className="flex-1 overflow-auto">
            {!selected ? (
              <div className="flex h-full items-center justify-center text-xs text-[var(--text-secondary)]">
                Select a table to view records.
              </div>
            ) : rowsLoading ? (
              <div className="flex h-full items-center justify-center text-xs text-[var(--text-secondary)]">
                Loading rows...
              </div>
            ) : (
              <table className="w-full text-left text-xs">
                <thead className="sticky top-0 border-b border-[var(--border)] bg-[var(--sidebar)] text-[var(--text-secondary)]">
                  <tr>
                    {tableData?.columns.map((c) => (
                      <th key={c.column_name} className="px-3 py-2 font-mono font-medium whitespace-nowrap">
                        {c.column_name}
                      </th>
                    ))}
                    <th className="px-3 py-2 text-right font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[var(--border)]">
                  {tableData?.rows.map((row, idx) => (
                    <tr key={idx} className="hover:bg-[var(--surface-elevated)]">
                      {tableData.columns.map((c) => {
                        const val = row[c.column_name];
                        return (
                          <td key={c.column_name} className="px-3 py-1.5 font-mono max-w-xs truncate">
                            {val === null ? (
                              <span className="text-[var(--text-secondary)] italic">NULL</span>
                            ) : typeof val === "object" ? (
                              JSON.stringify(val)
                            ) : (
                              String(val)
                            )}
                          </td>
                        );
                      })}
                      <td className="px-3 py-1.5 text-right">
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => setDeleteRow(row)}
                          title="Delete row"
                        >
                          <Trash2 className="h-3.5 w-3.5 text-[var(--danger)]" />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {tableData && (
            <div className="flex items-center justify-between border-t border-[var(--border)] px-4 py-2 text-xs text-[var(--text-secondary)]">
              <span>
                Showing {page * pageSize + 1}–
                {Math.min((page + 1) * pageSize, tableData.total)} of {tableData.total}
              </span>
              <div className="flex items-center gap-1.5">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={page === 0}
                  onClick={() => setPage((p) => Math.max(0, p - 1))}
                >
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={(page + 1) * pageSize >= tableData.total}
                  onClick={() => setPage((p) => p + 1)}
                >
                  Next
                </Button>
              </div>
            </div>
          )}
        </Card>
      </div>

      {/* Insert Row Dialog */}
      <Dialog
        open={insertOpen}
        onOpenChange={setInsertOpen}
        title={`Insert Row into ${selected?.schema}.${selected?.table}`}
      >
        <form
          onSubmit={(e) => {
            e.preventDefault();
            insertMutation.mutate();
          }}
          className="space-y-3"
        >
          <div className="max-h-72 overflow-y-auto space-y-2.5 pr-1">
            {tableData?.columns.map((c) => (
              <div key={c.column_name} className="space-y-1">
                <label className="text-xs font-mono font-medium">
                  {c.column_name}{" "}
                  <span className="text-[var(--text-secondary)]">({c.data_type})</span>
                </label>
                <Input
                  placeholder={c.is_nullable === "YES" ? "NULL (leave blank)" : "Required"}
                  value={insertValues[c.column_name] || ""}
                  onChange={(e) =>
                    setInsertValues((prev) => ({
                      ...prev,
                      [c.column_name]: e.target.value,
                    }))
                  }
                />
              </div>
            ))}
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <Button type="button" variant="outline" onClick={() => setInsertOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={insertMutation.isPending}>
              {insertMutation.isPending ? "Inserting..." : "Insert Row"}
            </Button>
          </div>
        </form>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog
        open={Boolean(deleteRow)}
        onOpenChange={(open) => !open && setDeleteRow(null)}
        title="Confirm Row Deletion"
        description="This executes a DELETE statement against your PostgreSQL table."
      >
        <div className="space-y-4">
          <pre className="max-h-40 overflow-auto rounded border border-[var(--border)] bg-[var(--bg)] p-2.5 font-mono text-[11px]">
            {JSON.stringify(deleteRow, null, 2)}
          </pre>
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={() => setDeleteRow(null)}>
              Cancel
            </Button>
            <Button
              variant="danger"
              disabled={deleteMutation.isPending}
              onClick={() => deleteRow && deleteMutation.mutate(deleteRow)}
            >
              {deleteMutation.isPending ? "Deleting..." : "Delete Row"}
            </Button>
          </div>
        </div>
      </Dialog>
    </div>
  );
}
