"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ColumnDef,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  SortingState,
  useReactTable,
} from "@tanstack/react-table";
import {
  Bookmark,
  Copy,
  Download,
  History,
  Play,
  Plus,
  RefreshCw,
  Table2,
  X,
  Code2,
  Sparkles,
} from "lucide-react";
import { apiClient } from "@/lib/axios";
import { useConsoleStore } from "@/stores/console-store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/components/ui/toast";

interface SchemaObject {
  schema_name: string;
  table_name: string;
  object_type: string;
}

interface SqlExecResponse {
  command: string;
  rowCount: number;
  durationMs: number;
  fields: Array<{ name: string; dataTypeID: number }>;
  rows: Record<string, unknown>[];
}

export default function SqlEditorPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const queryClient = useQueryClient();
  const { toast } = useToast();

  const {
    getSqlTabs,
    getActiveSqlTabId,
    setActiveSqlTab,
    addSqlTab,
    updateSqlTab,
    closeSqlTab,
  } = useConsoleStore();

  const tabs = getSqlTabs(projectId);
  const activeTabId = getActiveSqlTabId(projectId);
  const activeTab = tabs.find((t) => t.id === activeTabId) || tabs[0];

  const [bottomView, setBottomView] = React.useState<"results" | "json" | "history">("results");
  const [result, setResult] = React.useState<SqlExecResponse | null>(null);
  const [execError, setExecError] = React.useState<string | null>(null);
  const [sorting, setSorting] = React.useState<SortingState>([]);
  const [globalFilter, setGlobalFilter] = React.useState("");
  const [saveName, setSaveName] = React.useState("");
  const [showSaveInput, setShowSaveInput] = React.useState(false);

  const { data: schemaData, refetch: refetchSchema } = useQuery({
    queryKey: ["project-schema", projectId],
    queryFn: async () => {
      const res = await apiClient.get<{
        schemas: string[];
        objects: SchemaObject[];
      }>(`/projects/${projectId}/schema`);
      return res.data;
    },
    retry: false,
  });

  const { data: sqlMeta, refetch: refetchMeta } = useQuery({
    queryKey: ["project-sql-meta", projectId],
    queryFn: async () => {
      const res = await apiClient.get<{
        history: Array<{
          id: string;
          sql: string;
          durationMs: number;
          rowCount: number;
          status: string;
          errorMessage: string | null;
          createdAt: string;
        }>;
        savedQueries: Array<{ id: string; name: string; sql: string }>;
      }>(`/projects/${projectId}/sql`);
      return res.data;
    },
  });

  const execMutation = useMutation({
    mutationFn: async (sqlToRun: string) => {
      const res = await apiClient.post<SqlExecResponse>(
        `/projects/${projectId}/sql`,
        { sql: sqlToRun }
      );
      return res.data;
    },
    onSuccess: (data) => {
      setExecError(null);
      setResult(data);
      setBottomView("results");
      refetchMeta();
    },
    onError: (err: Error) => {
      setExecError(err.message);
      setResult(null);
      refetchMeta();
    },
  });

  const saveMutation = useMutation({
    mutationFn: async () => {
      await apiClient.post(`/projects/${projectId}/saved-queries`, {
        name: saveName,
        sql: activeTab.sql,
      });
    },
    onSuccess: () => {
      setSaveName("");
      setShowSaveInput(false);
      queryClient.invalidateQueries({ queryKey: ["project-sql-meta", projectId] });
      toast({ title: "Query saved", variant: "success" });
    },
    onError: (err: Error) => {
      toast({ title: "Save failed", description: err.message, variant: "error" });
    },
  });

  function formatSql() {
    const formatted = activeTab.sql
      .replace(/\b(SELECT|FROM|WHERE|JOIN|LEFT JOIN|INNER JOIN|GROUP BY|ORDER BY|LIMIT|INSERT INTO|VALUES|UPDATE|SET|DELETE FROM|CREATE TABLE)\b/gi, "\n$1")
      .trim();
    updateSqlTab(projectId, activeTab.id, formatted);
  }

  function exportCsv() {
    if (!result?.rows?.length) return;
    const cols = result.fields.map((f) => f.name);
    const lines = [
      cols.join(","),
      ...result.rows.map((r) =>
        cols.map((c) => JSON.stringify(r[c] ?? "")).join(",")
      ),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `air-query-${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  const columns = React.useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    if (!result?.fields) return [];
    return result.fields.map((f) => ({
      accessorKey: f.name,
      header: f.name,
      cell: ({ getValue }) => {
        const val = getValue();
        if (val === null) return <span className="text-[var(--text-secondary)] italic">NULL</span>;
        if (typeof val === "object") return <span className="font-mono">{JSON.stringify(val)}</span>;
        return <span className="font-mono">{String(val)}</span>;
      },
    }));
  }, [result]);

  const table = useReactTable({
    data: result?.rows || [],
    columns,
    state: { sorting, globalFilter },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
  });

  return (
    <div className="flex h-[calc(100dvh-6.5rem)] gap-3 overflow-hidden">
      {/* Left Panel: Schema Explorer & Saved Queries */}
      <div className="hidden md:flex w-64 shrink-0 flex-col rounded-lg border border-[var(--border)] bg-[var(--surface)] overflow-hidden">
        <div className="flex items-center justify-between border-b border-[var(--border)] px-3 py-2">
          <span className="text-xs font-semibold">Schema Explorer</span>
          <button
            type="button"
            onClick={() => refetchSchema()}
            className="rounded p-1 text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)]"
            title="Refresh schema"
          >
            <RefreshCw className="h-3 w-3" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-2 space-y-4">
          <div className="space-y-1">
            <p className="px-1.5 text-[10px] font-semibold uppercase tracking-wider text-[var(--text-secondary)]">
              Tables & Views
            </p>
            {!schemaData?.objects?.length ? (
              <p className="px-1.5 py-2 text-[11px] text-[var(--text-secondary)]">
                No user tables or database offline.
              </p>
            ) : (
              schemaData.objects.map((obj) => (
                <button
                  key={`${obj.schema_name}.${obj.table_name}`}
                  type="button"
                  onClick={() =>
                    updateSqlTab(
                      projectId,
                      activeTab.id,
                      `SELECT * FROM "${obj.schema_name}"."${obj.table_name}" LIMIT 100;`
                    )
                  }
                  className="flex w-full items-center justify-between rounded px-2 py-1 text-left text-xs text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
                >
                  <span className="flex items-center gap-1.5 truncate font-mono text-[11px]">
                    <Table2 className="h-3 w-3 shrink-0 text-[var(--accent)]" />
                    {obj.schema_name}.{obj.table_name}
                  </span>
                </button>
              ))
            )}
          </div>

          <div className="space-y-1">
            <p className="px-1.5 text-[10px] font-semibold uppercase tracking-wider text-[var(--text-secondary)]">
              Saved Queries
            </p>
            {!sqlMeta?.savedQueries?.length ? (
              <p className="px-1.5 py-1 text-[11px] text-[var(--text-secondary)]">
                No saved queries yet.
              </p>
            ) : (
              sqlMeta.savedQueries.map((sq) => (
                <button
                  key={sq.id}
                  type="button"
                  onClick={() => addSqlTab(projectId, sq.name, sq.sql)}
                  className="flex w-full items-center gap-1.5 rounded px-2 py-1 text-left text-xs text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
                >
                  <Bookmark className="h-3 w-3 shrink-0 text-[var(--accent)]" />
                  <span className="truncate">{sq.name}</span>
                </button>
              ))
            )}
          </div>
        </div>
      </div>

      {/* Center + Bottom Workspace */}
      <div className="flex flex-1 flex-col min-w-0 gap-3 overflow-hidden">
        {/* Editor Card */}
        <div className="flex flex-col rounded-lg border border-[var(--border)] bg-[var(--surface)] overflow-hidden h-[46%]">
          {/* Tabs Bar */}
          <div className="flex items-center justify-between border-b border-[var(--border)] bg-[var(--sidebar)] px-2">
            <div className="flex items-center gap-1 overflow-x-auto py-1">
              {tabs.map((t) => (
                <div
                  key={t.id}
                  onClick={() => setActiveSqlTab(projectId, t.id)}
                  className={`flex items-center gap-1.5 rounded px-2.5 py-1 text-xs cursor-pointer ${
                    t.id === activeTab.id
                      ? "bg-[var(--surface)] text-[var(--text-primary)] font-medium border border-[var(--border)]"
                      : "text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
                  }`}
                >
                  <span>{t.title}</span>
                  {tabs.length > 1 && (
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        closeSqlTab(projectId, t.id);
                      }}
                      className="hover:text-[var(--danger)]"
                    >
                      <X className="h-3 w-3" />
                    </button>
                  )}
                </div>
              ))}
              <button
                type="button"
                onClick={() => addSqlTab(projectId)}
                className="rounded p-1 text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)]"
                title="New tab"
              >
                <Plus className="h-3.5 w-3.5" />
              </button>
            </div>

            <div className="flex items-center gap-1.5 py-1">
              <Button variant="ghost" size="sm" onClick={formatSql}>
                <Sparkles className="h-3 w-3" /> Format
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => {
                  navigator.clipboard.writeText(activeTab.sql);
                  toast({ title: "SQL copied", variant: "success" });
                }}
              >
                <Copy className="h-3 w-3" />
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowSaveInput((v) => !v)}
              >
                <Bookmark className="h-3 w-3" /> Save
              </Button>
              <Button
                size="sm"
                disabled={execMutation.isPending}
                onClick={() => execMutation.mutate(activeTab.sql)}
              >
                <Play className="h-3 w-3" />
                {execMutation.isPending ? "Running..." : "Run (⌘↵)"}
              </Button>
            </div>
          </div>

          {showSaveInput && (
            <form
              onSubmit={(e) => {
                e.preventDefault();
                saveMutation.mutate();
              }}
              className="flex items-center gap-2 border-b border-[var(--border)] bg-[var(--surface-elevated)] px-3 py-1.5"
            >
              <Input
                required
                placeholder="Query name..."
                value={saveName}
                onChange={(e) => setSaveName(e.target.value)}
                className="h-7 max-w-xs"
              />
              <Button type="submit" size="sm" disabled={saveMutation.isPending}>
                Save Query
              </Button>
            </form>
          )}

          <textarea
            value={activeTab.sql}
            onChange={(e) => updateSqlTab(projectId, activeTab.id, e.target.value)}
            onKeyDown={(e) => {
              if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
                e.preventDefault();
                execMutation.mutate(activeTab.sql);
              }
            }}
            spellCheck={false}
            className="flex-1 w-full resize-none bg-[var(--bg)] p-3.5 font-mono text-xs leading-relaxed text-[var(--text-primary)] focus:outline-none"
            placeholder="-- Write PostgreSQL SQL here (Ctrl/Cmd + Enter to execute)..."
          />
        </div>

        {/* Bottom Results / History Panel */}
        <div className="flex flex-1 flex-col rounded-lg border border-[var(--border)] bg-[var(--surface)] overflow-hidden">
          <div className="flex items-center justify-between border-b border-[var(--border)] px-3 py-1.5">
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => setBottomView("results")}
                className={`rounded px-2 py-1 text-xs font-medium ${
                  bottomView === "results"
                    ? "bg-[var(--surface-elevated)] text-[var(--text-primary)]"
                    : "text-[var(--text-secondary)]"
                }`}
              >
                Results
              </button>
              <button
                type="button"
                onClick={() => setBottomView("json")}
                className={`flex items-center gap-1 rounded px-2 py-1 text-xs font-medium ${
                  bottomView === "json"
                    ? "bg-[var(--surface-elevated)] text-[var(--text-primary)]"
                    : "text-[var(--text-secondary)]"
                }`}
              >
                <Code2 className="h-3 w-3" /> JSON
              </button>
              <button
                type="button"
                onClick={() => setBottomView("history")}
                className={`flex items-center gap-1 rounded px-2 py-1 text-xs font-medium ${
                  bottomView === "history"
                    ? "bg-[var(--surface-elevated)] text-[var(--text-primary)]"
                    : "text-[var(--text-secondary)]"
                }`}
              >
                <History className="h-3 w-3" /> History
              </button>
              {result && (
                <Badge variant="success">
                  {result.command} · {result.rowCount} rows · {result.durationMs}ms
                </Badge>
              )}
            </div>

            {result && result.rows.length > 0 && (
              <div className="flex items-center gap-2">
                <Input
                  placeholder="Filter results..."
                  value={globalFilter}
                  onChange={(e) => setGlobalFilter(e.target.value)}
                  className="h-7 w-40"
                />
                <Button variant="outline" size="sm" onClick={exportCsv}>
                  <Download className="h-3 w-3" /> CSV
                </Button>
              </div>
            )}
          </div>

          <div className="flex-1 overflow-auto">
            {execError ? (
              <div className="m-3 rounded-md border border-[var(--danger-border)] bg-[var(--danger-soft)] p-3 font-mono text-xs text-[var(--danger)]">
                {execError}
              </div>
            ) : bottomView === "history" ? (
              <div className="divide-y divide-[var(--border)]">
                {(sqlMeta?.history || []).map((h) => (
                  <div
                    key={h.id}
                    onClick={() => updateSqlTab(projectId, activeTab.id, h.sql)}
                    className="flex items-center justify-between px-3 py-2 text-xs hover:bg-[var(--surface-elevated)] cursor-pointer"
                  >
                    <code className="font-mono truncate max-w-xl">{h.sql}</code>
                    <div className="flex items-center gap-2 shrink-0">
                      <Badge variant={h.status === "success" ? "success" : "danger"}>
                        {h.status}
                      </Badge>
                      <span className="font-mono text-[11px] text-[var(--text-secondary)]">
                        {h.durationMs}ms
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            ) : bottomView === "json" ? (
              <pre className="p-3 font-mono text-xs text-[var(--text-primary)]">
                {result ? JSON.stringify(result.rows, null, 2) : "No results yet."}
              </pre>
            ) : !result ? (
              <div className="flex h-full items-center justify-center text-xs text-[var(--text-secondary)]">
                Execute a SQL query to inspect results.
              </div>
            ) : (
              <table className="w-full text-left text-xs">
                <thead className="sticky top-0 border-b border-[var(--border)] bg-[var(--sidebar)] text-[var(--text-secondary)]">
                  {table.getHeaderGroups().map((hg) => (
                    <tr key={hg.id}>
                      {hg.headers.map((header) => (
                        <th
                          key={header.id}
                          onClick={header.column.getToggleSortingHandler()}
                          className="px-3 py-2 font-mono font-medium cursor-pointer select-none whitespace-nowrap"
                        >
                          {flexRender(
                            header.column.columnDef.header,
                            header.getContext()
                          )}
                        </th>
                      ))}
                    </tr>
                  ))}
                </thead>
                <tbody className="divide-y divide-[var(--border)]">
                  {table.getRowModel().rows.map((row) => (
                    <tr key={row.id} className="hover:bg-[var(--surface-elevated)]">
                      {row.getVisibleCells().map((cell) => (
                        <td
                          key={cell.id}
                          onClick={() => {
                            const val = cell.getValue();
                            if (val !== null && val !== undefined) {
                              navigator.clipboard.writeText(String(val));
                              toast({ title: "Cell copied", variant: "info" });
                            }
                          }}
                          className="px-3 py-1.5 max-w-xs truncate cursor-pointer"
                          title="Click to copy cell value"
                        >
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
