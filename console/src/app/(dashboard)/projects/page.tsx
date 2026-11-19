"use client";

import * as React from "react";
import Link from "next/link";
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
import { ArrowUpDown, Plus, Search, Trash2, ExternalLink } from "lucide-react";
import { apiClient } from "@/lib/axios";
import { useConsoleStore } from "@/stores/console-store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { Dialog } from "@/components/ui/dialog";
import { useToast } from "@/components/ui/toast";
import { formatRelativeTime } from "@/lib/utils";

interface ProjectRow {
  id: string;
  name: string;
  tenantId: string;
  region: string;
  pgVersion: string;
  status: string;
  computeSize: string;
  createdAt: string;
}

export default function ProjectsPage() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const activeWorkspaceId = useConsoleStore((s) => s.activeWorkspaceId);

  const [sorting, setSorting] = React.useState<SortingState>([]);
  const [globalFilter, setGlobalFilter] = React.useState("");
  const [rowSelection, setRowSelection] = React.useState({});
  const [deleteTarget, setDeleteTarget] = React.useState<ProjectRow | null>(null);

  const { data: projects = [], isLoading } = useQuery({
    queryKey: ["projects", activeWorkspaceId],
    queryFn: async () => {
      const res = await apiClient.get<{ projects: ProjectRow[] }>(
        `/projects?workspaceId=${activeWorkspaceId}`
      );
      return res.data.projects;
    },
    enabled: Boolean(activeWorkspaceId),
  });

  const deleteMutation = useMutation({
    mutationFn: async (projectId: string) => {
      await apiClient.delete(`/projects/${projectId}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["projects", activeWorkspaceId] });
      toast({ title: "Project deleted", variant: "success" });
      setDeleteTarget(null);
    },
    onError: (err: Error) => {
      toast({ title: "Delete failed", description: err.message, variant: "error" });
    },
  });

  const columns = React.useMemo<ColumnDef<ProjectRow>[]>(
    () => [
      {
        id: "select",
        header: ({ table }) => (
          <input
            type="checkbox"
            checked={table.getIsAllPageRowsSelected()}
            onChange={table.getToggleAllPageRowsSelectedHandler()}
            className="accent-[var(--accent)]"
          />
        ),
        cell: ({ row }) => (
          <input
            type="checkbox"
            checked={row.getIsSelected()}
            onChange={row.getToggleSelectedHandler()}
            className="accent-[var(--accent)]"
          />
        ),
      },
      {
        accessorKey: "name",
        header: ({ column }) => (
          <button
            type="button"
            className="flex items-center gap-1 font-medium"
            onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
          >
            Project <ArrowUpDown className="h-3 w-3" />
          </button>
        ),
        cell: ({ row }) => (
          <div>
            <Link
              href={`/projects/${row.original.id}`}
              className="font-medium text-[var(--text-primary)] hover:text-[var(--accent)]"
            >
              {row.original.name}
            </Link>
            <div className="text-[11px] font-mono text-[var(--text-secondary)]">
              {row.original.id}
            </div>
          </div>
        ),
      },
      {
        accessorKey: "tenantId",
        header: "Tenant ID",
        cell: ({ row }) => (
          <span className="font-mono text-xs">{row.original.tenantId}</span>
        ),
      },
      {
        accessorKey: "region",
        header: "Region",
        cell: ({ row }) => (
          <span className="font-mono text-xs text-[var(--text-secondary)]">
            {row.original.region}
          </span>
        ),
      },
      {
        accessorKey: "pgVersion",
        header: "PG Version",
        cell: ({ row }) => <span className="font-mono">v{row.original.pgVersion}</span>,
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ row }) => (
          <Badge
            variant={
              row.original.status === "ACTIVE"
                ? "success"
                : row.original.status === "SUSPENDED"
                ? "warning"
                : "default"
            }
          >
            {row.original.status}
          </Badge>
        ),
      },
      {
        accessorKey: "computeSize",
        header: "Compute",
        cell: ({ row }) => (
          <span className="font-mono text-[var(--text-secondary)]">
            {row.original.computeSize}
          </span>
        ),
      },
      {
        accessorKey: "createdAt",
        header: "Created",
        cell: ({ row }) => (
          <span className="text-[var(--text-secondary)]">
            {formatRelativeTime(row.original.createdAt)}
          </span>
        ),
      },
      {
        id: "actions",
        header: "",
        cell: ({ row }) => (
          <div className="flex items-center justify-end gap-1">
            <Link href={`/projects/${row.original.id}`}>
              <Button variant="ghost" size="icon" title="Open project">
                <ExternalLink className="h-3.5 w-3.5" />
              </Button>
            </Link>
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setDeleteTarget(row.original)}
              title="Delete project"
            >
              <Trash2 className="h-3.5 w-3.5 text-[var(--danger)]" />
            </Button>
          </div>
        ),
      },
    ],
    []
  );

  const table = useReactTable({
    data: projects,
    columns,
    state: { sorting, globalFilter, rowSelection },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    onRowSelectionChange: setRowSelection,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
  });

  return (
    <div className="space-y-4 max-w-7xl mx-auto">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Projects</h1>
          <p className="text-xs text-[var(--text-secondary)]">
            Isolated PostgreSQL 17 database clusters in this workspace.
          </p>
        </div>
        <Link href="/projects/new">
          <Button>
            <Plus className="h-3.5 w-3.5" /> Create Project
          </Button>
        </Link>
      </div>

      <div className="flex items-center justify-between gap-3">
        <div className="relative w-full max-w-xs">
          <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-[var(--text-secondary)]" />
          <Input
            value={globalFilter}
            onChange={(e) => setGlobalFilter(e.target.value)}
            placeholder="Filter projects..."
            className="pl-8"
          />
        </div>
        {Object.keys(rowSelection).length > 0 && (
          <span className="text-xs text-[var(--text-secondary)]">
            {Object.keys(rowSelection).length} selected
          </span>
        )}
      </div>

      <Card>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-[var(--border)] text-[var(--text-secondary)]">
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id}>
                  {hg.headers.map((header) => (
                    <th key={header.id} className="px-4 py-2.5 font-medium">
                      {header.isPlaceholder
                        ? null
                        : flexRender(
                            header.column.columnDef.header,
                            header.getContext()
                          )}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {isLoading ? (
                <tr>
                  <td colSpan={9} className="px-4 py-10 text-center text-[var(--text-secondary)]">
                    Loading projects...
                  </td>
                </tr>
              ) : table.getRowModel().rows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-4 py-10 text-center text-[var(--text-secondary)]">
                    No projects found.
                  </td>
                </tr>
              ) : (
                table.getRowModel().rows.map((row) => (
                  <tr
                    key={row.id}
                    className="hover:bg-[var(--surface-elevated)] transition-colors"
                  >
                    {row.getVisibleCells().map((cell) => (
                      <td key={cell.id} className="px-4 py-2.5">
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <div className="flex items-center justify-between border-t border-[var(--border)] px-4 py-2.5 text-xs text-[var(--text-secondary)]">
          <span>
            Page {table.getState().pagination.pageIndex + 1} of{" "}
            {Math.max(table.getPageCount(), 1)}
          </span>
          <div className="flex items-center gap-1.5">
            <Button
              variant="outline"
              size="sm"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
            >
              Previous
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
            >
              Next
            </Button>
          </div>
        </div>
      </Card>

      <Dialog
        open={Boolean(deleteTarget)}
        onOpenChange={(open) => !open && setDeleteTarget(null)}
        title="Delete PostgreSQL Project"
        description="This permanently stops the PG17 cluster, purges tenant storage, and shreds its DEK."
      >
        <div className="space-y-4">
          <p className="text-xs text-[var(--text-secondary)]">
            Are you sure you want to delete{" "}
            <strong className="text-[var(--text-primary)]">
              {deleteTarget?.name}
            </strong>{" "}
            (<code className="font-mono">{deleteTarget?.tenantId}</code>)? This action
            cannot be undone.
          </p>
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={() => setDeleteTarget(null)}>
              Cancel
            </Button>
            <Button
              variant="danger"
              disabled={deleteMutation.isPending}
              onClick={() => deleteTarget && deleteMutation.mutate(deleteTarget.id)}
            >
              {deleteMutation.isPending ? "Purging..." : "Delete Project"}
            </Button>
          </div>
        </div>
      </Dialog>
    </div>
  );
}
