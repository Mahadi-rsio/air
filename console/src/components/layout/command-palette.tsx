"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import {
  Search,
  Database,
  Terminal,
  Plus,
  Key,
  Settings,
  GitBranch,
  Table2,
  Activity,
} from "lucide-react";
import { useConsoleStore } from "@/stores/console-store";

interface CommandItem {
  id: string;
  label: string;
  group: string;
  icon: React.ReactNode;
  href?: string;
  action?: () => void;
}

export function CommandPalette({
  projects = [],
}: {
  projects?: Array<{ id: string; name: string; tenantId: string }>;
}) {
  const router = useRouter();
  const open = useConsoleStore((s) => s.commandPaletteOpen);
  const setOpen = useConsoleStore((s) => s.setCommandPaletteOpen);
  const activeProjectId = useConsoleStore((s) => s.activeProjectId);

  const [query, setQuery] = React.useState("");
  const [selectedIndex, setSelectedIndex] = React.useState(0);

  React.useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen(!open);
      } else if (e.key === "Escape" && open) {
        setOpen(false);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, setOpen]);

  const items = React.useMemo<CommandItem[]>(() => {
    const list: CommandItem[] = [
      {
        id: "new-project",
        label: "Create new PostgreSQL project",
        group: "Quick Commands",
        icon: <Plus className="h-4 w-4 text-[var(--accent)]" />,
        href: "/projects/new",
      },
      {
        id: "go-overview",
        label: "Workspace Overview",
        group: "Navigation",
        icon: <Activity className="h-4 w-4" />,
        href: "/dashboard",
      },
      {
        id: "go-projects",
        label: "All Projects",
        group: "Navigation",
        icon: <Database className="h-4 w-4" />,
        href: "/projects",
      },
      {
        id: "go-billing",
        label: "Usage & Billing",
        group: "Navigation",
        icon: <Activity className="h-4 w-4" />,
        href: "/billing",
      },
      {
        id: "go-settings",
        label: "Workspace Settings",
        group: "Navigation",
        icon: <Settings className="h-4 w-4" />,
        href: "/settings",
      },
    ];

    if (activeProjectId) {
      list.unshift(
        {
          id: "sql-editor",
          label: "Open SQL Editor",
          group: "Active Project",
          icon: <Terminal className="h-4 w-4 text-[var(--accent)]" />,
          href: `/projects/${activeProjectId}/sql`,
        },
        {
          id: "tables",
          label: "Open Table Studio",
          group: "Active Project",
          icon: <Table2 className="h-4 w-4" />,
          href: `/projects/${activeProjectId}/tables`,
        },
        {
          id: "branches",
          label: "Manage Branches",
          group: "Active Project",
          icon: <GitBranch className="h-4 w-4" />,
          href: `/projects/${activeProjectId}/branches`,
        },
        {
          id: "connection",
          label: "Open Connection Details",
          group: "Active Project",
          icon: <Key className="h-4 w-4" />,
          href: `/projects/${activeProjectId}/connection`,
        }
      );
    }

    for (const p of projects) {
      list.push({
        id: `prj-${p.id}`,
        label: `Switch to project: ${p.name} (${p.tenantId})`,
        group: "Projects",
        icon: <Database className="h-4 w-4" />,
        href: `/projects/${p.id}`,
      });
    }

    return list;
  }, [activeProjectId, projects]);

  const filtered = React.useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return items;
    return items.filter(
      (i) =>
        i.label.toLowerCase().includes(q) || i.group.toLowerCase().includes(q)
    );
  }, [items, query]);

  React.useEffect(() => {
    setSelectedIndex(0);
  }, [query, open]);

  if (!open) return null;

  function runItem(item: CommandItem) {
    setOpen(false);
    setQuery("");
    if (item.href) router.push(item.href);
    if (item.action) item.action();
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center bg-black/65 backdrop-blur-sm pt-[12vh] px-4"
      onClick={() => setOpen(false)}
    >
      <div
        className="w-full max-w-xl overflow-hidden rounded-xl border border-[var(--border-strong)] bg-[var(--surface)]"
        style={{ boxShadow: "var(--elevated-shadow)" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center border-b border-[var(--border)] px-4">
          <Search className="h-4 w-4 text-[var(--text-muted)]" />
          <input
            autoFocus
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "ArrowDown") {
                e.preventDefault();
                setSelectedIndex((i) => Math.min(i + 1, filtered.length - 1));
              } else if (e.key === "ArrowUp") {
                e.preventDefault();
                setSelectedIndex((i) => Math.max(i - 1, 0));
              } else if (e.key === "Enter" && filtered[selectedIndex]) {
                e.preventDefault();
                runItem(filtered[selectedIndex]);
              }
            }}
            placeholder="Search projects, SQL editor, branches, settings…"
            className="h-12 w-full bg-transparent px-3 text-[13px] text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none"
          />
          <kbd className="rounded-md border border-[var(--border)] bg-[var(--surface-elevated)] px-1.5 py-0.5 font-mono text-[10px] text-[var(--text-muted)]">
            ESC
          </kbd>
        </div>

        <div className="max-h-80 overflow-y-auto p-2 space-y-0.5">
          {filtered.length === 0 ? (
            <div className="py-10 text-center text-xs text-[var(--text-secondary)]">
              No matching commands or resources.
            </div>
          ) : (
            filtered.map((item, idx) => (
              <button
                key={item.id}
                type="button"
                onClick={() => runItem(item)}
                onMouseEnter={() => setSelectedIndex(idx)}
                className={`flex w-full items-center justify-between rounded-lg px-3 py-2.5 text-left text-[13px] transition-colors cursor-pointer ${
                  idx === selectedIndex
                    ? "bg-[var(--accent-soft)] text-[var(--text-primary)] border border-[var(--accent-border)]"
                    : "border border-transparent text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)]"
                }`}
              >
                <span className="flex items-center gap-2.5 min-w-0">
                  {item.icon}
                  <span className="truncate">{item.label}</span>
                </span>
                <span className="ml-3 shrink-0 rounded-md bg-[var(--surface-elevated)] px-2 py-0.5 text-[10px] font-medium text-[var(--text-muted)]">
                  {item.group}
                </span>
              </button>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
