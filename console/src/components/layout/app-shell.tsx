"use client";

import * as React from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  Archive,
  Bell,
  BookOpen,
  ChevronLeft,
  ChevronRight,
  CreditCard,
  Database,
  FileCode2,
  FolderTree,
  GitBranch,
  Key,
  LayoutDashboard,
  Link2,
  LogOut,
  Moon,
  Puzzle,
  Search,
  Settings,
  Shield,
  Sun,
  Table2,
  Terminal,
  Users,
} from "lucide-react";
import { apiClient } from "@/lib/axios";
import { signOut, useSession } from "@/lib/auth/client";
import { useConsoleStore } from "@/stores/console-store";
import { CommandPalette } from "./command-palette";
import { cn } from "@/lib/utils";

interface WorkspaceItem {
  id: string;
  name: string;
  slug: string;
  role: string;
}

interface ProjectItem {
  id: string;
  name: string;
  tenantId: string;
  status: string;
  region: string;
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const { data: sessionData, isPending: sessionLoading } = useSession();

  const {
    activeWorkspaceId,
    setActiveWorkspaceId,
    activeProjectId,
    setActiveProjectId,
    activeBranchId,
    setActiveBranchId,
    sidebarCollapsed,
    toggleSidebar,
    setCommandPaletteOpen,
  } = useConsoleStore();

  const [isDark, setIsDark] = React.useState(true);
  const [profileOpen, setProfileOpen] = React.useState(false);

  React.useEffect(() => {
    setIsDark(document.documentElement.classList.contains("dark"));
  }, []);

  React.useEffect(() => {
    if (!sessionLoading && !sessionData?.user) {
      router.replace("/login");
    }
  }, [sessionLoading, sessionData, router]);

  const { data: wsData } = useQuery({
    queryKey: ["workspaces"],
    queryFn: async () => {
      const res = await apiClient.get<{ workspaces: WorkspaceItem[] }>("/workspaces");
      return res.data.workspaces;
    },
    enabled: Boolean(sessionData?.user),
  });

  React.useEffect(() => {
    if (wsData && wsData.length > 0 && !activeWorkspaceId) {
      setActiveWorkspaceId(wsData[0].id);
    } else if (wsData && wsData.length === 0) {
      router.replace("/onboarding");
    }
  }, [wsData, activeWorkspaceId, setActiveWorkspaceId, router]);

  const { data: projectsData } = useQuery({
    queryKey: ["projects", activeWorkspaceId],
    queryFn: async () => {
      const res = await apiClient.get<{ projects: ProjectItem[] }>(
        `/projects?workspaceId=${activeWorkspaceId}`
      );
      return res.data.projects;
    },
    enabled: Boolean(activeWorkspaceId),
  });

  // Sync activeProjectId from URL if inside /projects/[projectId]
  const projectMatch = pathname.match(/^\/projects\/([^/]+)/);
  const urlProjectId =
    projectMatch && projectMatch[1] !== "new" ? projectMatch[1] : null;

  React.useEffect(() => {
    if (urlProjectId && urlProjectId !== activeProjectId) {
      setActiveProjectId(urlProjectId);
    }
  }, [urlProjectId, activeProjectId, setActiveProjectId]);

  const currentProjectId = urlProjectId || activeProjectId;
  const currentProject = projectsData?.find((p) => p.id === currentProjectId);

  function toggleTheme() {
    const next = !isDark;
    setIsDark(next);
    document.documentElement.classList.toggle("dark", next);
    localStorage.setItem("air-theme", next ? "dark" : "light");
  }

  const workspaceNav = [
    { label: "Overview", href: "/dashboard", icon: LayoutDashboard },
    { label: "Projects", href: "/projects", icon: Database },
    { label: "Usage & Billing", href: "/billing", icon: CreditCard },
    { label: "Members", href: "/members", icon: Users },
    { label: "Settings", href: "/settings", icon: Settings },
  ];

  const projectNav = currentProjectId
    ? [
        { label: "Overview", href: `/projects/${currentProjectId}`, icon: LayoutDashboard },
        { label: "SQL Editor", href: `/projects/${currentProjectId}/sql`, icon: Terminal },
        { label: "Tables", href: `/projects/${currentProjectId}/tables`, icon: Table2 },
        { label: "Database Explorer", href: `/projects/${currentProjectId}/explorer`, icon: FolderTree },
        { label: "Branches", href: `/projects/${currentProjectId}/branches`, icon: GitBranch },
        { label: "Roles & Users", href: `/projects/${currentProjectId}/roles`, icon: Shield },
        { label: "Extensions", href: `/projects/${currentProjectId}/extensions`, icon: Puzzle },
        { label: "Backups", href: `/projects/${currentProjectId}/backups`, icon: Archive },
        { label: "Monitoring", href: `/projects/${currentProjectId}/monitoring`, icon: Activity },
        { label: "Connection Details", href: `/projects/${currentProjectId}/connection`, icon: Link2 },
        { label: "API Keys", href: `/projects/${currentProjectId}/api-keys`, icon: Key },
        { label: "Project Settings", href: `/projects/${currentProjectId}/settings`, icon: Settings },
      ]
    : [];

  if (sessionLoading || !sessionData?.user) {
    return (
      <div className="flex h-screen items-center justify-center bg-[var(--bg)]">
        <div className="h-5 w-5 animate-spin rounded-full border-2 border-[var(--accent)] border-t-transparent" />
      </div>
    );
  }

  return (
    <div className="flex h-screen overflow-hidden bg-[var(--bg)] text-[var(--text-primary)]">
      <CommandPalette projects={projectsData || []} />

      {/* Global Sidebar */}
      <aside
        className={cn(
          "flex flex-col border-r border-[var(--border)] bg-[var(--sidebar)] transition-all duration-150 shrink-0",
          sidebarCollapsed ? "w-14" : "w-60"
        )}
      >
        {/* Brand + Workspace Selector */}
        <div className="flex h-12 items-center justify-between border-b border-[var(--border)] px-3">
          <Link href="/dashboard" className="flex items-center gap-2 min-w-0">
            <div className="flex h-6 w-6 shrink-0 items-center justify-center rounded bg-[var(--accent)] text-[var(--accent-fg)]">
              <Database className="h-3.5 w-3.5" />
            </div>
            {!sidebarCollapsed && (
              <span className="text-sm font-semibold tracking-tight">AIR</span>
            )}
          </Link>
          <button
            type="button"
            onClick={toggleSidebar}
            className="rounded p-1 text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
            title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {sidebarCollapsed ? (
              <ChevronRight className="h-3.5 w-3.5" />
            ) : (
              <ChevronLeft className="h-3.5 w-3.5" />
            )}
          </button>
        </div>

        {!sidebarCollapsed && wsData && wsData.length > 0 && (
          <div className="border-b border-[var(--border)] px-3 py-2">
            <select
              value={activeWorkspaceId || ""}
              onChange={(e) => setActiveWorkspaceId(e.target.value)}
              className="w-full rounded-md border border-[var(--border)] bg-[var(--surface)] px-2 py-1 text-xs font-medium text-[var(--text-primary)] focus:outline-none"
            >
              {wsData.map((ws) => (
                <option key={ws.id} value={ws.id}>
                  {ws.name}
                </option>
              ))}
            </select>
          </div>
        )}

        <div className="flex-1 overflow-y-auto px-2 py-3 space-y-5">
          {/* Project-specific Sidebar when inside a project */}
          {urlProjectId && projectNav.length > 0 ? (
            <div>
              {!sidebarCollapsed && (
                <div className="mb-1.5 flex items-center justify-between px-2">
                  <span className="text-[10px] font-semibold uppercase tracking-wider text-[var(--text-secondary)]">
                    Project
                  </span>
                  <Link
                    href="/projects"
                    className="text-[10px] text-[var(--accent)] hover:underline"
                  >
                    All projects
                  </Link>
                </div>
              )}
              <nav className="space-y-0.5">
                {projectNav.map((item) => {
                  const Icon = item.icon;
                  const active = pathname === item.href;
                  return (
                    <Link
                      key={item.href}
                      href={item.href}
                      title={item.label}
                      className={cn(
                        "flex items-center gap-2.5 rounded-md px-2.5 py-1.5 text-xs transition-colors",
                        active
                          ? "bg-[var(--surface-elevated)] text-[var(--text-primary)] font-medium"
                          : "text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
                      )}
                    >
                      <Icon
                        className={cn(
                          "h-3.5 w-3.5 shrink-0",
                          active && "text-[var(--accent)]"
                        )}
                      />
                      {!sidebarCollapsed && <span className="truncate">{item.label}</span>}
                    </Link>
                  );
                })}
              </nav>
            </div>
          ) : (
            <div>
              {!sidebarCollapsed && (
                <p className="mb-1.5 px-2 text-[10px] font-semibold uppercase tracking-wider text-[var(--text-secondary)]">
                  Workspace
                </p>
              )}
              <nav className="space-y-0.5">
                {workspaceNav.map((item) => {
                  const Icon = item.icon;
                  const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
                  return (
                    <Link
                      key={item.href}
                      href={item.href}
                      title={item.label}
                      className={cn(
                        "flex items-center gap-2.5 rounded-md px-2.5 py-1.5 text-xs transition-colors",
                        active
                          ? "bg-[var(--surface-elevated)] text-[var(--text-primary)] font-medium"
                          : "text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
                      )}
                    >
                      <Icon
                        className={cn(
                          "h-3.5 w-3.5 shrink-0",
                          active && "text-[var(--accent)]"
                        )}
                      />
                      {!sidebarCollapsed && <span className="truncate">{item.label}</span>}
                    </Link>
                  );
                })}
              </nav>
            </div>
          )}

          {/* Resources */}
          <div>
            {!sidebarCollapsed && (
              <p className="mb-1.5 px-2 text-[10px] font-semibold uppercase tracking-wider text-[var(--text-secondary)]">
                Resources
              </p>
            )}
            <nav className="space-y-0.5">
              <Link
                href="/docs"
                className="flex items-center gap-2.5 rounded-md px-2.5 py-1.5 text-xs text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
              >
                <BookOpen className="h-3.5 w-3.5 shrink-0" />
                {!sidebarCollapsed && <span>Documentation</span>}
              </Link>
              <Link
                href="/api-reference"
                className="flex items-center gap-2.5 rounded-md px-2.5 py-1.5 text-xs text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
              >
                <FileCode2 className="h-3.5 w-3.5 shrink-0" />
                {!sidebarCollapsed && <span>API Reference</span>}
              </Link>
            </nav>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <div className="flex flex-1 flex-col min-w-0 overflow-hidden">
        {/* Top Navigation */}
        <header className="flex h-12 shrink-0 items-center justify-between border-b border-[var(--border)] bg-[var(--surface)] px-4 gap-3">
          <div className="flex items-center gap-2 min-w-0 text-xs">
            <Link
              href="/dashboard"
              className="text-[var(--text-secondary)] hover:text-[var(--text-primary)] truncate"
            >
              {wsData?.find((w) => w.id === activeWorkspaceId)?.name || "Workspace"}
            </Link>

            {projectsData && projectsData.length > 0 && (
              <>
                <span className="text-[var(--text-secondary)]">/</span>
                <select
                  value={currentProjectId || ""}
                  onChange={(e) => {
                    const pid = e.target.value;
                    if (pid) {
                      setActiveProjectId(pid);
                      router.push(`/projects/${pid}`);
                    }
                  }}
                  className="h-7 max-w-[160px] rounded border border-[var(--border)] bg-[var(--surface-elevated)] px-2 text-xs font-medium text-[var(--text-primary)] focus:outline-none"
                >
                  <option value="">Select project...</option>
                  {projectsData.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.name}
                    </option>
                  ))}
                </select>
              </>
            )}

            {currentProject && (
              <>
                <span className="text-[var(--text-secondary)]">/</span>
                <select
                  value={activeBranchId || "main"}
                  onChange={(e) => setActiveBranchId(e.target.value)}
                  className="h-7 rounded border border-[var(--border)] bg-[var(--surface-elevated)] px-2 text-xs font-mono text-[var(--text-primary)] focus:outline-none"
                >
                  <option value="main">main</option>
                </select>
              </>
            )}
          </div>

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setCommandPaletteOpen(true)}
              className="hidden sm:flex h-7 items-center gap-2 rounded-md border border-[var(--border)] bg-[var(--surface-elevated)] px-2.5 text-xs text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
            >
              <Search className="h-3 w-3" />
              <span>Search...</span>
              <kbd className="rounded border border-[var(--border)] px-1 text-[10px]">
                ⌘K
              </kbd>
            </button>

            <Link
              href="/docs"
              className="rounded-md p-1.5 text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
              title="Documentation"
            >
              <BookOpen className="h-4 w-4" />
            </Link>

            <button
              type="button"
              onClick={toggleTheme}
              className="rounded-md p-1.5 text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
              title="Toggle theme"
            >
              {isDark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
            </button>

            <button
              type="button"
              className="rounded-md p-1.5 text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
              title="Notifications"
            >
              <Bell className="h-4 w-4" />
            </button>

            <div className="relative">
              <button
                type="button"
                onClick={() => setProfileOpen((v) => !v)}
                className="flex h-7 items-center gap-1.5 rounded-md border border-[var(--border)] bg-[var(--surface-elevated)] px-2 text-xs font-medium"
              >
                <span className="flex h-4 w-4 items-center justify-center rounded-full bg-[var(--accent)] text-[10px] font-bold text-[var(--accent-fg)]">
                  {sessionData.user.name?.[0]?.toUpperCase() || "U"}
                </span>
                <span className="hidden md:inline max-w-[100px] truncate">
                  {sessionData.user.name}
                </span>
              </button>

              {profileOpen && (
                <div className="absolute right-0 mt-1.5 w-48 rounded-md border border-[var(--border)] bg-[var(--surface)] p-1 shadow-lg z-40">
                  <div className="border-b border-[var(--border)] px-2.5 py-1.5">
                    <p className="text-xs font-medium truncate">
                      {sessionData.user.name}
                    </p>
                    <p className="text-[11px] text-[var(--text-secondary)] truncate">
                      {sessionData.user.email}
                    </p>
                  </div>
                  <Link
                    href="/settings"
                    onClick={() => setProfileOpen(false)}
                    className="flex items-center gap-2 rounded px-2.5 py-1.5 text-xs text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
                  >
                    <Settings className="h-3.5 w-3.5" /> Settings
                  </Link>
                  <button
                    type="button"
                    onClick={async () => {
                      await signOut();
                      router.push("/login");
                    }}
                    className="flex w-full items-center gap-2 rounded px-2.5 py-1.5 text-xs text-[var(--danger)] hover:bg-[var(--surface-elevated)]"
                  >
                    <LogOut className="h-3.5 w-3.5" /> Sign out
                  </button>
                </div>
              )}
            </div>
          </div>
        </header>

        <main className="flex-1 overflow-y-auto p-5">{children}</main>
      </div>
    </div>
  );
}
