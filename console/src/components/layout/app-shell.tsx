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
  ChevronsUpDown,
  CreditCard,
  Database,
  FileCode2,
  FolderTree,
  GitBranch,
  Key,
  LayoutDashboard,
  Link2,
  LogOut,
  Menu,
  Moon,
  PanelLeftClose,
  PanelLeftOpen,
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
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
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

interface NavItem {
  label: string;
  href: string;
  icon: React.ComponentType<{ className?: string }>;
  exact?: boolean;
}

function NavLink({
  item,
  active,
  collapsed,
  onNavigate,
}: {
  item: NavItem;
  active: boolean;
  collapsed: boolean;
  onNavigate?: () => void;
}) {
  const Icon = item.icon;
  return (
    <Link
      href={item.href}
      title={collapsed ? item.label : undefined}
      aria-current={active ? "page" : undefined}
      onClick={onNavigate}
      className={cn(
        "group flex h-9 items-center gap-2.5 rounded-md px-2.5 text-[13px] transition-colors",
        collapsed && "justify-center px-0",
        active
          ? "bg-[var(--accent-soft)] font-medium text-[var(--text-primary)]"
          : "text-[var(--text-secondary)] hover:bg-[var(--accent-soft)] hover:text-[var(--text-primary)]"
      )}
    >
      <Icon
        className={cn(
          "h-4 w-4 shrink-0 transition-colors",
          active
            ? "text-[var(--text-primary)]"
            : "text-[var(--text-muted)] group-hover:text-[var(--text-primary)]"
        )}
      />
      {!collapsed && <span className="truncate">{item.label}</span>}
    </Link>
  );
}

function SectionLabel({
  children,
  action,
}: {
  children: React.ReactNode;
  action?: React.ReactNode;
}) {
  return (
    <div className="mb-1.5 flex items-center justify-between px-2.5">
      <span className="text-[11px] font-medium uppercase tracking-wider text-[var(--text-muted)]">
        {children}
      </span>
      {action}
    </div>
  );
}

function SelectField({
  value,
  onChange,
  label,
  className,
  children,
}: {
  value: string;
  onChange: (v: string) => void;
  label: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="relative">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        aria-label={label}
        className={cn(
          "h-9 w-full appearance-none rounded-md border border-[var(--border)] bg-[var(--surface)] pl-3 pr-8 text-[13px] font-medium text-[var(--text-primary)] transition-colors hover:border-[var(--border-strong)] focus:border-[var(--border-strong)] focus:outline-none cursor-pointer",
          className
        )}
      >
        {children}
      </select>
      <ChevronsUpDown className="pointer-events-none absolute right-2.5 top-2.5 h-3.5 w-3.5 text-[var(--text-muted)]" />
    </div>
  );
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
  const [mobileNavOpen, setMobileNavOpen] = React.useState(false);
  const [notificationsOpen, setNotificationsOpen] = React.useState(false);
  const profileRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    setIsDark(document.documentElement.classList.contains("dark"));
  }, []);

  // Close menus on route change.
  React.useEffect(() => {
    setMobileNavOpen(false);
    setProfileOpen(false);
  }, [pathname]);

  React.useEffect(() => {
    if (!profileOpen) return;
    function onDown(e: MouseEvent) {
      if (profileRef.current && !profileRef.current.contains(e.target as Node)) {
        setProfileOpen(false);
      }
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setProfileOpen(false);
    }
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [profileOpen]);

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

  function selectProject(pid: string) {
    if (!pid) return;
    setActiveProjectId(pid);
    router.push(`/projects/${pid}`);
  }

  const workspaceNav: NavItem[] = [
    { label: "Overview", href: "/dashboard", icon: LayoutDashboard },
    { label: "Projects", href: "/projects", icon: Database, exact: false },
    { label: "Usage & Billing", href: "/billing", icon: CreditCard },
    { label: "Members", href: "/members", icon: Users },
    { label: "Settings", href: "/settings", icon: Settings },
  ];

  const projectNav: NavItem[] = currentProjectId
    ? [
        { label: "Overview", href: `/projects/${currentProjectId}`, icon: LayoutDashboard, exact: true },
        { label: "SQL Editor", href: `/projects/${currentProjectId}/sql`, icon: Terminal },
        { label: "Tables", href: `/projects/${currentProjectId}/tables`, icon: Table2 },
        { label: "Explorer", href: `/projects/${currentProjectId}/explorer`, icon: FolderTree },
        { label: "Branches", href: `/projects/${currentProjectId}/branches`, icon: GitBranch },
        { label: "Roles & Users", href: `/projects/${currentProjectId}/roles`, icon: Shield },
        { label: "Extensions", href: `/projects/${currentProjectId}/extensions`, icon: Puzzle },
        { label: "Backups", href: `/projects/${currentProjectId}/backups`, icon: Archive },
        { label: "Monitoring", href: `/projects/${currentProjectId}/monitoring`, icon: Activity },
        { label: "Connection", href: `/projects/${currentProjectId}/connection`, icon: Link2 },
        { label: "API Keys", href: `/projects/${currentProjectId}/api-keys`, icon: Key },
        { label: "Project Settings", href: `/projects/${currentProjectId}/settings`, icon: Settings },
      ]
    : [];

  const resourceNav: NavItem[] = [
    { label: "Documentation", href: "/docs", icon: BookOpen },
    { label: "API Reference", href: "/api-reference", icon: FileCode2 },
  ];

  function isActive(item: NavItem) {
    if (item.exact) return pathname === item.href;
    return pathname === item.href || pathname.startsWith(`${item.href}/`);
  }

  function renderSidebar(collapsed: boolean, onNavigate?: () => void) {
    const showProject = Boolean(urlProjectId && projectNav.length > 0);
    return (
      <>
        {!collapsed && wsData && wsData.length > 0 && (
          <div className="space-y-2 border-b border-[var(--border)] p-3">
            <SelectField
              label="Active workspace"
              value={activeWorkspaceId || ""}
              onChange={setActiveWorkspaceId}
            >
              {wsData.map((ws) => (
                <option key={ws.id} value={ws.id}>
                  {ws.name}
                </option>
              ))}
            </SelectField>
            {projectsData && projectsData.length > 0 && (
              <div className="lg:hidden">
                <SelectField
                  label="Select project"
                  value={currentProjectId || ""}
                  onChange={(v) => {
                    selectProject(v);
                    onNavigate?.();
                  }}
                >
                  <option value="">Select project…</option>
                  {projectsData.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.name}
                    </option>
                  ))}
                </SelectField>
              </div>
            )}
          </div>
        )}

        <div className="flex-1 space-y-6 overflow-y-auto px-3 py-4">
          <div>
            {!collapsed && (
              <SectionLabel
                action={
                  showProject ? (
                    <Link
                      href="/projects"
                      onClick={onNavigate}
                      className="text-[11px] text-[var(--text-muted)] transition-colors hover:text-[var(--text-primary)]"
                    >
                      All projects
                    </Link>
                  ) : undefined
                }
              >
                {showProject ? "Project" : "Workspace"}
              </SectionLabel>
            )}
            <nav className="space-y-0.5" aria-label={showProject ? "Project" : "Workspace"}>
              {(showProject ? projectNav : workspaceNav).map((item) => (
                <NavLink
                  key={item.href}
                  item={item}
                  active={isActive(item)}
                  collapsed={collapsed}
                  onNavigate={onNavigate}
                />
              ))}
            </nav>
          </div>

          <div>
            {!collapsed && <SectionLabel>Resources</SectionLabel>}
            <nav className="space-y-0.5" aria-label="Resources">
              {resourceNav.map((item) => (
                <NavLink
                  key={item.href}
                  item={item}
                  active={pathname === item.href}
                  collapsed={collapsed}
                  onNavigate={onNavigate}
                />
              ))}
            </nav>
          </div>
        </div>
      </>
    );
  }

  const brand = (collapsed: boolean) => (
    <Link href="/dashboard" className="flex items-center gap-2.5 min-w-0" aria-label="AIR home">
      <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-[var(--accent)] text-[var(--accent-fg)]">
        <Database className="h-3.5 w-3.5" />
      </span>
      {!collapsed && (
        <span className="flex items-center gap-2">
          <span className="text-[15px] font-semibold tracking-tight">AIR</span>
          <span className="rounded border border-[var(--border)] px-1.5 py-px font-mono text-[10px] text-[var(--text-muted)]">
            PG 17
          </span>
        </span>
      )}
    </Link>
  );

  if (sessionLoading || !sessionData?.user) {
    return (
      <div className="flex h-screen items-center justify-center bg-[var(--bg)]">
        <div className="flex items-center gap-2.5 text-xs text-[var(--text-secondary)]">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-[var(--text-primary)] border-t-transparent" />
          <span>Loading AIR Console…</span>
        </div>
      </div>
    );
  }

  const iconBtn =
    "inline-flex h-9 w-9 items-center justify-center rounded-md text-[var(--text-secondary)] transition-colors hover:bg-[var(--accent-soft)] hover:text-[var(--text-primary)] cursor-pointer";

  return (
    <div className="min-h-screen bg-[var(--bg)] text-[var(--text-primary)]">
      <CommandPalette projects={projectsData || []} />

      {/* Desktop sidebar (fixed) */}
      <aside
        className={cn(
          "fixed inset-y-0 left-0 z-40 hidden flex-col border-r border-[var(--border)] bg-[var(--sidebar)] transition-[width] duration-200 lg:flex",
          sidebarCollapsed ? "w-[4.25rem]" : "w-64"
        )}
      >
        <div
          className={cn(
            "flex h-14 shrink-0 items-center border-b border-[var(--border)]",
            sidebarCollapsed ? "justify-center" : "px-4"
          )}
        >
          {brand(sidebarCollapsed)}
        </div>
        {renderSidebar(sidebarCollapsed)}
        <div className="border-t border-[var(--border)] p-3">
          <button
            type="button"
            onClick={toggleSidebar}
            className={cn(
              "flex h-9 w-full items-center gap-2.5 rounded-md px-2.5 text-[13px] text-[var(--text-secondary)] transition-colors hover:bg-[var(--accent-soft)] hover:text-[var(--text-primary)] cursor-pointer",
              sidebarCollapsed && "justify-center px-0"
            )}
            aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {sidebarCollapsed ? (
              <PanelLeftOpen className="h-4 w-4" />
            ) : (
              <>
                <PanelLeftClose className="h-4 w-4" />
                <span>Collapse</span>
              </>
            )}
          </button>
        </div>
      </aside>

      {/* Mobile navigation (Sheet) */}
      <Sheet open={mobileNavOpen} onOpenChange={setMobileNavOpen}>
        <SheetContent side="left" className="bg-[var(--sidebar)]">
          <SheetHeader className="h-14 flex-row items-center py-0">
            <SheetTitle className="sr-only">Navigation</SheetTitle>
            <SheetDescription className="sr-only">
              Workspace and project navigation
            </SheetDescription>
            {brand(false)}
          </SheetHeader>
          {renderSidebar(false, () => setMobileNavOpen(false))}
        </SheetContent>
      </Sheet>

      {/* Notifications (Sheet) */}
      <Sheet open={notificationsOpen} onOpenChange={setNotificationsOpen}>
        <SheetContent side="right">
          <SheetHeader>
            <SheetTitle>Notifications</SheetTitle>
            <SheetDescription>Workspace alerts and activity</SheetDescription>
          </SheetHeader>
          <div className="flex flex-1 flex-col items-center justify-center gap-2 p-8 text-center">
            <span className="flex h-10 w-10 items-center justify-center rounded-full border border-[var(--border)] text-[var(--text-muted)]">
              <Bell className="h-4 w-4" />
            </span>
            <p className="text-sm font-medium">You&apos;re all caught up</p>
            <p className="max-w-[16rem] text-xs text-[var(--text-secondary)]">
              Backup, lifecycle and security events will appear here once the
              control plane reports them.
            </p>
          </div>
        </SheetContent>
      </Sheet>

      {/* Fixed app bar */}
      <header
        className={cn(
          "fixed inset-x-0 top-0 z-30 flex h-14 items-center justify-between gap-3 border-b border-[var(--border)] bg-[var(--bg)]/80 px-3 backdrop-blur-xl transition-[left] duration-200 sm:px-5",
          sidebarCollapsed ? "lg:left-[4.25rem]" : "lg:left-64"
        )}
      >
        <div className="flex min-w-0 items-center gap-2">
          <button
            type="button"
            onClick={() => setMobileNavOpen(true)}
            className={cn(iconBtn, "lg:hidden")}
            aria-label="Open navigation"
          >
            <Menu className="h-[18px] w-[18px]" />
          </button>
          <div className="lg:hidden">{brand(true)}</div>

          <nav
            aria-label="Breadcrumb"
            className="ml-1 hidden min-w-0 items-center gap-2 text-[13px] md:flex"
          >
            <Link
              href="/dashboard"
              className="max-w-[10rem] truncate font-medium text-[var(--text-secondary)] transition-colors hover:text-[var(--text-primary)]"
            >
              {wsData?.find((w) => w.id === activeWorkspaceId)?.name || "Workspace"}
            </Link>
            {projectsData && projectsData.length > 0 && (
              <>
                <span className="text-[var(--border-strong)]">/</span>
                <SelectField
                  label="Select project"
                  value={currentProjectId || ""}
                  onChange={selectProject}
                  className="h-8 max-w-[11rem] border-transparent bg-transparent hover:bg-[var(--accent-soft)]"
                >
                  <option value="">Select project…</option>
                  {projectsData.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.name}
                    </option>
                  ))}
                </SelectField>
              </>
            )}
            {currentProject && (
              <>
                <span className="text-[var(--border-strong)]">/</span>
                <SelectField
                  label="Active branch"
                  value={activeBranchId || "main"}
                  onChange={setActiveBranchId}
                  className="h-8 w-auto border-transparent bg-transparent font-mono text-xs hover:bg-[var(--accent-soft)]"
                >
                  <option value="main">main</option>
                </SelectField>
              </>
            )}
          </nav>
        </div>

        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={() => setCommandPaletteOpen(true)}
            className="hidden h-9 items-center gap-2 rounded-md border border-[var(--border)] bg-[var(--surface)] px-3 text-[13px] text-[var(--text-muted)] transition-colors hover:border-[var(--border-strong)] hover:text-[var(--text-primary)] cursor-pointer sm:flex sm:w-48 lg:w-56"
            aria-label="Open command palette"
          >
            <Search className="h-3.5 w-3.5" />
            <span className="flex-1 text-left">Search…</span>
            <kbd className="rounded border border-[var(--border)] px-1.5 font-mono text-[10px]">
              ⌘K
            </kbd>
          </button>
          <button
            type="button"
            onClick={() => setCommandPaletteOpen(true)}
            className={cn(iconBtn, "sm:hidden")}
            aria-label="Search"
          >
            <Search className="h-4 w-4" />
          </button>

          <button
            type="button"
            onClick={toggleTheme}
            className={iconBtn}
            aria-label={isDark ? "Switch to light mode" : "Switch to dark mode"}
          >
            {isDark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
          </button>

          <button
            type="button"
            onClick={() => setNotificationsOpen(true)}
            className={iconBtn}
            aria-label="Notifications"
          >
            <Bell className="h-4 w-4" />
          </button>

          <div className="relative ml-1" ref={profileRef}>
            <button
              type="button"
              onClick={() => setProfileOpen((v) => !v)}
              aria-haspopup="menu"
              aria-expanded={profileOpen}
              aria-label="Account menu"
              className="flex h-8 w-8 items-center justify-center rounded-full bg-[var(--accent)] text-xs font-semibold text-[var(--accent-fg)] transition-opacity hover:opacity-90 cursor-pointer"
            >
              {sessionData.user.name?.[0]?.toUpperCase() || "U"}
            </button>

            {profileOpen && (
              <div
                role="menu"
                className="absolute right-0 mt-2 w-60 rounded-lg border border-[var(--border-strong)] bg-[var(--surface)] p-1"
                style={{ boxShadow: "var(--elevated-shadow)" }}
              >
                <div className="mb-1 border-b border-[var(--border)] px-3 py-2.5">
                  <p className="truncate text-[13px] font-medium">
                    {sessionData.user.name}
                  </p>
                  <p className="truncate text-xs text-[var(--text-secondary)]">
                    {sessionData.user.email}
                  </p>
                </div>
                <Link
                  href="/settings"
                  role="menuitem"
                  className="flex h-9 items-center gap-2 rounded-md px-3 text-[13px] text-[var(--text-secondary)] transition-colors hover:bg-[var(--accent-soft)] hover:text-[var(--text-primary)]"
                >
                  <Settings className="h-3.5 w-3.5" /> Settings
                </Link>
                <button
                  type="button"
                  role="menuitem"
                  onClick={async () => {
                    await signOut();
                    router.push("/login");
                  }}
                  className="flex h-9 w-full items-center gap-2 rounded-md px-3 text-[13px] text-[var(--danger)] transition-colors hover:bg-[var(--danger-soft)] cursor-pointer"
                >
                  <LogOut className="h-3.5 w-3.5" /> Sign out
                </button>
              </div>
            )}
          </div>
        </div>
      </header>

      {/* Content */}
      <div
        className={cn(
          "transition-[padding] duration-200",
          sidebarCollapsed ? "lg:pl-[4.25rem]" : "lg:pl-64"
        )}
      >
        <main className="px-4 pb-10 pt-[calc(3.5rem+1.5rem)] sm:px-6 lg:px-8">
          {children}
        </main>
      </div>
    </div>
  );
}
