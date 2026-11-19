import { create } from "zustand";
import { persist } from "zustand/middleware";

export interface SqlTab {
  id: string;
  title: string;
  sql: string;
}

interface ConsoleState {
  activeWorkspaceId: string | null;
  activeProjectId: string | null;
  activeBranchId: string | null;
  sidebarCollapsed: boolean;
  commandPaletteOpen: boolean;
  sqlTabsByProject: Record<string, SqlTab[]>;
  activeSqlTabByProject: Record<string, string>;

  setActiveWorkspaceId: (id: string | null) => void;
  setActiveProjectId: (id: string | null) => void;
  setActiveBranchId: (id: string | null) => void;
  toggleSidebar: () => void;
  setSidebarCollapsed: (collapsed: boolean) => void;
  setCommandPaletteOpen: (open: boolean) => void;

  getSqlTabs: (projectId: string) => SqlTab[];
  getActiveSqlTabId: (projectId: string) => string;
  setActiveSqlTab: (projectId: string, tabId: string) => void;
  addSqlTab: (projectId: string, title?: string, sql?: string) => string;
  updateSqlTab: (projectId: string, tabId: string, sql: string) => void;
  closeSqlTab: (projectId: string, tabId: string) => void;
}

const DEFAULT_TAB: SqlTab = {
  id: "tab-1",
  title: "Query 1",
  sql: "-- Select a table from the explorer or write PostgreSQL queries\nSELECT current_database(), current_user, version();",
};

export const useConsoleStore = create<ConsoleState>()(
  persist(
    (set, get) => ({
      activeWorkspaceId: null,
      activeProjectId: null,
      activeBranchId: null,
      sidebarCollapsed: false,
      commandPaletteOpen: false,
      sqlTabsByProject: {},
      activeSqlTabByProject: {},

      setActiveWorkspaceId: (id) => set({ activeWorkspaceId: id }),
      setActiveProjectId: (id) => set({ activeProjectId: id }),
      setActiveBranchId: (id) => set({ activeBranchId: id }),
      toggleSidebar: () => set((s) => ({ sidebarCollapsed: !s.sidebarCollapsed })),
      setSidebarCollapsed: (collapsed) => set({ sidebarCollapsed: collapsed }),
      setCommandPaletteOpen: (open) => set({ commandPaletteOpen: open }),

      getSqlTabs: (projectId) => {
        return get().sqlTabsByProject[projectId] ?? [DEFAULT_TAB];
      },
      getActiveSqlTabId: (projectId) => {
        const tabs = get().sqlTabsByProject[projectId] ?? [DEFAULT_TAB];
        const current = get().activeSqlTabByProject[projectId];
        if (current && tabs.some((t) => t.id === current)) return current;
        return tabs[0].id;
      },
      setActiveSqlTab: (projectId, tabId) =>
        set((s) => ({
          activeSqlTabByProject: { ...s.activeSqlTabByProject, [projectId]: tabId },
        })),
      addSqlTab: (projectId, title, sql) => {
        const tabs = get().sqlTabsByProject[projectId] ?? [DEFAULT_TAB];
        const id = `tab-${Date.now()}`;
        const newTab: SqlTab = {
          id,
          title: title || `Query ${tabs.length + 1}`,
          sql: sql ?? "SELECT 1;",
        };
        set((s) => ({
          sqlTabsByProject: {
            ...s.sqlTabsByProject,
            [projectId]: [...tabs, newTab],
          },
          activeSqlTabByProject: {
            ...s.activeSqlTabByProject,
            [projectId]: id,
          },
        }));
        return id;
      },
      updateSqlTab: (projectId, tabId, sql) => {
        const tabs = get().sqlTabsByProject[projectId] ?? [DEFAULT_TAB];
        set((s) => ({
          sqlTabsByProject: {
            ...s.sqlTabsByProject,
            [projectId]: tabs.map((t) => (t.id === tabId ? { ...t, sql } : t)),
          },
        }));
      },
      closeSqlTab: (projectId, tabId) => {
        const tabs = get().sqlTabsByProject[projectId] ?? [DEFAULT_TAB];
        if (tabs.length <= 1) return;
        const nextTabs = tabs.filter((t) => t.id !== tabId);
        const currentActive = get().activeSqlTabByProject[projectId];
        set((s) => ({
          sqlTabsByProject: {
            ...s.sqlTabsByProject,
            [projectId]: nextTabs,
          },
          activeSqlTabByProject: {
            ...s.activeSqlTabByProject,
            [projectId]:
              currentActive === tabId ? nextTabs[0].id : currentActive,
          },
        }));
      },
    }),
    {
      name: "air-console-ui-v1",
      partialize: (state) => ({
        activeWorkspaceId: state.activeWorkspaceId,
        activeProjectId: state.activeProjectId,
        activeBranchId: state.activeBranchId,
        sidebarCollapsed: state.sidebarCollapsed,
        sqlTabsByProject: state.sqlTabsByProject,
        activeSqlTabByProject: state.activeSqlTabByProject,
      }),
    }
  )
);
