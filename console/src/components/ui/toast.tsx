"use client";

import * as React from "react";
import { CheckCircle2, AlertCircle, X } from "lucide-react";

export interface ToastItem {
  id: string;
  title: string;
  description?: string;
  variant?: "success" | "error" | "info";
}

interface ToastContextValue {
  toast: (item: Omit<ToastItem, "id">) => void;
}

const ToastContext = React.createContext<ToastContextValue | null>(null);

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [items, setItems] = React.useState<ToastItem[]>([]);

  const toast = React.useCallback((item: Omit<ToastItem, "id">) => {
    const id = `t_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
    setItems((prev) => [...prev, { ...item, id }]);
    setTimeout(() => {
      setItems((prev) => prev.filter((x) => x.id !== id));
    }, 4500);
  }, []);

  return (
    <ToastContext.Provider value={{ toast }}>
      {children}
      <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 max-w-sm">
        {items.map((item) => (
          <div
            key={item.id}
            className="flex items-start gap-2.5 rounded-lg border border-[var(--border)] bg-[var(--surface-elevated)] px-3.5 py-2.5 shadow-lg"
          >
            {item.variant === "error" ? (
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0 text-[var(--danger)]" />
            ) : (
              <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-[var(--accent)]" />
            )}
            <div className="flex-1 min-w-0">
              <p className="text-xs font-medium text-[var(--text-primary)]">
                {item.title}
              </p>
              {item.description && (
                <p className="mt-0.5 text-[11px] text-[var(--text-secondary)] break-words">
                  {item.description}
                </p>
              )}
            </div>
            <button
              type="button"
              onClick={() =>
                setItems((prev) => prev.filter((x) => x.id !== item.id))
              }
              className="text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast() {
  const ctx = React.useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within ToastProvider");
  return ctx;
}
