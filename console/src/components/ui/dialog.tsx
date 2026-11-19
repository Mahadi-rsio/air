"use client";

import * as React from "react";
import { X } from "lucide-react";

interface DialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: string;
  children: React.ReactNode;
}

export function Dialog({
  open,
  onOpenChange,
  title,
  description,
  children,
}: DialogProps) {
  React.useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape" && open) onOpenChange(false);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onOpenChange]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <div
        className="w-full max-w-lg rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-xl"
        role="dialog"
        aria-modal="true"
      >
        <div className="flex items-start justify-between border-b border-[var(--border)] px-5 py-4">
          <div>
            <h2 className="text-sm font-semibold text-[var(--text-primary)]">
              {title}
            </h2>
            {description && (
              <p className="mt-0.5 text-xs text-[var(--text-secondary)]">
                {description}
              </p>
            )}
          </div>
          <button
            type="button"
            onClick={() => onOpenChange(false)}
            className="rounded p-1 text-[var(--text-secondary)] hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)]"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="p-5">{children}</div>
      </div>
    </div>
  );
}
