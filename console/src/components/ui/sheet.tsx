"use client";

/**
 * Sheet — shadcn/ui-compatible slide-over panel (Sheet, SheetContent, SheetHeader,
 * SheetTitle, SheetDescription, SheetFooter, SheetClose). Same API surface as
 * shadcn's Sheet, implemented without extra dependencies: portal, overlay,
 * Escape-to-close, scroll lock, focus trap, focus restore and exit animation.
 */
import * as React from "react";
import { createPortal } from "react-dom";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";

type Side = "left" | "right" | "top" | "bottom";

interface SheetContextValue {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  titleId: string;
  descriptionId: string;
}

const SheetContext = React.createContext<SheetContextValue | null>(null);

function useSheet() {
  const ctx = React.useContext(SheetContext);
  if (!ctx) throw new Error("Sheet components must be used within <Sheet>");
  return ctx;
}

export function Sheet({
  open,
  onOpenChange,
  children,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  children: React.ReactNode;
}) {
  const id = React.useId();
  const value = React.useMemo(
    () => ({
      open,
      onOpenChange,
      titleId: `${id}-title`,
      descriptionId: `${id}-description`,
    }),
    [open, onOpenChange, id]
  );
  return <SheetContext.Provider value={value}>{children}</SheetContext.Provider>;
}

const sideClasses: Record<Side, string> = {
  left: "inset-y-0 left-0 h-full w-[85%] max-w-xs border-r",
  right: "inset-y-0 right-0 h-full w-[92%] max-w-md border-l",
  top: "inset-x-0 top-0 max-h-[85vh] border-b",
  bottom: "inset-x-0 bottom-0 max-h-[85vh] border-t",
};

const FOCUSABLE =
  'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])';

export function SheetContent({
  side = "right",
  className,
  children,
  hideClose,
}: {
  side?: Side;
  className?: string;
  children: React.ReactNode;
  hideClose?: boolean;
}) {
  const { open, onOpenChange, titleId, descriptionId } = useSheet();
  const [mounted, setMounted] = React.useState(false);
  const [present, setPresent] = React.useState(open);
  const panelRef = React.useRef<HTMLDivElement>(null);
  const restoreRef = React.useRef<HTMLElement | null>(null);

  React.useEffect(() => setMounted(true), []);

  // Keep mounted until the exit animation completes.
  React.useEffect(() => {
    if (open) {
      setPresent(true);
      return;
    }
    const t = window.setTimeout(() => setPresent(false), 190);
    return () => window.clearTimeout(t);
  }, [open]);

  // Scroll lock, focus management, Escape.
  React.useEffect(() => {
    if (!open) return;
    restoreRef.current = document.activeElement as HTMLElement | null;
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    const raf = requestAnimationFrame(() => {
      const first = panelRef.current?.querySelector<HTMLElement>(FOCUSABLE);
      (first ?? panelRef.current)?.focus();
    });

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.stopPropagation();
        onOpenChange(false);
        return;
      }
      if (e.key !== "Tab" || !panelRef.current) return;
      const items = Array.from(
        panelRef.current.querySelectorAll<HTMLElement>(FOCUSABLE)
      );
      if (items.length === 0) {
        e.preventDefault();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => {
      cancelAnimationFrame(raf);
      document.removeEventListener("keydown", onKeyDown);
      document.body.style.overflow = prevOverflow;
      restoreRef.current?.focus?.();
    };
  }, [open, onOpenChange]);

  if (!mounted || !present) return null;
  const state = open ? "open" : "closed";

  return createPortal(
    <div className="fixed inset-0 z-[60]">
      <div
        className="air-sheet-overlay absolute inset-0 bg-black/60 backdrop-blur-[2px]"
        data-state={state}
        onClick={() => onOpenChange(false)}
        aria-hidden="true"
      />
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        tabIndex={-1}
        data-state={state}
        data-side={side}
        className={cn(
          "air-sheet absolute flex flex-col overflow-hidden border-[var(--border-strong)] bg-[var(--surface)] text-[var(--text-primary)] focus:outline-none",
          sideClasses[side],
          className
        )}
        style={{ boxShadow: "var(--elevated-shadow)" }}
      >
        {children}
        {!hideClose && (
          <button
            type="button"
            onClick={() => onOpenChange(false)}
            aria-label="Close"
            className="absolute right-3 top-3 inline-flex h-8 w-8 items-center justify-center rounded-md text-[var(--text-secondary)] transition-colors hover:bg-[var(--surface-elevated)] hover:text-[var(--text-primary)] cursor-pointer"
          >
            <X className="h-4 w-4" />
          </button>
        )}
      </div>
    </div>,
    document.body
  );
}

export function SheetHeader({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "flex flex-col gap-1 border-b border-[var(--border)] px-5 py-4 pr-12",
        className
      )}
      {...props}
    />
  );
}

export function SheetFooter({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "mt-auto flex items-center justify-end gap-2 border-t border-[var(--border)] px-5 py-3",
        className
      )}
      {...props}
    />
  );
}

export function SheetTitle({
  className,
  ...props
}: React.HTMLAttributes<HTMLHeadingElement>) {
  const { titleId } = useSheet();
  return (
    <h2
      id={titleId}
      className={cn("text-sm font-semibold tracking-tight", className)}
      {...props}
    />
  );
}

export function SheetDescription({
  className,
  ...props
}: React.HTMLAttributes<HTMLParagraphElement>) {
  const { descriptionId } = useSheet();
  return (
    <p
      id={descriptionId}
      className={cn("text-xs text-[var(--text-secondary)]", className)}
      {...props}
    />
  );
}

export function SheetClose({
  className,
  children,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  const { onOpenChange } = useSheet();
  return (
    <button
      type="button"
      onClick={() => onOpenChange(false)}
      className={className}
      {...props}
    >
      {children}
    </button>
  );
}
