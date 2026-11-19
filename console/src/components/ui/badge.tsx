import * as React from "react";
import { cn } from "@/lib/utils";

interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: "default" | "success" | "warning" | "danger" | "muted";
}

export function Badge({
  className,
  variant = "default",
  ...props
}: BadgeProps) {
  const styles: Record<NonNullable<BadgeProps["variant"]>, string> = {
    default: "bg-[var(--surface-elevated)] text-[var(--text-primary)] border-[var(--border)]",
    success: "bg-emerald-500/10 text-[var(--accent)] border-emerald-500/30",
    warning: "bg-amber-500/10 text-[var(--warning)] border-amber-500/30",
    danger: "bg-red-500/10 text-[var(--danger)] border-red-500/30",
    muted: "bg-transparent text-[var(--text-secondary)] border-[var(--border)]",
  };

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-medium leading-none",
        styles[variant],
        className
      )}
      {...props}
    />
  );
}
