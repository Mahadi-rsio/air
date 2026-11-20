import * as React from "react";
import { cn } from "@/lib/utils";

interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: "default" | "accent" | "success" | "warning" | "danger" | "muted";
}

export function Badge({
  className,
  variant = "default",
  ...props
}: BadgeProps) {
  const styles: Record<NonNullable<BadgeProps["variant"]>, string> = {
    default:
      "bg-[var(--surface-elevated)] text-[var(--text-primary)] border-[var(--border)]",
    accent:
      "bg-[var(--accent-soft)] text-[var(--accent)] border-[var(--accent-border)]",
    success:
      "bg-[var(--success-soft)] text-[var(--success)] border-[var(--success-border)]",
    warning:
      "bg-[var(--warning-soft)] text-[var(--warning)] border-[var(--warning-border)]",
    danger:
      "bg-[var(--danger-soft)] text-[var(--danger)] border-[var(--danger-border)]",
    muted:
      "bg-transparent text-[var(--text-secondary)] border-[var(--border)]",
  };

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-[11px] font-medium leading-tight tracking-tight",
        styles[variant],
        className
      )}
      {...props}
    />
  );
}
