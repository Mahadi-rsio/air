import * as React from "react";
import { cn } from "@/lib/utils";

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "ghost" | "danger" | "outline";
  size?: "sm" | "md" | "lg" | "icon";
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = "primary", size = "md", ...props }, ref) => {
    const variants: Record<NonNullable<ButtonProps["variant"]>, string> = {
      primary:
        "bg-[var(--accent)] text-[var(--accent-fg)] font-medium hover:bg-[var(--accent-hover)]",
      secondary:
        "bg-[var(--surface-elevated)] text-[var(--text-primary)] font-medium border border-[var(--border)] hover:border-[var(--border-strong)] hover:bg-[var(--surface-subtle)]",
      outline:
        "bg-transparent border border-[var(--border)] text-[var(--text-primary)] font-medium hover:bg-[var(--surface-elevated)] hover:border-[var(--border-strong)]",
      ghost:
        "bg-transparent text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--surface-elevated)]",
      danger:
        "bg-[var(--danger)] text-[var(--bg)] font-medium hover:opacity-90",
    };

    const sizes: Record<NonNullable<ButtonProps["size"]>, string> = {
      sm: "h-8 px-3 text-xs rounded-md",
      md: "h-9 px-4 text-[13px] rounded-md",
      lg: "h-10 px-5 text-sm rounded-md",
      icon: "h-8 w-8 rounded-md flex items-center justify-center",
    };

    return (
      <button
        ref={ref}
        className={cn(
          "inline-flex items-center justify-center gap-1.5 whitespace-nowrap transition-all duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text-primary)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--bg)] disabled:pointer-events-none disabled:opacity-50 cursor-pointer",
          variants[variant],
          sizes[size],
          className
        )}
        {...props}
      />
    );
  }
);
Button.displayName = "Button";
