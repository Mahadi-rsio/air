import * as React from "react";
import { cn } from "@/lib/utils";

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "ghost" | "danger" | "outline";
  size?: "sm" | "md" | "lg" | "icon";
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = "primary", size = "md", ...props }, ref) => {
    const variants: Record<NonNullable<ButtonProps["variant"]>, string> = {
      primary:
        "bg-[var(--accent)] text-[var(--accent-fg)] hover:opacity-90 font-medium",
      secondary:
        "bg-[var(--surface-elevated)] text-[var(--text-primary)] border border-[var(--border)] hover:opacity-90",
      outline:
        "bg-transparent border border-[var(--border)] text-[var(--text-primary)] hover:bg-[var(--surface-elevated)]",
      ghost:
        "bg-transparent text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--surface-elevated)]",
      danger:
        "bg-[var(--danger)] text-white hover:opacity-90 font-medium",
    };

    const sizes: Record<NonNullable<ButtonProps["size"]>, string> = {
      sm: "h-7 px-2.5 text-xs rounded-md",
      md: "h-8 px-3.5 text-xs rounded-md",
      lg: "h-9 px-4 text-sm rounded-md",
      icon: "h-8 w-8 rounded-md flex items-center justify-center",
    };

    return (
      <button
        ref={ref}
        className={cn(
          "inline-flex items-center justify-center gap-1.5 transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-[var(--accent)] disabled:pointer-events-none disabled:opacity-50 cursor-pointer",
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
