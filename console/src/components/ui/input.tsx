import * as React from "react";
import { cn } from "@/lib/utils";

export const Input = React.forwardRef<
  HTMLInputElement,
  React.InputHTMLAttributes<HTMLInputElement>
>(({ className, ...props }, ref) => {
  return (
    <input
      ref={ref}
      className={cn(
        "flex h-8 w-full rounded-md border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 text-xs text-[var(--text-primary)] placeholder:text-[var(--text-secondary)] focus:outline-none focus:ring-1 focus:ring-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      {...props}
    />
  );
});
Input.displayName = "Input";
