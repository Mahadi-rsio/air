import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatBytes(bytes: number | string | null | undefined): string {
  const n = Number(bytes || 0);
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const idx = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
  return `${(n / Math.pow(1024, idx)).toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`;
}

export function formatRelativeTime(dateInput: string | Date | number | null | undefined): string {
  if (!dateInput) return "—";
  const d = new Date(dateInput);
  const diffSec = Math.floor((Date.now() - d.getTime()) / 1000);
  if (diffSec < 60) return `${Math.max(diffSec, 0)}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return d.toLocaleDateString();
}
