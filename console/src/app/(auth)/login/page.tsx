"use client";

import * as React from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Eye, EyeOff, Database, Loader2 } from "lucide-react";
import { signIn } from "@/lib/auth/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useToast } from "@/components/ui/toast";

export default function LoginPage() {
  const router = useRouter();
  const { toast } = useToast();
  const [email, setEmail] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [showPassword, setShowPassword] = React.useState(false);
  const [rememberMe, setRememberMe] = React.useState(true);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const res = await signIn.email({
        email,
        password,
        rememberMe,
      });
      if (res.error) {
        setError(res.error.message || "Invalid email or password");
        return;
      }
      toast({ title: "Signed in to AIR Console", variant: "success" });
      router.push("/dashboard");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to sign in");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-[var(--bg)] px-4">
      <div className="w-full max-w-sm rounded-lg border border-[var(--border)] bg-[var(--surface)] p-6">
        <div className="mb-6 flex items-center gap-2.5">
          <div className="flex h-8 w-8 items-center justify-center rounded-md bg-[var(--accent)] text-[var(--accent-fg)]">
            <Database className="h-4 w-4" />
          </div>
          <div>
            <h1 className="text-sm font-semibold text-[var(--text-primary)]">
              Sign in to AIR
            </h1>
            <p className="text-xs text-[var(--text-secondary)]">
              Serverless PostgreSQL Database Console
            </p>
          </div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          {error && (
            <div className="rounded-md border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-[var(--danger)]">
              {error}
            </div>
          )}

          <div className="space-y-1.5">
            <label className="text-xs font-medium text-[var(--text-primary)]">
              Email
            </label>
            <Input
              type="email"
              required
              autoComplete="email"
              placeholder="dev@company.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
            />
          </div>

          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <label className="text-xs font-medium text-[var(--text-primary)]">
                Password
              </label>
              <Link
                href="/forgot-password"
                className="text-[11px] text-[var(--accent)] hover:underline"
              >
                Forgot password?
              </Link>
            </div>
            <div className="relative">
              <Input
                type={showPassword ? "text" : "password"}
                required
                autoComplete="current-password"
                placeholder="••••••••"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="pr-9"
              />
              <button
                type="button"
                onClick={() => setShowPassword((v) => !v)}
                className="absolute right-2.5 top-2 text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
                aria-label={showPassword ? "Hide password" : "Show password"}
              >
                {showPassword ? (
                  <EyeOff className="h-4 w-4" />
                ) : (
                  <Eye className="h-4 w-4" />
                )}
              </button>
            </div>
          </div>

          <label className="flex items-center gap-2 text-xs text-[var(--text-secondary)] cursor-pointer">
            <input
              type="checkbox"
              checked={rememberMe}
              onChange={(e) => setRememberMe(e.target.checked)}
              className="rounded border-[var(--border)] accent-[var(--accent)]"
            />
            Remember session
          </label>

          <Button type="submit" className="w-full" disabled={loading}>
            {loading && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            Sign in
          </Button>
        </form>

        <p className="mt-5 text-center text-xs text-[var(--text-secondary)]">
          New to AIR?{" "}
          <Link href="/register" className="text-[var(--accent)] hover:underline">
            Create an account
          </Link>
        </p>
      </div>
    </div>
  );
}
