"use client";

import * as React from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Database, Loader2, ArrowLeft } from "lucide-react";
import { authClient } from "@/lib/auth/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

function ForgotPasswordForm() {
  const searchParams = useSearchParams();
  const token = searchParams.get("token");

  const [email, setEmail] = React.useState("");
  const [newPassword, setNewPassword] = React.useState("");
  const [loading, setLoading] = React.useState(false);
  const [message, setMessage] = React.useState<string | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  async function handleRequestReset(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setMessage(null);
    try {
      await authClient.requestPasswordReset({
        email,
        redirectTo: "/forgot-password",
      });
      setMessage(
        "If an account exists for that email, a password reset link has been dispatched."
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to request reset");
    } finally {
      setLoading(false);
    }
  }

  async function handleResetPassword(e: React.FormEvent) {
    e.preventDefault();
    if (!token) return;
    setLoading(true);
    setError(null);
    setMessage(null);
    try {
      await authClient.resetPassword({
        newPassword,
        token,
      });
      setMessage("Password updated successfully. You can now sign in.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to reset password");
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
            <h1 className="text-sm font-semibold">
              {token ? "Set new password" : "Reset your password"}
            </h1>
            <p className="text-xs text-[var(--text-secondary)]">
              AIR Account Recovery
            </p>
          </div>
        </div>

        {message && (
          <div className="mb-4 rounded-md border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-xs text-[var(--accent)]">
            {message}
          </div>
        )}
        {error && (
          <div className="mb-4 rounded-md border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-[var(--danger)]">
            {error}
          </div>
        )}

        {token ? (
          <form onSubmit={handleResetPassword} className="space-y-4">
            <div className="space-y-1.5">
              <label className="text-xs font-medium">New password</label>
              <Input
                type="password"
                required
                minLength={8}
                placeholder="Min. 8 characters"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
              />
            </div>
            <Button type="submit" className="w-full" disabled={loading}>
              {loading && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              Update password
            </Button>
          </form>
        ) : (
          <form onSubmit={handleRequestReset} className="space-y-4">
            <div className="space-y-1.5">
              <label className="text-xs font-medium">Email address</label>
              <Input
                type="email"
                required
                placeholder="dev@company.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />
            </div>
            <Button type="submit" className="w-full" disabled={loading}>
              {loading && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              Send reset link
            </Button>
          </form>
        )}

        <div className="mt-5 text-center">
          <Link
            href="/login"
            className="inline-flex items-center gap-1 text-xs text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
          >
            <ArrowLeft className="h-3 w-3" /> Back to sign in
          </Link>
        </div>
      </div>
    </div>
  );
}

export default function ForgotPasswordPage() {
  return (
    <React.Suspense fallback={null}>
      <ForgotPasswordForm />
    </React.Suspense>
  );
}
