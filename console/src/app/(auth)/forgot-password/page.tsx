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
    <div className="air-auth-bg flex min-h-screen flex-col items-center justify-center px-4 py-12">
      <div className="w-full max-w-[400px]">
        <div className="mb-7 flex flex-col items-center text-center">
          <div className="mb-4 flex h-11 w-11 items-center justify-center rounded-lg bg-[var(--accent)] text-[var(--accent-fg)]">

            <Database className="h-5 w-5" />
          </div>
          <h1 className="text-xl font-semibold tracking-tight text-[var(--text-primary)]">
            {token ? "Set new password" : "Reset your password"}
          </h1>
          <p className="mt-1 text-[13px] text-[var(--text-secondary)]">
            AIR Account Recovery
          </p>
        </div>

        <div
          className="rounded-xl border border-[var(--border-strong)] bg-[var(--surface)]/95 backdrop-blur-md p-6"
          style={{ boxShadow: "var(--elevated-shadow)" }}
        >
          {message && (
            <div className="mb-4 rounded-lg border border-[var(--success-border)] bg-[var(--success-soft)] px-3.5 py-2.5 text-xs text-[var(--success)]">
              {message}
            </div>
          )}
          {error && (
            <div className="mb-4 rounded-lg border border-[var(--danger-border)] bg-[var(--danger-soft)] px-3.5 py-2.5 text-xs text-[var(--danger)]">
              {error}
            </div>
          )}

          {token ? (
            <form onSubmit={handleResetPassword} className="space-y-4">
              <div className="space-y-1.5">
                <label className="text-xs font-medium text-[var(--text-primary)]">
                  New password
                </label>
                <Input
                  type="password"
                  required
                  minLength={8}
                  placeholder="Min. 8 characters"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                />
              </div>
              <Button type="submit" size="lg" className="w-full" disabled={loading}>
                {loading && <Loader2 className="h-4 w-4 animate-spin" />}
                Update password
              </Button>
            </form>
          ) : (
            <form onSubmit={handleRequestReset} className="space-y-4">
              <div className="space-y-1.5">
                <label className="text-xs font-medium text-[var(--text-primary)]">
                  Email address
                </label>
                <Input
                  type="email"
                  required
                  placeholder="dev@company.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                />
              </div>
              <Button type="submit" size="lg" className="w-full" disabled={loading}>
                {loading && <Loader2 className="h-4 w-4 animate-spin" />}
                Send reset link
              </Button>
            </form>
          )}
        </div>

        <div className="mt-6 text-center">
          <Link
            href="/login"
            className="inline-flex items-center gap-1.5 text-[13px] text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors"
          >
            <ArrowLeft className="h-3.5 w-3.5" /> Back to sign in
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
