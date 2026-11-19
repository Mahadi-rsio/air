"use client";

import * as React from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Database, Eye, EyeOff, Loader2 } from "lucide-react";
import { signUp } from "@/lib/auth/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export default function RegisterPage() {
  const router = useRouter();
  const [name, setName] = React.useState("");
  const [email, setEmail] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [confirmPassword, setConfirmPassword] = React.useState("");
  const [showPassword, setShowPassword] = React.useState(false);
  const [acceptedTerms, setAcceptedTerms] = React.useState(false);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (password.length < 8) {
      setError("Password must be at least 8 characters.");
      return;
    }
    if (password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }
    if (!acceptedTerms) {
      setError("You must accept the Terms of Service.");
      return;
    }

    setLoading(true);
    try {
      const res = await signUp.email({ name, email, password });
      if (res.error) {
        setError(res.error.message || "Registration failed");
        return;
      }
      router.push("/onboarding");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Registration failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-[var(--bg)] px-4 py-8">
      <div className="w-full max-w-sm rounded-lg border border-[var(--border)] bg-[var(--surface)] p-6">
        <div className="mb-6 flex items-center gap-2.5">
          <div className="flex h-8 w-8 items-center justify-center rounded-md bg-[var(--accent)] text-[var(--accent-fg)]">
            <Database className="h-4 w-4" />
          </div>
          <div>
            <h1 className="text-sm font-semibold text-[var(--text-primary)]">
              Create your AIR account
            </h1>
            <p className="text-xs text-[var(--text-secondary)]">
              Provision serverless PostgreSQL 17 in seconds
            </p>
          </div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-3.5">
          {error && (
            <div className="rounded-md border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-[var(--danger)]">
              {error}
            </div>
          )}

          <div className="space-y-1.5">
            <label className="text-xs font-medium">Full name</label>
            <Input
              required
              placeholder="Ada Lovelace"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-medium">Work email</label>
            <Input
              type="email"
              required
              placeholder="ada@company.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
            />
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-medium">Password</label>
            <div className="relative">
              <Input
                type={showPassword ? "text" : "password"}
                required
                minLength={8}
                placeholder="Min. 8 characters"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="pr-9"
              />
              <button
                type="button"
                onClick={() => setShowPassword((v) => !v)}
                className="absolute right-2.5 top-2 text-[var(--text-secondary)]"
              >
                {showPassword ? (
                  <EyeOff className="h-4 w-4" />
                ) : (
                  <Eye className="h-4 w-4" />
                )}
              </button>
            </div>
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-medium">Confirm password</label>
            <Input
              type={showPassword ? "text" : "password"}
              required
              placeholder="Re-enter password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
            />
          </div>

          <label className="flex items-start gap-2 text-xs text-[var(--text-secondary)] cursor-pointer pt-1">
            <input
              type="checkbox"
              checked={acceptedTerms}
              onChange={(e) => setAcceptedTerms(e.target.checked)}
              className="mt-0.5 rounded border-[var(--border)] accent-[var(--accent)]"
            />
            <span>
              I agree to the AIR Terms of Service and Acceptable Use Policy.
            </span>
          </label>

          <Button type="submit" className="w-full" disabled={loading}>
            {loading && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            Create account
          </Button>
        </form>

        <p className="mt-5 text-center text-xs text-[var(--text-secondary)]">
          Already have an account?{" "}
          <Link href="/login" className="text-[var(--accent)] hover:underline">
            Sign in
          </Link>
        </p>
      </div>
    </div>
  );
}
