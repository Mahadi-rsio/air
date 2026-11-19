import { betterAuth } from "better-auth";
import { drizzleAdapter } from "better-auth/adapters/drizzle";
import { jwt, bearer } from "better-auth/plugins";
import { nextCookies } from "better-auth/next-js";
import { getDb, schema } from "@/db";

export const auth = betterAuth({
  database: drizzleAdapter(getDb(), {
    provider: "sqlite",
    schema: {
      user: schema.user,
      session: schema.session,
      account: schema.account,
      verification: schema.verification,
      jwks: schema.jwks,
    },
  }),
  secret:
    process.env.BETTER_AUTH_SECRET ||
    "air-console-dev-secret-change-me-in-production-32bytes",
  baseURL: process.env.BETTER_AUTH_URL || "http://localhost:3000",
  // Trust same-origin requests (Origin host === Host header) so dev ports / Codespaces / Workers
  // custom domains work without extra config; cross-site origins stay blocked.
  trustedOrigins: async (request?: Request) => {
    const origins = process.env.BETTER_AUTH_TRUSTED_ORIGINS?.split(",").map((o) => o.trim()).filter(Boolean) ?? [];
    const origin = request?.headers.get("origin");
    const host = request?.headers.get("x-forwarded-host") || request?.headers.get("host");
    if (origin && host) {
      try {
        if (new URL(origin).host === host) origins.push(origin);
      } catch {}
    }
    return origins;
  },
  emailAndPassword: {
    enabled: true,
    minPasswordLength: 8,
    maxPasswordLength: 128,
    autoSignIn: true,
    async sendResetPassword({ user, url }) {
      // In production, wire to your transactional email provider (e.g., Resend / Postmark).
      // Logged server-side for local development verification without exposing to client.
      console.info(`[AIR Auth] Password reset requested for ${user.email}: ${url}`);
    },
  },
  session: {
    expiresIn: 60 * 60 * 24 * 7, // 7 days
    updateAge: 60 * 60 * 24, // 1 day
    cookieCache: {
      enabled: true,
      maxAge: 60 * 5,
    },
  },
  plugins: [
    jwt({
      jwt: {
        expirationTime: "1h",
      },
    }),
    bearer(),
    nextCookies(),
  ],
});

export type Session = typeof auth.$Infer.Session;
