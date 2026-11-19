import type { Metadata } from "next";
import "./globals.css";
import { Providers } from "@/components/providers";

export const metadata: Metadata = {
  title: "AIR — Serverless PostgreSQL Database Console",
  description:
    "Serverless PostgreSQL 17 database platform, SQL studio, branching, backups, and infrastructure console.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <body className="min-h-screen bg-[var(--bg)] text-[var(--text-primary)] antialiased">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
