import { env } from "cloudflare:workers";
import { drizzle } from "drizzle-orm/d1";
import * as schema from "./schema";

// Worker runtime: Cloudflare D1 via the `DB` binding (schema applied with `drizzle/` migrations).
// Cast keeps the Node (better-sqlite3) type as the shared app-facing type; queries are awaited everywhere.
export function createDriverDb() {
  return drizzle((env as unknown as { DB: D1Database }).DB, { schema }) as unknown as ReturnType<
    typeof import("./driver.node").createDriverDb
  >;
}
