import { createDriverDb } from "./driver";
import * as schema from "./schema";

type DBInstance = ReturnType<typeof createDriverDb>;

let cachedDb: DBInstance | null = null;

export function getDb(): DBInstance {
  if (!cachedDb) cachedDb = createDriverDb();
  return cachedDb;
}

export { schema };
