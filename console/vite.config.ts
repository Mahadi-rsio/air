import path from "node:path";
import { defineConfig } from "vite";
import vinext from "vinext";
import { cloudflare } from "@cloudflare/vite-plugin";

export default defineConfig({
  resolve: {
    alias: [
      // Workers build uses D1 instead of the local better-sqlite3 driver.
      { find: /^\.\/driver$/, replacement: path.resolve(__dirname, "src/db/driver.cloudflare.ts") },
    ],
  },
  plugins: [
    vinext(),
    cloudflare({
      viteEnvironment: {
        name: "rsc",
        childEnvironments: ["ssr"],
      },
    }),
  ],
});
