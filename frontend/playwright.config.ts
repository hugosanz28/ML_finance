import { defineConfig } from "@playwright/test";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

const localPython = resolve(
  "../.venv",
  process.platform === "win32" ? "Scripts/python.exe" : "bin/python",
);
const python = existsSync(localPython) ? `"${localPython}"` : "python";
export default defineConfig({
  testDir: "./e2e",
  testIgnore: "**/showcase.spec.ts",
  workers: 1,
  retries: 0,
  timeout: 180000,
  expect: { timeout: 30000 },
  use: {
    baseURL: "http://127.0.0.1:5173",
    browserName: "chromium",
    viewport: { width: 1440, height: 1000 },
    trace: "retain-on-failure",
  },
  // Never connect browser mutation tests to a pre-existing (possibly real) server.
  webServer: [
    {
      command: `${python} ../scripts/run_frontend_e2e_api.py`,
      url: "http://127.0.0.1:8000/api/v1/health",
      reuseExistingServer: false,
      timeout: 90000,
    },
    {
      command: "npm run dev",
      url: "http://127.0.0.1:5173",
      reuseExistingServer: false,
    },
  ],
});
