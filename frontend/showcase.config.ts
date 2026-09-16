import { defineConfig } from "@playwright/test";
import base from "./playwright.config";

// Keep the same fresh offline API and refuse occupied ports, including in CI.
export default defineConfig({
  ...base,
  testMatch: "**/showcase.spec.ts",
  testIgnore: [],
});
