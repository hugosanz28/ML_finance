import { test, expect } from "@playwright/test";
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const output = resolve("test-results/showcase");
const fixtures = resolve("../demo/synthetic_degiro_exports/incoming");
const sha256 = (path: string) =>
  createHash("sha256").update(readFileSync(path)).digest("hex");
const inputs = () =>
  Object.fromEntries(
    readdirSync(fixtures)
      .filter((name) => name.endsWith(".csv"))
      .sort()
      .map((name) => [name, sha256(resolve(fixtures, name))]),
  );

test("capture the real v2 UI on a fresh synthetic workspace", async ({
  browser,
  request,
}) => {
  mkdirSync(output, { recursive: true });
  const before = inputs();
  const health = await (
    await request.get("http://127.0.0.1:8000/api/v1/health")
  ).json();
  expect(health.workspace_mode).toBe("demo");
  expect(health.mode).toBe("operations");
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
    reducedMotion: "reduce",
    recordVideo: {
      dir: resolve("test-results/showcase-recording"),
      size: { width: 1440, height: 1000 },
    },
  });
  const external: string[] = [];
  await context.route("**/*", (route) => {
    const url = new URL(route.request().url());
    if (url.hostname !== "127.0.0.1") {
      external.push(url.origin);
      return route.abort();
    }
    return route.continue();
  });
  const page = await context.newPage();
  const video = page.video();
  try {
    await page.goto("http://127.0.0.1:5173");
    await expect(
      page.getByText("DEMO SINTÉTICA · No subas datos reales"),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "La fotografía actual" }),
    ).toBeVisible();
    await page.screenshot({
      path: resolve(output, "overview.png"),
      fullPage: true,
    });
    // Deliberate reading time for the recording, never a substitute for readiness assertions.
    await page.waitForTimeout(3000);
    await page
      .getByRole("heading", { name: "Evolución del valor de tus posiciones" })
      .scrollIntoViewIfNeeded();
    await page.waitForTimeout(2000);
    await page.keyboard.press("Control+Home");
    await page
      .getByRole("button", { name: "Rentabilidad", exact: true })
      .click();
    await expect(
      page.getByText(
        "TWR acumulado y MWR anualizado no son directamente comparables.",
        { exact: false },
      ),
    ).toBeVisible();
    await page.screenshot({
      path: resolve(output, "performance.png"),
      fullPage: true,
    });
    await page.waitForTimeout(3000);
    await page
      .getByRole("heading", { name: "Crecimiento de una inversión base" })
      .scrollIntoViewIfNeeded();
    await page.waitForTimeout(2000);
    await page.keyboard.press("Control+Home");
    await page
      .getByRole("button", { name: "Operaciones", exact: true })
      .click();
    await expect(
      page.getByRole("button", { name: "Revisar importación" }),
    ).toBeVisible();
    await page.waitForTimeout(2000);
    await page
      .getByRole("button", { name: "Aportaciones", exact: true })
      .click();
    await page
      .getByLabel("Fecha de valoración (vacío: última disponible)", {
        exact: true,
      })
      .fill("2026-04-30");
    await page
      .getByRole("button", { name: "Revisar simulación", exact: true })
      .click();
    await expect(
      page.getByRole("region", { name: "Confirmar operación" }),
    ).toBeVisible();
    await page.waitForTimeout(2000);
    await page
      .getByRole("button", { name: "Confirmar y ejecutar", exact: true })
      .click();
    await expect(
      page
        .getByRole("region", { name: "Resultado de operación" })
        .getByRole("heading"),
    ).toHaveText(/^Completado(?: con avisos)? · simulation$/);
    await expect(page.getByText(/Caja residual:/)).toBeVisible();
    await page.keyboard.press("Control+Home");
    await page.screenshot({
      path: resolve(output, "contribution.png"),
      fullPage: true,
    });
    await page.getByText(/Caja residual:/).scrollIntoViewIfNeeded();
    await page.waitForTimeout(3000);
    expect(external).toEqual([]);
    expect(inputs()).toEqual(before);
  } finally {
    await context.close();
  }
  if (!video) throw new Error("Showcase recording missing");
  await video.saveAs(resolve(output, "walkthrough.webm"));

  // Render an editorial SVG cover separately: it does not pretend to be a UI screenshot.
  const cover = await browser.newPage({
    viewport: { width: 1200, height: 630 },
  });
  await cover.route("**/*", (route) => route.abort());
  await cover.setContent(
    readFileSync(resolve("../docs/assets/showcase/social-preview.svg"), "utf8"),
  );
  await cover
    .locator("svg")
    .screenshot({ path: resolve(output, "social-preview.png") });
  await cover.close();
  const names = [
    "overview.png",
    "performance.png",
    "contribution.png",
    "social-preview.png",
    "walkthrough.webm",
  ];
  writeFileSync(
    resolve(output, "manifest.json"),
    JSON.stringify(
      {
        source: "fresh synthetic workspace via scripts/run_frontend_e2e_api.py",
        command: "cd frontend && npm run showcase",
        as_of_date: "2026-04-30",
        inputs_sha256: before,
        cover_source_sha256: sha256(
          resolve("../docs/assets/showcase/social-preview.svg"),
        ),
        assets_sha256: Object.fromEntries(
          names.map((name) => [name, sha256(resolve(output, name))]),
        ),
      },
      null,
      2,
    ) + "\n",
  );
});
