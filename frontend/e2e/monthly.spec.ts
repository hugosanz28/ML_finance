import { test, expect, type Page } from "@playwright/test";
import { resolve } from "node:path";
import { readdirSync, readFileSync } from "node:fs";
import { createHash } from "node:crypto";

const fixtureDir = resolve("../demo/synthetic_degiro_exports/incoming");
const fixtures = readdirSync(fixtureDir)
  .filter((name) => name.endsWith(".csv"))
  .map((name) => resolve(fixtureDir, name));
const digest = () =>
  fixtures.map((path) =>
    createHash("sha256").update(readFileSync(path)).digest("hex"),
  );
async function execute(page: Page, review: string, operation: string) {
  await page.getByRole("button", { name: review, exact: true }).click();
  await expect(
    page.getByRole("region", { name: "Confirmar operación" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Confirmar y ejecutar", exact: true })
    .click();
  await expect(
    page
      .getByRole("region", { name: "Resultado de operación" })
      .getByRole("heading"),
  ).toHaveText(new RegExp(`Completado(?: con avisos)? · ${operation}$`));
}

test("monthly workflow uses the real local API, only synthetic copies and no external network", async ({
  page,
  request,
}) => {
  const before = digest();
  const status = await (
    await request.get("http://127.0.0.1:8000/api/v1/health")
  ).json();
  expect(status.workspace_mode).toBe("demo");
  expect(status.mode).toBe("operations");
  const external: string[] = [];
  await page.route("**/*", (route) => {
    const url = new URL(route.request().url());
    if (url.hostname !== "127.0.0.1") {
      external.push(url.origin);
      return route.abort();
    }
    return route.continue();
  });
  await page.goto("/");
  await expect(
    page.getByText("DEMO SINTÉTICA · No subas datos reales"),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Riesgo y activos", exact: true })
    .click();
  await page
    .getByText("Riesgo avanzado de cartera y activos", { exact: true })
    .click();
  await expect(
    page.getByLabel("Activo para consultar evolución").locator("option"),
  ).not.toHaveCount(0);
  await expect(
    page.getByRole("img", {
      name: /Evolución del precio de valoración por activo/,
    }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Operaciones", exact: true }).click();
  await page.getByLabel("Exportaciones CSV").setInputFiles(fixtures);
  await page
    .getByLabel("Fecha de la exportación", { exact: true })
    .fill("2026-04-30");
  await execute(page, "Revisar subida", "uploads");
  await execute(page, "Revisar importación", "import");
  await execute(page, "Revisar actualización", "refresh");
  await page.getByRole("button", { name: "Informes", exact: true }).click();
  await page
    .getByLabel("Fecha de valoración (vacío: última disponible)", {
      exact: true,
    })
    .fill("2026-04-30");
  await execute(page, "Revisar informe", "report");
  await page.getByRole("button", { name: "Actualizar informes" }).click();
  await expect(
    page.getByLabel("Informe", { exact: true }).locator("option"),
  ).toHaveCount(2);
  await page.getByLabel("Informe", { exact: true }).selectOption({ index: 1 });
  await expect(page.getByLabel("Contenido del informe")).toContainText(
    "2026-04-30",
  );
  await page.getByRole("button", { name: "Aportaciones", exact: true }).click();
  await page
    .getByLabel("Fecha de valoración (vacío: última disponible)", {
      exact: true,
    })
    .fill("2026-04-30");
  await execute(page, "Revisar simulación", "simulation");
  await expect(page.getByText(/Caja residual:/)).toBeVisible();
  await page
    .getByRole("button", { name: "Configuración", exact: true })
    .click();
  await expect(page.getByLabel("Investment brief")).not.toHaveValue("");
  await page
    .getByLabel("Investment brief")
    .fill("Plan sintético E2E: horizonte largo, sin órdenes automáticas.");
  await execute(page, "Revisar plan", "brief");
  await page.getByRole("button", { name: /Recargar configuración/ }).click();
  await expect(
    page.getByRole("button", { name: "Revisar objetivos" }),
  ).toBeEnabled();
  await execute(page, "Revisar objetivos", "targets");
  await page.getByRole("button", { name: "Agentes", exact: true }).click();
  await page.getByText("Entradas avanzadas de esta ejecución").click();
  await expect(
    page.getByRole("combobox", { name: "Informe mensual", exact: true }).locator("option"),
  ).toHaveCount(2);
  await page
    .getByRole("combobox", { name: "Informe mensual", exact: true })
    .selectOption({ index: 1 });
  await page.getByLabel("Objetivos para esta ejecución").selectOption("custom");
  await page
    .getByLabel("Pesos personalizados (decimales, suma 1)")
    .fill('{"core_global_equity":0.8,"cash":0.2}');
  await execute(page, "Revisar ejecución de agentes", "agents");
  await page.getByRole("button", { name: "Actualizar historial" }).click();
  await expect(
    page.getByLabel("Ejecución de agentes").locator("option"),
  ).toHaveCount(2);
  await page.getByLabel("Ejecución de agentes").selectOption({ index: 1 });
  await expect(
    page.getByRole("region", { name: "Auditoría de agentes" }),
  ).toBeVisible();
  await page.getByText("Preflight: comprobaciones previas").click();
  await page.screenshot({
    path: "test-results/operations-demo.png",
    fullPage: true,
  });
  await page.reload();
  await page.getByRole("button", { name: "Operaciones", exact: true }).click();
  await page.getByRole("button", { name: "Ejecuciones", exact: true }).click();
  await expect(page.getByRole("button", { name: /Ver resultado/ })).toHaveCount(
    8,
  );
  expect(digest()).toEqual(before);
  expect(external).toEqual([]);
  await page.getByRole("button", { name: "Datos", exact: true }).click();
  await page.keyboard.press("Control+Home");
  await page.screenshot({
    path: "test-results/operations-desktop.png",
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(
    page.getByRole("button", { name: "Revisar importación" }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () =>
        document.documentElement.scrollWidth <=
        document.documentElement.clientWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: "test-results/operations-mobile.png",
    fullPage: true,
  });
});
