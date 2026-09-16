import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import { AgentsForm, DataForms } from "../operation-forms";
import { portfolioSchema } from "../contracts";
import fixture from "./synthetic-api.json";

it("passes run-only agent overrides without editing saved settings", async () => {
  const prepare = vi.fn();
  render(
    <AgentsForm
      mode="demo"
      disabled={false}
      prepare={prepare}
      reports={["monthly_test"]}
    />,
  );
  const user = userEvent.setup();
  await user.click(screen.getByText("Entradas avanzadas de esta ejecución"));
  await user.selectOptions(
    screen.getByLabelText("Informe mensual"),
    "monthly_test",
  );
  await user.selectOptions(
    screen.getByLabelText("Objetivos para esta ejecución"),
    "custom",
  );
  await user.clear(screen.getByLabelText(/Pesos personalizados \(decimales/));
  await user.paste('{"core":0.8,"cash":0.2}');
  await user.type(
    screen.getByLabelText(/Brief solo/),
    "Brief sintético temporal",
  );
  await user.click(screen.getByText("Revisar ejecución de agentes"));
  expect(prepare).toHaveBeenCalledWith(
    "agents",
    expect.objectContaining({
      report_id: "monthly_test",
      target_weights: { core: 0.8, cash: 0.2 },
      investment_brief_text: "Brief sintético temporal",
      llm_provider: "static",
      search_provider: "null",
    }),
    "Ejecutar agentes mensuales",
  );
});

it("rejects malformed target JSON before preparing a run", async () => {
  const prepare = vi.fn();
  render(<AgentsForm mode="demo" disabled={false} prepare={prepare} />);
  const user = userEvent.setup();
  await user.click(screen.getByText("Entradas avanzadas de esta ejecución"));
  await user.selectOptions(
    screen.getByLabelText("Objetivos para esta ejecución"),
    "custom",
  );
  await user.clear(screen.getByLabelText(/Pesos personalizados \(decimales/));
  await user.paste('{"core":NaN}');
  await user.click(screen.getByText("Revisar ejecución de agentes"));
  expect(prepare).not.toHaveBeenCalled();
  expect(screen.getByRole("alert")).toHaveTextContent("JSON");
});

it("keeps isolated FX controls in the confirmation payload", async () => {
  const prepare = vi.fn();
  render(<DataForms mode="demo" disabled={false} prepare={prepare} />);
  const user = userEvent.setup();
  await user.selectOptions(
    screen.getByLabelText("Actualizar", { exact: true }),
    "fx",
  );
  await user.click(
    screen.getByLabelText("Inferir FX solo para filas sin importe base"),
  );
  await user.click(screen.getByText("Revisar actualización"));
  expect(prepare).toHaveBeenCalledWith(
    "refresh",
    expect.objectContaining({ scope: "fx", only_missing_base: true }),
    "Actualizar FX y precios",
  );
});

it("validates asset evolution without replacing nulls or accepting invented values", () => {
  const asset = {
    asset_id: "a",
    asset_name: "Sintético",
    asset_type: "etf",
    series_kind: "valuation_price_proxy",
    points: [{ valuation_date: "2026-04-30", price_change: null }],
  };
  const data = { ...fixture.portfolio, asset_history: [asset] };
  expect(
    portfolioSchema.parse(data).asset_history[0]?.points[0]?.price_change,
  ).toBeNull();
  expect(
    portfolioSchema.safeParse({
      ...data,
      asset_history: [
        {
          ...asset,
          points: [{ valuation_date: "2026-04-30", price_change: Infinity }],
        },
      ],
    }).success,
  ).toBe(false);
});
