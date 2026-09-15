import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { App } from "../App";
import { api, ApiError } from "../api";
import {
  analyticsSchema,
  definitionsSchema,
  metricSchema,
  portfolioSchema,
} from "../contracts";
import { Explanation, LineChart, MetricCard } from "../components";
import { dateLabel, format } from "../format";
import fixture from "./synthetic-api.json";

function mockApi(data = structuredClone(fixture)) {
  const fetchMock = vi.fn(async (input: string) => {
    const value = input.includes("metric-definitions")
      ? data.definitions
      : input.includes("/portfolio/state")
        ? data.portfolio
        : data.analytics;
    return new Response(JSON.stringify(value), { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}
async function ready() {
  await screen.findByRole("heading", { name: "La fotografía actual" });
}

describe("contracts and formatting", () => {
  it("rejects invalid dates and currencies before rendering", () => {
    expect(
      portfolioSchema.safeParse({
        ...fixture.portfolio,
        as_of_date: "2026-02-31",
      }).success,
    ).toBe(false);
    expect(
      portfolioSchema.safeParse({
        ...fixture.portfolio,
        base_currency: "invalid",
      }).success,
    ).toBe(false);
  });
  it("accepts genuine synthetic FastAPI projections including partial metrics", () => {
    expect(analyticsSchema.parse(fixture.analytics).status).toBe("partial");
    expect(
      portfolioSchema.parse(fixture.portfolio).summary.total_market_value_base,
    ).toBe(9172.7);
    expect(
      definitionsSchema.parse(fixture.definitions).definitions.length,
    ).toBeGreaterThan(20);
  });
  it("formats EUR, dates, zero and unavailable without inventing values", () => {
    expect(format(null)).toBe("No disponible");
    expect(format(NaN)).toBe("No disponible");
    expect(format(0)).toMatch(/^0/);
    expect(format(-0.2)).toContain("-20");
    expect(format(1234.5, "money")).toContain("€");
    expect(dateLabel("2026-04-30")).toBe("30 abr 2026");
  });
  it("rejects malformed responses instead of trusting TypeScript casts", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response('{"data":42}')),
    );
    await expect(
      api.analytics("since_inception", "sp500", new AbortController().signal),
    ).rejects.toEqual(new ApiError("invalid_response"));
  });
  it("preserves HTTP error codes without displaying arbitrary server messages", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: { code: "workspace_busy", message: "private detail" },
          }),
          { status: 409 },
        ),
      ),
    );
    await expect(api.portfolio(new AbortController().signal)).rejects.toEqual(
      new ApiError("workspace_busy"),
    );
  });
});
describe("educational components", () => {
  it("does not render unavailable numbers as zero or as available values", () => {
    const metric = metricSchema.parse({
      ...fixture.analytics.data.performance.period.twr,
      value: 0.8,
      status: "unavailable",
      reason_code: "cash_flow_data_missing",
    });
    render(<MetricCard metric={metric} definitions={[]} />);
    expect(screen.getByText("No disponible")).toBeInTheDocument();
    expect(screen.queryByText("80 %")).not.toBeInTheDocument();
    expect(screen.getByText(/Faltan movimientos/)).toBeVisible();
  });
  it("focuses explanations and opens definition, interpretation and formula", async () => {
    const user = userEvent.setup();
    render(<Explanation definition={fixture.definitions.definitions[0]} />);
    await user.tab();
    expect(screen.getByText("Entender esta métrica")).toHaveFocus();
    // jsdom does not implement native summary activation via Enter; verified in browser.
    await user.click(screen.getByText("Entender esta métrica"));
    expect(
      screen.getByText(fixture.definitions.definitions[0]!.description),
    ).toBeVisible();
    await user.click(screen.getByText("Fórmula y condiciones"));
    expect(
      screen.getByText(fixture.definitions.definitions[0]!.formula),
    ).toBeVisible();
  });
  it("breaks lines on nulls and exposes exact chart data in an accessible table", async () => {
    const { container } = render(
      <LineChart
        title="Prueba"
        description="Sin interpolar"
        labels={["Cartera"]}
        unit="index"
        points={[
          { date: "2026-01-01", values: [100] },
          { date: "2026-01-02", values: [null] },
          { date: "2026-01-03", values: [110] },
        ]}
      />,
    );
    const path = container.querySelector("path")!.getAttribute("d")!;
    expect(path.match(/M/g)).toHaveLength(2);
    expect(path).not.toContain("L");
    await userEvent.click(screen.getByText(/Consultar datos/));
    expect(screen.getByRole("table")).toBeVisible();
    expect(screen.getByText("No disponible")).toBeVisible();
  });
  it("renders an empty chart without fabricated example curves", () => {
    render(
      <LineChart
        title="Vacío"
        description="Sin datos"
        labels={["Cartera"]}
        unit="index"
        points={[]}
      />,
    );
    expect(screen.queryByRole("img")).not.toBeInTheDocument();
    expect(screen.getByText(/No se generan curvas de ejemplo/)).toBeVisible();
  });
});
describe("read-only workspace", () => {
  it("shows loading then portfolio values and visible limitations", async () => {
    const fetchMock = mockApi();
    render(<App />);
    expect(screen.getByRole("status")).toBeInTheDocument();
    await ready();
    const card = screen
      .getByRole("heading", { name: "Valor de las posiciones" })
      .closest("article")!;
    expect(within(card).getByText("9172,70 €")).toBeVisible();
    expect(screen.getByText(/BENCHMARK SINTÉTICO/)).toBeVisible();
    expect(
      screen.getByLabelText("Calidad y límites de los datos"),
    ).toBeVisible();
    expect(
      fetchMock.mock.calls.some(([url]) =>
        url.includes("as_of_date=2026-04-30"),
      ),
    ).toBe(true);
    for (const call of fetchMock.mock.calls)
      expect(call[0]).toMatch(/^http:\/\/127.0.0.1:8000\/api\/v1\//);
  });
  it("navigates through performance and risk and exposes original API values", async () => {
    mockApi();
    const user = userEvent.setup();
    render(<App />);
    await ready();
    await user.click(screen.getByRole("button", { name: "Rentabilidad" }));
    expect(
      screen.getByRole("heading", {
        name: /Crecimiento de una inversión base/,
      }),
    ).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Riesgo y activos" }));
    expect(
      screen.getByRole("heading", { name: /Cómo se reparte/ }),
    ).toBeVisible();
    await user.selectOptions(screen.getByLabelText("Agrupar por"), "sector");
    expect(screen.getByText("Sin clasificar", { exact: true })).toBeVisible();
    expect(
      screen.getByRole("heading", { name: /Correlaciones/ }),
    ).toBeVisible();
  });
  it("changes filters on the server and supports all four benchmarks", async () => {
    const fetchMock = mockApi();
    const user = userEvent.setup();
    render(<App />);
    await ready();
    await user.selectOptions(
      screen.getByLabelText("Periodo de análisis"),
      "last_month",
    );
    await ready();
    for (const id of ["sp500", "portfolio_60_40", "estr_cash", "msci_world"]) {
      await user.selectOptions(
        screen.getByLabelText("Referencia de mercado"),
        id,
      );
      await ready();
      expect(
        fetchMock.mock.calls.some(([url]) =>
          url.includes(`period=last_month&benchmark_id=${id}`),
        ),
      ).toBe(true);
    }
  });
  it("never substitutes a missing real benchmark with synthetic curves", async () => {
    const data = structuredClone(fixture);
    const analytic = analyticsSchema.parse(data.analytics);
    analytic.data.benchmarks!.comparison = null;
    analytic.data.benchmarks!.reason_code = "benchmark_provider_unavailable";
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async (url: string) =>
          new Response(
            JSON.stringify(
              url.includes("metric-definitions")
                ? data.definitions
                : url.includes("/portfolio/state")
                  ? data.portfolio
                  : analytic,
            ),
          ),
      ),
    );
    render(<App />);
    await ready();
    await userEvent.click(screen.getByRole("button", { name: "Rentabilidad" }));
    expect(screen.getByText(/fuente del benchmark real/)).toBeVisible();
    expect(screen.queryByRole("img")).not.toBeInTheDocument();
    expect(screen.queryByText(/BENCHMARK SINTÉTICO/)).not.toBeInTheDocument();
  });
  it("keeps analytics usable if the separate portfolio request fails", async () => {
    mockApi();
    vi.spyOn(api, "portfolio").mockRejectedValue(
      new ApiError("workspace_busy"),
    );
    render(<App />);
    await ready();
    expect(screen.getByRole("alert")).toHaveTextContent("operación en curso");
    expect(screen.getByText(/102,65/)).toBeVisible();
  });
  it("handles empty data and reconnects only when requested", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("offline")));
    const user = userEvent.setup();
    render(<App />);
    await ready();
    expect(screen.getAllByRole("alert").length).toBeGreaterThan(0);
    mockApi();
    await user.click(
      screen.getByRole("button", { name: /Actualizar lectura/ }),
    );
    await ready();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
  it("hides contribution totals if cash-flow exports are missing", async () => {
    const data = structuredClone(fixture);
    data.analytics.warnings.push("cash_flow_data_missing");
    mockApi(data);
    render(<App />);
    await ready();
    const card = screen
      .getByRole("heading", { name: "Aportaciones netas acumuladas" })
      .closest("article")!;
    expect(within(card).getByText("No disponible")).toBeVisible();
  });
  it("ignores late responses from an obsolete filter", async () => {
    mockApi();
    const actual = analyticsSchema.parse(fixture.analytics);
    let resolveOld!: (value: typeof actual) => void;
    vi.spyOn(api, "analytics")
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveOld = resolve;
          }),
      )
      .mockResolvedValue(actual);
    render(<App />);
    await userEvent.selectOptions(
      screen.getByLabelText("Periodo de análisis"),
      "last_month",
    );
    await ready();
    const stale = structuredClone(actual);
    stale.warnings.push("obsolete_response");
    await act(async () => resolveOld(stale));
    await waitFor(() =>
      expect(screen.queryByText(/obsolete_response/)).not.toBeInTheDocument(),
    );
  });
});
