import { useEffect, useState, type ReactNode } from "react";
import { api, ApiError } from "./api";
import type { Analytics, Definition, Period, Portfolio } from "./contracts";
import { Explanation, LineChart, MetricCard, Notices } from "./components";
import { dateLabel, format, reason } from "./format";

type View = "Resumen" | "Rentabilidad" | "Riesgo y activos";
type Loaded = {
  analytics?: Analytics;
  portfolio?: Portfolio;
  definitions: Definition[];
  errors: string[];
};
const benchmarks = [
  ["msci_world", "MSCI World"],
  ["sp500", "S&P 500"],
  ["portfolio_60_40", "Cartera 60/40"],
  ["estr_cash", "Efectivo · €STR"],
];
const errorCode = (error: unknown) =>
  error instanceof ApiError ? error.code : "request_failed";

export function App() {
  const [view, setView] = useState<View>("Resumen");
  const [period, setPeriod] = useState<Period>("since_inception");
  const [benchmark, setBenchmark] = useState("msci_world");
  const [reload, setReload] = useState(0);
  const [loaded, setLoaded] = useState<Loaded>();
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
      setLoaded({ definitions: [], errors: ["request_timeout"] });
      setLoading(false);
    }, 60000);
    setLoading(true);
    setLoaded(undefined);
    async function load() {
      const [analytics, definitions] = await Promise.allSettled([
        api.analytics(period, benchmark, controller.signal),
        api.definitions(controller.signal),
      ]);
      if (controller.signal.aborted) return;
      // Anchor the portfolio snapshot to the analytics end date, never mix dates silently.
      const portfolio = await api
        .portfolio(
          controller.signal,
          analytics.status === "fulfilled"
            ? analytics.value.period.end_date
            : undefined,
        )
        .then(
          (value) => ({ value, error: undefined }),
          (error) => ({ value: undefined, error: errorCode(error) }),
        );
      if (controller.signal.aborted) return;
      clearTimeout(timeout);
      setLoaded({
        analytics:
          analytics.status === "fulfilled" ? analytics.value : undefined,
        portfolio: portfolio.value,
        definitions:
          definitions.status === "fulfilled"
            ? definitions.value.definitions
            : [],
        errors: [
          analytics.status === "rejected"
            ? errorCode(analytics.reason)
            : undefined,
          definitions.status === "rejected"
            ? "definitions_unavailable"
            : undefined,
          portfolio.error,
        ].filter((code): code is string => !!code),
      });
      setLoading(false);
    }
    void load();
    // Abort also discards obsolete filter responses; no automatic retries or persistent cache.
    return () => {
      clearTimeout(timeout);
      controller.abort();
    };
  }, [period, benchmark, reload]);
  const analytics = loaded?.analytics,
    portfolio = loaded?.portfolio,
    definitions = loaded?.definitions ?? [];
  const comparison = analytics?.data.benchmarks?.comparison?.comparisons.find(
    (item) => item.benchmark_id === benchmark,
  );
  const warnings = [
    ...(analytics?.warnings ?? []),
    ...(portfolio?.data_quality.warnings ?? []),
  ];
  const synthetic = comparison?.provider_name === "synthetic";
  return (
    <div className="app-shell">
      <a className="skip-link" href="#content">
        Saltar al contenido
      </a>
      <aside className="sidebar">
        <a
          className="brand"
          href="#"
          aria-label="ML Finance inicio"
          onClick={() => setView("Resumen")}
        >
          <span className="brand-mark">
            M<span>↗</span>
          </span>
          <span>
            ML<span className="brand-light"> Finance</span>
            <small>PORTFOLIO WORKSPACE</small>
          </span>
        </a>
        <p className="nav-label">TU CARTERA</p>
        <nav aria-label="Navegación principal">
          {(["Resumen", "Rentabilidad", "Riesgo y activos"] as View[]).map(
            (item, index) => (
              <button
                key={item}
                aria-current={view === item ? "page" : undefined}
                onClick={() => setView(item)}
              >
                <span aria-hidden="true">{["◫", "↗", "◈"][index]}</span>
                {item}
              </button>
            ),
          )}
        </nav>
        <div className="sidebar-note">
          <span className="status-dot" /> Entorno local
          <p>
            Tu análisis, en tu equipo.
            <br />
            Sin ejecución de órdenes.
          </p>
          <span className="version">UI v2 · Solo lectura</span>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <span>
            Workspace <span className="slash">/</span> <strong>{view}</strong>
          </span>
          <span className="local-badge">● LOCAL FIRST</span>
        </header>
        <main id="content" tabIndex={-1}>
          <div className="page-heading">
            <div>
              <p className="eyebrow">PERSPECTIVA, NO PREDICCIONES</p>
              <h1>
                {view === "Resumen"
                  ? "Tu cartera, de un vistazo"
                  : view === "Rentabilidad"
                    ? "Pon tu evolución en contexto"
                    : "Entiende dónde está el riesgo"}
              </h1>
              <p className="subtitle">
                {view === "Resumen"
                  ? "Separa lo que aportas de lo que genera tu inversión."
                  : view === "Rentabilidad"
                    ? "Rentabilidad, aportaciones y referencias. Cada cifra, con sus límites."
                    : "Concentración y comportamiento histórico de tus activos."}
              </p>
            </div>
            <button
              className="refresh"
              disabled={loading}
              onClick={() => setReload((value) => value + 1)}
            >
              ↻ Actualizar lectura
            </button>
          </div>
          <div className="toolbar">
            <label>
              Periodo de análisis
              <select
                value={period}
                onChange={(event) => setPeriod(event.target.value as Period)}
              >
                <option value="since_inception">Desde el inicio</option>
                <option value="last_year">Último año</option>
                <option value="last_quarter">Último trimestre</option>
                <option value="last_month">Último mes</option>
              </select>
            </label>
            <label>
              Referencia de mercado
              <select
                value={benchmark}
                onChange={(event) => setBenchmark(event.target.value)}
              >
                {(
                  analytics?.data.benchmarks?.catalog.map((item) => [
                    item.benchmark_id,
                    item.name,
                  ]) ?? benchmarks
                ).map(([id, label]) => (
                  <option key={id} value={id}>
                    {label}
                  </option>
                ))}
              </select>
            </label>
            <div className="period-note">
              Periodo efectivo
              <strong>
                {analytics
                  ? `${dateLabel(analytics.period.actual_start)} — ${dateLabel(analytics.period.end_date)}`
                  : "Pendiente de lectura"}
              </strong>
            </div>
          </div>
          {loading ? (
            <section className="loading" role="status">
              <span className="loading-dot" />
              <h2>Leyendo tu cartera…</h2>
              <p>
                Calculando en la API local. No se consultan proveedores
                externos.
              </p>
            </section>
          ) : (
            <>
              {[...new Set(loaded?.errors)].map((code) => (
                <div className="error" role="alert" key={code}>
                  {reason(code)} <small>({code})</small>
                </div>
              ))}
              {synthetic && (
                <div className="demo-banner">
                  BENCHMARK SINTÉTICO · La referencia de mercado es ficticia. No
                  representa resultados reales.
                </div>
              )}
              {analytics?.status !== "available" && (
                <p className="context-note">
                  {analytics?.status === "partial"
                    ? "Lectura parcial"
                    : "Analítica no disponible"}
                  . Consulta los límites junto a los resultados.
                </p>
              )}
              {view === "Resumen" && (
                <Overview
                  portfolio={portfolio}
                  analytics={analytics}
                  definitions={definitions}
                  notices={<Notices codes={warnings} />}
                />
              )}
              {view === "Rentabilidad" && (
                <Performance
                  analytics={analytics}
                  benchmark={benchmark}
                  definitions={definitions}
                  notices={<Notices codes={warnings} />}
                />
              )}
              {view === "Riesgo y activos" && (
                <Risk
                  analytics={analytics}
                  portfolio={portfolio}
                  definitions={definitions}
                  notices={<Notices codes={warnings} />}
                />
              )}
            </>
          )}
          <footer>
            <span>ML Finance · Analítica local de cartera</span>
            <span>
              Datos históricos, no asesoramiento financiero. Revisa antes de
              decidir.
            </span>
          </footer>
        </main>
      </div>
    </div>
  );
}

function Overview({
  portfolio,
  analytics,
  definitions,
  notices,
}: {
  portfolio?: Portfolio;
  analytics?: Analytics;
  definitions: Definition[];
  notices: ReactNode;
}) {
  const currency =
    portfolio?.base_currency ?? analytics?.base_currency ?? "EUR";
  const flowsMissing = analytics?.warnings.includes("cash_flow_data_missing");
  const positions = portfolio?.positions ?? [];
  return (
    <>
      <div className="section-caption">
        <h2>La fotografía actual</h2>
        <span>
          Valoración: {dateLabel(portfolio?.as_of_date)} · {currency}
        </span>
      </div>
      <div className="summary-grid">
        <article className="summary-card featured">
          <h3>Valor de las posiciones</h3>
          <strong>
            {format(
              portfolio?.summary.total_market_value_base,
              "money",
              currency,
            )}
          </strong>
          <p>Valoración de activos; no equivale a saldo de efectivo.</p>
          <span className="coverage">
            Cobertura {format(portfolio?.summary.valuation_coverage_ratio)}
          </span>
        </article>
        <article className="summary-card">
          <h3>Aportaciones netas acumuladas</h3>
          <strong>
            {format(
              flowsMissing
                ? null
                : portfolio?.summary.net_external_contributions_base,
              "money",
              currency,
            )}
          </strong>
          <p>
            Depósitos menos retiradas hasta la fecha de valoración. No son
            ganancias.
          </p>
        </article>
        <article className="summary-card">
          <h3>Ganancia / pérdida no realizada</h3>
          <strong>
            {format(
              portfolio?.summary.total_unrealized_pnl_base,
              "money",
              currency,
            )}
          </strong>
          <p>
            Valor frente al coste de las posiciones abiertas. No es el resultado
            total ni el TWR.
          </p>
        </article>
      </div>
      {notices}
      <div className="two-columns">
        <MetricCard
          metric={analytics?.data.performance?.period.twr}
          label="Cómo ha rendido la cartera · TWR"
          definitions={definitions}
        />
        <article className="panel insight">
          <span className="eyebrow">PARA TU REVISIÓN MENSUAL</span>
          <h2>
            Primero los datos.
            <br />
            Después, la decisión.
          </h2>
          <p>
            Comprueba la cobertura, las posiciones con mayor peso y la
            rentabilidad ajustada por aportaciones. Una subida del valor no
            significa necesariamente una ganancia.
          </p>
          <p className="muted">
            Esta vista no genera recomendaciones ni ejecuta agentes.
          </p>
        </article>
      </div>
      <LineChart
        title="Evolución del valor de tus posiciones"
        description="Incluye el efecto de compras y ventas. No es una curva de rentabilidad."
        points={(portfolio?.history ?? [])
          .filter(
            (row) =>
              !analytics?.period.actual_start ||
              row.valuation_date >= analytics.period.actual_start,
          )
          .map((row) => ({
            date: row.valuation_date,
            values: [row.total_market_value_base],
          }))}
        labels={["Valor de posiciones"]}
        unit="money"
        currency={currency}
      />
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Posiciones con mayor peso</h2>
            <p>
              Pesos sobre el valor conocido. Sin desglose de los activos
              internos de ETFs.
            </p>
          </div>
        </div>
        <Positions positions={positions.slice(0, 5)} currency={currency} />
      </section>
    </>
  );
}
function Performance({
  analytics,
  benchmark,
  definitions,
  notices,
}: {
  analytics?: Analytics;
  benchmark: string;
  definitions: Definition[];
  notices: ReactNode;
}) {
  const performance = analytics?.data.performance?.period;
  const section = analytics?.data.benchmarks;
  const comparison = section?.comparison?.comparisons.find(
    (item) => item.benchmark_id === benchmark,
  );
  const catalog = section?.catalog.find(
    (item) => item.benchmark_id === benchmark,
  );
  return (
    <>
      <div className="two-columns">
        <MetricCard
          metric={performance?.twr}
          label="Rendimiento sin el efecto de aportar · TWR acumulado"
          definitions={definitions}
        />
        <MetricCard
          metric={performance?.mwr}
          label="Rendimiento de tu dinero · MWR anualizado"
          definitions={definitions}
        />
      </div>
      {notices}
      <p className="context-note">
        TWR acumulado y MWR anualizado no son directamente comparables. Flujos
        externos netos en el periodo:{" "}
        {format(
          analytics?.warnings.includes("cash_flow_data_missing")
            ? null
            : performance?.net_external_flow_base,
          "money",
          analytics?.base_currency,
        )}
        .
      </p>
      <section className="panel benchmark-intro">
        <span className="eyebrow">REFERENCIA, NO RECOMENDACIÓN</span>
        <h2>{catalog?.name ?? "Benchmark seleccionado"}</h2>
        <p>{catalog?.description ?? "Sin metadatos de la referencia."}</p>
        {comparison ? (
          <>
            <p>
              {dateLabel(comparison.period_start)} —{" "}
              {dateLabel(comparison.period_end)} · {comparison.observations}{" "}
              pares · Cobertura {format(comparison.coverage_ratio)}
            </p>
            <p className="muted">
              Fuente: {comparison.source_reference} · Proveedor:{" "}
              {comparison.provider_name} · Serie: {comparison.series_kind} ·
              Moneda: {comparison.source_currency} → {comparison.base_currency}
            </p>
            <Notices codes={comparison.reason_codes} />
            {comparison.sources?.map((source) => (
              <div key={source.source_id} className="source-provenance">
                <p>
                  {source.is_proxy ? "Aproximación ETF" : "Fuente oficial"} ·{" "}
                  {source.source_id} · {source.currency}
                  <br />
                  Datos disponibles: {dateLabel(
                    source.first_observation,
                  )} — {dateLabel(source.last_observation)}
                  <br />
                  Descarga (UTC): {source.fetched_at}
                </p>
                <details>
                  <summary>Procedencia y huella de los datos</summary>
                  <p>{source.reference}</p>
                  <code>{source.content_sha256}</code>
                </details>
              </div>
            ))}
          </>
        ) : (
          <p className="metric-warning">
            {reason(section?.reason_code ?? "analytics_unavailable")}
          </p>
        )}
      </section>
      <LineChart
        title="Crecimiento de una inversión base"
        description="Índices calculados por el servidor, con las mismas fechas y moneda. No son importes de tu cartera."
        points={(comparison?.growth ?? []).map((row) => ({
          date: row.observation_date,
          values: [row.portfolio_index, row.benchmark_index],
        }))}
        labels={["Cartera", comparison?.benchmark_name ?? "Benchmark"]}
        unit="index"
      />
      <LineChart
        title="Caídas desde máximos · Drawdown"
        description="Compara las caídas del índice de rentabilidad, no las retiradas de dinero."
        points={(comparison?.growth ?? []).map((row) => ({
          date: row.observation_date,
          values: [row.portfolio_drawdown, row.benchmark_drawdown],
        }))}
        labels={["Cartera", comparison?.benchmark_name ?? "Benchmark"]}
        unit="decimal"
      />
      <details className="advanced">
        <summary>Métricas comparativas avanzadas</summary>
        <div className="metric-grid">
          {comparison?.metrics.map((metric) => (
            <MetricCard
              key={metric.metric_id}
              metric={metric}
              definitions={definitions}
            />
          ))}
        </div>
        {!comparison && <p>No hay una comparación disponible.</p>}
      </details>
    </>
  );
}
function Positions({
  positions,
  currency,
}: {
  positions: Portfolio["positions"];
  currency: string;
}) {
  return positions.length ? (
    <div
      className="table-scroll"
      tabIndex={0}
      role="region"
      aria-label="Posiciones de cartera"
    >
      <table>
        <caption className="sr-only">
          Posiciones valoradas en {currency}
        </caption>
        <thead>
          <tr>
            <th scope="col">Activo</th>
            <th scope="col">Valoración</th>
            <th scope="col">Peso</th>
            <th scope="col">Pérdida / ganancia abierta</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((position) => (
            <tr key={position.asset_id}>
              <th scope="row">
                {position.asset_name ?? position.asset_id}
                <small>
                  {position.asset_id} ·{" "}
                  {position.valuation_status ?? "Estado desconocido"}
                </small>
              </th>
              <td>{format(position.market_value_base, "money", currency)}</td>
              <td>
                <div className="weight-cell">
                  <span>{format(position.weight)}</span>
                  <span className="weight-track">
                    <i
                      style={{
                        width: `${Math.max(0, Math.min(1, position.weight ?? 0)) * 100}%`,
                      }}
                    />
                  </span>
                </div>
              </td>
              <td>{format(position.unrealized_pnl_base, "money", currency)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ) : (
    <p className="empty">No hay posiciones disponibles.</p>
  );
}
function Risk({
  analytics,
  portfolio,
  definitions,
  notices,
}: {
  analytics?: Analytics;
  portfolio?: Portfolio;
  definitions: Definition[];
  notices: ReactNode;
}) {
  const risk = analytics?.data.risk;
  const [dimension, setDimension] = useState("asset_id");
  const labels: Record<string, string> = {
    asset_id: "Activo",
    bucket: "Grupo de inversión",
    currency: "Moneda",
    asset_type: "Tipo",
    sector: "Sector",
  };
  return (
    <>
      <div className="metric-grid">
        {(risk?.portfolio.metrics ?? [])
          .filter((metric) =>
            [
              "volatility_annualized",
              "max_drawdown",
              "drawdown_duration_days",
            ].includes(metric.metric_id),
          )
          .map((metric) => (
            <MetricCard
              key={metric.metric_id}
              metric={metric}
              definitions={definitions}
            />
          ))}
      </div>
      {notices}
      {!risk && <p className="empty">No hay analítica de riesgo disponible.</p>}
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Cómo se reparte tu exposición</h2>
            <p>
              Sin look-through de ETFs. Las clasificaciones ausentes no se
              infieren.
            </p>
          </div>
          <label>
            Agrupar por
            <select
              value={dimension}
              onChange={(event) => setDimension(event.target.value)}
            >
              {Object.entries(labels).map(([id, label]) => (
                <option key={id} value={id}>
                  {label}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="exposure-list">
          {risk?.positions.concentration.groups
            .filter((group) => group.dimension === dimension)
            .map((group) => (
              <div className="exposure" key={group.group}>
                <span>
                  {group.group === "unclassified"
                    ? "Sin clasificar"
                    : group.group}
                </span>
                <span className="exposure-track">
                  <i
                    style={{
                      width: `${Math.max(0, Math.min(1, group.weight.status === "unavailable" ? 0 : (group.weight.value ?? 0))) * 100}%`,
                    }}
                  />
                </span>
                <strong>
                  {format(
                    group.weight.status === "unavailable"
                      ? null
                      : group.weight.value,
                  )}
                </strong>
                <small>
                  Cobertura {format(group.weight.coverage_ratio)}
                  {group.weight.reason_code !== "ok" &&
                    ` · ${reason(group.weight.reason_code)}`}
                </small>
              </div>
            ))}
        </div>
        <Explanation
          definition={definitions.find(
            (item) => item.metric_id === "exposure_weight",
          )}
        />
        {risk?.positions.concentration.hhi_by_dimension[dimension] && (
          <MetricCard
            metric={risk.positions.concentration.hhi_by_dimension[dimension]}
            label={`Concentración HHI · ${labels[dimension]}`}
            definitions={definitions}
          />
        )}
      </section>
      <section className="panel">
        <h2>Activos de la cartera</h2>
        <Positions
          positions={portfolio?.positions ?? []}
          currency={portfolio?.base_currency ?? "EUR"}
        />
      </section>
      <details className="advanced">
        <summary>Riesgo avanzado de cartera y activos</summary>
        <p>
          Sharpe y Sortino no se calculan sin tasa explícita. Esta primera UI no
          configura esa tasa.
        </p>
        <div className="metric-grid">
          {risk?.portfolio.metrics
            ?.filter(
              (metric) =>
                ![
                  "volatility_annualized",
                  "max_drawdown",
                  "drawdown_duration_days",
                ].includes(metric.metric_id),
            )
            .map((metric) => (
              <MetricCard
                key={metric.metric_id}
                metric={metric}
                definitions={definitions}
              />
            ))}
        </div>
        <p className="metric-warning">
          Series por activo: {risk?.positions.series_kind ?? "No disponibles"}.
          Precios de valoración aproximados; no retornos totales.
        </p>
        {Object.entries(risk?.positions.asset_risk ?? {}).map(([id, asset]) => (
          <details key={id}>
            <summary>
              {portfolio?.positions.find((position) => position.asset_id === id)
                ?.asset_name ?? id}
            </summary>
            <div className="metric-grid">
              {asset.metrics?.map((metric) => (
                <MetricCard
                  key={metric.metric_id}
                  metric={metric}
                  definitions={definitions}
                />
              ))}
            </div>
          </details>
        ))}
      </details>
      <section className="panel">
        <h2>Cómo se mueven juntos · Correlaciones</h2>
        <p>
          Entre −1 y 1. No demuestra causalidad ni garantiza diversificación
          futura.
        </p>
        <p className="metric-warning">
          {risk?.positions.diversification
            ? "Cada pareja puede tener fechas y cobertura diferentes. Series aproximadas de precios de valoración."
            : reason(
                risk?.positions.diversification_reason_code ??
                  "analytics_unavailable",
              )}
        </p>
        <div className="metric-grid">
          {risk?.positions.diversification?.correlations.map((pair) => (
            <MetricCard
              key={`${pair.left_asset_id}-${pair.right_asset_id}`}
              metric={pair.metric}
              label={`${pair.left_asset_id} / ${pair.right_asset_id}`}
              definitions={definitions}
            />
          ))}
        </div>
        <details>
          <summary>Contribución aproximada al riesgo</summary>
          <p>Pesos actuales fijos; no es una atribución histórica exacta.</p>
          <div className="metric-grid">
            {Object.entries(
              risk?.positions.diversification?.risk_contributions ?? {},
            ).map(([id, metric]) => (
              <MetricCard
                key={id}
                metric={metric}
                label={id}
                definitions={definitions}
              />
            ))}
          </div>
        </details>
      </section>
    </>
  );
}
