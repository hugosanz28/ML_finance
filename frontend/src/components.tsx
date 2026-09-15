import { useId } from "react";
import type { Definition, Metric } from "./contracts";
import { dateLabel, format, reason } from "./format";

export function Explanation({ definition }: { definition?: Definition }) {
  if (!definition)
    return <p className="muted">Explicación del catálogo no disponible.</p>;
  return (
    <details className="explanation">
      <summary>Entender esta métrica</summary>
      <p>{definition.description}</p>
      <p>{definition.interpretation}</p>
      <p>
        <strong>Límites.</strong> {definition.limitations}
      </p>
      <details>
        <summary>Fórmula y condiciones</summary>
        <code>{definition.formula}</code>
        <p>{definition.required_data}</p>
        <p>{definition.validity_conditions}</p>
      </details>
    </details>
  );
}
export function MetricCard({
  metric,
  label,
  definitions,
}: {
  metric?: Metric;
  label?: string;
  definitions: Definition[];
}) {
  const definition = definitions.find(
    (item) => item.metric_id === metric?.metric_id,
  );
  const available = metric && metric.status !== "unavailable";
  return (
    <article className="metric-card">
      <h3>{label ?? definition?.name ?? metric?.metric_id ?? "Métrica"}</h3>
      <div className="metric-value">
        {format(available ? metric.value : null, metric?.unit)}
      </div>
      {metric && (
        <p className="metric-meta">
          {metric.observations} observaciones · Cobertura{" "}
          {format(metric.coverage_ratio)}
        </p>
      )}
      {metric &&
        (metric.reason_code !== "ok" || metric.status !== "available") && (
          <p className="metric-warning">
            {metric.status === "partial" ? "Parcial. " : ""}
            {reason(
              metric.reason_code === "ok"
                ? "incomplete_coverage"
                : metric.reason_code,
            )}
          </p>
        )}
      <Explanation definition={definition} />
    </article>
  );
}
export function Notices({ codes }: { codes: string[] }) {
  if (!codes.length) return null;
  return (
    <aside className="notices" aria-label="Calidad y límites de los datos">
      <strong>Antes de interpretar los resultados</strong>
      <ul>
        {[...new Set(codes)].map((code) => (
          <li key={code}>
            {reason(code)} <small>({code})</small>
          </li>
        ))}
      </ul>
    </aside>
  );
}
export type ChartPoint = { date: string; values: (number | null)[] };
// Only geometry and formatting live here. All plotted values come from the API.
export function LineChart({
  title,
  description,
  points,
  labels,
  unit,
  currency,
}: {
  title: string;
  description: string;
  points: ChartPoint[];
  labels: string[];
  unit: string;
  currency?: string;
}) {
  const id = useId();
  const values = points.flatMap((point) =>
    point.values.filter(
      (value): value is number => value !== null && Number.isFinite(value),
    ),
  );
  const low = Math.min(...values),
    high = Math.max(...values);
  const padding = Math.max(
    Math.abs(low) * 0.05,
    unit.startsWith("decimal") ? 0.01 : 1,
  );
  const minimum = low === high ? low - padding : low;
  const range = (high === low ? high + padding : high) - minimum;
  const start = Date.parse(points[0]?.date ?? ""),
    end = Date.parse(points.at(-1)?.date ?? "");
  const x = (date: string) =>
    82 + ((Date.parse(date) - start) / (end - start || 1)) * 760;
  const y = (value: number) => 224 - ((value - minimum) / range) * 190;
  const ticks = [0, 0.5, 1];
  return (
    <section className="panel chart-panel" aria-labelledby={id}>
      <div className="panel-heading">
        <div>
          <h2 id={id}>{title}</h2>
          <p>{description}</p>
        </div>
        <span className="eyebrow">HISTÓRICO</span>
      </div>
      <div className="legend">
        {labels.map((label, index) => (
          <span key={label}>
            <i className={`series-${index}`} />
            {label}
          </span>
        ))}
      </div>
      {values.length ? (
        <div
          className="chart-viewport"
          tabIndex={0}
          role="region"
          aria-label={`Gráfico desplazable: ${title}`}
        >
          <svg
            viewBox="0 0 870 268"
            role="img"
            aria-label={`${title}. Valores y fechas disponibles en la tabla desplegable.`}
          >
            {ticks.map((tick) => (
              <g key={tick}>
                <line
                  x1="82"
                  x2="842"
                  y1={34 + tick * 190}
                  y2={34 + tick * 190}
                  className="grid-line"
                />
                <text x="72" y={39 + tick * 190} textAnchor="end">
                  {format(minimum + range * (1 - tick), unit, currency)}
                </text>
              </g>
            ))}
            {labels.map((label, index) => {
              let drawing = false;
              const path = points
                .map((point) => {
                  const value = point.values[index];
                  // Nulls break the line; no interpolation across missing observations.
                  if (value == null) {
                    drawing = false;
                    return "";
                  }
                  const command = drawing ? "L" : "M";
                  drawing = true;
                  return `${command}${x(point.date)},${y(value)}`;
                })
                .join(" ");
              return (
                <g key={label}>
                  <path d={path} className={`chart-line series-${index}`} />
                  {points.length === 1 && points[0]?.values[index] != null && (
                    <circle
                      cx={x(points[0].date)}
                      cy={y(points[0].values[index])}
                      r="4"
                      className={`series-${index}`}
                    />
                  )}
                </g>
              );
            })}
            <text x="82" y="256">
              {dateLabel(points[0]?.date)}
            </text>
            <text x="842" y="256" textAnchor="end">
              {dateLabel(points.at(-1)?.date)}
            </text>
          </svg>
        </div>
      ) : (
        <p className="empty">
          No hay una serie disponible para este periodo. No se generan curvas de
          ejemplo.
        </p>
      )}
      {points.length > 0 && (
        <details className="chart-data">
          <summary>Consultar datos del gráfico ({points.length})</summary>
          <div
            className="table-scroll"
            tabIndex={0}
            role="region"
            aria-label={`Datos: ${title}`}
          >
            <table>
              <caption>{title}</caption>
              <thead>
                <tr>
                  <th scope="col">Fecha</th>
                  {labels.map((label) => (
                    <th scope="col" key={label}>
                      {label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {points.map((point) => (
                  <tr key={point.date}>
                    <th scope="row">{dateLabel(point.date)}</th>
                    {labels.map((label, index) => (
                      <td key={label}>
                        {format(point.values[index], unit, currency)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      )}
    </section>
  );
}
