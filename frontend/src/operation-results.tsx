import type { Audit, Job, Payload } from "./operations-api";
import { z } from "zod";
import { format } from "./format";

// Content from reports and providers is text, never executable HTML or remote media.
export function JsonDetails({
  label,
  value,
}: {
  label: string;
  value: unknown;
}) {
  return (
    <details>
      <summary>{label}</summary>
      <pre className="artifact-text">{JSON.stringify(value, null, 2)}</pre>
    </details>
  );
}
export function AuditView({ audit }: { audit: Audit }) {
  return (
    <section className="panel" aria-label="Auditoría de agentes">
      <h2>Auditoría · {audit.run_id}</h2>
      <p>
        Schema {audit.schema_version} ·{" "}
        {audit.is_legacy
          ? "Legacy: algunos campos no estaban disponibles"
          : "Auditoría reproducible"}
        . Las respuestas de IA son apoyo, no instrucciones de inversión.
      </p>
      {audit.compatibility_warnings.map((code) => (
        <p key={code}>{code}</p>
      ))}
      <JsonDetails
        label="Preflight: comprobaciones previas"
        value={audit.preflight}
      />
      <JsonDetails
        label="Metadatos y hashes de la ejecución"
        value={audit.run_metadata}
      />
      <JsonDetails label="Inputs de la ejecución" value={audit.input_payload} />
      {Object.entries(audit.agents).map(([name, data]) => (
        <details key={name}>
          <summary>{name}</summary>
          {Object.entries(data).map(([key, value]) => (
            <JsonDetails
              key={key}
              label={auditLabels[key] ?? key}
              value={value}
            />
          ))}
        </details>
      ))}
    </section>
  );
}
const auditLabels: Record<string, string> = {
  context: "Contexto: plan, acciones y fuentes",
  request: "Request efectiva",
  prompt_refs: "Prompts: referencias",
  prompt_rendered: "Prompt renderizado",
  provider: "Proveedor y modelo",
  raw_response: "Respuesta original",
  parsed_output: "Output estructurado",
  audit_metadata: "Hashes y metadata de auditoría",
};

export const jobLabels: Record<string, string> = {
  pending: "En cola",
  running: "En curso",
  succeeded: "Completado",
  partial: "Completado con avisos",
  failed: "Fallido",
};
export function JobResult({ job }: { job: Job }) {
  return (
    <section className="panel" aria-label="Resultado de operación">
      <h2>
        {jobLabels[job.state]} · {job.operation}
      </h2>
      <p>
        {job.job_id} · {job.phase}
      </p>
      <progress
        aria-label="Progreso por etapas"
        value={job.progress}
        max={100}
      />
      <p>{job.progress}% · Progreso por etapas, no estimación de tiempo.</p>
      {job.error_code && <p role="alert">{operationError(job.error_code)}</p>}
      {job.warnings.map((code) => (
        <p className="metric-warning" key={code}>
          {code}
        </p>
      ))}
      {job.result && (
        <>
          <p>
            {typeof job.result.message === "string"
              ? job.result.message
              : "Resultado disponible"}
          </p>
          {job.operation === "simulation" && (
            <SimulationResult result={job.result} />
          )}
          <JsonDetails
            label="Resultado completo y artefactos"
            value={job.result}
          />
        </>
      )}
    </section>
  );
}
// Validate the financial subset before presenting it. Unknown legacy results remain inspectable as JSON.
const simulationSchema = z.object({
  base_currency: z.string().regex(/^[A-Z]{3}$/),
  budget: z.number(),
  invested_amount: z.number(),
  remaining_cash: z.number(),
  orders: z.array(
    z.object({
      asset_id: z.string(),
      quantity: z.number(),
      amount_base: z.number(),
      weight_before: z.number(),
      weight_after: z.number(),
    }),
  ),
  bucket_allocations: z.array(
    z.object({
      bucket: z.string(),
      target_weight: z.number(),
      weight_before: z.number(),
      weight_after: z.number(),
    }),
  ),
});
function SimulationResult({ result }: { result: Payload }) {
  const parsed = simulationSchema.safeParse(result.simulation);
  if (!parsed.success)
    return (
      <p>
        Detalle de simulación no disponible en este formato. Consulta el
        resultado completo.
      </p>
    );
  const data = parsed.data;
  return (
    <div>
      <p>
        Solo compras propuestas de posiciones actuales. No se venden activos ni
        se ejecutan órdenes. La caja residual queda separada de los pesos
        posteriores. No incluye comisiones, impuestos ni deslizamiento.
      </p>
      <p>
        Presupuesto: {format(data.budget, "money", data.base_currency)} · A
        invertir: {format(data.invested_amount, "money", data.base_currency)} ·
        Caja residual:{" "}
        {format(data.remaining_cash, "money", data.base_currency)}
      </p>
      <div className="table-scroll">
        <table>
          <caption>Compras propuestas</caption>
          <thead>
            <tr>
              <th>Activo</th>
              <th>Unidades</th>
              <th>Importe</th>
              <th>Peso antes</th>
              <th>Peso después</th>
            </tr>
          </thead>
          <tbody>
            {data.orders.map((order) => (
              <tr key={order.asset_id}>
                <td>{order.asset_id}</td>
                <td>{order.quantity}</td>
                <td>
                  {format(order.amount_base, "money", data.base_currency)}
                </td>
                <td>{format(order.weight_before)}</td>
                <td>{format(order.weight_after)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!data.orders.length && (
        <p>No se proponen compras con estas restricciones.</p>
      )}
      <div className="table-scroll">
        <table>
          <caption>Distribución por categoría</caption>
          <thead>
            <tr>
              <th>Categoría</th>
              <th>Objetivo</th>
              <th>Antes</th>
              <th>Después</th>
            </tr>
          </thead>
          <tbody>
            {data.bucket_allocations.map((item) => (
              <tr key={item.bucket}>
                <td>{item.bucket}</td>
                <td>{format(item.target_weight)}</td>
                <td>{format(item.weight_before)}</td>
                <td>{format(item.weight_after)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
export function operationError(code: string) {
  const messages: Record<string, string> = {
    content_conflict:
      "El contenido cambió. Recarga y revisa el brief o los objetivos antes de guardar; no se ha sobrescrito.",
    workspace_busy: "Hay una operación en curso. Espera y vuelve a consultar.",
    workspace_mode_mismatch:
      "El entorno del servidor cambió. Recarga la aplicación antes de operar.",
    worker_interrupted:
      "La ejecución se interrumpió. Revisa sus posibles efectos; no se reejecutará automáticamente.",
    worker_stopped: "El worker se detuvo. No se reejecutará automáticamente.",
    invalid_request: "Revisa fechas, importes y campos obligatorios.",
    invalid_portfolio_targets:
      "Los objetivos no cumplen el contrato. Revisa pesos, límites y mapping exacto de activos.",
    invalid_uploads:
      "Selecciona entre 1 y 5 CSV, máximo 5 MiB cada uno y 10 MiB en total.",
    invalid_targets_json:
      "Introduce un objeto JSON válido para los objetivos (no YAML).",
    connection_failed: "No se pudo contactar con la API local.",
    request_timeout:
      "La consulta agotó su tiempo. Puedes consultar de nuevo sin repetir la operación.",
    invalid_response:
      "La API devolvió un contrato inesperado. No se interpreta como resultado financiero.",
    read_only:
      "Servidor en modo de solo lectura. Activa operaciones al arrancar la API.",
  };
  return (
    messages[code] ??
    `Operación no completada (${code}). Revisa el resultado antes de repetirla.`
  );
}
