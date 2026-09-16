import { useState, type FormEvent } from "react";
import { z } from "zod";
import {
  csvUploads,
  type Brief,
  type Health,
  type Operation,
  type Payload,
  type Targets,
} from "./operations-api";
import { operationError } from "./operation-results";

export type Prepare = (
  operation: Operation,
  payload: Payload,
  title: string,
  retryId?: string,
) => void;
type Props = {
  mode: Health["workspace_mode"];
  disabled: boolean;
  prepare: Prepare;
};
const values = (event: FormEvent<HTMLFormElement>) => {
  event.preventDefault();
  return new FormData(event.currentTarget);
};
const text = (data: FormData, key: string) => String(data.get(key) ?? "");
const optionalDate = (data: FormData) => ({
  as_of_date: text(data, "as_of_date") || null,
});
export function DateField({
  name = "as_of_date",
  label = "Fecha de valoración (vacío: última disponible)",
  required = false,
}: {
  name?: string;
  label?: string;
  required?: boolean;
}) {
  return (
    <label>
      {label}
      <input type="date" name={name} required={required} />
    </label>
  );
}
export function DataForms({ mode, disabled, prepare }: Props) {
  const [error, setError] = useState("");
  const [reading, setReading] = useState(false);
  return (
    <div className="operations-grid">
      <section className="panel">
        <h2>1. Subir exportaciones DEGIRO</h2>
        {mode === "demo" && (
          <p className="metric-warning">
            Usa exclusivamente CSV ficticios. El modo demo no anonimiza archivos
            reales.
          </p>
        )}
        <p>
          Transacciones, estado de cuenta y cartera. Subir no importa: revisa el
          resultado y continúa con el paso 2. Máximo 5 CSV, 5 MiB por archivo y
          10 MiB en total.
        </p>
        <form
          onSubmit={async (event) => {
            const data = values(event);
            setError("");
            setReading(true);
            try {
              const uploads = await csvUploads(
                data
                  .getAll("files")
                  .filter(
                    (item): item is File => item instanceof File && !!item.name,
                  ),
              );
              prepare(
                "uploads",
                { uploads, uploaded_at: text(data, "uploaded_at") },
                "Guardar CSV seleccionados",
              );
            } catch {
              setError("invalid_uploads");
            } finally {
              setReading(false);
            }
          }}
        >
          <fieldset disabled={disabled || reading}>
            <label>
              Exportaciones CSV
              <input name="files" type="file" accept=".csv" multiple required />
            </label>
            <DateField
              name="uploaded_at"
              label="Fecha de la exportación"
              required
            />
            <button>Revisar subida</button>
          </fieldset>
        </form>
        {error && <p role="alert">{operationError(error)}</p>}
      </section>
      <section className="panel">
        <h2>2. Importar los archivos guardados</h2>
        <p>
          Valida los CSV y actualiza el almacén local del entorno indicado. No
          modifica las exportaciones originales.
        </p>
        <button
          disabled={disabled || reading}
          onClick={() => prepare("import", {}, "Importar DEGIRO")}
        >
          Revisar importación
        </button>
      </section>
      <section className="panel">
        <h2>3. Actualizar FX y precios</h2>
        <p>
          {mode === "demo"
            ? "Conserva los datos sintéticos, sin red."
            : "Consulta Yahoo Finance. Puede modificar las valoraciones derivadas; no sustituye el precio absoluto del broker."}
        </p>
        <form
          onSubmit={(event) => {
            const data = values(event);
            const provider = text(data, "provider");
            prepare(
              "refresh",
              {
                start_date: text(data, "start_date") || null,
                end_date: text(data, "end_date") || null,
                fx_provider: provider,
                price_provider: provider,
                scope: text(data, "scope"),
                only_missing_base: data.has("only_missing_base"),
              },
              "Actualizar FX y precios",
            );
          }}
        >
          <fieldset disabled={disabled}>
            <label>
              Fuente de precios y FX
              <select
                name="provider"
                required
                defaultValue={mode === "demo" ? "synthetic" : ""}
              >
                {mode === "demo" ? (
                  <option value="synthetic">Sintética · sin red</option>
                ) : (
                  <>
                    <option value="">Selecciona explícitamente</option>
                    <option value="yfinance">
                      Yahoo Finance · red externa
                    </option>
                  </>
                )}
              </select>
            </label>
            <DateField name="start_date" label="Desde (opcional)" />
            <DateField name="end_date" label="Hasta (opcional)" />
            <label>
              Actualizar
              <select name="scope" defaultValue="both">
                <option value="both">FX y precios</option>
                <option value="fx">Solo FX</option>
                <option value="prices">Solo precios</option>
              </select>
            </label>
            <label className="check-label">
              <input type="checkbox" name="only_missing_base" />
              Inferir FX solo para filas sin importe base
            </label>
            <button>Revisar actualización</button>
          </fieldset>
        </form>
      </section>
      <section className="panel">
        <h2>4. Actualizar benchmarks</h2>
        <p>
          MSCI World, S&P 500 y 60/40 mediante proxies ETF; €STR del BCE.
          Reemplaza toda la ventana descargada: incluye el historial que
          necesites.
        </p>
        {mode === "demo" ? (
          <p>
            La demo utiliza referencias sintéticas. No permite descargas reales.
          </p>
        ) : (
          <form
            onSubmit={(event) => {
              const data = values(event);
              prepare(
                "benchmarks",
                {
                  provider: text(data, "provider"),
                  start_date: text(data, "start_date"),
                  end_date: text(data, "end_date"),
                },
                "Descargar benchmarks reales",
              );
            }}
          >
            <fieldset disabled={disabled}>
              <label>
                Fuente de benchmarks
                <select name="provider" required defaultValue="">
                  <option value="">Selecciona explícitamente</option>
                  <option value="yfinance_ecb">
                    Yahoo Finance + BCE · red externa
                  </option>
                </select>
              </label>
              <DateField
                name="start_date"
                label="Inicio del histórico"
                required
              />
              <DateField
                name="end_date"
                label="Fin del histórico (anterior a hoy)"
                required
              />
              <button>Revisar benchmarks</button>
            </fieldset>
          </form>
        )}
      </section>
    </div>
  );
}
export function SimulationForm({ disabled, prepare }: Props) {
  return (
    <section className="panel">
      <h2>Laboratorio de aportación</h2>
      <p>
        Explora una aportación usando tu cartera y objetivos guardados. Solo
        propone compras de posiciones actuales; no vende ni ejecuta órdenes.
      </p>
      <form
        onSubmit={(event) => {
          const data = values(event);
          prepare(
            "simulation",
            {
              ...optionalDate(data),
              contribution_amount: Number(data.get("amount")),
              allow_fractional_units: data.has("fractional"),
              minimum_order_value: Number(data.get("minimum")),
              max_orders: Number(data.get("orders")),
            },
            "Simular aportación",
          );
        }}
      >
        <fieldset disabled={disabled}>
          <div className="operations-grid">
            <label>
              Aportación (moneda base)
              <input
                type="number"
                name="amount"
                min="0"
                step="0.01"
                defaultValue="600"
                required
              />
            </label>
            <label>
              Importe mínimo por compra
              <input
                type="number"
                name="minimum"
                min="0"
                step="0.01"
                defaultValue="0"
                required
              />
            </label>
            <label>
              Máximo de compras
              <input
                type="number"
                name="orders"
                min="1"
                max="100"
                step="1"
                defaultValue="4"
                required
              />
            </label>
            <DateField />
          </div>
          <label className="check-label">
            <input name="fractional" type="checkbox" />
            Permitir fracciones (supuesto de simulación, no disponibilidad del
            broker)
          </label>
          <button>Revisar simulación</button>
        </fieldset>
      </form>
    </section>
  );
}
export function ReportForm({ disabled, prepare }: Props) {
  return (
    <section className="panel">
      <h2>Generar informe mensual</h2>
      <p>Guarda un nuevo informe para revisión. No ejecuta agentes.</p>
      <form
        onSubmit={(event) =>
          prepare(
            "report",
            optionalDate(values(event)),
            "Generar informe mensual",
          )
        }
      >
        <fieldset disabled={disabled}>
          <DateField />
          <button>Revisar informe</button>
        </fieldset>
      </form>
    </section>
  );
}
export function AgentsForm({
  mode,
  disabled,
  prepare,
  reports = [],
}: Props & { reports?: string[] }) {
  const [error, setError] = useState("");
  return (
    <section className="panel">
      <h2>Revisión mensual con agentes</h2>
      <p>
        Primero comprueba la calidad y las fechas de cartera e informe. Si falla
        el preflight, bloquea los proveedores y registra el intento.
      </p>
      <p>
        Static/null no usa IA externa. OpenAI o búsqueda externa pueden enviar
        contexto fuera del equipo y generar costes. Credenciales solo en el
        servidor; nunca se piden aquí.
      </p>
      <form
        onSubmit={(event) => {
          const data = values(event);
          setError("");
          let weights: Record<string, number> | null = null;
          try {
            if (text(data, "weights_mode") === "custom") {
              weights = z
                .record(z.string().min(1), z.number().finite().min(0).max(1))
                .parse(JSON.parse(text(data, "weights")));
            }
          } catch {
            setError(
              "Los pesos deben ser un objeto JSON con decimales entre 0 y 1; el servidor valida la suma.",
            );
            return;
          }
          prepare(
            "agents",
            {
              llm_provider: text(data, "llm"),
              search_provider: text(data, "search"),
              monthly_budget: text(data, "budget")
                ? Number(data.get("budget"))
                : null,
              user_satellite_interest: text(data, "interest") || null,
              report_id: text(data, "report_id") || null,
              target_weights: weights,
              investment_brief_text: text(data, "run_brief") || null,
            },
            "Ejecutar agentes mensuales",
          );
        }}
      >
        <fieldset disabled={disabled}>
          <div className="operations-grid">
            <label>
              Proveedor del modelo
              <select name="llm" defaultValue="static">
                <option value="static">
                  Static · respuesta determinista sin red
                </option>
                {mode === "real" && (
                  <option value="openai">OpenAI · externo</option>
                )}
              </select>
            </label>
            <label>
              Proveedor de búsqueda
              <select name="search" defaultValue="null">
                <option value="null">Sin búsqueda externa</option>
                <option value="static">Fixtures sintéticas</option>
                {mode === "real" && (
                  <>
                    <option value="tavily">Tavily · externo</option>
                    <option value="duckduckgo">DuckDuckGo · externo</option>
                  </>
                )}
              </select>
            </label>
            <label>
              Presupuesto de aportación (opcional)
              <input name="budget" type="number" min="0" step="0.01" />
            </label>
            <label>
              Interés satélite (opcional)
              <input name="interest" maxLength={2000} />
            </label>
          </div>
          <details>
            <summary>Entradas avanzadas de esta ejecución</summary>
            <p>
              Sin cambios se usan el último informe y la configuración guardada
              al ejecutar. Las métricas y la analítica siempre se construyen en
              el servidor; las fechas se validan antes de llamar a proveedores.
              Revisa el informe en Informes y la cartera en Resumen.
            </p>
            <label>
              Informe mensual
              <select name="report_id" defaultValue="">
                <option value="">Último informe disponible</option>
                {reports.map((id) => (
                  <option key={id} value={id}>
                    {id}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Objetivos para esta ejecución
              <select name="weights_mode" defaultValue="saved">
                <option value="saved">Objetivos guardados</option>
                <option value="custom">Pesos personalizados JSON</option>
              </select>
            </label>
            <label>
              Pesos personalizados (decimales, suma 1)
              <textarea name="weights" rows={3} defaultValue="{}" />
            </label>
            <label>
              Brief solo para esta ejecución (vacío: guardado)
              <textarea name="run_brief" rows={4} maxLength={65536} />
            </label>
            <p>
              Estas entradas no sobrescriben la configuración y quedan
              registradas en la auditoría privada.
            </p>
          </details>
          <button>Revisar ejecución de agentes</button>
        </fieldset>
      </form>
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
export function SettingsForms({
  disabled,
  prepare,
  brief,
  targets,
  stale,
}: Props & { brief?: Brief; targets?: Targets; stale: boolean }) {
  const [error, setError] = useState("");
  return (
    <div className="operations-grid">
      <section className="panel">
        <h2>Tu plan de inversión</h2>
        <p>
          Explica horizonte, necesidades y restricciones para contextualizar la
          revisión mensual. No incluyas credenciales.
        </p>
        <form
          key={brief?.content_hash ?? "empty"}
          onSubmit={(event) => {
            const data = values(event);
            if (brief)
              prepare(
                "brief",
                {
                  content: text(data, "brief"),
                  expected_previous_hash: brief.content_hash,
                },
                "Guardar plan de inversión",
              );
          }}
        >
          <fieldset disabled={disabled || stale || !brief}>
            <label>
              Investment brief
              <textarea
                name="brief"
                rows={12}
                maxLength={65536}
                defaultValue={brief?.content ?? ""}
              />
            </label>
            <button>Revisar plan</button>
          </fieldset>
        </form>
      </section>
      <section className="panel">
        <h2>Objetivos de cartera</h2>
        <p>
          Editor avanzado de objeto JSON: conserva moneda, pesos, límites y
          mapping exacto de cada activo a su categoría. El servidor valida el
          contrato completo. No admite YAML.
        </p>
        {targets?.validation_error && (
          <p role="alert">{operationError(targets.validation_error)}</p>
        )}
        <form
          key={targets?.content_hash ?? "empty"}
          onSubmit={(event) => {
            const data = values(event);
            setError("");
            try {
              const parsed = z
                .record(z.string(), z.json())
                .parse(JSON.parse(text(data, "targets")));
              if (targets)
                prepare(
                  "targets",
                  {
                    portfolio_targets: parsed,
                    expected_previous_hash: targets.content_hash,
                  },
                  "Guardar objetivos de cartera",
                );
            } catch {
              setError("invalid_targets_json");
            }
          }}
        >
          <fieldset disabled={disabled || stale || !targets}>
            <label>
              Objetivos JSON
              <textarea
                name="targets"
                rows={18}
                defaultValue={JSON.stringify(
                  targets?.portfolio_targets ?? {},
                  null,
                  2,
                )}
                required
              />
            </label>
            <button>Revisar objetivos</button>
          </fieldset>
        </form>
        {error && <p role="alert">{operationError(error)}</p>}
      </section>
      {stale && (
        <p role="alert">
          Recarga y revisa la configuración antes de volver a guardar. Tu
          borrador no se sobrescribe automáticamente.
        </p>
      )}
    </div>
  );
}
