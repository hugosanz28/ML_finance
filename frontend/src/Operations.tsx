import { useEffect, useRef, useState } from "react";
import { ApiError } from "./api";
import {
  opsApi,
  type Audit,
  type Brief,
  type Health,
  type Job,
  type Operation,
  type Payload,
  type Targets,
} from "./operations-api";
import {
  AgentsForm,
  DataForms,
  ReportForm,
  SettingsForms,
  SimulationForm,
  type Prepare,
} from "./operation-forms";
import {
  AuditView,
  JobResult,
  JsonDetails,
  jobLabels,
  operationError,
} from "./operation-results";

const sections = [
  "Datos",
  "Aportaciones",
  "Informes",
  "Agentes",
  "Configuración",
  "Ejecuciones",
] as const;
type Section = (typeof sections)[number];
type Pending = {
  operation: Operation;
  payload: Payload;
  title: string;
  key: string;
  retryId?: string;
  uncertain: boolean;
};
const codeOf = (error: unknown) =>
  error instanceof ApiError ? error.code : "request_failed";
const active = (job: Job) => job.state === "pending" || job.state === "running";

export function Operations({
  health,
  visible,
}: {
  health?: Health;
  visible: boolean;
}) {
  const [section, setSection] = useState<Section>("Datos");
  const [pending, setPending] = useState<Pending>();
  const [sending, setSending] = useState(false);
  const sendingRef = useRef(false);
  const confirmationRef = useRef<HTMLElement>(null);
  const [error, setError] = useState("");
  const [jobs, setJobs] = useState<Job[]>([]);
  const [poll, setPoll] = useState(0);
  const [selected, setSelected] = useState<string>();
  const [jobError, setJobError] = useState("");
  const [revision, setRevision] = useState(0);
  const [readError, setReadError] = useState("");
  const [reading, setReading] = useState(false);
  const [brief, setBrief] = useState<Brief>();
  const [targets, setTargets] = useState<Targets>();
  const [stale, setStale] = useState(false);
  const [reports, setReports] = useState<string[]>([]);
  const [runs, setRuns] = useState<
    Awaited<ReturnType<typeof opsApi.runs>>["runs"]
  >([]);
  const [artifact, setArtifact] = useState("");
  const [report, setReport] = useState<string>();
  const [audit, setAudit] = useState<Audit>();
  const writable = health?.mode === "operations";
  const mode = health?.workspace_mode ?? "real";
  useEffect(() => {
    if (pending) confirmationRef.current?.focus();
  }, [pending]);

  useEffect(() => {
    if (!writable) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function refresh() {
      try {
        const response = await opsApi.jobs(
          AbortSignal.any([controller.signal, AbortSignal.timeout(15000)]),
        );
        if (controller.signal.aborted) return;
        setJobs(response.jobs);
        setJobError("");
        // Poll reads only while there is work. Never resubmit a job automatically.
        if (response.jobs.some(active)) timer = setTimeout(refresh, 1500);
      } catch (error) {
        if (!controller.signal.aborted) setJobError(codeOf(error));
      }
    }
    void refresh();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [writable, mode, poll]);

  useEffect(() => {
    if (
      !visible ||
      !health ||
      !["Informes", "Agentes", "Configuración"].includes(section)
    )
      return;
    const controller = new AbortController();
    setReading(true);
    setReadError("");
    setReport(undefined);
    setAudit(undefined);
    const signal = AbortSignal.any([
      controller.signal,
      AbortSignal.timeout(15000),
    ]);
    async function read() {
      try {
        if (section === "Configuración" && writable) {
          // The server serializes data reads with writes. Do not race two reads for that lock.
          const nextBrief = await opsApi.brief(signal);
          const nextTargets = await opsApi.targets(signal);
          if (controller.signal.aborted) return;
          setBrief(nextBrief);
          setTargets(nextTargets);
          setStale(false);
        } else if (section === "Informes") {
          const next = await opsApi.reports(signal);
          const detail = artifact
            ? await opsApi.report(artifact, signal)
            : null;
          if (controller.signal.aborted) return;
          setReports(next.reports.map((item) => item.report_id));
          setReport(detail?.content_markdown);
        } else if (section === "Agentes") {
          const next = await opsApi.runs(signal);
          const detail = artifact ? await opsApi.audit(artifact, signal) : null;
          if (controller.signal.aborted) return;
          setRuns(next.runs);
          setAudit(detail ?? undefined);
        }
      } catch (error) {
        if (!controller.signal.aborted) setReadError(codeOf(error));
      } finally {
        if (!controller.signal.aborted) setReading(false);
      }
    }
    void read();
    return () => controller.abort();
  }, [visible, health, writable, section, revision, artifact]);

  const prepare: Prepare = (operation, payload, title, retryId) => {
    if (!writable || pending || sendingRef.current) return;
    // Capture immutable parameters and their key before confirmation; later edits cannot alter this request.
    setPending({
      operation,
      payload: {
        ...structuredClone(payload),
        workspace_mode: mode,
        confirm: true,
      },
      title,
      key: crypto.randomUUID(),
      retryId,
      uncertain: false,
    });
    setError("");
  };
  async function send() {
    if (!pending || sendingRef.current) return;
    sendingRef.current = true;
    setSending(true);
    setError("");
    let submitted = false;
    try {
      const current = await opsApi.health(AbortSignal.timeout(15000));
      if (
        current.mode !== "operations" ||
        current.workspace_mode !== pending.payload.workspace_mode
      )
        throw new ApiError("workspace_mode_mismatch");
      submitted = true;
      const job = await opsApi.send(
        pending.operation,
        pending.payload,
        pending.key,
        AbortSignal.timeout(30000),
        pending.retryId,
      );
      setJobs((previous) => [
        job,
        ...previous.filter((item) => item.job_id !== job.job_id),
      ]);
      setSelected(job.job_id);
      setPending(undefined);
      setPoll((value) => value + 1);
      if (["brief", "targets"].includes(pending.operation)) setStale(true);
    } catch (error) {
      const code = codeOf(error);
      // A lost/invalid response after POST does not prove failure: preserve the SAME body/key.
      if (
        submitted &&
        [
          "connection_failed",
          "invalid_response",
          "request_failed",
          "internal_error",
        ].includes(code)
      ) {
        setPending({ ...pending, uncertain: true });
        setError(
          "Envío sin confirmar. Puede estar ejecutándose. Consulta con la misma clave; no crees otra ejecución ni recargues esta página.",
        );
      } else {
        setError(operationError(code));
        if (!pending.uncertain) setPending(undefined);
      }
    } finally {
      sendingRef.current = false;
      setSending(false);
    }
  }
  const selectedJob = jobs.find((job) => job.job_id === selected);
  const disabled = !writable || !!pending || sending || jobs.some(active);
  const props = { mode, disabled, prepare };
  return (
    <div hidden={!visible} className="operations">
      <p>
        Tu revisión mensual, paso a paso. No ejecuta órdenes. No uses Streamlit
        ni CLI para escribir mientras el worker esté activo.
      </p>
      {!writable && (
        <div className="demo-banner">
          {health
            ? "Solo lectura. Para habilitar acciones, arranca la API con --operations demo o --operations real según tu entorno."
            : "No se ha verificado el entorno. Escrituras bloqueadas."}
        </div>
      )}
      <nav className="operation-nav" aria-label="Flujo mensual">
        {sections.map((item) => (
          <button
            key={item}
            aria-current={section === item ? "page" : undefined}
            onClick={() => {
              setSection(item);
              setArtifact("");
            }}
          >
            {item}
          </button>
        ))}
      </nav>
      {pending && (
        <section
          ref={confirmationRef}
          tabIndex={-1}
          className="panel confirmation"
          role="region"
          aria-label="Confirmar operación"
        >
          <h2>{pending.title}</h2>
          <p>
            Destino:{" "}
            <strong>
              {pending.payload.workspace_mode === "demo"
                ? "DEMO SINTÉTICA"
                : "DATOS REALES"}
            </strong>
            . Revisa antes de confirmar.
          </p>
          <p>
            Fuentes externas seleccionadas pueden usar red y, en agentes, enviar
            contexto a proveedores con costes. No hay reejecuciones automáticas.
          </p>
          <JsonDetails
            label="Parámetros exactos del envío"
            value={{
              ...pending.payload,
              ...(pending.operation === "uploads"
                ? {
                    uploads: (pending.payload.uploads as Payload[]).map(
                      (item) => ({
                        filename: item.filename,
                        content:
                          "CSV seleccionado; bytes omitidos en esta vista",
                      }),
                    ),
                  }
                : {}),
            }}
          />
          <p>Clave de envío: {pending.key}</p>
          <button disabled={sending} onClick={() => void send()}>
            {sending
              ? "Enviando…"
              : pending.uncertain
                ? "Consultar envío con la misma clave"
                : "Confirmar y ejecutar"}
          </button>
          {!pending.uncertain && (
            <button disabled={sending} onClick={() => setPending(undefined)}>
              Cancelar
            </button>
          )}
        </section>
      )}
      {error && (
        <p role="alert" className="error">
          {error}
        </p>
      )}
      {writable && (
        <div className="job-summary">
          <span>{jobs.filter(active).length} ejecuciones activas</span>
          <button onClick={() => setPoll((value) => value + 1)}>
            Actualizar ejecuciones
          </button>
          {jobError && <p role="alert">{operationError(jobError)}</p>}
        </div>
      )}
      {selectedJob && <JobResult job={selectedJob} />}
      {section === "Datos" && <DataForms {...props} />}
      {section === "Aportaciones" && <SimulationForm {...props} />}
      {section === "Informes" && (
        <>
          <ReportForm {...props} />
          <section className="panel">
            <h2>Informes guardados</h2>
            <button
              disabled={reading}
              onClick={() => setRevision((value) => value + 1)}
            >
              Actualizar informes
            </button>
            <label>
              Informe
              <select
                aria-label="Informe"
                value={artifact}
                onChange={(event) => setArtifact(event.target.value)}
              >
                <option value="">Selecciona un informe</option>
                {reports.map((id) => (
                  <option key={id}>{id}</option>
                ))}
              </select>
            </label>
            {!reports.length && !reading && <p>No hay informes disponibles.</p>}
            {report !== undefined && (
              <pre className="artifact-text" aria-label="Contenido del informe">
                {report}
              </pre>
            )}
          </section>
        </>
      )}
      {section === "Agentes" && (
        <>
          <AgentsForm {...props} />
          <section className="panel">
            <h2>Historial de agentes</h2>
            <button
              disabled={reading}
              onClick={() => setRevision((value) => value + 1)}
            >
              Actualizar historial
            </button>
            <label>
              Ejecución de agentes
              <select
                aria-label="Ejecución de agentes"
                value={artifact}
                onChange={(event) => setArtifact(event.target.value)}
              >
                <option value="">Selecciona una ejecución</option>
                {runs.map((run) => (
                  <option key={run.run_id} value={run.run_id}>
                    {run.as_of_date ?? "Sin fecha"} · {run.status} ·{" "}
                    {run.run_id}
                  </option>
                ))}
              </select>
            </label>
            {!runs.length && !reading && <p>No hay ejecuciones guardadas.</p>}
          </section>
          {audit && <AuditView audit={audit} />}
        </>
      )}
      {section === "Configuración" && (
        <>
          <p>
            Guardar exige el hash de la versión leída. Si cambió, recarga y
            revisa antes de reenviar.
          </p>
          <button
            disabled={!writable || reading || !!pending || jobs.some(active)}
            onClick={() => setRevision((value) => value + 1)}
          >
            Recargar configuración (descarta borrador)
          </button>
          <SettingsForms
            key={revision}
            {...props}
            disabled={disabled || reading || !!readError}
            brief={brief}
            targets={targets}
            stale={stale}
          />
        </>
      )}
      {reading && <p role="status">Leyendo…</p>}
      {readError && <p role="alert">{operationError(readError)}</p>}
      {section === "Ejecuciones" && (
        <section className="panel">
          <h2>Últimas ejecuciones (hasta 100)</h2>
          <p>
            Al reiniciar se conservan los resultados, no se repiten las acciones
            interrumpidas.
          </p>
          {!jobs.length && <p>No hay ejecuciones disponibles.</p>}
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Operación</th>
                  <th>Estado</th>
                  <th>Fecha</th>
                  <th>Acciones</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.job_id}>
                    <td>{job.operation}</td>
                    <td>{jobLabels[job.state]}</td>
                    <td>{job.created_at}</td>
                    <td>
                      <button onClick={() => setSelected(job.job_id)}>
                        Ver resultado {job.job_id.slice(0, 6)}
                      </button>
                      {job.operation === "simulation" &&
                        job.state === "failed" && (
                          <button
                            disabled={disabled}
                            onClick={() =>
                              prepare(
                                "simulation",
                                {},
                                "Reintentar simulación con el estado actual",
                                job.job_id,
                              )
                            }
                          >
                            Reintentar simulación
                          </button>
                        )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}
