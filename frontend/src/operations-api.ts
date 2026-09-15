import { z } from "zod";
import { get } from "./api";

export const healthSchema = z.object({
  status: z.literal("ok"),
  api_version: z.literal("v1"),
  mode: z.enum(["read_only", "operations"]),
  workspace_mode: z.enum(["demo", "real"]),
});
const object = z.record(z.string(), z.json());
const hash = z.string().regex(/^sha256:[a-f0-9]{64}$/);
export const jobSchema = z.object({
  job_id: z.string().regex(/^[a-f0-9]{32}$/),
  operation: z.string(),
  state: z.enum(["pending", "running", "succeeded", "partial", "failed"]),
  progress: z.number().min(0).max(100),
  phase: z.string(),
  created_at: z.string(),
  started_at: z.string().nullable(),
  finished_at: z.string().nullable(),
  warnings: z.array(z.string()),
  error_code: z.string().nullable(),
  result: object.nullable(),
});
export const briefSchema = z.object({
  content: z.string(),
  exists: z.boolean(),
  content_hash: hash,
});
export const targetsSchema = z.object({
  portfolio_targets: object.nullable(),
  target_weights: z.record(z.string(), z.number()),
  exists: z.boolean(),
  content_hash: hash,
  validation_error: z.string().nullable(),
});
export const auditSchema = z.object({
  run_id: z.string(),
  schema_version: z.number(),
  is_legacy: z.boolean(),
  compatibility_warnings: z.array(z.string()),
  run_metadata: object,
  input_payload: object,
  preflight: object,
  agents: z.record(z.string(), object),
});
export type Health = z.infer<typeof healthSchema>;
export type Job = z.infer<typeof jobSchema>;
export type Audit = z.infer<typeof auditSchema>;
export type Brief = z.infer<typeof briefSchema>;
export type Targets = z.infer<typeof targetsSchema>;
export type Payload = z.infer<typeof object>;
export const operationRoutes = {
  uploads: ["POST", "/degiro/uploads"],
  import: ["POST", "/degiro/import"],
  refresh: ["POST", "/market-data/refresh"],
  benchmarks: ["POST", "/benchmarks/refresh"],
  report: ["POST", "/reports/monthly"],
  simulation: ["POST", "/portfolio/contributions/simulate"],
  agents: ["POST", "/agents/monthly-runs"],
  brief: ["PUT", "/settings/investment-brief"],
  targets: ["PUT", "/settings/portfolio-targets"],
} as const;
export type Operation = keyof typeof operationRoutes;
export const opsApi = {
  health: (signal: AbortSignal) => get("/health", healthSchema, signal),
  jobs: (signal: AbortSignal) =>
    get("/jobs?limit=100", z.object({ jobs: z.array(jobSchema) }), signal),
  brief: (signal: AbortSignal) =>
    get("/settings/investment-brief", briefSchema, signal),
  targets: (signal: AbortSignal) =>
    get("/settings/portfolio-targets", targetsSchema, signal),
  reports: (signal: AbortSignal) =>
    get(
      "/reports?limit=100",
      z.object({ reports: z.array(z.object({ report_id: z.string() })) }),
      signal,
    ),
  report: (id: string, signal: AbortSignal) =>
    get(
      `/reports/${encodeURIComponent(id)}`,
      z.object({ report_id: z.string(), content_markdown: z.string() }),
      signal,
    ),
  runs: (signal: AbortSignal) =>
    get(
      "/agents/runs?limit=100",
      z.object({
        runs: z.array(
          z.object({
            run_id: z.string(),
            as_of_date: z.string().nullable(),
            generated_at: z.string().nullable(),
            status: z.string(),
            agent_statuses: z.record(z.string(), z.string()),
          }),
        ),
      }),
      signal,
    ),
  audit: (id: string, signal: AbortSignal) =>
    get(`/agents/runs/${encodeURIComponent(id)}`, auditSchema, signal),
  send: (
    operation: Operation,
    payload: Payload,
    key: string,
    signal: AbortSignal,
    retryId?: string,
  ) => {
    const [method, path] = retryId
      ? ["POST", `/jobs/${encodeURIComponent(retryId)}/retry`]
      : operationRoutes[operation];
    return get(path, jobSchema, signal, {
      method,
      headers: {
        "Content-Type": "application/json",
        "X-ML-Finance-Confirm": "local-write",
        "Idempotency-Key": key,
      },
      body: JSON.stringify(payload),
    });
  },
};

// Only bounded CSV bytes are read in the browser, never local paths or configuration.
export async function csvUploads(files: File[]) {
  if (
    !files.length ||
    files.length > 5 ||
    files.some((f) => !/\.csv$/i.test(f.name) || f.size > 5 * 1024 * 1024) ||
    files.reduce((size, file) => size + file.size, 0) > 10 * 1024 * 1024
  )
    throw new Error("invalid_uploads");
  return Promise.all(
    files.map(async (file) => {
      const bytes = new Uint8Array(await file.arrayBuffer());
      let binary = "";
      for (let i = 0; i < bytes.length; i += 8192)
        binary += String.fromCharCode(...bytes.subarray(i, i + 8192));
      return { filename: file.name, content_base64: btoa(binary) };
    }),
  );
}
