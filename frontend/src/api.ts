import { z } from "zod";
import {
  analyticsSchema,
  definitionsSchema,
  portfolioSchema,
  type Period,
} from "./contracts";

const base = "http://127.0.0.1:8000/api/v1";
export class ApiError extends Error {
  constructor(public readonly code: string) {
    super(code);
  }
}
// Runtime validation prevents a changed/malformed API response becoming financial data.
async function get<T>(
  path: string,
  schema: z.ZodType<T>,
  signal: AbortSignal,
): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${base}${path}`, {
      signal,
      cache: "no-store",
      credentials: "omit",
      redirect: "error",
    });
  } catch (error) {
    if (signal.aborted) throw error;
    throw new ApiError("connection_failed");
  }
  if (!response.ok) {
    const error = z
      .object({ error: z.object({ code: z.string() }) })
      .safeParse(await response.json().catch(() => null));
    throw new ApiError(
      error.success ? error.data.error.code : "request_failed",
    );
  }
  const result = schema.safeParse(await response.json().catch(() => null));
  if (!result.success) throw new ApiError("invalid_response");
  return result.data;
}
export const api = {
  definitions: (signal: AbortSignal) =>
    get("/analytics/metric-definitions", definitionsSchema, signal),
  portfolio: (signal: AbortSignal, date?: string | null) =>
    get(
      `/portfolio/state?include_history=true${date ? `&as_of_date=${encodeURIComponent(date)}` : ""}`,
      portfolioSchema,
      signal,
    ),
  analytics: (period: Period, benchmark: string, signal: AbortSignal) =>
    get(
      `/analytics/summary?${new URLSearchParams({ period, benchmark_id: benchmark })}`,
      analyticsSchema,
      signal,
    ),
};
