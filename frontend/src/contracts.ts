import { z } from "zod";

const number = z.number().finite();
const date = z.iso.date();
const currency = z.string().regex(/^[A-Z]{3}$/);
const status = z.enum(["available", "partial", "unavailable"]);
export const metricSchema = z.object({
  metric_id: z.string(),
  value: number.nullable(),
  unit: z.string(),
  status,
  reason_code: z.string(),
  observations: number,
  coverage_ratio: number,
});
export type Metric = z.infer<typeof metricSchema>;
const riskResult = z.object({ metrics: z.array(metricSchema).optional() });
const growth = z.object({
  observation_date: date,
  portfolio_index: number.nullable(),
  benchmark_index: number.nullable(),
  portfolio_drawdown: number.nullable(),
  benchmark_drawdown: number.nullable(),
});
const comparison = z.object({
  benchmark_id: z.string(),
  benchmark_name: z.string(),
  source_reference: z.string(),
  source_currency: currency,
  base_currency: currency,
  series_kind: z.string(),
  provider_name: z.string(),
  period_start: date.nullable(),
  period_end: date.nullable(),
  observations: number,
  coverage_ratio: number,
  status,
  reason_codes: z.array(z.string()),
  growth: z.array(growth),
  metrics: z.array(metricSchema),
});
export const analyticsSchema = z.object({
  schema_version: z.literal(1),
  section: z.literal("summary"),
  status,
  reason_code: z.string(),
  base_currency: currency,
  period: z.object({
    period_id: z.string(),
    requested_start: date.nullable(),
    actual_start: date.nullable(),
    end_date: date.nullable(),
  }),
  warnings: z.array(z.string()),
  data: z.object({
    performance: z
      .object({
        period: z.object({
          net_external_flow_base: number,
          twr: metricSchema,
          mwr: metricSchema,
        }),
      })
      .optional(),
    risk: z
      .object({
        portfolio: riskResult,
        positions: z.object({
          concentration: z.object({
            groups: z.array(
              z.object({
                dimension: z.string(),
                group: z.string(),
                weight: metricSchema,
              }),
            ),
            hhi_by_dimension: z.record(z.string(), metricSchema),
          }),
          asset_risk: z.record(z.string(), riskResult),
          series_kind: z.string(),
          diversification_reason_code: z.string(),
          diversification: z
            .object({
              correlations: z.array(
                z.object({
                  left_asset_id: z.string(),
                  right_asset_id: z.string(),
                  metric: metricSchema,
                }),
              ),
              risk_contributions: z.record(z.string(), metricSchema),
            })
            .nullable(),
        }),
      })
      .optional(),
    benchmarks: z
      .object({
        reason_code: z.string(),
        catalog: z.array(
          z.object({
            benchmark_id: z.string(),
            name: z.string(),
            description: z.string(),
          }),
        ),
        comparison: z.object({ comparisons: z.array(comparison) }).nullable(),
      })
      .optional(),
  }),
});
export type Analytics = z.infer<typeof analyticsSchema>;
export const portfolioSchema = z.object({
  as_of_date: date,
  base_currency: currency,
  summary: z.record(z.string(), number.nullable()),
  positions: z.array(
    z.object({
      asset_id: z.string(),
      asset_name: z.string().nullable(),
      asset_type: z.string().nullable(),
      market_value_base: number.nullable(),
      weight: number.nullable(),
      unrealized_pnl_base: number.nullable(),
      valuation_status: z.string().nullable(),
    }),
  ),
  history: z.array(
    z.object({
      valuation_date: date,
      total_market_value_base: number.nullable(),
      valuation_coverage_ratio: number.nullable(),
    }),
  ),
  data_quality: z.object({ warnings: z.array(z.string()) }),
});
export type Portfolio = z.infer<typeof portfolioSchema>;
export const definitionsSchema = z.object({
  schema_version: z.literal(1),
  definitions: z.array(
    z.object({
      metric_id: z.string(),
      name: z.string(),
      description: z.string(),
      formula: z.string(),
      unit: z.string(),
      interpretation: z.string(),
      limitations: z.string(),
      required_data: z.string(),
      validity_conditions: z.string(),
    }),
  ),
});
export type Definition = z.infer<
  typeof definitionsSchema
>["definitions"][number];
export type Period =
  "since_inception" | "last_year" | "last_quarter" | "last_month";
