import { z } from "zod";
import { get } from "./api";

const cents = z.number().int().min(0);
const day = z.number().int().min(1).max(31);
const isoDate = z.iso.date();
const eventId = z.string().regex(/^[a-f0-9]{32}$/);
const categorySchema = z.object({
  id: z.string().regex(/^[a-z][a-z0-9_-]{0,39}$/),
  name: z.string().min(1).max(80),
  kind: z.enum(["fixed", "variable", "irregular"]),
  monthly_cents: cents,
  due_day: day.nullable(),
});
const actualExpenseSchema = z.object({
  id: eventId, date: isoDate, category_id: z.string(), amount_cents: cents,
  note: z.string().max(200),
});
const actualIncomeSchema = z.object({ id: eventId, date: isoDate, amount_cents: cents });
const goalAllocationSchema = z.object({
  id: eventId, date: isoDate, amount_cents: z.number().int(), note: z.string().max(200),
});
const goalSchema = z.object({
  id: z.string().regex(/^[a-z][a-z0-9_-]{0,39}$/),
  name: z.string().min(1).max(80),
  source: z.enum(["bank", "degiro"]),
  target_cents: cents,
  due_date: isoDate.nullable(),
  contribution_mode: z.enum(["deadline", "manual"]),
  manual_monthly_cents: cents,
  opening_reserved_cents: cents,
  allocations: z.array(goalAllocationSchema),
});
export const planSchema = z.object({
  payday_day: day,
  net_salary_cents: cents,
  bank_name: z.string().min(1).max(80),
  bank_balance_cents: cents.nullable(),
  bank_balance_date: isoDate.nullable(),
  emergency_reserved_cents: cents,
  immediate_commitments_cents: cents,
  goals: z.array(goalSchema),
  expenses: z.array(categorySchema),
  actual_expenses: z.array(actualExpenseSchema),
  actual_income: z.array(actualIncomeSchema),
}).superRefine((plan, context) => {
  if ((plan.bank_balance_cents === null) !== (plan.bank_balance_date === null))
    context.addIssue({ code: "custom", message: "Saldo y fecha deben ir juntos" });
  if (plan.goals.filter((goal) => goal.source === "degiro").length > 1)
    context.addIssue({ code: "custom", message: "Solo una meta puede usar DEGIRO" });
  if (new Set(plan.goals.map((goal) => goal.id)).size !== plan.goals.length)
    context.addIssue({ code: "custom", message: "Metas duplicadas" });
  for (const goal of plan.goals) {
    if (goal.contribution_mode === "deadline" && goal.due_date === null)
      context.addIssue({ code: "custom", message: "La meta necesita fecha", path: ["goals", goal.id] });
    if (goal.source === "degiro" && goal.opening_reserved_cents !== 0)
      context.addIssue({ code: "custom", message: "La valoración DEGIRO se lee de la cartera", path: ["goals", goal.id] });
    if (goal.source === "bank" && goal.opening_reserved_cents + goal.allocations.reduce((sum, item) => sum + item.amount_cents, 0) < 0)
      context.addIssue({ code: "custom", message: "La reserva no puede ser negativa", path: ["goals", goal.id] });
  }
});
const goalSummarySchema = z.object({
  id: z.string(),
  current_cents: z.number().int().nullable(),
  remaining_cents: z.number().int().nullable(),
  paydays_remaining: z.number().int().nullable(),
  required_monthly_cents: z.number().int().nullable(),
  planned_monthly_cents: z.number().int().nullable(),
  status: z.string(),
});
export const summarySchema = z.object({
  unallocated_bank_cents: z.number().int().nullable(),
  bank_reserve_shortfall_cents: z.number().int().nullable(),
  monthly_expenses_cents: z.number().int(),
  monthly_margin_cents: z.number().int(),
  goals_planned_monthly_cents: z.number().int().nullable(),
  after_goals_cents: z.number().int().nullable(),
  status: z.string(),
  goals: z.array(goalSummarySchema),
  portfolio_value_cents: z.number().int().nullable(),
  portfolio_as_of_date: isoDate.nullable(),
  portfolio_warnings: z.array(z.string()),
});
export const savedPlanSchema = z.object({
  configured: z.boolean(),
  plan: planSchema.nullable(),
  content_hash: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  summary: summarySchema.nullable(),
});
export type Plan = z.infer<typeof planSchema>;
export type Goal = Plan["goals"][number];
export type PlanSummary = z.infer<typeof summarySchema>;
export const planningApi = {
  read: (signal: AbortSignal) => get("/planning/plan", savedPlanSchema, signal),
  preview: (plan: Plan, signal: AbortSignal) =>
    get("/planning/preview", summarySchema, signal, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(plan),
    }),
};

export const localDate = (value: Date) =>
  `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;

export function newGoal(name = "Nueva meta", source: Goal["source"] = "bank"): Goal {
  const today = new Date();
  const nextYear = new Date(today.getFullYear() + 1, today.getMonth(), today.getDate());
  return {
    id: `g_${crypto.randomUUID().replaceAll("-", "")}`,
    name, source, target_cents: 0,
    due_date: localDate(nextYear),
    contribution_mode: source === "bank" ? "deadline" : "manual",
    manual_monthly_cents: 0, opening_reserved_cents: 0, allocations: [],
  };
}

export function emptyPlan(): Plan {
  return {
    payday_day: 1, net_salary_cents: 0,
    bank_name: "Cuenta bancaria",
    bank_balance_cents: null, bank_balance_date: null,
    emergency_reserved_cents: 0, immediate_commitments_cents: 0,
    goals: [],
    expenses: [
      { id: "home_food", name: "Casa y comida", kind: "fixed", monthly_cents: 0, due_day: 1 },
      { id: "fuel", name: "Gasolina", kind: "variable", monthly_cents: 0, due_day: null },
      { id: "leisure", name: "Ocio", kind: "variable", monthly_cents: 0, due_day: null },
      { id: "car_maintenance", name: "Imprevistos del coche", kind: "irregular", monthly_cents: 0, due_day: null },
      { id: "other", name: "Compras puntuales", kind: "irregular", monthly_cents: 0, due_day: null },
    ],
    actual_expenses: [], actual_income: [],
  };
}
