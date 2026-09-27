import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Planning } from "../Planning";
import { calendarEvents } from "../PlanningCalendar";
import { emptyPlan, type Goal } from "../planning-api";

const emptyHash = `sha256:${"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`;
const preview = {
  unallocated_bank_cents: null,
  bank_reserve_shortfall_cents: null,
  monthly_expenses_cents: 0,
  monthly_margin_cents: 250000,
  goals_planned_monthly_cents: 0,
  after_goals_cents: 250000,
  status: "feasible",
  goals: [],
  portfolio_value_cents: null,
  portfolio_as_of_date: null,
  portfolio_warnings: [],
};

describe("personal planning", () => {
  it("lets the user add a goal without DEGIRO data", async () => {
    const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
      if (input.includes("/planning/plan"))
        return new Response(JSON.stringify({ configured: false, plan: null, content_hash: emptyHash, summary: null }));
      if (input.includes("/planning/preview")) {
        const plan = JSON.parse(String(init?.body));
        return new Response(JSON.stringify({ ...preview, monthly_margin_cents: plan.net_salary_cents,
          goals: plan.goals.map((goal: Goal) => ({
            id: goal.id, current_cents: 0, remaining_cents: goal.target_cents,
            paydays_remaining: 12, required_monthly_cents: 0, planned_monthly_cents: 0, status: "feasible",
          })),
        }));
      }
      throw new Error("Unexpected request");
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<Planning visible health={{ status: "ok", api_version: "v1", mode: "read_only", workspace_mode: "real" }} />);
    await screen.findByRole("heading", { name: "Metas configurables" });
    expect(screen.getByText(/Inversiones funciona aunque no guardes este plan/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Añadir meta bancaria" }));
    fireEvent.change(screen.getByLabelText("Nombre de la meta"), { target: { value: "Coche" } });
    fireEvent.change(screen.getByLabelText("Sueldo neto mensual previsto"), { target: { value: "2500" } });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/planning/preview"),
      expect.objectContaining({ method: "POST", body: expect.stringContaining('"name":"Coche"') }),
    ));
    expect(screen.getByRole("button", { name: "Revisar y guardar plan" })).toBeDisabled();
  });

  it("keeps a one-off expense and configurable goal in the calendar", () => {
    const plan = emptyPlan();
    plan.payday_day = 31;
    plan.net_salary_cents = 200000;
    plan.goals = [{
      id: "car", name: "Mi coche", source: "bank", target_cents: 1000000,
      due_date: "2030-02-28", contribution_mode: "deadline", manual_monthly_cents: 0,
      opening_reserved_cents: 0, allocations: [],
    }];
    plan.actual_expenses = [{
      id: "a".repeat(32), date: "2030-02-14", category_id: "other", amount_cents: 50000, note: "Móvil",
    }];
    const events = calendarEvents(plan, { ...preview, goals: [{
      id: "car", current_cents: 0, remaining_cents: 1000000, paydays_remaining: 12,
      required_monthly_cents: 83334, planned_monthly_cents: 83334, status: "feasible",
    }] }, 2030, 1);
    expect(events.find((item) => item.id === "salary")?.date).toBe("2030-02-28");
    expect(events.find((item) => item.id === "goal-car")?.label).toContain("Mi coche");
    expect(events.find((item) => item.id === "due-car")?.date).toBe("2030-02-28");
    expect(events.find((item) => item.id === "a".repeat(32))?.label).toContain("500,00");
  });
});
