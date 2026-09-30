import { useState } from "react";
import { localDate, type Plan, type PlanSummary } from "./planning-api";

type CalendarEvent = { id: string; date: string; label: string; kind: "planned" | "actual" };
const euro = (cents: number) =>
  new Intl.NumberFormat("es-ES", { style: "currency", currency: "EUR" }).format(cents / 100);
const inMonth = (value: string, month: string) => value.startsWith(month);
const monthDay = (year: number, month: number, day: number) =>
  localDate(new Date(year, month, Math.min(day, new Date(year, month + 1, 0).getDate())));

export function calendarEvents(plan: Plan, summary: PlanSummary | undefined, year: number, month: number): CalendarEvent[] {
  const prefix = `${year}-${String(month + 1).padStart(2, "0")}`;
  const payday = monthDay(year, month, plan.payday_day);
  const today = localDate(new Date());
  const events: CalendarEvent[] = [];
  if (payday >= today) events.push({ id: "salary", date: payday, label: `Nómina prevista · ${euro(plan.net_salary_cents)}`, kind: "planned" });
  for (const expense of plan.expenses) {
    if (expense.due_day !== null && expense.monthly_cents > 0 && monthDay(year, month, expense.due_day) >= today) {
      events.push({
        id: `expense-${expense.id}`, date: monthDay(year, month, expense.due_day),
        label: `${expense.name} previsto · ${euro(expense.monthly_cents)}`, kind: "planned",
      });
    }
  }
  for (const goal of plan.goals) {
    const result = summary?.goals.find((item) => item.id === goal.id);
    if (result?.planned_monthly_cents && payday >= today && (!goal.due_date || payday <= goal.due_date))
      events.push({ id: `goal-${goal.id}`, date: payday, label: `${goal.name}: aportación prevista · ${euro(result.planned_monthly_cents)}`, kind: "planned" });
    if (goal.due_date && inMonth(goal.due_date, prefix))
      events.push({ id: `due-${goal.id}`, date: goal.due_date, label: `Fecha objetivo: ${goal.name}`, kind: "planned" });
    for (const allocation of goal.allocations.filter((item) => inMonth(item.date, prefix)))
      events.push({
        id: allocation.id, date: allocation.date,
        label: `${goal.name}: ${allocation.amount_cents >= 0 ? "aportación" : "retirada"} registrada · ${euro(Math.abs(allocation.amount_cents))}`,
        kind: "actual",
      });
  }
  for (const expense of plan.actual_expenses.filter((item) => inMonth(item.date, prefix))) {
    const category = plan.expenses.find((item) => item.id === expense.category_id);
    events.push({ id: expense.id, date: expense.date, label: `${category?.name ?? "Gasto"} registrado · ${euro(expense.amount_cents)}`, kind: "actual" });
  }
  for (const income of plan.actual_income.filter((item) => inMonth(item.date, prefix)))
    events.push({ id: income.id, date: income.date, label: `Ingreso registrado · ${euro(income.amount_cents)}`, kind: "actual" });
  return events.sort((a, b) => a.date.localeCompare(b.date) || a.label.localeCompare(b.label));
}

export function PlanningCalendar({ plan, summary }: { plan: Plan; summary?: PlanSummary }) {
  const [month, setMonth] = useState(() => new Date(new Date().getFullYear(), new Date().getMonth(), 1));
  const year = month.getFullYear(), monthIndex = month.getMonth();
  const days = new Date(year, monthIndex + 1, 0).getDate();
  const leading = (new Date(year, monthIndex, 1).getDay() + 6) % 7;
  const events = calendarEvents(plan, summary, year, monthIndex);
  const cells = Array.from({ length: Math.ceil((leading + days) / 7) * 7 }, (_, index) => index - leading + 1);
  const monthLabel = new Intl.DateTimeFormat("es-ES", { month: "long", year: "numeric" }).format(month);
  return (
    <section className="panel planning-calendar" aria-label="Calendario de planificación">
      <div className="calendar-heading">
        <h2>Calendario · {monthLabel}</h2>
        <div>
          <button type="button" aria-label="Mes anterior" onClick={() => setMonth(new Date(year, monthIndex - 1, 1))}>←</button>
          <button type="button" aria-label="Mes siguiente" onClick={() => setMonth(new Date(year, monthIndex + 1, 1))}>→</button>
        </div>
      </div>
      <p>Las fechas previstas se actualizan con tu plan. Las entradas registradas se conservan.</p>
      <div className="table-scroll">
        <table>
          <thead><tr>{["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"].map((day) => <th key={day} scope="col">{day}</th>)}</tr></thead>
          <tbody>{Array.from({ length: cells.length / 7 }, (_, week) => (
            <tr key={week}>{cells.slice(week * 7, week * 7 + 7).map((day, dayIndex) => {
              if (day < 1 || day > days) return <td key={dayIndex} className="outside" />;
              const date = monthDay(year, monthIndex, day);
              return <td key={dayIndex}>
                <time dateTime={date}>{day}</time>
                {events.filter((item) => item.date === date).map((item) =>
                  <span className={`calendar-event ${item.kind}`} key={item.id}>{item.label}</span>)}
              </td>;
            })}</tr>
          ))}</tbody>
        </table>
      </div>
      <h3>Eventos del mes</h3>
      {events.length ? <ul className="calendar-list">{events.map((item) =>
        <li key={item.id}><time dateTime={item.date}>{item.date.slice(8)}</time> · {item.label} <small>({item.kind === "actual" ? "registrado" : "previsto"})</small></li>)}</ul>
        : <p>Sin eventos para este mes.</p>}
    </section>
  );
}
