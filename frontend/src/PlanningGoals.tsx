import { useState } from "react";
import { localDate, type Goal, type PlanSummary } from "./planning-api";

const euro = (cents: number | null | undefined) => cents == null
  ? "No disponible"
  : new Intl.NumberFormat("es-ES", { style: "currency", currency: "EUR" }).format(cents / 100);
const cents = (value: string) => Math.round((Number(value) || 0) * 100);
const id = () => crypto.randomUUID().replaceAll("-", "");

function statusText(status: string) {
  const labels: Record<string, string> = {
    feasible: "Según el plazo", funded: "Meta cubierta", overdue: "Fecha vencida",
    no_paydays_before_due: "Sin cobros pendientes antes de la fecha",
    valuation_unavailable: "Esperando valoración DEGIRO", behind_plan: "Aportación manual inferior a la necesaria",
    no_target: "Añade un importe objetivo",
  };
  return labels[status] ?? status;
}

export function PlanningGoals({ goals, summary, disabled, update, remove }: {
  goals: Goal[];
  summary?: PlanSummary;
  disabled: boolean;
  update: (id: string, patch: Partial<Goal>) => void;
  remove: (id: string) => void;
}) {
  const [amounts, setAmounts] = useState<Record<string, number>>({});
  const [dates, setDates] = useState<Record<string, string>>({});
  const [withdrawals, setWithdrawals] = useState<Record<string, boolean>>({});
  const degiroGoal = goals.find((goal) => goal.source === "degiro");
  return <div className="planning-goals">{goals.map((goal) => {
    const result = summary?.goals.find((item) => item.id === goal.id);
    const reserved = goal.opening_reserved_cents + goal.allocations.reduce((sum, item) => sum + item.amount_cents, 0);
    const amount = amounts[goal.id] ?? 0;
    return <section className="panel planning-goal" key={goal.id} aria-label={`Meta ${goal.name}`}>
      <div className="calendar-heading"><h3>{goal.name}</h3><button type="button" disabled={disabled} onClick={() => remove(goal.id)}>Quitar meta</button></div>
      <fieldset disabled={disabled} className="planning-goal-fields">
        <label>Nombre de la meta<input value={goal.name} maxLength={80} onChange={(event) => update(goal.id, { name: event.target.value })} /></label>
        <label>Origen del dinero<select value={goal.source} disabled={goal.allocations.length > 0} onChange={(event) => update(goal.id, {
          source: event.target.value as Goal["source"], opening_reserved_cents: 0,
          contribution_mode: event.target.value === "degiro" ? "manual" : goal.contribution_mode,
        })}>
          <option value="bank">Reserva dentro de la cuenta bancaria</option>
          <option value="degiro" disabled={!!degiroGoal && degiroGoal.id !== goal.id}>Toda la cartera DEGIRO</option>
        </select></label>
        {goal.allocations.length > 0 && <p>Para cambiar el origen, quita primero los movimientos registrados.</p>}
        <label>Importe objetivo (€)<input type="number" min="0" step="0.01" value={goal.target_cents / 100} onChange={(event) => update(goal.id, { target_cents: cents(event.target.value) })} /></label>
        <label>Fecha objetivo<input type="date" value={goal.due_date ?? ""} onChange={(event) => update(goal.id, {
          due_date: event.target.value || null,
          contribution_mode: event.target.value ? goal.contribution_mode : "manual",
        })} /></label>
        <label>Forma de aportar<select value={goal.contribution_mode} onChange={(event) => update(goal.id, { contribution_mode: event.target.value as Goal["contribution_mode"] })}>
          <option value="deadline" disabled={!goal.due_date}>Calcular según el plazo</option>
          <option value="manual">Cantidad mensual manual</option>
        </select></label>
        {goal.contribution_mode === "manual" && <label>Aportación mensual prevista (€)<input type="number" min="0" step="0.01" value={goal.manual_monthly_cents / 100} onChange={(event) => update(goal.id, { manual_monthly_cents: cents(event.target.value) })} /></label>}
        {goal.source === "bank" && <label>Reserva inicial en la cuenta (€)<input type="number" min="0" step="0.01" value={goal.opening_reserved_cents / 100} onChange={(event) => update(goal.id, { opening_reserved_cents: cents(event.target.value) })} /></label>}
      </fieldset>
      <div className="planning-goal-result">
        <span>Acumulado: <strong>{euro(result?.current_cents)}</strong></span>
        <span>Queda para la meta: <strong>{euro(result?.remaining_cents)}</strong></span>
        <span>Necesario al mes: <strong>{euro(result?.required_monthly_cents)}</strong></span>
        <span>Planificado al mes: <strong>{euro(result?.planned_monthly_cents)}</strong></span>
      </div>
      <p>{result ? statusText(result.status) : "Calculando…"}{goal.source === "degiro" && summary?.portfolio_as_of_date ? ` · DEGIRO valorado el ${summary.portfolio_as_of_date}` : ""}</p>
      {goal.source === "degiro" && <p>Los movimientos registrados aquí son anotaciones. La valoración procede de DEGIRO y puede variar.</p>}
      <form className="planning-goal-movement" onSubmit={(event) => {
        event.preventDefault();
        if (!amount || (goal.source === "bank" && withdrawals[goal.id] && amount > reserved)) return;
        update(goal.id, { allocations: [...goal.allocations, {
          id: id(), date: dates[goal.id] ?? localDate(new Date()),
          amount_cents: withdrawals[goal.id] ? -amount : amount, note: "",
        }] });
        setAmounts((previous) => ({ ...previous, [goal.id]: 0 }));
      }}>
        <fieldset disabled={disabled}>
          <legend>Registrar {goal.source === "bank" ? "asignación a la reserva" : "aportación a DEGIRO"}</legend>
          <label>Fecha<input type="date" max={localDate(new Date())} value={dates[goal.id] ?? localDate(new Date())} onChange={(event) => setDates((previous) => ({ ...previous, [goal.id]: event.target.value }))} /></label>
          <label>Movimiento<select value={withdrawals[goal.id] ? "withdraw" : "add"} onChange={(event) => setWithdrawals((previous) => ({ ...previous, [goal.id]: event.target.value === "withdraw" }))}>
            <option value="add">Aportación</option><option value="withdraw">Retirada</option>
          </select></label>
          <label>Importe del movimiento (€)<input type="number" min="0" step="0.01" value={amount / 100} onChange={(event) => setAmounts((previous) => ({ ...previous, [goal.id]: cents(event.target.value) }))} /></label>
          <button type="submit">Registrar movimiento</button>
        </fieldset>
      </form>
      {!!goal.allocations.length && <ul className="planning-history">{goal.allocations.map((item) =>
        <li key={item.id}>{item.date} · {item.amount_cents >= 0 ? "Aportación" : "Retirada"} · {euro(Math.abs(item.amount_cents))}
          <button type="button" disabled={disabled} onClick={() => update(goal.id, { allocations: goal.allocations.filter((value) => value.id !== item.id) })}>Quitar</button>
        </li>)}</ul>}
    </section>;
  })}</div>;
}
