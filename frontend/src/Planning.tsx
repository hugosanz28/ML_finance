import { useEffect, useRef, useState } from "react";
import { ApiError } from "./api";
import { PlanningCalendar } from "./PlanningCalendar";
import { PlanningGoals } from "./PlanningGoals";
import { emptyPlan, localDate, newGoal, planSchema, planningApi, type Goal, type Plan, type PlanSummary } from "./planning-api";
import { opsApi, type Health } from "./operations-api";

const euro = (cents: number | null | undefined) => cents == null
  ? "No disponible"
  : new Intl.NumberFormat("es-ES", { style: "currency", currency: "EUR" }).format(cents / 100);
const money = (cents: number) => cents / 100;
const cents = (value: string) => Math.round((Number(value) || 0) * 100);
const errorCode = (error: unknown) => error instanceof ApiError ? error.code : "request_failed";
const eventId = () => crypto.randomUUID().replaceAll("-", "");

async function retryBusy<T>(request: () => Promise<T>, signal: AbortSignal): Promise<T> {
  // Another local read or job may briefly hold the shared workspace lock.
  for (let attempt = 0; attempt < 20; attempt++) {
    try { return await request(); }
    catch (error) {
      if (errorCode(error) !== "workspace_busy" || signal.aborted || attempt === 19) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
  throw new Error("planning_read_unavailable");
}

function MoneyField({ label, value, change, disabled = false }: { label: string; value: number; change: (value: number) => void; disabled?: boolean }) {
  return <label>{label}<input type="number" min="0" step="0.01" disabled={disabled} value={money(value)} onChange={(event) => change(cents(event.target.value))} /></label>;
}

export function Planning({ health, visible }: { health?: Health; visible: boolean }) {
  const [draft, setDraft] = useState<Plan>();
  const [revision, setRevision] = useState("");
  const [configured, setConfigured] = useState(false);
  const [summary, setSummary] = useState<PlanSummary>();
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [reload, setReload] = useState(0);
  const lastLoaded = useRef(-1);
  const [pending, setPending] = useState<{ plan: Plan; revision: string; key: string }>();
  const [expenseAmount, setExpenseAmount] = useState(0);
  const [expenseCategory, setExpenseCategory] = useState("other");
  const [expenseDate, setExpenseDate] = useState(() => localDate(new Date()));
  const [expenseNote, setExpenseNote] = useState("");
  const [incomeAmount, setIncomeAmount] = useState(0);
  const [incomeDate, setIncomeDate] = useState(() => localDate(new Date()));
  const [newCategory, setNewCategory] = useState("");
  const writable = health?.mode === "operations";

  useEffect(() => {
    if (!visible || (draft && lastLoaded.current === reload)) return;
    const controller = new AbortController();
    setLoading(true);
    setError("");
    void retryBusy(() => planningApi.read(controller.signal), controller.signal).then(
      (value) => {
        if (controller.signal.aborted) return;
        lastLoaded.current = reload;
        setDraft(value.plan ?? emptyPlan());
        setRevision(value.content_hash);
        setConfigured(value.configured);
        setSummary(value.summary ?? undefined);
        setLoading(false);
      },
      (failure) => { if (!controller.signal.aborted) { setError(errorCode(failure)); setLoading(false); } },
    );
    return () => controller.abort();
  }, [visible, reload, draft]);

  useEffect(() => {
    if (!visible || !draft) return;
    setSummary(undefined);
    const valid = planSchema.safeParse(draft);
    if (!valid.success) return;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      void retryBusy(() => planningApi.preview(valid.data, controller.signal), controller.signal).then(
        (value) => { if (!controller.signal.aborted) setSummary(value); },
        () => { if (!controller.signal.aborted) setSummary(undefined); },
      );
    }, 250);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [draft, visible]);

  const change = <K extends keyof Plan>(key: K, value: Plan[K]) =>
    setDraft((current) => current ? { ...current, [key]: value } : current);
  const changeExpense = (id: string, patch: Partial<Plan["expenses"][number]>) =>
    setDraft((current) => current ? {
      ...current, expenses: current.expenses.map((item) => item.id === id ? { ...item, ...patch } : item),
    } : current);
  const changeGoal = (id: string, patch: Partial<Goal>) =>
    setDraft((current) => current ? {
      ...current, goals: current.goals.map((item) => item.id === id ? { ...item, ...patch } : item),
    } : current);
  const removeEvent = (kind: "actual_expenses" | "actual_income", id: string) =>
    setDraft((current) => current ? {
      ...current, [kind]: current[kind].filter((item) => item.id !== id),
    } : current);

  async function save() {
    if (!pending || !writable || !health) return;
    setSaving(true);
    setError("");
    const controller = new AbortController();
    try {
      const job = await opsApi.send("personal_plan", {
        workspace_mode: health.workspace_mode, confirm: true,
        plan: pending.plan, expected_previous_hash: pending.revision,
      }, pending.key, controller.signal);
      let result = job;
      for (let attempt = 0; attempt < 30 && ["pending", "running"].includes(result.state); attempt++) {
        await new Promise((resolve) => setTimeout(resolve, 350));
        result = await opsApi.job(job.job_id, controller.signal);
      }
      if (result.state === "succeeded") {
        setPending(undefined);
        setNotice("Plan guardado en la base local.");
        setReload((value) => value + 1);
      } else if (result.state === "failed") {
        setError(result.error_code === "content_conflict"
          ? "El plan cambió desde que lo abriste. Recarga y revisa antes de guardar."
          : `No se pudo guardar (${result.error_code ?? "operation_failed"}).`);
        if (result.error_code === "content_conflict") setPending(undefined);
      } else setError("No se confirmó el resultado. Reintenta con la misma solicitud.");
    } catch (failure) {
      // Keep the frozen body and idempotency key after an uncertain send.
      setError(`No se confirmó el guardado (${errorCode(failure)}). Reintenta con la misma solicitud.`);
    } finally { setSaving(false); }
  }

  if (!visible) return null;
  if (loading && !draft) return <section className="loading" role="status"><h2>Cargando planificación…</h2></section>;
  if (!draft) return <section className="panel"><p role="alert">No se pudo leer el plan. {error}</p><button onClick={() => setReload((value) => value + 1)}>Reintentar</button></section>;
  const validation = planSchema.safeParse(draft);
  const currentMonth = localDate(new Date()).slice(0, 7);
  const actualSpent = draft.actual_expenses.filter((item) => item.date.startsWith(currentMonth))
    .reduce((sum, item) => sum + item.amount_cents, 0);
  const actualEarned = draft.actual_income.filter((item) => item.date.startsWith(currentMonth))
    .reduce((sum, item) => sum + item.amount_cents, 0);
  const bankReserved = draft.goals.filter((goal) => goal.source === "bank")
    .reduce((sum, goal) => sum + goal.opening_reserved_cents + goal.allocations.reduce((value, item) => value + item.amount_cents, 0), 0);
  const degiroGoal = draft.goals.find((goal) => goal.source === "degiro");
  return <div className="planning">
    <button type="button" disabled={!!pending} onClick={() => setReload((value) => value + 1)}>↻ Recargar datos (descarta cambios sin guardar)</button>
    {!configured && <div className="context-note">Empieza con tus cifras y añade las metas que quieras. Inversiones funciona aunque no guardes este plan.</div>}
    {error && <div className="error" role="alert">{error}</div>}
    {notice && <div className="context-note" role="status">{notice}</div>}
    {!validation.success && <div className="error" role="alert">Revisa el plan: {validation.error.issues[0]?.message}</div>}
    <div className="planning-summary">
      <div className="panel"><span>Sueldo neto previsto</span><strong>{euro(draft.net_salary_cents)}</strong></div>
      <div className="panel"><span>Reservado en metas bancarias</span><strong>{euro(bankReserved)}</strong></div>
      <div className="panel"><span>Libre en {draft.bank_name} tras reservas</span><strong>{euro(summary?.unallocated_bank_cents)}</strong></div>
      <div className="panel"><span>Aportación mensual a metas</span><strong>{euro(summary?.goals_planned_monthly_cents)}</strong></div>
      <div className="panel"><span>Margen tras metas</span><strong>{euro(summary?.after_goals_cents)}</strong></div>
      <div className="panel"><span>Este mes · gastos registrados</span><strong>{euro(actualSpent)}</strong></div>
      <div className="panel"><span>Este mes · ingresos registrados</span><strong>{euro(actualEarned)}</strong></div>
      {degiroGoal && <div className="panel"><span>DEGIRO para {degiroGoal.name}</span><strong>{euro(summary?.portfolio_value_cents)}</strong><small>{summary?.portfolio_as_of_date ?? "Sin valoración disponible"}</small></div>}
    </div>
    {!!summary?.portfolio_warnings.length && <div className="context-note">Avisos de valoración DEGIRO: {summary.portfolio_warnings.join(" · ")}</div>}
    {summary?.status === "goals_infeasible" && <div className="error" role="alert">Las aportaciones previstas superan el margen mensual en {euro(-(summary.after_goals_cents ?? 0))}.</div>}
    {summary?.status === "goals_need_attention" && <div className="error" role="alert">Una o más metas necesitan revisar plazo, valoración o aportación.</div>}
    {!!summary?.bank_reserve_shortfall_cents && <div className="error" role="alert">Las reservas superan el saldo de {draft.bank_name} en {euro(summary.bank_reserve_shortfall_cents)}.</div>}
    <div className="operations-grid">
      <section className="panel">
        <h2>Ingresos y cuenta bancaria</h2>
        <fieldset disabled={!!pending}>
          <MoneyField label="Sueldo neto mensual previsto" value={draft.net_salary_cents} change={(value) => change("net_salary_cents", value)} />
          <label>Día habitual de cobro<input type="number" min="1" max="31" value={draft.payday_day} onChange={(event) => change("payday_day", Number(event.target.value))} /></label>
          <label>Nombre del banco o cuenta<input value={draft.bank_name} maxLength={80} onChange={(event) => change("bank_name", event.target.value)} /></label>
          <label>Saldo bancario observado (€)<input type="number" min="0" step="0.01" value={draft.bank_balance_cents == null ? "" : money(draft.bank_balance_cents)} onChange={(event) => {
            const value = event.target.value;
            change("bank_balance_cents", value === "" ? null : cents(value));
            if (value && !draft.bank_balance_date) change("bank_balance_date", localDate(new Date()));
            if (!value) change("bank_balance_date", null);
          }} /></label>
          <label>Fecha del saldo<input type="date" value={draft.bank_balance_date ?? ""} onChange={(event) => change("bank_balance_date", event.target.value || null)} /></label>
          <MoneyField label="Reserva para imprevistos" value={draft.emergency_reserved_cents} change={(value) => change("emergency_reserved_cents", value)} />
          <MoneyField label="Recibos próximos ya comprometidos" value={draft.immediate_commitments_cents} change={(value) => change("immediate_commitments_cents", value)} />
        </fieldset>
        <p>El saldo se actualiza a mano. Registrar ingresos, gastos o asignaciones no lo modifica automáticamente.</p>
      </section>
      <section className="panel">
        <h2>Metas configurables</h2>
        <p>Crea una meta para cada propósito. Puedes reservar parte del saldo bancario para varias metas y vincular toda la cartera DEGIRO a una meta elegida.</p>
        <div className="planning-add">
          <button type="button" disabled={!!pending} onClick={() => change("goals", [...draft.goals, newGoal()])}>Añadir meta bancaria</button>
          <button type="button" disabled={!!pending || !!degiroGoal} onClick={() => change("goals", [...draft.goals, newGoal("Nueva meta de inversión", "degiro")])}>Añadir meta con DEGIRO</button>
        </div>
      </section>
    </div>
    <PlanningGoals goals={draft.goals} summary={summary} disabled={!!pending} update={changeGoal} remove={(id) => change("goals", draft.goals.filter((goal) => goal.id !== id))} />
    <section className="panel">
      <h2>Presupuesto mensual de gastos</h2>
      <p>Previsto: {euro(summary?.monthly_expenses_cents)} al mes. Registrado este mes: {euro(actualSpent)}.</p>
      <div className="planning-categories">{draft.expenses.map((item) =>
        <div key={item.id} className="planning-category">
          <label>Categoría<input value={item.name} disabled={!!pending} onChange={(event) => changeExpense(item.id, { name: event.target.value })} /></label>
          <label>Tipo<select value={item.kind} disabled={!!pending} onChange={(event) => changeExpense(item.id, { kind: event.target.value as typeof item.kind })}>
            <option value="fixed">Fijo</option><option value="variable">Variable</option><option value="irregular">Puntual / provisión</option>
          </select></label>
          <MoneyField label="Previsión al mes" value={item.monthly_cents} disabled={!!pending} change={(value) => changeExpense(item.id, { monthly_cents: value })} />
          <label>Día si es recurrente<input type="number" min="1" max="31" value={item.due_day ?? ""} disabled={!!pending} onChange={(event) => changeExpense(item.id, { due_day: event.target.value ? Number(event.target.value) : null })} /></label>
        </div>)}
      </div>
      <div className="planning-add"><label>Nueva categoría<input value={newCategory} disabled={!!pending} onChange={(event) => setNewCategory(event.target.value)} /></label>
        <button type="button" disabled={!!pending || !newCategory.trim()} onClick={() => {
          change("expenses", [...draft.expenses, { id: `cat_${eventId()}`, name: newCategory.trim(), kind: "variable", monthly_cents: 0, due_day: null }]);
          setNewCategory("");
        }}>Añadir categoría</button></div>
      <p>Una compra aislada, como un móvil, se registra abajo como gasto real. Su presupuesto mensual puede quedar en cero.</p>
    </section>
    <PlanningCalendar plan={draft} summary={summary} />
    <section className="panel">
      <h2>Movimientos registrados por ti</h2>
      <div className="operations-grid">
        <form onSubmit={(event) => {
          event.preventDefault();
          if (!expenseAmount || !draft.expenses.some((item) => item.id === expenseCategory)) return;
          change("actual_expenses", [...draft.actual_expenses, { id: eventId(), date: expenseDate, category_id: expenseCategory, amount_cents: expenseAmount, note: expenseNote }]);
          setExpenseAmount(0); setExpenseNote("");
        }}>
          <h3>Nuevo gasto</h3>
          <fieldset disabled={!!pending}>
            <label>Fecha<input type="date" max={localDate(new Date())} value={expenseDate} onChange={(event) => setExpenseDate(event.target.value)} /></label>
            <label>Categoría<select value={expenseCategory} onChange={(event) => setExpenseCategory(event.target.value)}>{draft.expenses.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select></label>
            <MoneyField label="Importe" value={expenseAmount} change={setExpenseAmount} />
            <label>Nota<input value={expenseNote} maxLength={200} onChange={(event) => setExpenseNote(event.target.value)} /></label>
            <button type="submit">Añadir gasto</button>
          </fieldset>
        </form>
        <form onSubmit={(event) => {
          event.preventDefault();
          if (!incomeAmount) return;
          change("actual_income", [...draft.actual_income, { id: eventId(), date: incomeDate, amount_cents: incomeAmount }]);
          setIncomeAmount(0);
        }}>
          <h3>Ingreso recibido</h3>
          <fieldset disabled={!!pending}>
            <label>Fecha<input type="date" max={localDate(new Date())} value={incomeDate} onChange={(event) => setIncomeDate(event.target.value)} /></label>
            <MoneyField label="Importe recibido" value={incomeAmount} change={setIncomeAmount} />
            <button type="submit">Añadir ingreso</button>
          </fieldset>
        </form>
      </div>
      <ul className="planning-history">
        {draft.actual_expenses.map((item) => <li key={item.id}>{item.date} · {draft.expenses.find((value) => value.id === item.category_id)?.name} · {euro(item.amount_cents)} <button type="button" disabled={!!pending} onClick={() => removeEvent("actual_expenses", item.id)}>Quitar</button></li>)}
        {draft.actual_income.map((item) => <li key={item.id}>{item.date} · Ingreso · {euro(item.amount_cents)} <button type="button" disabled={!!pending} onClick={() => removeEvent("actual_income", item.id)}>Quitar</button></li>)}
      </ul>
    </section>
    <div className="planning-actions">
      {!writable && <p>Para guardar, arranca la API con <code>--operations {health?.workspace_mode ?? "real"}</code>. Puedes explorar el plan sin guardar.</p>}
      {!pending && <button type="button" disabled={!writable || !validation.success || !summary} onClick={() => {
        setNotice(""); setError("");
        setPending({ plan: structuredClone(draft), revision, key: crypto.randomUUID() });
      }}>Revisar y guardar plan</button>}
      {pending && <div className="panel confirmation" role="group" aria-label="Confirmar guardado">
        <h2>Guardar planificación</h2>
        <p>Se guardarán tus cifras, metas y movimientos en la base privada de este entorno.</p>
        <button type="button" disabled={saving} onClick={() => void save()}>{saving ? "Guardando…" : "Confirmar guardado"}</button>
        <button type="button" disabled={saving} onClick={() => setPending(undefined)}>Cancelar</button>
      </div>}
    </div>
  </div>;
}
