"""Monthly agents Streamlit tab."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import streamlit as st

from src.application import (
    BuildAgentDashboardSnapshotRequest,
    BuildAgentDashboardSnapshotUseCase,
    GetAgentRunAuditRequest,
    GetAgentRunAuditUseCase,
    ListAgentRunsUseCase,
    ReadInvestmentBriefUseCase,
    RunMonthlyAgentsRequest,
    RunMonthlyAgentsUseCase,
    UpdateInvestmentBriefRequest,
    UpdateInvestmentBriefUseCase,
    extract_monthly_report_as_of_date,
)
from src.config import Settings
from src.portfolio.dashboard_common import (
    dashboard_data_fingerprint,
    list_reports,
    load_metrics,
    load_snapshots,
    read_default_target_weights,
    section_header,
    show_metrics_error,
)
from src.portfolio.dashboard_transforms import _parse_target_weights_input

LLM_PROVIDER_OPTIONS = ("static", "openai")
SEARCH_PROVIDER_OPTIONS = ("null", "static", "duckduckgo", "tavily")


def render_agents_tab(settings: Settings) -> None:
    section_header(
        "Agentes",
        "Pipeline mensual: contexto tematico -> juicio por activo -> recomendacion mensual.",
    )
    data_fingerprint = dashboard_data_fingerprint(settings)
    metrics = load_metrics(settings, data_fingerprint)
    if metrics is None:
        show_metrics_error()
        return
    snapshots = load_snapshots(settings, data_fingerprint)

    st.markdown(
        """
        **Flujo entre agentes**
        1. `monitor_tematico`: extrae contexto externo relevante para tu cartera.
        2. `analista_activos`: evalua encaje y riesgo de cada activo con el mandato.
        3. `asistente_aportacion_mensual`: sintetiza y propone accion mensual.
        """
    )

    left, right = st.columns([2, 1])
    with left:
        brief_state = ReadInvestmentBriefUseCase(settings=settings).execute()
        brief_hash_state_key = f"investment_brief_hash::{brief_state.path}"
        if brief_hash_state_key not in st.session_state:
            st.session_state[brief_hash_state_key] = brief_state.content_hash
        investment_brief = st.text_area("Investment brief", value=brief_state.content, height=240)
        st.caption("Este texto es el mandato de la cuenta: objetivo, horizonte, tolerancia al riesgo y reglas personales.")
        if st.button("Guardar investment brief"):
            update = UpdateInvestmentBriefUseCase(settings=settings).execute(
                UpdateInvestmentBriefRequest(
                    content=investment_brief,
                    expected_previous_hash=st.session_state[brief_hash_state_key],
                )
            )
            if update.result.failed:
                st.error(update.result.message)
            else:
                st.session_state[brief_hash_state_key] = update.content_hash
                st.success(f"Guardado en {update.result.artifacts['path']}")
    with right:
        st.markdown("#### Configuracion")
        user_interest = st.text_input("Idea puntual de satellite")
        monthly_budget = st.number_input(
            "Monthly budget (EUR)",
            min_value=0.0,
            value=float(settings.monthly_contribution_eur),
            step=50.0,
            help="Presupuesto mensual que recibira `asistente_aportacion_mensual` para proponer acciones del mes.",
        )
        send_target_weights = st.checkbox(
            "Enviar target_weights al pipeline",
            value=True,
            help="Si lo desactivas, los agentes no reciben pesos objetivo y evaluan sin esa restriccion.",
        )
        default_target_weights = read_default_target_weights(settings)
        target_weights_text = st.text_area(
            "Pesos objetivo (JSON opcional)",
            value=json.dumps(default_target_weights, ensure_ascii=False, indent=2),
            height=110,
            disabled=not send_target_weights,
            help="Se pasa a `asistente_aportacion_mensual` para evaluar rebalanceo con criterio cuantitativo.",
        )
        llm_provider = st.selectbox("LLM provider", options=LLM_PROVIDER_OPTIONS, index=0)
        search_provider = st.selectbox(
            "Search provider",
            options=SEARCH_PROVIDER_OPTIONS,
            index=0,
        )
        st.caption(
            "Usa `static/null` para demo sin coste ni red. "
            "Usa `openai/tavily` para busqueda API o `openai/duckduckgo` como fallback best-effort."
        )
    target_weights = _parse_target_weights_input(target_weights_text) if send_target_weights else {}
    target_weights_invalid = send_target_weights and target_weights_text.strip() and not target_weights
    if target_weights_invalid:
        st.warning("`Pesos objetivo` no se pudo parsear como JSON valido; se ejecutara sin pesos objetivo.")

    reports = list_reports(settings)
    report_option = None
    report_text_input = ""
    report_as_of: date | None = None
    if reports:
        report_option = st.selectbox(
            "latest_monthly_report (fuente)",
            options=reports,
            format_func=lambda item: f"{item['label']} - {item['path'].name}",
        )
        report_path = Path(report_option["path"])
        report_text_input = st.text_area(
            "latest_monthly_report (editable)",
            value=report_path.read_text(encoding="utf-8"),
            height=240,
        )
        report_as_of = extract_monthly_report_as_of_date(report_text_input, path=report_path) or report_option.get("as_of_date")
        st.caption(f"Fuente seleccionada: {report_path}")
    else:
        st.warning("No hay informes `.md` detectados en reports_history/reports. Genera uno primero.")

    snapshot_default = BuildAgentDashboardSnapshotUseCase(settings=settings).execute(
        BuildAgentDashboardSnapshotRequest(metrics=metrics, snapshots=snapshots, as_of_date=metrics.end_date)
    ).snapshot
    snapshot_text_input = st.text_area(
        "portfolio_metrics_snapshot (JSON editable)",
        value=json.dumps(snapshot_default, ensure_ascii=False, indent=2),
        height=240,
    )
    _render_agent_input_dates(metrics.end_date, report_as_of, snapshot_default, report_option)
    _render_agent_inputs_summary(send_target_weights=send_target_weights, target_weights=target_weights)

    if st.button("Ejecutar red de agentes"):
        _run_agents_from_ui(
            settings=settings,
            report_option=report_option,
            report_text_input=report_text_input,
            snapshot_text_input=snapshot_text_input,
            metrics_end_date=metrics.end_date,
            target_weights_invalid=target_weights_invalid,
            send_target_weights=send_target_weights,
            target_weights=target_weights,
            investment_brief=investment_brief,
            user_interest=user_interest,
            monthly_budget=float(monthly_budget),
            llm_provider=llm_provider,
            search_provider=search_provider,
        )

    _render_persisted_agent_audit(settings)


def render_agent_result(name: str, result) -> None:
    with st.expander(f"{name}: {result.status}", expanded=True):
        st.write(result.summary)
        if result.warnings:
            st.warning("\n".join(f"- {warning}" for warning in result.warnings))
        if result.errors:
            st.error("\n".join(f"- {error}" for error in result.errors))
        render_agent_autonomy(result.metadata)
        if result.findings:
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "title": finding.title,
                            "category": finding.category,
                            "severity": finding.severity,
                            "asset_id": finding.asset_id,
                            "detail": finding.detail,
                        }
                        for finding in result.findings
                    ]
                ),
                width="stretch",
                hide_index=True,
            )
        st.json(
            {
                "metadata": dict(result.metadata),
                "sources": [source.location for source in result.sources],
                "artifacts": [artifact.title for artifact in result.artifacts],
            }
        )


def _render_persisted_agent_audit(settings: Settings) -> None:
    st.divider()
    section_header(
        "Auditoria de agentes",
        "Inspecciona runs guardados: plan interno, acciones, fuentes, prompts, warnings, inputs y outputs.",
    )
    runs = ListAgentRunsUseCase(settings=settings).execute(limit=30).runs
    if not runs:
        st.info("Todavia no hay runs persistidos de agentes en este entorno.")
        return

    selected = st.selectbox(
        "Run guardado",
        options=runs,
        format_func=lambda run: (
            f"{run.as_of_date or 'sin_fecha'} | {run.status} | {run.run_id}"
        ),
    )
    try:
        audit = GetAgentRunAuditUseCase(settings=settings).execute(
            GetAgentRunAuditRequest(run_id=selected.run_id)
        )
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        return

    _render_run_overview(audit.run_metadata, audit.input_payload, audit.output_dir)
    agent_tabs = st.tabs(["monitor_tematico", "analista_activos", "asistente_aportacion_mensual"])
    for tab, agent_name in zip(
        agent_tabs,
        ("monitor_tematico", "analista_activos", "asistente_aportacion_mensual"),
        strict=True,
    ):
        with tab:
            _render_agent_audit(agent_name, audit.agents.get(agent_name, {}))


def _render_run_overview(run_metadata: Mapping[str, Any], input_payload: Mapping[str, Any], output_dir: Path) -> None:
    st.markdown("#### Run")
    columns = st.columns(4)
    columns[0].metric("run_id", str(run_metadata.get("run_id") or output_dir.name))
    columns[1].metric("as_of_date", str(run_metadata.get("as_of_date") or "-"))
    columns[2].metric("generated_at", str(run_metadata.get("generated_at") or "-"))
    columns[3].metric("inputs", str(len(input_payload.get("inputs") or [])))

    agent_statuses = [
        {"agent": agent_name, "status": payload.get("status")}
        for agent_name, payload in (run_metadata.get("agents") or {}).items()
    ]
    if agent_statuses:
        st.dataframe(pd.DataFrame(agent_statuses), width="stretch", hide_index=True)

    with st.expander("Inputs del run", expanded=False):
        _render_input_refs(input_payload.get("inputs") or [], key_prefix="run")
    with st.expander("Prompt versions", expanded=False):
        st.json(run_metadata.get("prompt_versions") or {})
    st.caption(f"Directorio local: {output_dir}")


def _render_agent_audit(agent_name: str, audit: Mapping[str, Any]) -> None:
    parsed = audit.get("parsed_output") or {}
    metadata = parsed.get("metadata") or {}
    context = audit.get("context") or {}
    request = audit.get("request") or {}
    prompt_refs = audit.get("prompt_refs") or {}
    prompt_rendered = audit.get("prompt_rendered") or ""
    raw_response = audit.get("raw_response") or {}

    status = parsed.get("status") or "sin_datos"
    st.markdown(f"#### {agent_name}: `{status}`")
    if parsed.get("summary"):
        st.write(parsed.get("summary"))

    _render_warning_error_blocks(parsed)
    render_agent_autonomy(metadata)

    detail_tabs = st.tabs(["Outputs", "Fuentes", "Prompts", "Inputs", "Request", "Raw"])
    with detail_tabs[0]:
        _render_agent_outputs(parsed, metadata)
    with detail_tabs[1]:
        _render_sources(parsed)
    with detail_tabs[2]:
        _render_prompts(prompt_refs, prompt_rendered, key_prefix=agent_name)
    with detail_tabs[3]:
        _render_input_refs(context.get("input_refs") or [], key_prefix=agent_name)
    with detail_tabs[4]:
        st.json(request)
    with detail_tabs[5]:
        st.json(raw_response)


def _render_warning_error_blocks(parsed: Mapping[str, Any]) -> None:
    warnings = parsed.get("warnings") or []
    errors = parsed.get("errors") or []
    if warnings:
        st.warning("\n".join(f"- {warning}" for warning in warnings))
    if errors:
        st.error("\n".join(f"- {error}" for error in errors))


def _render_agent_outputs(parsed: Mapping[str, Any], metadata: Mapping[str, Any]) -> None:
    findings = parsed.get("findings") or []
    artifacts = parsed.get("artifacts") or []
    if findings:
        st.markdown("##### Findings")
        st.dataframe(pd.DataFrame(_flatten_findings(findings)), width="stretch", hide_index=True)
        with st.expander("Findings completos", expanded=False):
            st.json(findings)
    else:
        st.info("Este agente no dejo findings estructurados.")

    if artifacts:
        st.markdown("##### Artifacts")
        st.dataframe(pd.DataFrame(artifacts), width="stretch", hide_index=True)

    with st.expander("Metadata completa", expanded=False):
        st.json(metadata)


def _render_sources(parsed: Mapping[str, Any]) -> None:
    sources = parsed.get("sources") or []
    finding_sources = [
        source
        for finding in (parsed.get("findings") or [])
        for source in (finding.get("sources") or [])
    ]
    all_sources = _deduplicate_source_dicts([*sources, *finding_sources])
    if not all_sources:
        st.info("No hay fuentes registradas para este agente.")
        return
    st.dataframe(pd.DataFrame(_flatten_sources(all_sources)), width="stretch", hide_index=True)
    with st.expander("Fuentes completas", expanded=False):
        st.json(all_sources)


def _render_prompts(prompt_refs: Mapping[str, Any], prompt_rendered: str, *, key_prefix: str) -> None:
    prompts = prompt_refs.get("prompts") or []
    if prompts:
        st.dataframe(pd.DataFrame(prompts), width="stretch", hide_index=True)
    if prompt_rendered:
        st.text_area(
            "Prompt renderizado",
            value=prompt_rendered,
            height=360,
            disabled=True,
            key=f"audit_prompt_{key_prefix}",
        )
    else:
        st.info("No hay prompt renderizado guardado.")


def _render_input_refs(inputs: list[Mapping[str, Any]], *, key_prefix: str) -> None:
    if not inputs:
        st.info("No hay inputs registrados.")
        return
    st.dataframe(pd.DataFrame(_flatten_inputs(inputs)), width="stretch", hide_index=True)
    for index, input_ref in enumerate(inputs):
        metadata = input_ref.get("metadata") or {}
        content = metadata.get("content") or metadata.get("text")
        if content:
            with st.expander(f"Contenido: {input_ref.get('key')}", expanded=False):
                st.text_area(
                    str(input_ref.get("key")),
                    value=str(content),
                    height=220,
                    disabled=True,
                    key=f"audit_input_{key_prefix}_{index}_{input_ref.get('key')}",
                )


def render_agent_autonomy(metadata: Mapping[str, Any]) -> None:
    autonomy_keys = {
        "agent_plan",
        "allowed_actions",
        "selected_actions",
        "skipped_actions",
        "applied_constraints",
        "decision_basis",
    }
    if not any(key in metadata for key in autonomy_keys):
        return
    with st.container(border=True):
        st.markdown("##### Autonomia acotada")
        columns = st.columns(3)
        with columns[0]:
            st.markdown("**Plan interno**")
            st.markdown(_markdown_list(metadata.get("agent_plan") or ()))
        with columns[1]:
            st.markdown("**Acciones usadas**")
            st.markdown(_markdown_list(metadata.get("selected_actions") or ()))
        with columns[2]:
            st.markdown("**Restricciones**")
            st.markdown(_markdown_list(metadata.get("applied_constraints") or ()))
        skipped_actions = metadata.get("skipped_actions") or ()
        if skipped_actions:
            st.markdown("**Acciones descartadas**")
            st.dataframe(pd.DataFrame(skipped_actions), width="stretch", hide_index=True)
        allowed_actions = metadata.get("allowed_actions") or ()
        decision_basis = metadata.get("decision_basis") or ()
        if allowed_actions or decision_basis:
            lower = st.columns(2)
            with lower[0]:
                st.markdown("**Acciones permitidas**")
                st.markdown(_markdown_list(allowed_actions))
            with lower[1]:
                st.markdown("**Base de decision**")
                st.markdown(_markdown_list(decision_basis))


def _run_agents_from_ui(
    *,
    settings: Settings,
    report_option,
    report_text_input: str,
    snapshot_text_input: str,
    metrics_end_date: date,
    target_weights_invalid: bool,
    send_target_weights: bool,
    target_weights: dict[str, Any],
    investment_brief: str,
    user_interest: str,
    monthly_budget: float,
    llm_provider: str,
    search_provider: str,
) -> None:
    if report_option is None:
        st.error("No hay `latest_monthly_report` seleccionado. Genera o selecciona un informe primero.")
        return
    try:
        portfolio_metrics_snapshot = json.loads(snapshot_text_input)
        if not isinstance(portfolio_metrics_snapshot, dict):
            raise ValueError("snapshot_no_dict")
    except Exception:
        st.error("`portfolio_metrics_snapshot` no es JSON valido.")
        return
    snapshot_as_of = _snapshot_as_of_date(portfolio_metrics_snapshot)
    selected_report_date = extract_monthly_report_as_of_date(report_text_input, path=Path(report_option["path"]))
    if selected_report_date is None:
        st.error("El informe seleccionado no contiene una fecha `as_of_date` detectable.")
        return
    if selected_report_date != metrics_end_date:
        st.error(
            "Bloqueado: el informe mensual no corresponde a la cartera actual. "
            f"Informe: {selected_report_date.isoformat()} | cartera: {metrics_end_date.isoformat()}."
        )
        return
    if snapshot_as_of != metrics_end_date:
        st.error(
            "`portfolio_metrics_snapshot` no corresponde a la cartera actual. "
            f"Snapshot: {snapshot_as_of.isoformat() if snapshot_as_of else 'sin fecha'} | "
            f"cartera: {metrics_end_date.isoformat()}."
        )
        return
    if target_weights_invalid:
        st.error("`target_weights` esta activado pero no es un JSON valido.")
        return

    override_dir = settings.data_dir / "agents" / "input_overrides"
    override_dir.mkdir(parents=True, exist_ok=True)
    report_override_path = override_dir / f"latest_monthly_report_override_{selected_report_date.isoformat()}.md"
    report_override_path.write_text(report_text_input, encoding="utf-8")

    request_parameters: dict[str, Any] = {}
    if send_target_weights and target_weights:
        request_parameters["target_weights"] = target_weights
    try:
        with st.spinner("Ejecutando agentes..."):
            agents_result = RunMonthlyAgentsUseCase(settings=settings).execute(
                RunMonthlyAgentsRequest(
                    investment_brief_text=investment_brief,
                    monthly_report_path=report_override_path,
                    user_satellite_interest=user_interest or None,
                    monthly_budget=monthly_budget,
                    llm_provider=llm_provider,
                    search_provider=search_provider,
                    persist=True,
                    request_parameters=request_parameters,
                    portfolio_metrics_snapshot=portfolio_metrics_snapshot,
                )
            )
            result = agents_result.pipeline_result
    except ValueError as exc:
        st.error(str(exc))
        return
    st.success(f"Run {result.run_id} guardado en {result.output_dir}")
    render_agent_result("monitor_tematico", result.monitor_tematico)
    render_agent_result("analista_activos", result.analista_activos)
    render_agent_result("asistente_aportacion_mensual", result.asistente_aportacion_mensual)


def _render_agent_input_dates(
    metrics_end_date: date,
    report_as_of: date | None,
    snapshot_default: Mapping[str, Any],
    report_option,
) -> None:
    st.markdown("#### Fechas de entrada")
    st.json(
        {
            "portfolio_metrics_current": metrics_end_date.isoformat(),
            "monthly_report_selected": report_as_of.isoformat() if report_as_of else None,
            "portfolio_metrics_snapshot_default": snapshot_default.get("as_of_date"),
        }
    )
    if report_as_of is None and report_option is not None:
        st.warning("No se ha podido detectar `as_of_date` en el informe seleccionado.")
    elif report_as_of is not None and report_as_of != metrics_end_date:
        st.error(
            "El informe mensual seleccionado no corresponde a la fecha valorada actual. "
            f"Informe: {report_as_of.isoformat()} | cartera: {metrics_end_date.isoformat()}. "
            "Genera un informe nuevo antes de ejecutar agentes."
        )


def _render_agent_inputs_summary(*, send_target_weights: bool, target_weights: Mapping[str, Any]) -> None:
    with st.expander("Inputs que recibiran los agentes", expanded=True):
        st.markdown("#### Resumen de inputs")
        st.markdown(
            "- `investment_brief`\n"
            "- `latest_monthly_report` (editable arriba)\n"
            "- `portfolio_metrics_snapshot` (JSON editable arriba)\n"
            "- `monthly_budget` (EUR, editable en configuracion)\n"
            "- `target_weights` (opcional)\n"
            "- `user_satellite_interest` (opcional)"
        )
        st.markdown("#### Flujo de ejecucion")
        st.markdown(
            "1. `monitor_tematico` lee brief+informe+snapshot y genera contexto.\n"
            "2. `analista_activos` consume lo anterior y juzga encaje por activo.\n"
            "3. `asistente_aportacion_mensual` sintetiza ambos y propone accion mensual."
        )
        st.markdown("#### Target weights (opcional)")
        if not send_target_weights:
            st.json({"_status": "desactivado_por_usuario"})
        else:
            st.json(target_weights if target_weights else {"_status": "no definido"})


def _snapshot_as_of_date(portfolio_metrics_snapshot: Mapping[str, Any]) -> date | None:
    snapshot_as_of_raw = portfolio_metrics_snapshot.get("as_of_date")
    try:
        return date.fromisoformat(str(snapshot_as_of_raw)[:10]) if snapshot_as_of_raw else None
    except ValueError:
        st.error("`portfolio_metrics_snapshot.as_of_date` no es una fecha valida.")
        return None


def _markdown_list(values: Any) -> str:
    if not values:
        return "- _Sin datos_"
    if isinstance(values, str):
        values = (values,)
    return "\n".join(f"- {value}" for value in values)


def _flatten_findings(findings: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for finding in findings:
        rows.append(
            {
                "title": finding.get("title"),
                "category": finding.get("category"),
                "severity": finding.get("severity"),
                "asset_id": finding.get("asset_id"),
                "tags": ", ".join(finding.get("tags") or []),
                "sources": len(finding.get("sources") or []),
                "detail": finding.get("detail"),
            }
        )
    return rows


def _flatten_sources(sources: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "type": source.get("source_type"),
            "label": source.get("label"),
            "location": source.get("location"),
            "retrieved_at": source.get("retrieved_at"),
            "effective_date": source.get("effective_date"),
            "provider": (source.get("metadata") or {}).get("provider"),
            "query": (source.get("metadata") or {}).get("query"),
        }
        for source in sources
    ]


def _flatten_inputs(inputs: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for input_ref in inputs:
        metadata = input_ref.get("metadata") or {}
        rows.append(
            {
                "key": input_ref.get("key"),
                "label": input_ref.get("label"),
                "source_type": input_ref.get("source_type"),
                "as_of_date": input_ref.get("as_of_date"),
                "location": input_ref.get("location"),
                "metadata_keys": ", ".join(sorted(str(key) for key in metadata.keys())),
            }
        )
    return rows


def _deduplicate_source_dicts(sources: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped = []
    for source in sources:
        key = (str(source.get("source_type")), str(source.get("location")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(source)
    return deduped
