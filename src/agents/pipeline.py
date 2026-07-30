"""Reusable monthly agent pipeline for CLI and Streamlit."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass, replace
from datetime import date, datetime
import hashlib
import json
from math import isfinite
from pathlib import Path
import re
from typing import Any, Mapping

import pandas as pd

from src.agents.analista_activos import AnalistaActivosAgent, OpenAIAssetLLMProvider, StaticAssetLLMProvider
from src.agents.asistente_aportacion_mensual import (
    AsistenteAportacionMensualAgent,
    OpenAIContributionLLMProvider,
    StaticContributionLLMProvider,
)
from src.agents.models import AgentContext, AgentInputRef, AgentRequest, AgentResult, build_agent_context
from src.agents.provider_audit import (
    provider_audit_config,
    providers_raw_response_audit,
    redact_sensitive_audit_payload,
)
from src.agents.prompts import load_prompt, prompt_version
from src.agents.monitor_tematico import (
    DuckDuckGoHtmlSearchProvider,
    MonitorTematicoAgent,
    NullSearchProvider,
    OpenAIThemeLLMProvider,
    StaticSearchProvider,
    StaticThemeLLMProvider,
    TavilySearchProvider,
)
from src.config import Settings, get_settings
from src.market_data import load_asset_overrides_frame
from src.portfolio import PortfolioMetricsResult, calculate_portfolio_metrics_from_normalized_degiro
from src.portfolio.targets import PortfolioTargets, load_portfolio_targets
from src.reports import generate_monthly_report, get_latest_monthly_report


AUDIT_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class MonthlyAgentPipelineResult:
    """Results and persisted artifact paths for one monthly agent pipeline run."""

    run_id: str
    as_of_date: date
    input_refs: tuple[AgentInputRef, ...]
    monitor_tematico: AgentResult
    analista_activos: AgentResult
    asistente_aportacion_mensual: AgentResult
    output_dir: Path | None = None
    agent_requests: Mapping[str, AgentRequest] = field(default_factory=dict)
    agent_contexts: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    prompt_audits: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    provider_configs: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    raw_responses: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)


def run_monthly_agent_pipeline(
    *,
    settings: Settings | None = None,
    investment_brief_text: str | None = None,
    investment_brief_path: str | Path | None = None,
    monthly_report_path: str | Path | None = None,
    metrics: PortfolioMetricsResult | None = None,
    user_satellite_interest: str | None = None,
    llm_provider: str = "static",
    search_provider: str = "null",
    persist: bool = True,
    output_dir: str | Path | None = None,
    request_scope: Mapping[str, Any] | None = None,
    request_parameters: Mapping[str, Any] | None = None,
    request_constraints: Mapping[str, Any] | None = None,
    request_metadata: Mapping[str, Any] | None = None,
    portfolio_metrics_snapshot: Mapping[str, Any] | None = None,
    monthly_budget: float | None = None,
) -> MonthlyAgentPipelineResult:
    """Run monitor, asset analyst, and monthly assistant with shared inputs."""
    resolved_settings = get_settings() if settings is None else settings
    resolved_metrics = metrics or calculate_portfolio_metrics_from_normalized_degiro(settings=resolved_settings)
    report_path, report_text, report_date = _resolve_monthly_report(
        settings=resolved_settings,
        monthly_report_path=monthly_report_path,
    )
    snapshot_date = _extract_snapshot_as_of_date(portfolio_metrics_snapshot)
    as_of_date = report_date or snapshot_date or resolved_metrics.end_date
    generated_at = datetime.now().astimezone()
    run_id = generated_at.strftime("%Y%m%dT%H%M%S%f")
    investment_brief, investment_location = _resolve_investment_brief(
        settings=resolved_settings,
        investment_brief_text=investment_brief_text,
        investment_brief_path=investment_brief_path,
    )
    metrics_snapshot = (
        _json_ready(dict(portfolio_metrics_snapshot))
        if portfolio_metrics_snapshot is not None
        else build_portfolio_metrics_snapshot(resolved_metrics, as_of_date=as_of_date)
    )
    metrics_snapshot = prepare_agent_metrics_snapshot(metrics_snapshot, settings=resolved_settings)
    portfolio_targets = load_portfolio_targets(settings=resolved_settings)
    report_text = _append_agent_asset_reference(report_text, metrics_snapshot=metrics_snapshot)
    _validate_agent_input_dates(
        monthly_report_date=report_date,
        metrics_snapshot=metrics_snapshot,
        fallback_as_of_date=as_of_date,
    )

    common_refs = _build_common_input_refs(
        investment_brief=investment_brief,
        investment_location=investment_location,
        monthly_report_path=report_path,
        monthly_report_text=report_text,
        monthly_report_date=as_of_date,
        metrics_snapshot=metrics_snapshot,
        user_satellite_interest=user_satellite_interest,
        portfolio_targets=portfolio_targets,
        portfolio_targets_location=str(resolved_settings.portfolio_targets_path),
    )
    base_request = AgentRequest(
        scope=dict(request_scope or {}),
        parameters=dict(request_parameters or {}),
        constraints=dict(request_constraints or {}),
        metadata=dict(request_metadata or {}),
    )
    # Snapshot prompts before execution so the audit cannot drift if files change mid-run.
    prompt_audits = {
        agent_name: {
            "prompt_refs": {
                "schema_version": AUDIT_SCHEMA_VERSION,
                **_agent_prompt_refs(agent_name),
            },
            "prompt_rendered": _agent_prompt_markdown(agent_name),
        }
        for agent_name in _agent_prompt_keys()
    }

    monitor_search_provider = _build_search_provider(search_provider)
    monitor_llm_provider = _build_monitor_llm_provider(llm_provider)
    monitor_agent = MonitorTematicoAgent(
        search_provider=monitor_search_provider,
        llm_provider=monitor_llm_provider,
    )
    monitor_context = build_agent_context(
        agent_name=monitor_agent.name,
        as_of_date=as_of_date,
        generated_at=generated_at,
        base_currency=resolved_settings.default_currency,
        settings=resolved_settings,
        input_refs=common_refs,
        run_id=run_id,
    )
    monitor_request = _request_for_context(base_request, monitor_context)
    monitor_result = monitor_agent.execute(monitor_request, monitor_context)

    monitor_ref = _result_input_ref("monitor_tematico_result", "Monitor tematico result", monitor_result)
    analista_llm_provider = _build_asset_llm_provider(llm_provider)
    analista_agent = AnalistaActivosAgent(llm_provider=analista_llm_provider)
    analista_context = build_agent_context(
        agent_name=analista_agent.name,
        as_of_date=as_of_date,
        generated_at=generated_at,
        base_currency=resolved_settings.default_currency,
        settings=resolved_settings,
        input_refs=(*common_refs, monitor_ref),
        run_id=run_id,
    )
    analista_request = _request_for_context(base_request, analista_context)
    analista_result = analista_agent.execute(analista_request, analista_context)

    analista_ref = _result_input_ref("analista_activos_result", "Analista activos result", analista_result)
    asistente_llm_provider = _build_contribution_llm_provider(llm_provider)
    asistente_agent = AsistenteAportacionMensualAgent(llm_provider=asistente_llm_provider)
    asistente_context = build_agent_context(
        agent_name=asistente_agent.name,
        as_of_date=as_of_date,
        generated_at=generated_at,
        base_currency=resolved_settings.default_currency,
        settings=resolved_settings,
        input_refs=(*common_refs, monitor_ref, analista_ref),
        metadata={
            "monthly_budget": (
                float(monthly_budget)
                if monthly_budget is not None
                else (
                    portfolio_targets.monthly_contribution
                    if portfolio_targets is not None and portfolio_targets.monthly_contribution is not None
                    else resolved_settings.monthly_contribution_eur
                )
            )
        },
        run_id=run_id,
    )
    asistente_request = _request_for_context(base_request, asistente_context)
    asistente_result = asistente_agent.execute(asistente_request, asistente_context)

    agent_requests = {
        "monitor_tematico": monitor_request,
        "analista_activos": analista_request,
        "asistente_aportacion_mensual": asistente_request,
    }
    agent_contexts = {
        "monitor_tematico": _serialize_agent_context(monitor_context),
        "analista_activos": _serialize_agent_context(analista_context),
        "asistente_aportacion_mensual": _serialize_agent_context(asistente_context),
    }
    provider_configs = {
        "monitor_tematico": {
            "llm": provider_audit_config(monitor_llm_provider, role="llm"),
            "search": provider_audit_config(monitor_search_provider, role="search"),
        },
        "analista_activos": {
            "llm": provider_audit_config(analista_llm_provider, role="llm"),
        },
        "asistente_aportacion_mensual": {
            "llm": provider_audit_config(asistente_llm_provider, role="llm"),
        },
    }
    raw_responses = {
        "monitor_tematico": providers_raw_response_audit(
            {
                "llm": monitor_llm_provider,
                "search": monitor_search_provider,
            }
        ),
        "analista_activos": providers_raw_response_audit({"llm": analista_llm_provider}),
        "asistente_aportacion_mensual": providers_raw_response_audit(
            {"llm": asistente_llm_provider}
        ),
    }

    resolved_output_dir = None
    result = MonthlyAgentPipelineResult(
        run_id=run_id,
        as_of_date=as_of_date,
        input_refs=common_refs,
        monitor_tematico=monitor_result,
        analista_activos=analista_result,
        asistente_aportacion_mensual=asistente_result,
        output_dir=None,
        agent_requests=agent_requests,
        agent_contexts=agent_contexts,
        prompt_audits=prompt_audits,
        provider_configs=provider_configs,
        raw_responses=raw_responses,
    )
    if persist:
        resolved_output_dir = _persist_pipeline_result(
            result,
            settings=resolved_settings,
            output_dir=output_dir,
        )
        result = replace(result, output_dir=resolved_output_dir)
    return result


def _request_for_context(request: AgentRequest, context: AgentContext) -> AgentRequest:
    """Bind one request to the exact input references available to the agent."""
    return AgentRequest(
        scope=dict(request.scope),
        parameters=dict(request.parameters),
        constraints=dict(request.constraints),
        input_refs=context.available_input_keys,
        metadata=dict(request.metadata),
    )


def build_portfolio_metrics_snapshot(metrics: PortfolioMetricsResult, *, as_of_date: date) -> dict[str, Any]:
    """Build the compact metrics payload consumed by agents and Streamlit."""
    daily = metrics.portfolio_daily_metrics.copy()
    daily["valuation_date"] = pd.to_datetime(daily["valuation_date"]).dt.date
    positions = metrics.position_metrics.copy()
    positions["valuation_date"] = pd.to_datetime(positions["valuation_date"]).dt.date
    eligible_daily = daily.loc[daily["valuation_date"] <= as_of_date]
    if eligible_daily.empty:
        raise ValueError(f"No portfolio metrics available on or before {as_of_date.isoformat()}.")
    daily_row = eligible_daily.iloc[-1].to_dict()
    resolved_date = daily_row["valuation_date"]
    current_positions = positions.loc[positions["valuation_date"] == resolved_date].copy()
    current_positions = current_positions.sort_values(["weight", "asset_name"], ascending=[False, True])
    selected_columns = [
        "asset_id",
        "asset_name",
        "asset_type",
        "isin",
        "quantity",
        "market_value_base",
        "cost_basis_base",
        "unrealized_pnl_base",
        "unrealized_return_pct",
        "weight",
        "valuation_status",
    ]
    return {
        "as_of_date": resolved_date.isoformat(),
        "base_currency": metrics.base_currency,
        "daily": _json_ready(daily_row),
        "positions": _json_ready(current_positions.loc[:, selected_columns].to_dict(orient="records")),
    }


def prepare_agent_metrics_snapshot(
    metrics_snapshot: Mapping[str, Any],
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Return a JSON-ready agent snapshot enriched with local asset overrides."""
    resolved_settings = get_settings() if settings is None else settings
    return _apply_asset_overrides_to_metrics_snapshot(
        _json_ready(dict(metrics_snapshot)),
        settings=resolved_settings,
    )


def load_investment_brief(*, settings: Settings | None = None, path: str | Path | None = None) -> str:
    """Read the configured investment brief text."""
    resolved_settings = get_settings() if settings is None else settings
    brief_path = resolved_settings.investment_brief_path if path is None else Path(path).expanduser().resolve()
    if not brief_path.exists():
        raise FileNotFoundError(
            f"Investment brief not found: {brief_path}. Create it or pass investment_brief_text explicitly."
        )
    return brief_path.read_text(encoding="utf-8")


def extract_monthly_report_as_of_date(content: str, *, path: Path | None = None) -> date | None:
    """Extract the effective date from a monthly report Markdown document."""
    patterns = [
        r"(?im)^\s*as_of_date\s*:\s*['\"]?(\d{4}-\d{2}-\d{2})['\"]?\s*$",
        r"(?im)^\s*date\s*:\s*['\"]?(\d{4}-\d{2}-\d{2})['\"]?\s*$",
        r"(?im)^\s*fecha\s*:\s*['\"]?(\d{4}-\d{2}-\d{2})['\"]?\s*$",
        r"(?i)\bas[_ -]?of[_ -]?date\b[^0-9]{0,20}(\d{4}-\d{2}-\d{2})",
        r"(?i)\binforme mensual\b[^\n]*(\d{4}-\d{2}-\d{2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, content)
        if match:
            return date.fromisoformat(match.group(1))
    if path is not None:
        match = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        if match:
            return date.fromisoformat(match.group(1))
    return None


def _resolve_monthly_report(
    *,
    settings: Settings,
    monthly_report_path: str | Path | None,
) -> tuple[Path, str, date | None]:
    if monthly_report_path is not None:
        path = Path(monthly_report_path).expanduser().resolve()
        content = path.read_text(encoding="utf-8")
        return path, content, extract_monthly_report_as_of_date(content, path=path)
    latest = get_latest_monthly_report(settings=settings)
    if latest is None:
        report = generate_monthly_report(settings=settings, persist=True)
        if report.output_path is None:
            raise ValueError("Monthly report generation did not produce an output path.")
        return report.output_path, report.content, report.as_of_date
    path = Path(latest.report_path).expanduser().resolve()
    return path, path.read_text(encoding="utf-8"), latest.as_of_date


def _extract_snapshot_as_of_date(snapshot: Mapping[str, Any] | None) -> date | None:
    if snapshot is None:
        return None
    raw_date = snapshot.get("as_of_date")
    if raw_date is None:
        daily = snapshot.get("daily")
        if isinstance(daily, Mapping):
            raw_date = daily.get("valuation_date")
    if isinstance(raw_date, datetime):
        return raw_date.date()
    if isinstance(raw_date, date):
        return raw_date
    if isinstance(raw_date, str) and raw_date.strip():
        return date.fromisoformat(raw_date[:10])
    return None


def _validate_agent_input_dates(
    *,
    monthly_report_date: date | None,
    metrics_snapshot: Mapping[str, Any],
    fallback_as_of_date: date,
) -> None:
    snapshot_date = _extract_snapshot_as_of_date(metrics_snapshot)
    if snapshot_date is None:
        raise ValueError("portfolio_metrics_snapshot must include an `as_of_date` or `daily.valuation_date`.")
    if monthly_report_date is not None and monthly_report_date != snapshot_date:
        raise ValueError(
            "Monthly agent inputs have inconsistent dates: "
            f"monthly_report={monthly_report_date.isoformat()} "
            f"portfolio_metrics_snapshot={snapshot_date.isoformat()}. "
            "Generate/select a monthly report for the same date as the snapshot before running agents."
        )
    if snapshot_date != fallback_as_of_date:
        raise ValueError(
            "Monthly agent input date resolution failed: "
            f"pipeline_as_of_date={fallback_as_of_date.isoformat()} "
            f"portfolio_metrics_snapshot={snapshot_date.isoformat()}."
        )


def _resolve_investment_brief(
    *,
    settings: Settings,
    investment_brief_text: str | None,
    investment_brief_path: str | Path | None,
) -> tuple[str, str]:
    if investment_brief_text:
        return investment_brief_text, "manual://investment-brief"
    path = settings.investment_brief_path if investment_brief_path is None else Path(investment_brief_path).expanduser().resolve()
    return load_investment_brief(settings=settings, path=path), str(path)


def _build_common_input_refs(
    *,
    investment_brief: str,
    investment_location: str,
    monthly_report_path: Path,
    monthly_report_text: str,
    monthly_report_date: date,
    metrics_snapshot: dict[str, Any],
    user_satellite_interest: str | None,
    portfolio_targets: PortfolioTargets | None = None,
    portfolio_targets_location: str | None = None,
) -> tuple[AgentInputRef, ...]:
    refs = [
        AgentInputRef(
            key="investment_brief",
            label="Investment brief",
            location=investment_location,
            source_type="manual",
            metadata={"content": investment_brief},
        ),
        AgentInputRef(
            key="latest_monthly_report",
            label="Latest monthly report",
            location=str(monthly_report_path),
            source_type="report",
            as_of_date=monthly_report_date,
            metadata={"content": monthly_report_text, "positions": metrics_snapshot.get("positions", [])},
        ),
        AgentInputRef(
            key="portfolio_metrics_snapshot",
            label="Portfolio metrics snapshot",
            location="derived://portfolio_metrics_snapshot",
            source_type="derived",
            as_of_date=monthly_report_date,
            metadata={
                **metrics_snapshot,
                "content": _canonical_json_text(metrics_snapshot),
            },
        ),
    ]
    if user_satellite_interest:
        refs.append(
            AgentInputRef(
                key="user_satellite_interest",
                label="User satellite interest",
                location="manual://user-satellite-interest",
                source_type="manual",
                metadata={"text": user_satellite_interest},
            )
        )
    if portfolio_targets is not None:
        payload = portfolio_targets.to_agent_payload()
        refs.append(
            AgentInputRef(
                key="target_weights",
                label="Portfolio targets",
                location=portfolio_targets_location or "manual://portfolio-targets",
                source_type="manual",
                metadata={
                    "weights": portfolio_targets.target_weights(),
                    "target_weights": portfolio_targets.target_weights(),
                    "portfolio_targets": payload,
                    "content": _canonical_json_text(payload),
                },
            )
        )
    return tuple(refs)


def _result_input_ref(key: str, label: str, result: AgentResult) -> AgentInputRef:
    payload = _serialize_agent_result_for_input_ref(result)
    return AgentInputRef(
        key=key,
        label=label,
        location=f"derived://{key}",
        source_type="derived",
        metadata={
            "findings": result.findings,
            "content": _canonical_json_text(payload),
            "summary": result.summary,
            "status": result.status,
        },
    )


def _build_monitor_llm_provider(provider_name: str):
    if provider_name == "static":
        return StaticThemeLLMProvider()
    if provider_name == "openai":
        return OpenAIThemeLLMProvider()
    raise ValueError(f"Unsupported agent LLM provider: {provider_name}")


def _build_asset_llm_provider(provider_name: str):
    if provider_name == "static":
        return StaticAssetLLMProvider()
    if provider_name == "openai":
        return OpenAIAssetLLMProvider()
    raise ValueError(f"Unsupported agent LLM provider: {provider_name}")


def _build_contribution_llm_provider(provider_name: str):
    if provider_name == "static":
        return StaticContributionLLMProvider()
    if provider_name == "openai":
        return OpenAIContributionLLMProvider()
    raise ValueError(f"Unsupported agent LLM provider: {provider_name}")


def _build_search_provider(provider_name: str):
    if provider_name == "null":
        return NullSearchProvider()
    if provider_name == "static":
        return StaticSearchProvider()
    if provider_name == "duckduckgo":
        return DuckDuckGoHtmlSearchProvider()
    if provider_name == "tavily":
        return TavilySearchProvider()
    raise ValueError(f"Unsupported search provider: {provider_name}")


def _persist_pipeline_result(
    result: MonthlyAgentPipelineResult,
    *,
    settings: Settings,
    output_dir: str | Path | None,
) -> Path:
    base_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else settings.data_dir / "agents" / "monthly_pipeline" / result.run_id
    )
    base_dir.mkdir(parents=True, exist_ok=True)
    agent_payloads = _build_agent_audit_payloads(result, settings=settings)
    input_hash = _stable_hash(
        {
            name: payload["audit_metadata"]["input_hash"]
            for name, payload in sorted(agent_payloads.items())
        }
    )
    output_hash = _stable_hash(
        {
            name: payload["audit_metadata"]["output_hash"]
            for name, payload in sorted(agent_payloads.items())
        }
    )
    _write_json(
        base_dir / "pipeline_result.json",
        {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "input_hash": input_hash,
            "output_hash": output_hash,
            **_serialize_pipeline_result(result),
        },
    )
    _persist_pipeline_audit_trail(
        result,
        settings=settings,
        base_dir=base_dir,
        agent_payloads=agent_payloads,
        input_hash=input_hash,
        output_hash=output_hash,
    )
    return base_dir


def _persist_pipeline_audit_trail(
    result: MonthlyAgentPipelineResult,
    *,
    settings: Settings,
    base_dir: Path,
    agent_payloads: Mapping[str, Mapping[str, Any]],
    input_hash: str,
    output_hash: str,
) -> None:
    generated_at = _audit_generated_at(result)
    run_metadata = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "run_id": result.run_id,
        "as_of_date": result.as_of_date.isoformat(),
        "generated_at": generated_at,
        "base_currency": settings.default_currency,
        "output_dir": str(base_dir),
        "pipeline_result_path": str(base_dir / "pipeline_result.json"),
        "hash_algorithm": "sha256",
        "input_hash": input_hash,
        "output_hash": output_hash,
        "agents": {
            agent_name: {
                "status": payload["parsed_output"]["status"],
                "input_hash": payload["audit_metadata"]["input_hash"],
                "output_hash": payload["audit_metadata"]["output_hash"],
                "providers": payload["provider"]["providers"],
            }
            for agent_name, payload in agent_payloads.items()
        },
        "prompt_versions": {
            agent_name: {
                str(prompt["key"]): str(prompt["version"])
                for prompt in payload["prompt_refs"].get("prompts", [])
            }
            for agent_name, payload in agent_payloads.items()
        },
    }
    _write_json(base_dir / "run_metadata.json", run_metadata)
    _write_json(
        base_dir / "input_payload.json",
        {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "run_id": result.run_id,
            "as_of_date": result.as_of_date.isoformat(),
            "input_hash": input_hash,
            "inputs": [_serialize_input_ref_full(input_ref) for input_ref in result.input_refs],
        },
    )

    for agent_name, payload in agent_payloads.items():
        agent_dir = base_dir / "agents" / agent_name
        agent_dir.mkdir(parents=True, exist_ok=True)
        _write_json(agent_dir / "context.json", payload["context"])
        _write_json(agent_dir / "request.json", payload["request"])
        _write_json(agent_dir / "prompt_refs.json", payload["prompt_refs"])
        _write_text(agent_dir / "prompt_rendered.md", str(payload["prompt_rendered"]))
        _write_json(agent_dir / "provider.json", payload["provider"])
        _write_json(agent_dir / "raw_response.json", payload["raw_response"])
        _write_json(agent_dir / "parsed_output.json", payload["parsed_output"])
        _write_json(agent_dir / "audit_metadata.json", payload["audit_metadata"])


def _build_agent_audit_payloads(
    result: MonthlyAgentPipelineResult,
    *,
    settings: Settings,
) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for agent_name, agent_result, input_refs, context_metadata in _agent_audit_specs(result):
        context_snapshot = result.agent_contexts.get(agent_name)
        context_payload = (
            _json_ready(dict(context_snapshot))
            if context_snapshot is not None
            else {
                "schema_version": AUDIT_SCHEMA_VERSION,
                "agent_name": agent_name,
                "run_id": result.run_id,
                "as_of_date": result.as_of_date.isoformat(),
                "generated_at": _audit_generated_at(result),
                "base_currency": settings.default_currency,
                "input_refs": [_serialize_input_ref_full(input_ref) for input_ref in input_refs],
                "metadata": _json_ready(context_metadata),
            }
        )
        context_payload = redact_sensitive_audit_payload(context_payload)
        request = result.agent_requests.get(agent_name) or AgentRequest(
            input_refs=tuple(input_ref.key for input_ref in input_refs)
        )
        # request.json intentionally stays a pure AgentRequest payload for round-trips.
        request_payload = redact_sensitive_audit_payload(
            _serialize_agent_request(request)
        )
        prompt_snapshot = result.prompt_audits.get(agent_name) or {}
        prompt_refs = redact_sensitive_audit_payload(
            dict(
                prompt_snapshot.get("prompt_refs")
                or {
                    "schema_version": AUDIT_SCHEMA_VERSION,
                    **_agent_prompt_refs(agent_name),
                }
            )
        )
        prompt_rendered = str(
            prompt_snapshot.get("prompt_rendered") or _agent_prompt_markdown(agent_name)
        )
        provider_payload = redact_sensitive_audit_payload(
            {
                "schema_version": AUDIT_SCHEMA_VERSION,
                "providers": _json_ready(
                    dict(result.provider_configs.get(agent_name) or {})
                ),
            }
        )
        raw_response = redact_sensitive_audit_payload(
            dict(
                result.raw_responses.get(agent_name)
                or {
                    "schema_version": AUDIT_SCHEMA_VERSION,
                    "status": "not_captured",
                    "reason_code": "provider_contract_no_raw_response",
                    "providers": {},
                    "responses": [],
                }
            )
        )
        parsed_output = redact_sensitive_audit_payload(
            {
                "schema_version": AUDIT_SCHEMA_VERSION,
                **_serialize_agent_result(agent_result),
            }
        )
        audit_metadata = {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "hash_algorithm": "sha256",
            "hash_projection": "semantic-v1",
            "input_hash": _stable_hash(
                {
                    "context": _context_payload_for_hash(context_payload),
                    "request": _semantic_hash_payload(
                        request_payload,
                        excluded_keys=frozenset(
                            {"run_id", "generated_at", "retrieved_at"}
                        ),
                    ),
                    "prompt_refs": prompt_refs,
                    "prompt_rendered": prompt_rendered,
                    "provider": provider_payload,
                }
            ),
            "output_hash": _stable_hash(_output_payload_for_hash(parsed_output)),
        }
        payloads[agent_name] = {
            "context": context_payload,
            "request": request_payload,
            "prompt_refs": prompt_refs,
            "prompt_rendered": prompt_rendered,
            "provider": provider_payload,
            "raw_response": raw_response,
            "parsed_output": parsed_output,
            "audit_metadata": audit_metadata,
        }
    return payloads


def _agent_audit_specs(
    result: MonthlyAgentPipelineResult,
) -> tuple[tuple[str, AgentResult, tuple[AgentInputRef, ...], Mapping[str, Any]], ...]:
    return (
        (
            "monitor_tematico",
            result.monitor_tematico,
            result.input_refs,
            {},
        ),
        (
            "analista_activos",
            result.analista_activos,
            (
                *result.input_refs,
                _result_input_ref(
                    "monitor_tematico_result",
                    "Monitor tematico result",
                    result.monitor_tematico,
                ),
            ),
            {},
        ),
        (
            "asistente_aportacion_mensual",
            result.asistente_aportacion_mensual,
            (
                *result.input_refs,
                _result_input_ref(
                    "monitor_tematico_result",
                    "Monitor tematico result",
                    result.monitor_tematico,
                ),
                _result_input_ref(
                    "analista_activos_result",
                    "Analista activos result",
                    result.analista_activos,
                ),
            ),
            {},
        ),
    )


def _serialize_agent_context(context: AgentContext) -> dict[str, Any]:
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "agent_name": context.agent_name,
        "run_id": context.run_id,
        "as_of_date": context.as_of_date.isoformat(),
        "generated_at": context.generated_at.isoformat(),
        "base_currency": context.base_currency,
        "input_refs": [_serialize_input_ref_full(input_ref) for input_ref in context.input_refs],
        "metadata": _json_ready(dict(context.metadata)),
    }


def _context_payload_for_hash(context_payload: Mapping[str, Any]) -> dict[str, Any]:
    return _semantic_hash_payload(
        context_payload,
        excluded_keys=frozenset({"run_id", "generated_at", "retrieved_at"}),
    )


def _output_payload_for_hash(output_payload: Mapping[str, Any]) -> dict[str, Any]:
    return _semantic_hash_payload(
        output_payload,
        excluded_keys=frozenset({"run_id", "generated_at", "retrieved_at"}),
    )


def _semantic_hash_payload(
    value: Any,
    *,
    excluded_keys: frozenset[str],
) -> Any:
    """Remove runtime-only data while preserving semantic content for hashing."""
    ready = _json_ready(value)
    if isinstance(ready, Mapping):
        projected: dict[str, Any] = {}
        for key, item in ready.items():
            key_str = str(key)
            if key_str in excluded_keys:
                continue
            if key_str in {"location", "path"} and _looks_like_local_path(item):
                projected[key_str] = "<local-path>"
            else:
                projected[key_str] = _semantic_hash_payload(
                    item,
                    excluded_keys=excluded_keys,
                )
        return projected
    if isinstance(ready, list):
        return [
            _semantic_hash_payload(item, excluded_keys=excluded_keys)
            for item in ready
        ]
    return ready


def _looks_like_local_path(value: Any) -> bool:
    if not isinstance(value, str) or "://" in value:
        return False
    return bool(re.match(r"^[A-Za-z]:[\\/]", value)) or Path(value).is_absolute()


def _audit_generated_at(result: MonthlyAgentPipelineResult) -> str | None:
    for context in result.agent_contexts.values():
        generated_at = context.get("generated_at")
        if generated_at:
            return str(generated_at)
    for input_ref in result.input_refs:
        if input_ref.generated_at is not None:
            return input_ref.generated_at.isoformat()
    return None


def _agent_prompt_refs(agent_name: str) -> dict[str, Any]:
    prompt_keys = _agent_prompt_keys().get(agent_name, ())
    return {
        "agent_name": agent_name,
        "prompts": [
            {
                "key": key,
                "version": prompt_version(key),
            }
            for key in prompt_keys
        ],
    }


def _agent_prompt_markdown(agent_name: str) -> str:
    sections = [f"# {agent_name} prompts"]
    for key in _agent_prompt_keys().get(agent_name, ()):
        sections.append(f"## {key} ({prompt_version(key)})\n\n{load_prompt(key)}")
    return "\n\n".join(sections).strip() + "\n"


def _agent_prompt_keys() -> dict[str, tuple[str, ...]]:
    return {
        "monitor_tematico": ("monitor_tematico.query", "monitor_tematico.synthesis"),
        "analista_activos": ("analista_activos.analysis",),
        "asistente_aportacion_mensual": ("asistente_aportacion_mensual.decision",),
    }


def _serialize_input_ref_full(input_ref: AgentInputRef) -> dict[str, Any]:
    return {
        "key": input_ref.key,
        "label": input_ref.label,
        "location": input_ref.location,
        "source_type": input_ref.source_type,
        "as_of_date": input_ref.as_of_date.isoformat() if input_ref.as_of_date else None,
        "generated_at": input_ref.generated_at.isoformat() if input_ref.generated_at else None,
        "description": input_ref.description,
        "metadata": _json_ready(dict(input_ref.metadata)),
    }


def _serialize_agent_request(request: AgentRequest) -> dict[str, Any]:
    return {
        "scope": _json_ready(dict(request.scope)),
        "parameters": _json_ready(dict(request.parameters)),
        "constraints": _json_ready(dict(request.constraints)),
        "input_refs": list(request.input_refs),
        "metadata": _json_ready(dict(request.metadata)),
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    safe_payload = redact_sensitive_audit_payload(
        _json_ready(dict(payload))
    )
    _write_text(
        path,
        json.dumps(
            safe_payload,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ),
    )


def _write_text(path: Path, content: str) -> None:
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)


def _stable_hash(payload: Any) -> str:
    canonical = _canonical_json_text(payload)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _canonical_json_text(payload: Any) -> str:
    return json.dumps(
        redact_sensitive_audit_payload(_json_ready(payload)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _serialize_pipeline_result(result: MonthlyAgentPipelineResult) -> dict[str, Any]:
    return {
        "run_id": result.run_id,
        "as_of_date": result.as_of_date.isoformat(),
        "inputs": [_serialize_input_ref(input_ref) for input_ref in result.input_refs],
        "results": {
            "monitor_tematico": _serialize_agent_result(result.monitor_tematico),
            "analista_activos": _serialize_agent_result(result.analista_activos),
            "asistente_aportacion_mensual": _serialize_agent_result(result.asistente_aportacion_mensual),
        },
    }


def _serialize_input_ref(input_ref: AgentInputRef) -> dict[str, Any]:
    return {
        "key": input_ref.key,
        "label": input_ref.label,
        "location": input_ref.location,
        "source_type": input_ref.source_type,
        "as_of_date": input_ref.as_of_date.isoformat() if input_ref.as_of_date else None,
        "metadata_keys": sorted(input_ref.metadata.keys()),
    }


def _serialize_agent_result(result: AgentResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "summary": result.summary,
        "warnings": list(result.warnings),
        "errors": list(result.errors),
        "metadata": _json_ready(dict(result.metadata)),
        "findings": [_serialize_finding(finding) for finding in result.findings],
        "sources": [_serialize_source(source) for source in result.sources],
        "artifacts": [_serialize_artifact(artifact) for artifact in result.artifacts],
    }


def _serialize_agent_result_for_input_ref(result: AgentResult) -> dict[str, Any]:
    # Downstream agents receive semantic output, not retrieval timestamps or local paths.
    return _semantic_hash_payload(
        {
            "status": result.status,
            "summary": result.summary,
            "warnings": list(result.warnings),
            "errors": list(result.errors),
            "findings": [_serialize_finding(finding) for finding in result.findings],
        },
        excluded_keys=frozenset({"run_id", "generated_at", "retrieved_at"}),
    )


def _serialize_finding(finding) -> dict[str, Any]:
    return {
        "title": finding.title,
        "detail": finding.detail,
        "category": finding.category,
        "severity": finding.severity,
        "asset_id": finding.asset_id,
        "tags": list(finding.tags),
        "sources": [_serialize_source(source) for source in finding.sources],
        "metadata": _json_ready(dict(finding.metadata)),
    }


def _serialize_source(source) -> dict[str, Any]:
    return {
        "source_type": source.source_type,
        "label": source.label,
        "location": source.location,
        "retrieved_at": source.retrieved_at.isoformat(),
        "effective_date": source.effective_date.isoformat() if source.effective_date else None,
        "metadata": _sanitize_source_metadata(source.metadata),
    }


def _serialize_artifact(artifact) -> dict[str, Any]:
    return {
        "artifact_type": artifact.artifact_type,
        "title": artifact.title,
        "content": artifact.content,
        "path": artifact.path,
        "metadata": _json_ready(dict(artifact.metadata)),
    }


def _sanitize_source_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    omitted: list[str] = []
    for key, value in metadata.items():
        key_str = str(key)
        if key_str in {"content", "text", "agent_result", "result", "findings", "positions", "daily"}:
            omitted.append(key_str)
            continue
        compact[key_str] = _json_ready(value)
    if omitted:
        compact["omitted_metadata_keys"] = sorted(omitted)
    return compact


def _apply_asset_overrides_to_metrics_snapshot(
    metrics_snapshot: dict[str, Any],
    *,
    settings: Settings,
) -> dict[str, Any]:
    overrides = _asset_overrides_by_id(settings=settings)
    if not overrides:
        return metrics_snapshot

    updated = dict(metrics_snapshot)
    positions: list[dict[str, Any]] = []
    for raw_position in metrics_snapshot.get("positions", []):
        position = dict(raw_position)
        override = overrides.get(str(position.get("asset_id") or ""))
        if override:
            original_name = str(position.get("asset_name") or "").strip()
            override_name = str(override.get("asset_name") or "").strip()
            if override_name and override_name != original_name:
                position["broker_asset_name"] = original_name
                position["asset_name"] = override_name
            for source_key, target_key in (
                ("ticker", "ticker"),
                ("broker_symbol", "broker_symbol"),
                ("exchange_mic", "exchange_mic"),
                ("trading_currency", "trading_currency"),
            ):
                value = override.get(source_key)
                if value is not None and str(value).strip():
                    position[target_key] = value
            override_type = override.get("asset_type")
            if override_type is not None and str(override_type).strip():
                position["asset_type"] = override_type
        positions.append(position)
    updated["positions"] = _json_ready(positions)
    updated["content"] = _canonical_json_text(updated)
    return updated


def _asset_overrides_by_id(*, settings: Settings) -> dict[str, dict[str, Any]]:
    frame = load_asset_overrides_frame(settings=settings)
    if frame.empty or "asset_id" not in frame.columns:
        return {}
    rows = {}
    for row in frame.to_dict(orient="records"):
        asset_id = row.get("asset_id")
        if asset_id is not None and str(asset_id).strip():
            rows[str(asset_id)] = row
    return rows


def _append_agent_asset_reference(report_text: str, *, metrics_snapshot: Mapping[str, Any]) -> str:
    positions = metrics_snapshot.get("positions", [])
    if not isinstance(positions, list) or not positions:
        return report_text

    rows = []
    for position in positions:
        if not isinstance(position, Mapping):
            continue
        rows.append(
            "| "
            + " | ".join(
                _markdown_cell(position.get(key))
                for key in ("asset_name", "broker_asset_name", "isin", "ticker", "trading_currency", "asset_type")
            )
            + " |"
        )
    if not rows:
        return report_text

    section = "\n".join(
        [
            "",
            "## Referencia de activos para agentes",
            "",
            "| Nombre normalizado | Nombre broker | ISIN | Ticker | Divisa | Tipo |",
            "| --- | --- | --- | --- | --- | --- |",
            *rows,
        ]
    )
    if "## Referencia de activos para agentes" in report_text:
        return report_text
    return report_text.rstrip() + "\n" + section + "\n"


def _markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).replace("|", "\\|")


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not isfinite(value):
        return None
    try:
        if value is None or bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    item = getattr(value, "item", None)
    if callable(item):
        return _json_ready(item())
    return value
