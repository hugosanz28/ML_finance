"""Reusable monthly agent pipeline for CLI and Streamlit."""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from src.agents.analista_activos import AnalistaActivosAgent, OpenAIAssetLLMProvider, StaticAssetLLMProvider
from src.agents.asistente_aportacion_mensual import (
    AsistenteAportacionMensualAgent,
    OpenAIContributionLLMProvider,
    StaticContributionLLMProvider,
)
from src.agents.models import AgentInputRef, AgentRequest, AgentResult, build_agent_context
from src.agents.monitor_tematico import (
    DuckDuckGoHtmlSearchProvider,
    MonitorTematicoAgent,
    NullSearchProvider,
    OpenAIThemeLLMProvider,
    StaticThemeLLMProvider,
)
from src.config import Settings, get_settings
from src.portfolio import PortfolioMetricsResult, calculate_portfolio_metrics_from_normalized_degiro
from src.reports import generate_monthly_report, get_latest_monthly_report


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
    request_parameters: Mapping[str, Any] | None = None,
) -> MonthlyAgentPipelineResult:
    """Run monitor, asset analyst, and monthly assistant with shared inputs."""
    resolved_settings = get_settings() if settings is None else settings
    resolved_metrics = metrics or calculate_portfolio_metrics_from_normalized_degiro(settings=resolved_settings)
    report_path, report_text, report_date = _resolve_monthly_report(
        settings=resolved_settings,
        monthly_report_path=monthly_report_path,
    )
    as_of_date = report_date or resolved_metrics.end_date
    generated_at = datetime.now().astimezone()
    run_id = generated_at.strftime("%Y%m%dT%H%M%S%f")
    investment_brief, investment_location = _resolve_investment_brief(
        settings=resolved_settings,
        investment_brief_text=investment_brief_text,
        investment_brief_path=investment_brief_path,
    )
    metrics_snapshot = build_portfolio_metrics_snapshot(resolved_metrics, as_of_date=as_of_date)

    common_refs = _build_common_input_refs(
        investment_brief=investment_brief,
        investment_location=investment_location,
        monthly_report_path=report_path,
        monthly_report_text=report_text,
        monthly_report_date=as_of_date,
        metrics_snapshot=metrics_snapshot,
        user_satellite_interest=user_satellite_interest,
    )
    request = AgentRequest(parameters=dict(request_parameters or {}))

    monitor_agent = MonitorTematicoAgent(
        search_provider=_build_search_provider(search_provider),
        llm_provider=_build_monitor_llm_provider(llm_provider),
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
    monitor_result = monitor_agent.execute(request, monitor_context)

    monitor_ref = _result_input_ref("monitor_tematico_result", "Monitor tematico result", monitor_result)
    analista_agent = AnalistaActivosAgent(llm_provider=_build_asset_llm_provider(llm_provider))
    analista_context = build_agent_context(
        agent_name=analista_agent.name,
        as_of_date=as_of_date,
        generated_at=generated_at,
        base_currency=resolved_settings.default_currency,
        settings=resolved_settings,
        input_refs=(*common_refs, monitor_ref),
        run_id=run_id,
    )
    analista_result = analista_agent.execute(request, analista_context)

    analista_ref = _result_input_ref("analista_activos_result", "Analista activos result", analista_result)
    asistente_agent = AsistenteAportacionMensualAgent(
        llm_provider=_build_contribution_llm_provider(llm_provider),
    )
    asistente_context = build_agent_context(
        agent_name=asistente_agent.name,
        as_of_date=as_of_date,
        generated_at=generated_at,
        base_currency=resolved_settings.default_currency,
        settings=resolved_settings,
        input_refs=(*common_refs, monitor_ref, analista_ref),
        metadata={"monthly_budget": resolved_settings.monthly_contribution_eur},
        run_id=run_id,
    )
    asistente_result = asistente_agent.execute(request, asistente_context)

    resolved_output_dir = None
    result = MonthlyAgentPipelineResult(
        run_id=run_id,
        as_of_date=as_of_date,
        input_refs=common_refs,
        monitor_tematico=monitor_result,
        analista_activos=analista_result,
        asistente_aportacion_mensual=asistente_result,
        output_dir=None,
    )
    if persist:
        resolved_output_dir = _persist_pipeline_result(
            result,
            settings=resolved_settings,
            output_dir=output_dir,
        )
        result = replace(result, output_dir=resolved_output_dir)
    return result


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


def load_investment_brief(*, settings: Settings | None = None, path: str | Path | None = None) -> str:
    """Read the configured investment brief text."""
    resolved_settings = get_settings() if settings is None else settings
    brief_path = resolved_settings.investment_brief_path if path is None else Path(path).expanduser().resolve()
    if not brief_path.exists():
        raise FileNotFoundError(
            f"Investment brief not found: {brief_path}. Create it or pass investment_brief_text explicitly."
        )
    return brief_path.read_text(encoding="utf-8")


def _resolve_monthly_report(
    *,
    settings: Settings,
    monthly_report_path: str | Path | None,
) -> tuple[Path, str, date | None]:
    if monthly_report_path is not None:
        path = Path(monthly_report_path).expanduser().resolve()
        return path, path.read_text(encoding="utf-8"), None
    latest = get_latest_monthly_report(settings=settings)
    if latest is None:
        report = generate_monthly_report(settings=settings, persist=True)
        if report.output_path is None:
            raise ValueError("Monthly report generation did not produce an output path.")
        return report.output_path, report.content, report.as_of_date
    path = Path(latest.report_path).expanduser().resolve()
    return path, path.read_text(encoding="utf-8"), latest.as_of_date


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
            metadata={"content": monthly_report_text},
        ),
        AgentInputRef(
            key="portfolio_metrics_snapshot",
            label="Portfolio metrics snapshot",
            location="derived://portfolio_metrics_snapshot",
            source_type="derived",
            as_of_date=monthly_report_date,
            metadata={**metrics_snapshot, "content": json.dumps(metrics_snapshot, ensure_ascii=False)},
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
    return tuple(refs)


def _result_input_ref(key: str, label: str, result: AgentResult) -> AgentInputRef:
    payload = _serialize_agent_result(result)
    return AgentInputRef(
        key=key,
        label=label,
        location=f"derived://{key}",
        source_type="derived",
        metadata={
            "agent_result": result,
            "findings": result.findings,
            "content": json.dumps(payload, ensure_ascii=False),
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
    if provider_name == "duckduckgo":
        return DuckDuckGoHtmlSearchProvider()
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
    (base_dir / "pipeline_result.json").write_text(
        json.dumps(_serialize_pipeline_result(result), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return base_dir


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
        "findings": [_json_ready(finding) for finding in result.findings],
        "sources": [_json_ready(source) for source in result.sources],
        "artifacts": [_json_ready(artifact) for artifact in result.artifacts],
    }


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value
