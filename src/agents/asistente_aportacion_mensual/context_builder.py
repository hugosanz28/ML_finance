"""Build context for the monthly contribution assistant."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.agents.asistente_aportacion_mensual._types import PriorAgentFinding
from src.agents.models import AgentContext, AgentFinding, AgentInputRef, AgentRequest, AgentResult


def collect_input_texts(context: AgentContext) -> dict[str, str]:
    """Read textual payloads used by the LLM."""
    values = {
        "investment_brief": read_input_text(context.get_input("investment_brief"), context),
        "latest_monthly_report": read_input_text(context.get_input("latest_monthly_report"), context),
    }
    for optional_key in (
        "portfolio_metrics_snapshot",
        "target_weights",
        "monitor_tematico_result",
        "monitor_tematico",
        "theme_monitor_result",
        "analista_activos_result",
        "analista_activos",
        "asset_analysis_result",
        "user_satellite_interest",
    ):
        if context.has_input(optional_key):
            values[optional_key] = read_input_text(context.get_input(optional_key), context)
    return values


def resolve_monthly_budget(request: AgentRequest, context: AgentContext) -> float:
    """Resolve the monthly contribution budget from request data or settings."""
    for container in (request.parameters, request.scope, request.constraints, context.metadata):
        value = (
            container.get("monthly_budget")
            or container.get("monthly_budget_eur")
            or container.get("contribution_budget")
            or container.get("ingreso_mensual")
        )
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return float(context.settings.monthly_contribution_eur)


def resolve_target_weights(request: AgentRequest, context: AgentContext) -> Mapping[str, Any]:
    """Resolve target weights from constraints, parameters, metadata, or optional input."""
    for container in (request.constraints, request.parameters, request.scope, context.metadata):
        value = container.get("target_weights") or container.get("pesos_objetivo")
        if isinstance(value, Mapping):
            return value
    if context.has_input("target_weights"):
        input_ref = context.get_input("target_weights")
        value = input_ref.metadata.get("weights") or input_ref.metadata.get("target_weights")
        if isinstance(value, Mapping):
            return value
        loaded = load_structured_input(input_ref, context)
        if isinstance(loaded, Mapping):
            return loaded
    return {}


def extract_current_allocation(context: AgentContext) -> tuple[Mapping[str, Any], ...]:
    """Extract current positions or allocation from the metrics snapshot when available."""
    if not context.has_input("portfolio_metrics_snapshot"):
        return ()
    input_ref = context.get_input("portfolio_metrics_snapshot")
    raw_positions = input_ref.metadata.get("positions") or input_ref.metadata.get("assets") or input_ref.metadata.get("allocation")
    if raw_positions is None:
        loaded = load_structured_input(input_ref, context)
        if isinstance(loaded, Mapping):
            raw_positions = loaded.get("positions") or loaded.get("assets") or loaded.get("allocation")
        else:
            raw_positions = loaded
    if not isinstance(raw_positions, Iterable) or isinstance(raw_positions, (str, bytes, Mapping)):
        return ()
    return tuple(position for position in raw_positions if isinstance(position, Mapping))


def extract_prior_findings(context: AgentContext) -> tuple[PriorAgentFinding, ...]:
    """Extract upstream agent findings from optional monitor and asset-analysis inputs."""
    findings: list[PriorAgentFinding] = []
    for source_agent, keys in (
        ("monitor_tematico", ("monitor_tematico_result", "monitor_tematico", "theme_monitor_result")),
        ("analista_activos", ("analista_activos_result", "analista_activos", "asset_analysis_result")),
    ):
        input_ref = _first_existing_input(context, keys)
        if input_ref is None:
            continue
        findings.extend(_findings_from_input(input_ref, context, source_agent=source_agent))
    return tuple(findings)


def read_input_text(input_ref: AgentInputRef, context: AgentContext) -> str:
    inline = input_ref.metadata.get("content") or input_ref.metadata.get("text")
    if inline:
        return str(inline)
    if input_ref.metadata:
        return str(dict(input_ref.metadata))
    path = _resolve_input_path(input_ref.location, context)
    if path is not None and path.is_file():
        return path.read_text(encoding="utf-8")
    return input_ref.description or ""


def load_structured_input(input_ref: AgentInputRef, context: AgentContext) -> Any:
    content = read_input_text(input_ref, context)
    if not content:
        return None
    location = input_ref.location.lower()
    if location.endswith(".json"):
        return json.loads(content)
    if location.endswith(".csv"):
        return tuple(csv.DictReader(content.splitlines()))
    return tuple(line.strip("- ").strip() for line in content.splitlines() if line.strip())


def _findings_from_input(
    input_ref: AgentInputRef,
    context: AgentContext,
    *,
    source_agent: str,
) -> tuple[PriorAgentFinding, ...]:
    raw_findings = input_ref.metadata.get("findings")
    agent_result = input_ref.metadata.get("agent_result") or input_ref.metadata.get("result")
    if raw_findings is None and isinstance(agent_result, AgentResult):
        raw_findings = agent_result.findings
    if raw_findings is None and isinstance(agent_result, Mapping):
        raw_findings = agent_result.get("findings")
    if raw_findings is None:
        loaded = load_structured_input(input_ref, context)
        if isinstance(loaded, Mapping):
            raw_findings = loaded.get("findings")
        else:
            raw_findings = loaded
    if not isinstance(raw_findings, Iterable) or isinstance(raw_findings, (str, bytes, Mapping)):
        return ()
    return tuple(_prior_finding_from_raw(item, source_agent=source_agent) for item in raw_findings)


def _prior_finding_from_raw(item: Any, *, source_agent: str) -> PriorAgentFinding:
    if isinstance(item, AgentFinding):
        return PriorAgentFinding(
            title=item.title,
            detail=item.detail,
            source_agent=source_agent,
            category=item.category,
            severity=item.severity,
            asset_id=item.asset_id,
            metadata=item.metadata,
        )
    if isinstance(item, Mapping):
        return PriorAgentFinding(
            title=str(item.get("title") or ""),
            detail=str(item.get("detail") or item.get("summary") or ""),
            source_agent=source_agent,
            category=str(item.get("category") or "general"),
            severity=str(item.get("severity") or "info"),
            asset_id=_optional_str(item.get("asset_id")),
            metadata=item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {},
        )
    return PriorAgentFinding(title=str(item), detail="", source_agent=source_agent)


def _first_existing_input(context: AgentContext, keys: tuple[str, ...]) -> AgentInputRef | None:
    for key in keys:
        if context.has_input(key):
            return context.get_input(key)
    return None


def _resolve_input_path(location: str, context: AgentContext) -> Path | None:
    if not location:
        return None
    path = Path(location).expanduser()
    if not path.is_absolute():
        path = context.settings.repo_root / path
    return path.resolve()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
