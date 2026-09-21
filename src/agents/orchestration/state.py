"""JSON-safe shared state for the monthly LangGraph workflow."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from typing import Any, TypedDict

from src.agents.models import AgentArtifact, AgentFinding, AgentResult, AgentSource


GRAPH_VERSION = 1


class MonthlyAgentState(TypedDict, total=False):
    run_id: str
    as_of_date: str
    base_currency: str
    graph_version: int
    inputs: dict[str, Any]
    input_summary: dict[str, Any]
    decisions: list[dict[str, Any]]
    attempts: dict[str, int]
    repair_counts: dict[str, int]
    attempt_log: list[dict[str, Any]]
    results: dict[str, dict[str, Any]]
    result_history: dict[str, list[dict[str, Any]]]
    next_agent: str
    delegation_instruction: str
    total_delegations: int
    warnings: list[str]
    errors: list[str]
    terminal_status: str
    terminal_reason: str


def agent_result_to_payload(result: AgentResult) -> dict[str, Any]:
    return _json_ready(asdict(result))


def agent_result_from_payload(payload: dict[str, Any]) -> AgentResult:
    return AgentResult(
        status=str(payload.get("status", "failed")),  # type: ignore[arg-type]
        summary=str(payload.get("summary", "")),
        findings=tuple(_finding_from_payload(item) for item in payload.get("findings", [])),
        artifacts=tuple(_artifact_from_payload(item) for item in payload.get("artifacts", [])),
        sources=tuple(_source_from_payload(item) for item in payload.get("sources", [])),
        warnings=tuple(str(item) for item in payload.get("warnings", [])),
        errors=tuple(str(item) for item in payload.get("errors", [])),
        metadata=dict(payload.get("metadata") or {}),
    )


def _finding_from_payload(payload: dict[str, Any]) -> AgentFinding:
    return AgentFinding(
        title=str(payload.get("title", "")),
        detail=str(payload.get("detail", "")),
        category=str(payload.get("category", "general")),
        severity=str(payload.get("severity", "info")),
        asset_id=str(payload["asset_id"]) if payload.get("asset_id") is not None else None,
        tags=tuple(str(item) for item in payload.get("tags", [])),
        sources=tuple(_source_from_payload(item) for item in payload.get("sources", [])),
        metadata=dict(payload.get("metadata") or {}),
    )


def _source_from_payload(payload: dict[str, Any]) -> AgentSource:
    effective_date = payload.get("effective_date")
    retrieved_at = payload.get("retrieved_at")
    return AgentSource(
        source_type=str(payload.get("source_type", "derived")),  # type: ignore[arg-type]
        label=str(payload.get("label", "")),
        location=str(payload.get("location", "")),
        retrieved_at=(
            datetime.fromisoformat(str(retrieved_at))
            if retrieved_at
            else datetime.now().astimezone()
        ),
        effective_date=date.fromisoformat(str(effective_date)) if effective_date else None,
        metadata=dict(payload.get("metadata") or {}),
    )


def _artifact_from_payload(payload: dict[str, Any]) -> AgentArtifact:
    return AgentArtifact(
        artifact_type=str(payload.get("artifact_type", "json")),  # type: ignore[arg-type]
        title=str(payload.get("title", "")),
        content=str(payload["content"]) if payload.get("content") is not None else None,
        path=str(payload["path"]) if payload.get("path") is not None else None,
        metadata=dict(payload.get("metadata") or {}),
    )


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


__all__ = [
    "GRAPH_VERSION",
    "MonthlyAgentState",
    "agent_result_from_payload",
    "agent_result_to_payload",
]
