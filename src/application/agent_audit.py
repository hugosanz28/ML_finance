"""Application read models for persisted monthly agent audit trails."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from src.config import Settings, get_settings


AGENT_NAMES = ("monitor_tematico", "analista_activos", "asistente_aportacion_mensual")


@dataclass(frozen=True)
class AgentRunSummary:
    run_id: str
    as_of_date: str | None
    generated_at: str | None
    output_dir: Path
    status: str
    agent_statuses: dict[str, str]


@dataclass(frozen=True)
class ListAgentRunsResult:
    runs: list[AgentRunSummary]


@dataclass(frozen=True)
class GetAgentRunAuditRequest:
    run_id: str


@dataclass(frozen=True)
class GetAgentRunAuditResult:
    run_id: str
    output_dir: Path
    run_metadata: dict[str, Any]
    input_payload: dict[str, Any]
    agents: dict[str, dict[str, Any]]


class ListAgentRunsUseCase:
    name = "list_agent_runs"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, *, limit: int = 20) -> ListAgentRunsResult:
        base_dir = self.settings.data_dir / "agents" / "monthly_pipeline"
        if not base_dir.exists():
            return ListAgentRunsResult(runs=[])

        runs: list[AgentRunSummary] = []
        for run_dir in sorted((path for path in base_dir.iterdir() if path.is_dir()), reverse=True):
            metadata = _read_json_or_empty(run_dir / "run_metadata.json")
            if not metadata:
                pipeline_result = _read_json_or_empty(run_dir / "pipeline_result.json")
                metadata = _metadata_from_pipeline_result(pipeline_result, run_dir=run_dir)
            if not metadata:
                continue
            runs.append(_summary_from_metadata(metadata, run_dir=run_dir))
            if len(runs) >= limit:
                break
        return ListAgentRunsResult(runs=runs)


class GetAgentRunAuditUseCase:
    name = "get_agent_run_audit"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: GetAgentRunAuditRequest) -> GetAgentRunAuditResult:
        run_id = _safe_run_id(request.run_id)
        output_dir = self.settings.data_dir / "agents" / "monthly_pipeline" / run_id
        if not output_dir.exists():
            raise FileNotFoundError(f"Agent run not found: {run_id}")

        run_metadata = _read_json_or_empty(output_dir / "run_metadata.json")
        input_payload = _read_json_or_empty(output_dir / "input_payload.json")
        agents = {
            agent_name: _read_agent_audit(output_dir / "agents" / agent_name)
            for agent_name in AGENT_NAMES
        }
        return GetAgentRunAuditResult(
            run_id=run_id,
            output_dir=output_dir,
            run_metadata=run_metadata,
            input_payload=input_payload,
            agents=agents,
        )


def _safe_run_id(value: str) -> str:
    run_id = str(value or "").strip()
    if not run_id or any(char in run_id for char in ("/", "\\", "..")):
        raise ValueError(f"Invalid agent run id: {value!r}")
    return run_id


def _read_agent_audit(agent_dir: Path) -> dict[str, Any]:
    return {
        "context": _read_json_or_empty(agent_dir / "context.json"),
        "request": _read_json_or_empty(agent_dir / "request.json"),
        "prompt_refs": _read_json_or_empty(agent_dir / "prompt_refs.json"),
        "prompt_rendered": _read_text_or_empty(agent_dir / "prompt_rendered.md"),
        "raw_response": _read_json_or_empty(agent_dir / "raw_response.json"),
        "parsed_output": _read_json_or_empty(agent_dir / "parsed_output.json"),
    }


def _summary_from_metadata(metadata: dict[str, Any], *, run_dir: Path) -> AgentRunSummary:
    agent_statuses = {
        name: str((metadata.get("agents") or {}).get(name, {}).get("status") or "unknown")
        for name in AGENT_NAMES
    }
    if any(status == "failed" for status in agent_statuses.values()):
        status = "failed"
    elif any(status in {"partial", "unknown"} for status in agent_statuses.values()):
        status = "partial"
    else:
        status = "succeeded"
    return AgentRunSummary(
        run_id=str(metadata.get("run_id") or run_dir.name),
        as_of_date=metadata.get("as_of_date"),
        generated_at=metadata.get("generated_at"),
        output_dir=run_dir,
        status=status,
        agent_statuses=agent_statuses,
    )


def _metadata_from_pipeline_result(payload: dict[str, Any], *, run_dir: Path) -> dict[str, Any]:
    if not payload:
        return {}
    results = payload.get("results") or {}
    return {
        "run_id": payload.get("run_id") or run_dir.name,
        "as_of_date": payload.get("as_of_date"),
        "generated_at": None,
        "output_dir": str(run_dir),
        "agents": {
            name: {"status": (results.get(name) or {}).get("status", "unknown")}
            for name in AGENT_NAMES
        },
    }


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"_error": f"Invalid JSON: {path.name}"}
    return value if isinstance(value, dict) else {"value": value}


def _read_text_or_empty(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


__all__ = [
    "AgentRunSummary",
    "GetAgentRunAuditRequest",
    "GetAgentRunAuditResult",
    "GetAgentRunAuditUseCase",
    "ListAgentRunsResult",
    "ListAgentRunsUseCase",
]
