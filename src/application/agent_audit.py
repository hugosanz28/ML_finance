"""Application read models for persisted monthly agent audit trails."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

from src.agents.provider_audit import redact_sensitive_audit_payload
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
    preflight: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1
    is_legacy: bool = True
    compatibility_warnings: tuple[str, ...] = ()


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

        run_metadata = redact_sensitive_audit_payload(
            _read_json_or_empty(output_dir / "run_metadata.json")
        )
        input_payload = redact_sensitive_audit_payload(
            _read_json_or_empty(output_dir / "input_payload.json")
        )
        preflight = redact_sensitive_audit_payload(
            _read_json_or_empty(output_dir / "preflight.json")
        )
        schema_version, compatibility_warnings = _audit_schema_compatibility(run_metadata)
        agents = {
            agent_name: _read_agent_audit(output_dir / "agents" / agent_name)
            for agent_name in AGENT_NAMES
        }
        return GetAgentRunAuditResult(
            run_id=run_id,
            output_dir=output_dir,
            run_metadata=run_metadata,
            input_payload=input_payload,
            preflight=preflight,
            agents=agents,
            schema_version=schema_version,
            is_legacy=schema_version < 2,
            compatibility_warnings=compatibility_warnings,
        )


def persist_agent_preflight_audit(
    *,
    settings: Settings,
    run_id: str,
    as_of_date: str | None,
    generated_at: str,
    execution_status: str,
    preflight: dict[str, Any],
    output_dir: Path | None = None,
    attach_to_existing_run: bool = False,
) -> tuple[Path, Path]:
    """Attach a quality preflight to a run or persist a blocked attempt."""
    requested_dir = (
        output_dir.expanduser().resolve()
        if output_dir is not None
        else settings.data_dir / "agents" / "monthly_pipeline" / run_id
    )
    if (
        output_dir is not None
        and not attach_to_existing_run
        and requested_dir.exists()
        and any(requested_dir.iterdir())
    ):
        # Preserve an existing audit directory instead of mixing two attempts.
        base_dir = requested_dir / run_id
    else:
        base_dir = requested_dir
    base_dir.mkdir(parents=True, exist_ok=True)
    preflight_path = base_dir / "preflight.json"
    _write_json(preflight_path, preflight)

    metadata_path = base_dir / "run_metadata.json"
    metadata = _read_json_or_empty(metadata_path)
    if not metadata:
        metadata = {
            "run_id": run_id,
            "as_of_date": as_of_date,
            "generated_at": generated_at,
            "base_currency": settings.default_currency,
            "output_dir": str(base_dir),
            "pipeline_result_path": None,
            "agents": {name: {"status": "not_run"} for name in AGENT_NAMES},
            "prompt_versions": {},
        }
    metadata["execution_status"] = execution_status
    metadata["preflight"] = {
        "status": preflight.get("status"),
        "can_run_agents": preflight.get("can_run_agents"),
        "error_count": (preflight.get("counts") or {}).get("error", 0),
        "warning_count": (preflight.get("counts") or {}).get("warning", 0),
        "path": preflight_path.name,
    }
    _write_json(metadata_path, metadata)

    input_payload_path = base_dir / "input_payload.json"
    if not input_payload_path.exists():
        _write_json(
            input_payload_path,
            {
                "run_id": run_id,
                "as_of_date": as_of_date,
                "inputs": [],
                "preflight_inputs": preflight.get("inputs") or {},
            },
        )
    return base_dir, preflight_path


def _safe_run_id(value: str) -> str:
    run_id = str(value or "").strip()
    if not run_id or any(char in run_id for char in ("/", "\\", "..")):
        raise ValueError(f"Invalid agent run id: {value!r}")
    return run_id


def _read_agent_audit(agent_dir: Path) -> dict[str, Any]:
    # Redact every JSON artifact in case a legacy/manually edited run contains credentials.
    return redact_sensitive_audit_payload(
        {
            "context": _read_json_or_empty(agent_dir / "context.json"),
            "request": _read_json_or_empty(agent_dir / "request.json"),
            "prompt_refs": _read_json_or_empty(agent_dir / "prompt_refs.json"),
            "prompt_rendered": _read_text_or_empty(agent_dir / "prompt_rendered.md"),
            "provider": _read_json_or_empty(agent_dir / "provider.json"),
            "raw_response": _read_json_or_empty(agent_dir / "raw_response.json"),
            "parsed_output": _read_json_or_empty(agent_dir / "parsed_output.json"),
            "audit_metadata": _read_json_or_empty(agent_dir / "audit_metadata.json"),
        }
    )


def _audit_schema_compatibility(
    run_metadata: dict[str, Any],
) -> tuple[int, tuple[str, ...]]:
    raw_version = run_metadata.get("schema_version", 1)
    try:
        schema_version = int(raw_version)
    except (TypeError, ValueError):
        return 1, (f"Invalid audit schema_version: {raw_version!r}; read as legacy v1.",)
    if schema_version < 2:
        return schema_version, (
            "Legacy audit: provider metadata and reproducibility hashes may be unavailable.",
        )
    if schema_version > 2:
        return schema_version, (
            f"Audit schema v{schema_version} is newer than supported v2; showing raw compatible fields.",
        )
    return schema_version, ()


def _summary_from_metadata(metadata: dict[str, Any], *, run_dir: Path) -> AgentRunSummary:
    agent_statuses = {
        name: str((metadata.get("agents") or {}).get(name, {}).get("status") or "unknown")
        for name in AGENT_NAMES
    }
    execution_status = str(metadata.get("execution_status") or "")
    if execution_status in {"succeeded", "partial", "failed", "blocked"}:
        status = execution_status
    elif any(status == "failed" for status in agent_statuses.values()):
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


__all__ = [
    "AgentRunSummary",
    "GetAgentRunAuditRequest",
    "GetAgentRunAuditResult",
    "GetAgentRunAuditUseCase",
    "ListAgentRunsResult",
    "ListAgentRunsUseCase",
    "persist_agent_preflight_audit",
]
