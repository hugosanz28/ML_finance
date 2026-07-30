from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import shutil
from uuid import uuid4

from src.agents.models import AgentFinding, AgentInputRef, AgentResult
from src.agents.pipeline import MonthlyAgentPipelineResult, _persist_pipeline_result
from src.application.agent_audit import (
    GetAgentRunAuditRequest,
    GetAgentRunAuditUseCase,
    ListAgentRunsUseCase,
    persist_agent_preflight_audit,
)
from src.config import default_repo_root, load_settings


def test_persist_pipeline_result_writes_audit_trail_files() -> None:
    workspace = _make_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        output_dir = workspace / "audit"
        result = MonthlyAgentPipelineResult(
            run_id="run-audit-001",
            as_of_date=date(2026, 5, 26),
            input_refs=(
                AgentInputRef(
                    key="investment_brief",
                    label="Investment brief",
                    location="manual://investment-brief",
                    source_type="manual",
                    as_of_date=date(2026, 5, 26),
                    metadata={"content": "brief text"},
                ),
            ),
            monitor_tematico=_agent_result("monitor ok"),
            analista_activos=_agent_result("asset ok"),
            asistente_aportacion_mensual=_agent_result("assistant ok"),
        )

        persisted_dir = _persist_pipeline_result(result, settings=settings, output_dir=output_dir)
        _, preflight_path = persist_agent_preflight_audit(
            settings=settings,
            run_id=result.run_id,
            as_of_date=result.as_of_date.isoformat(),
            generated_at="2026-05-26T10:00:00+02:00",
            execution_status="succeeded",
            preflight=_preflight_payload(status="passed", can_run_agents=True),
            output_dir=persisted_dir,
            attach_to_existing_run=True,
        )

        assert persisted_dir == output_dir.resolve()
        assert (persisted_dir / "pipeline_result.json").exists()
        assert preflight_path == persisted_dir / "preflight.json"
        run_metadata = _read_json(persisted_dir / "run_metadata.json")
        input_payload = _read_json(persisted_dir / "input_payload.json")

        assert run_metadata["run_id"] == "run-audit-001"
        assert run_metadata["execution_status"] == "succeeded"
        assert run_metadata["preflight"]["status"] == "passed"
        assert run_metadata["prompt_versions"]["monitor_tematico"]["monitor_tematico.query"] == "v1"
        assert input_payload["inputs"][0]["metadata"]["content"] == "brief text"

        for agent_name in ("monitor_tematico", "analista_activos", "asistente_aportacion_mensual"):
            agent_dir = persisted_dir / "agents" / agent_name
            assert (agent_dir / "context.json").exists()
            assert (agent_dir / "request.json").exists()
            assert (agent_dir / "prompt_refs.json").exists()
            assert (agent_dir / "prompt_rendered.md").exists()
            assert (agent_dir / "raw_response.json").exists()
            assert (agent_dir / "parsed_output.json").exists()

            context = _read_json(agent_dir / "context.json")
            raw_response = _read_json(agent_dir / "raw_response.json")
            parsed_output = _read_json(agent_dir / "parsed_output.json")

            assert context["agent_name"] == agent_name
            assert raw_response["status"] == "not_captured"
            assert parsed_output["status"] == "success"
            assert parsed_output["metadata"]["agent_plan"] == ["Plan step"]
            assert parsed_output["metadata"]["selected_actions"] == ["test_action"]
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_blocked_preflight_attempt_is_auditable_without_agent_outputs() -> None:
    workspace = _make_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        output_dir, _ = persist_agent_preflight_audit(
            settings=settings,
            run_id="blocked-001",
            as_of_date="2026-05-26",
            generated_at="2026-05-26T10:00:00+02:00",
            execution_status="blocked",
            preflight=_preflight_payload(status="blocked", can_run_agents=False),
        )

        runs = ListAgentRunsUseCase(settings=settings).execute().runs
        audit = GetAgentRunAuditUseCase(settings=settings).execute(
            GetAgentRunAuditRequest(run_id="blocked-001")
        )

        assert output_dir.name == "blocked-001"
        assert len(runs) == 1
        assert runs[0].status == "blocked"
        assert set(runs[0].agent_statuses.values()) == {"not_run"}
        assert audit.preflight["status"] == "blocked"
        assert audit.preflight["issues"][0]["code"] == "missing_prices"
        assert not (output_dir / "pipeline_result.json").exists()
        assert not (output_dir / "agents").exists()
        assert all(not item["parsed_output"] for item in audit.agents.values())
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_blocked_preflight_does_not_mix_with_existing_output_directory() -> None:
    workspace = _make_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        requested_output_dir = workspace / "existing-audit"
        requested_output_dir.mkdir()
        sentinel = requested_output_dir / "keep.txt"
        sentinel.write_text("existing", encoding="utf-8")

        output_dir, _ = persist_agent_preflight_audit(
            settings=settings,
            run_id="blocked-002",
            as_of_date="2026-05-26",
            generated_at="2026-05-26T10:00:00+02:00",
            execution_status="blocked",
            preflight=_preflight_payload(status="blocked", can_run_agents=False),
            output_dir=requested_output_dir,
        )

        assert output_dir == requested_output_dir / "blocked-002"
        assert sentinel.read_text(encoding="utf-8") == "existing"
        assert not (requested_output_dir / "run_metadata.json").exists()
        assert (output_dir / "run_metadata.json").exists()
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def _agent_result(summary: str) -> AgentResult:
    return AgentResult(
        status="success",
        summary=summary,
        findings=(AgentFinding(title="Finding", detail="Detail"),),
        metadata={
            "agent_plan": ("Plan step",),
            "selected_actions": ("test_action",),
        },
    )


def _preflight_payload(*, status: str, can_run_agents: bool) -> dict:
    error_count = 0 if can_run_agents else 1
    return {
        "schema_version": 1,
        "status": status,
        "can_run_agents": can_run_agents,
        "as_of_date": "2026-05-26",
        "counts": {"error": error_count, "warning": 0, "info": int(can_run_agents)},
        "issues": [
            {
                "code": "portfolio_metrics_ready" if can_run_agents else "missing_prices",
                "severity": "info" if can_run_agents else "error",
                "message": "ready" if can_run_agents else "missing",
                "details": {},
            }
        ],
        "inputs": {
            "monthly_report_date": "2026-05-26",
            "snapshot_as_of_date": "2026-05-26",
            "min_valuation_coverage_ratio": 1.0,
            "min_return_coverage_ratio": 0.8,
        },
    }


def _make_workspace() -> Path:
    base_dir = default_repo_root() / ".test_tmp"
    base_dir.mkdir(exist_ok=True)
    workspace = base_dir / f"agent-audit-{uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=False)
    return workspace


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
