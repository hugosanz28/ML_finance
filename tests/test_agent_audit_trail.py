from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import shutil
from uuid import uuid4

from src.agents.models import AgentFinding, AgentInputRef, AgentResult
from src.agents.pipeline import MonthlyAgentPipelineResult, _persist_pipeline_result
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

        assert persisted_dir == output_dir.resolve()
        assert (persisted_dir / "pipeline_result.json").exists()
        run_metadata = _read_json(persisted_dir / "run_metadata.json")
        input_payload = _read_json(persisted_dir / "input_payload.json")

        assert run_metadata["run_id"] == "run-audit-001"
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


def _make_workspace() -> Path:
    base_dir = default_repo_root() / ".test_tmp"
    base_dir.mkdir(exist_ok=True)
    workspace = base_dir / f"agent-audit-{uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=False)
    return workspace


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
