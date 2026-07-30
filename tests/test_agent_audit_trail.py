from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
import json
from pathlib import Path
import shutil
from uuid import uuid4

import numpy as np

from src.agents.models import (
    AgentFinding,
    AgentInputRef,
    AgentRequest,
    AgentResult,
    AgentSource,
)
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
            agent_requests={
                agent_name: AgentRequest(
                    scope={"universe": "portfolio"},
                    parameters={"max_items": 5},
                    constraints={"network": "offline"},
                    input_refs=("investment_brief",),
                    metadata={"origin": "test"},
                )
                for agent_name in (
                    "monitor_tematico",
                    "analista_activos",
                    "asistente_aportacion_mensual",
                )
            },
            provider_configs={
                "monitor_tematico": {
                    "llm": {
                        "role": "llm",
                        "provider": "static_llm",
                        "model": None,
                        "options": {"mode": "deterministic_offline"},
                    },
                    "search": {
                        "role": "search",
                        "provider": "null",
                        "model": None,
                        "options": {"mode": "deterministic_offline"},
                    },
                }
            },
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
        pipeline_result = _read_json(persisted_dir / "pipeline_result.json")

        assert run_metadata["run_id"] == "run-audit-001"
        assert run_metadata["execution_status"] == "succeeded"
        assert run_metadata["preflight"]["status"] == "passed"
        assert run_metadata["schema_version"] == 2
        assert run_metadata["hash_algorithm"] == "sha256"
        assert run_metadata["input_hash"].startswith("sha256:")
        assert len(run_metadata["input_hash"]) == 71
        assert run_metadata["output_hash"].startswith("sha256:")
        assert len(run_metadata["output_hash"]) == 71
        assert run_metadata["prompt_versions"]["monitor_tematico"]["monitor_tematico.query"] == "v1"
        assert input_payload["schema_version"] == 2
        assert input_payload["input_hash"] == run_metadata["input_hash"]
        assert input_payload["inputs"][0]["metadata"]["content"] == "brief text"
        assert pipeline_result["input_hash"] == run_metadata["input_hash"]
        assert pipeline_result["output_hash"] == run_metadata["output_hash"]

        for agent_name in ("monitor_tematico", "analista_activos", "asistente_aportacion_mensual"):
            agent_dir = persisted_dir / "agents" / agent_name
            assert (agent_dir / "context.json").exists()
            assert (agent_dir / "request.json").exists()
            assert (agent_dir / "prompt_refs.json").exists()
            assert (agent_dir / "prompt_rendered.md").exists()
            assert (agent_dir / "provider.json").exists()
            assert (agent_dir / "raw_response.json").exists()
            assert (agent_dir / "parsed_output.json").exists()
            assert (agent_dir / "audit_metadata.json").exists()

            context = _read_json(agent_dir / "context.json")
            request = _read_json(agent_dir / "request.json")
            provider = _read_json(agent_dir / "provider.json")
            raw_response = _read_json(agent_dir / "raw_response.json")
            parsed_output = _read_json(agent_dir / "parsed_output.json")
            audit_metadata = _read_json(agent_dir / "audit_metadata.json")

            assert context["agent_name"] == agent_name
            assert request["scope"] == {"universe": "portfolio"}
            assert request["parameters"] == {"max_items": 5}
            assert request["constraints"] == {"network": "offline"}
            assert request["input_refs"] == ["investment_brief"]
            assert "schema_version" not in request
            restored_request = AgentRequest(
                scope=request["scope"],
                parameters=request["parameters"],
                constraints=request["constraints"],
                input_refs=tuple(request["input_refs"]),
                metadata=request["metadata"],
            )
            assert restored_request == result.agent_requests[agent_name]
            assert provider["schema_version"] == 2
            assert raw_response["status"] == "not_captured"
            assert raw_response["reason_code"] == "provider_contract_no_raw_response"
            assert parsed_output["status"] == "success"
            assert parsed_output["metadata"]["agent_plan"] == ["Plan step"]
            assert parsed_output["metadata"]["selected_actions"] == ["test_action"]
            assert audit_metadata["hash_projection"] == "semantic-v1"
            assert audit_metadata["input_hash"].startswith("sha256:")
            assert audit_metadata["output_hash"].startswith("sha256:")
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


def test_audit_hashes_are_deterministic_and_change_with_input_content() -> None:
    workspace = _make_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        first_dir = _persist_pipeline_result(
            _pipeline_result(
                run_id="run-1",
                input_content="same",
                input_location=str(workspace / "one" / "brief.md"),
                extra_metadata={"alpha": 1, "beta": 2},
                finding_metadata={"alpha": 1, "beta": 2},
                retrieved_at=datetime(2026, 5, 26, 8, 0, tzinfo=timezone.utc),
            ),
            settings=settings,
            output_dir=workspace / "audit-1",
        )
        second_dir = _persist_pipeline_result(
            _pipeline_result(
                run_id="run-2",
                input_content="same",
                input_location=str(workspace / "two" / "brief.md"),
                extra_metadata={"beta": 2, "alpha": 1},
                finding_metadata={"beta": 2, "alpha": 1},
                retrieved_at=datetime(2026, 5, 27, 9, 0, tzinfo=timezone.utc),
            ),
            settings=settings,
            output_dir=workspace / "audit-2",
        )
        changed_dir = _persist_pipeline_result(
            _pipeline_result(
                run_id="run-3",
                input_content="changed",
                finding_metadata={"alpha": 1, "beta": 2},
            ),
            settings=settings,
            output_dir=workspace / "audit-3",
        )

        first = _read_json(first_dir / "run_metadata.json")
        second = _read_json(second_dir / "run_metadata.json")
        changed = _read_json(changed_dir / "run_metadata.json")

        assert first["input_hash"] == second["input_hash"]
        assert first["output_hash"] == second["output_hash"]
        assert changed["input_hash"] != first["input_hash"]
        assert changed["output_hash"] == first["output_hash"]
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


def test_audit_output_hash_changes_with_semantic_output() -> None:
    workspace = _make_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        first_dir = _persist_pipeline_result(
            _pipeline_result(run_id="run-output-1", input_content="same"),
            settings=settings,
            output_dir=workspace / "audit-output-1",
        )
        changed_dir = _persist_pipeline_result(
            _pipeline_result(
                run_id="run-output-2",
                input_content="same",
                result_summary="semantic change",
            ),
            settings=settings,
            output_dir=workspace / "audit-output-2",
        )

        first = _read_json(first_dir / "run_metadata.json")
        changed = _read_json(changed_dir / "run_metadata.json")

        assert changed["input_hash"] == first["input_hash"]
        assert changed["output_hash"] != first["output_hash"]
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_audit_input_hash_tracks_request_prompt_and_provider_changes() -> None:
    workspace = _make_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        variants = {
            "base": _pipeline_result_with_audit_inputs(run_id="run-components-base"),
            "operational_timestamp": _pipeline_result_with_audit_inputs(
                run_id="run-components-time",
                request_generated_at="2026-05-27T10:00:00+00:00",
            ),
            "request": _pipeline_result_with_audit_inputs(
                run_id="run-components-request",
                request_mode="changed",
            ),
            "prompt": _pipeline_result_with_audit_inputs(
                run_id="run-components-prompt",
                prompt_text="changed prompt",
            ),
            "provider": _pipeline_result_with_audit_inputs(
                run_id="run-components-provider",
                provider_model="changed-model",
            ),
        }
        hashes = {}
        for name, result in variants.items():
            output_dir = _persist_pipeline_result(
                result,
                settings=settings,
                output_dir=workspace / f"audit-components-{name}",
            )
            hashes[name] = _read_json(output_dir / "run_metadata.json")["input_hash"]

        assert hashes["operational_timestamp"] == hashes["base"]
        assert hashes["request"] != hashes["base"]
        assert hashes["prompt"] != hashes["base"]
        assert hashes["provider"] != hashes["base"]
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_audit_serialization_converts_non_finite_values_to_null() -> None:
    workspace = _make_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        output_dir = _persist_pipeline_result(
            _pipeline_result(
                run_id="run-json",
                input_content="strict",
                extra_metadata={
                    "nan": float("nan"),
                    "positive_infinity": float("inf"),
                    "negative_infinity": float("-inf"),
                    "numpy_integer": np.int64(7),
                },
            ),
            settings=settings,
            output_dir=workspace / "audit-json",
        )

        payload = _read_json(output_dir / "input_payload.json")
        assert payload["inputs"][0]["metadata"]["nan"] is None
        assert payload["inputs"][0]["metadata"]["positive_infinity"] is None
        assert payload["inputs"][0]["metadata"]["negative_infinity"] is None
        assert payload["inputs"][0]["metadata"]["numpy_integer"] == 7
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_audit_writer_redacts_credentials_from_all_json_artifacts() -> None:
    workspace = _make_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        base_result = _pipeline_result(
            run_id="run-redacted",
            input_content="strict",
            extra_metadata={
                "token": "sentinel-input-token",  # pragma: allowlist secret
            },
            finding_metadata={
                "apiKey": "sentinel-finding-key",  # pragma: allowlist secret
            },
        )
        result = replace(
            base_result,
            agent_requests={
                "monitor_tematico": AgentRequest(
                    input_refs=("investment_brief",),
                    metadata={
                        "apiKey": "sentinel-request-key",  # pragma: allowlist secret
                    },
                )
            },
        )

        output_dir = _persist_pipeline_result(
            result,
            settings=settings,
            output_dir=workspace / "audit-redacted",
        )

        serialized = "\n".join(
            path.read_text(encoding="utf-8")
            for path in output_dir.rglob("*.json")
        )
        assert "sentinel-input-token" not in serialized
        assert "sentinel-request-key" not in serialized
        assert "sentinel-finding-key" not in serialized
        assert "[REDACTED]" in serialized
        request = _read_json(
            output_dir / "agents" / "monitor_tematico" / "request.json"
        )
        assert request["metadata"]["apiKey"] == "[REDACTED]"

        other_base = _pipeline_result(
            run_id="run-redacted-other",
            input_content="strict",
            extra_metadata={
                "token": "different-input-token",  # pragma: allowlist secret
            },
            finding_metadata={
                "apiKey": "different-finding-key",  # pragma: allowlist secret
            },
        )
        other_result = replace(
            other_base,
            agent_requests={
                "monitor_tematico": AgentRequest(
                    input_refs=("investment_brief",),
                    metadata={
                        "apiKey": "different-request-key",  # pragma: allowlist secret
                    },
                )
            },
        )
        other_dir = _persist_pipeline_result(
            other_result,
            settings=settings,
            output_dir=workspace / "audit-redacted-other",
        )
        assert (
            _read_json(other_dir / "run_metadata.json")["input_hash"]
            == _read_json(output_dir / "run_metadata.json")["input_hash"]
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def _agent_result(
    summary: str,
    *,
    retrieved_at: datetime | None = None,
    finding_metadata: dict | None = None,
) -> AgentResult:
    source = AgentSource(
        source_type="derived",
        label="Test source",
        location="derived://test-source",
        retrieved_at=retrieved_at
        or datetime(2026, 5, 26, 8, 0, tzinfo=timezone.utc),
    )
    return AgentResult(
        status="success",
        summary=summary,
        findings=(
            AgentFinding(
                title="Finding",
                detail="Detail",
                sources=(source,),
                metadata=finding_metadata or {},
            ),
        ),
        sources=(source,),
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


def _pipeline_result(
    *,
    run_id: str,
    input_content: str,
    input_location: str = "manual://investment-brief",
    extra_metadata: dict | None = None,
    result_summary: str | None = None,
    retrieved_at: datetime | None = None,
    finding_metadata: dict | None = None,
) -> MonthlyAgentPipelineResult:
    return MonthlyAgentPipelineResult(
        run_id=run_id,
        as_of_date=date(2026, 5, 26),
        input_refs=(
            AgentInputRef(
                key="investment_brief",
                label="Investment brief",
                location=input_location,
                source_type="manual",
                as_of_date=date(2026, 5, 26),
                metadata={"content": input_content, **(extra_metadata or {})},
            ),
        ),
        monitor_tematico=_agent_result(
            "monitor ok",
            retrieved_at=retrieved_at,
            finding_metadata=finding_metadata,
        ),
        analista_activos=_agent_result(
            "asset ok",
            retrieved_at=retrieved_at,
            finding_metadata=finding_metadata,
        ),
        asistente_aportacion_mensual=_agent_result(
            result_summary or "assistant ok",
            retrieved_at=retrieved_at,
            finding_metadata=finding_metadata,
        ),
    )


def _pipeline_result_with_audit_inputs(
    *,
    run_id: str,
    request_mode: str = "base",
    request_generated_at: str = "2026-05-26T10:00:00+00:00",
    prompt_text: str = "base prompt",
    provider_model: str = "base-model",
) -> MonthlyAgentPipelineResult:
    result = _pipeline_result(run_id=run_id, input_content="same")
    return replace(
        result,
        agent_requests={
            "monitor_tematico": AgentRequest(
                parameters={"mode": request_mode},
                input_refs=("investment_brief",),
                metadata={"generated_at": request_generated_at},
            )
        },
        prompt_audits={
            "monitor_tematico": {
                "prompt_refs": {
                    "schema_version": 2,
                    "agent_name": "monitor_tematico",
                    "prompts": [{"key": "test.prompt", "version": "v1"}],
                },
                "prompt_rendered": prompt_text,
            }
        },
        provider_configs={
            "monitor_tematico": {
                "llm": {
                    "role": "llm",
                    "provider": "test",
                    "model": provider_model,
                    "options": {"temperature": 0},
                }
            }
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
