from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import shutil
from uuid import uuid4

import pytest

from src.agents import (
    AgentContext,
    AgentInputRef,
    AgentRequest,
    AgentResult,
    AgentSource,
    AgentValidationError,
    BaseAgent,
    build_agent_context,
)
from src.config import default_repo_root, load_settings
from src.reports.history import ReportHistoryEntry


class StubAgent(BaseAgent):
    @property
    def name(self) -> str:
        return "stub_agent"

    @property
    def description(self) -> str:
        return "Stub agent used for contract tests."

    def required_inputs(self) -> tuple[str, ...]:
        return ("latest_monthly_report",)

    def run(self, request: AgentRequest, context: AgentContext) -> AgentResult:
        report_input = context.get_input("latest_monthly_report")
        return AgentResult(
            status="success",
            summary=f"Used {report_input.label}",
            sources=(
                AgentSource(
                    source_type=report_input.source_type,
                    label=report_input.label,
                    location=report_input.location,
                    retrieved_at=context.generated_at,
                    effective_date=report_input.as_of_date,
                ),
            ),
        )


@pytest.fixture
def workspace_tmp_path() -> Path:
    base_dir = default_repo_root() / ".test_tmp"
    base_dir.mkdir(exist_ok=True)

    temp_dir = base_dir / uuid4().hex
    temp_dir.mkdir()

    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_build_agent_context_generates_run_id_and_exposes_latest_report(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path)
    generated_at = datetime(2026, 4, 17, 10, 30, tzinfo=timezone.utc)
    monthly_report = ReportHistoryEntry(
        report_id="monthly-001",
        report_type="monthly",
        report_period_start=date(2026, 3, 1),
        report_period_end=date(2026, 3, 31),
        as_of_date=date(2026, 3, 31),
        generated_at=generated_at,
        report_format="md",
        report_path="reports/monthly-001.md",
        status="generated",
        base_currency="EUR",
        source_snapshot_date=date(2026, 3, 31),
        parameters_json=None,
        report_hash=None,
        notes=None,
    )

    context = build_agent_context(
        agent_name="stub_agent",
        as_of_date=date(2026, 4, 17),
        generated_at=generated_at,
        base_currency="EUR",
        settings=settings,
        input_refs=(
            AgentInputRef(
                key="latest_monthly_report",
                label="Latest monthly report",
                location="reports/monthly-001.md",
                source_type="report",
                as_of_date=date(2026, 3, 31),
                generated_at=generated_at,
            ),
        ),
        report_history=(monthly_report,),
    )

    assert context.run_id
    assert context.has_input("latest_monthly_report")
    assert context.available_input_keys == ("latest_monthly_report",)
    assert context.latest_report("monthly") == monthly_report


def test_base_agent_execute_validates_context_and_returns_result(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path)
    generated_at = datetime(2026, 4, 17, 10, 30, tzinfo=timezone.utc)
    context = build_agent_context(
        agent_name="stub_agent",
        as_of_date=date(2026, 4, 17),
        generated_at=generated_at,
        base_currency="EUR",
        settings=settings,
        input_refs=(
            AgentInputRef(
                key="latest_monthly_report",
                label="Latest monthly report",
                location="reports/monthly-001.md",
                source_type="report",
                as_of_date=date(2026, 3, 31),
                generated_at=generated_at,
            ),
        ),
    )

    result = StubAgent().execute(AgentRequest(input_refs=("latest_monthly_report",)), context)

    assert result.status == "success"
    assert result.sources[0].location == "reports/monthly-001.md"
    assert result.summary == "Used Latest monthly report"


def test_base_agent_validate_request_rejects_missing_context_input(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path)
    context = build_agent_context(
        agent_name="stub_agent",
        as_of_date=date(2026, 4, 17),
        generated_at=datetime(2026, 4, 17, 10, 30, tzinfo=timezone.utc),
        base_currency="EUR",
        settings=settings,
    )

    with pytest.raises(AgentValidationError, match="Missing required agent inputs"):
        StubAgent().execute(AgentRequest(), context)


def test_base_agent_validate_request_rejects_unknown_requested_input(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path)
    context = build_agent_context(
        agent_name="stub_agent",
        as_of_date=date(2026, 4, 17),
        generated_at=datetime(2026, 4, 17, 10, 30, tzinfo=timezone.utc),
        base_currency="EUR",
        settings=settings,
        input_refs=(
            AgentInputRef(
                key="latest_monthly_report",
                label="Latest monthly report",
                location="reports/monthly-001.md",
                source_type="report",
            ),
        ),
    )

    with pytest.raises(AgentValidationError, match="Unknown request input references"):
        StubAgent().execute(AgentRequest(input_refs=("missing_input",)), context)
