from __future__ import annotations

import csv
from dataclasses import replace
from datetime import date
import json
from pathlib import Path
import shutil
from types import SimpleNamespace
from uuid import uuid4

import duckdb
import pandas as pd

from src.application import (
    ApplicationResult,
    GenerateMonthlyReportRequest,
    GenerateMonthlyReportUseCase,
    GetAgentRunAuditRequest,
    GetAgentRunAuditUseCase,
    GetNetExternalContributionsUseCase,
    GetPendingDegiroImportStatusUseCase,
    GetPortfolioStateRequest,
    GetPortfolioStateUseCase,
    GetWarehouseCountsUseCase,
    FxRequirementView,
    InferFxRequirementsResult,
    InferFxRequirementsUseCase,
    ImportDegiroRequest,
    ImportDegiroUseCase,
    ListAgentRunsUseCase,
    ListDashboardReportsUseCase,
    LoadPortfolioMetricsRequest,
    LoadPortfolioMetricsUseCase,
    ReadInvestmentBriefUseCase,
    ReadTargetWeightsUseCase,
    RefreshFxRequest,
    RefreshFxUseCase,
    RefreshMarketDataRequest,
    RefreshMarketDataUseCase,
    RunMonthlyAgentsRequest,
    RunMonthlyAgentsUseCase,
    RunMonitorTematicoRequest,
    RunMonitorTematicoUseCase,
    SaveDegiroUploadsUseCase,
    UpdateInvestmentBriefRequest,
    UpdateInvestmentBriefUseCase,
)
from src.application.serialization import json_ready_value
from src.agents import AgentResult, MonthlyAgentPipelineResult
from src.agents.pipeline import _persist_pipeline_result
from src.config import default_repo_root, load_settings
from src.degiro_exports.cash_movements import EXPECTED_ACCOUNT_HEADERS
from src.degiro_exports.portfolio_snapshots import EXPECTED_PORTFOLIO_HEADERS
from src.degiro_exports.transactions import EXPECTED_TRANSACTION_HEADERS
from src.portfolio import PortfolioMetricsResult


def make_test_workspace() -> Path:
    base_dir = default_repo_root() / ".test_tmp"
    base_dir.mkdir(exist_ok=True)
    workspace = base_dir / f"application-{uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=False)
    return workspace


def test_application_result_status_helpers() -> None:
    result = ApplicationResult(name="example", status="succeeded", message="ok")

    assert result.ok is True
    assert result.failed is False
    result.require_success()


def test_application_json_serialization_rejects_non_finite_numbers() -> None:
    payload = json_ready_value(
        {
            "nan": float("nan"),
            "positive_inf": float("inf"),
            "negative_inf": float("-inf"),
            "missing": pd.NA,
            "as_of_date": date(2026, 7, 30),
        }
    )

    assert payload == {
        "nan": None,
        "positive_inf": None,
        "negative_inf": None,
        "missing": None,
        "as_of_date": "2026-07-30",
    }
    json.dumps(payload, allow_nan=False)


def test_application_public_use_cases_are_importable() -> None:
    assert ImportDegiroUseCase.name == "import_degiro"
    assert RefreshFxUseCase.name == "refresh_fx"
    assert RefreshMarketDataUseCase.name == "refresh_market_data"
    assert GenerateMonthlyReportUseCase.name == "generate_monthly_report"
    assert RunMonthlyAgentsUseCase.name == "run_monthly_agents"
    assert RunMonitorTematicoUseCase.name == "run_monitor_tematico"
    assert ListAgentRunsUseCase.name == "list_agent_runs"
    assert GetAgentRunAuditUseCase.name == "get_agent_run_audit"
    assert LoadPortfolioMetricsUseCase.name == "load_portfolio_metrics"
    assert GetPortfolioStateUseCase.name == "get_portfolio_state"
    assert InferFxRequirementsUseCase.name == "infer_fx_requirements"
    assert SaveDegiroUploadsUseCase.name == "save_degiro_uploads"
    assert GetWarehouseCountsUseCase.name == "get_warehouse_counts"
    assert GetPendingDegiroImportStatusUseCase.name == "get_pending_degiro_import_status"
    assert GetNetExternalContributionsUseCase.name == "get_net_external_contributions"
    assert ListDashboardReportsUseCase.name == "list_dashboard_reports"
    assert UpdateInvestmentBriefUseCase.name == "update_investment_brief"


def test_application_request_defaults_are_safe_for_construction() -> None:
    import_request = ImportDegiroRequest()
    fx_request = RefreshFxRequest()
    market_request = RefreshMarketDataRequest()
    report_request = GenerateMonthlyReportRequest()
    metrics_request = LoadPortfolioMetricsRequest()
    state_request = GetPortfolioStateRequest()

    assert import_request.load_duckdb is True
    assert fx_request.infer_from_normalized is True
    assert market_request.bootstrap_degiro_assets is True
    assert report_request.persist is True
    assert metrics_request.persist is True
    assert state_request.include_positions is True
    assert state_request.include_history is False
    json.dumps(state_request.to_dict())


def test_dashboard_read_use_cases_return_safe_defaults() -> None:
    workspace = make_test_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")

        brief = ReadInvestmentBriefUseCase(settings=settings).execute()
        targets = ReadTargetWeightsUseCase(settings=settings).execute()
        reports = ListDashboardReportsUseCase(settings=settings).execute()
        fx_requirements = InferFxRequirementsUseCase(settings=settings).execute()

        assert brief.content == ""
        assert brief.exists is False
        json.dumps(brief.to_dict())
        assert targets.target_weights == {"core": 0.80, "satellite": 0.20}
        assert reports.reports == []
        assert fx_requirements.requirements == ()
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_update_investment_brief_uses_configured_path_and_returns_json_ready_metadata() -> None:
    workspace = make_test_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        initial = ReadInvestmentBriefUseCase(settings=settings).execute()

        updated = UpdateInvestmentBriefUseCase(settings=settings).execute(
            UpdateInvestmentBriefRequest(
                content="# Mandate\n\nLong-term.",
                expected_previous_hash=initial.content_hash,
            )
        )

        assert updated.result.status == "succeeded"
        assert settings.investment_brief_path.read_text(encoding="utf-8") == "# Mandate\n\nLong-term."
        assert updated.result.artifacts["path"] == str(settings.investment_brief_path)
        assert updated.result.artifacts["content_hash"] == updated.content_hash
        assert ReadInvestmentBriefUseCase(settings=settings).execute().content_hash == updated.content_hash
        assert list(settings.investment_brief_path.parent.glob(".investment_brief.md.*.tmp")) == []
        json.dumps(dict(updated.result.artifacts), allow_nan=False)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_update_investment_brief_rejects_a_stale_hash_without_overwriting() -> None:
    workspace = make_test_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        initial = ReadInvestmentBriefUseCase(settings=settings).execute()
        use_case = UpdateInvestmentBriefUseCase(settings=settings)
        first_update = use_case.execute(
            UpdateInvestmentBriefRequest(
                content="First version",
                expected_previous_hash=initial.content_hash,
            )
        )

        stale_update = use_case.execute(
            UpdateInvestmentBriefRequest(
                content="Stale version",
                expected_previous_hash=initial.content_hash,
            )
        )

        assert first_update.result.status == "succeeded"
        assert stale_update.result.status == "failed"
        assert stale_update.content_hash == first_update.content_hash
        assert settings.investment_brief_path.read_text(encoding="utf-8") == "First version"
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_fx_requirement_result_serializes_dates_as_iso_strings() -> None:
    result = InferFxRequirementsResult(
        requirements=(
            FxRequirementView(
                pair="EUR/USD",
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 31),
                source_rows=3,
                missing_base_rows=1,
            ),
        )
    )

    payload = result.to_dict()

    assert payload["requirements"][0]["start_date"] == "2026-01-01"
    assert payload["requirements"][0]["end_date"] == "2026-01-31"
    json.dumps(payload)


def test_monitor_use_case_is_offline_and_json_ready_in_dry_run(tmp_path) -> None:
    settings = load_settings(repo_root=tmp_path, env={}, env_file=tmp_path / ".env.missing")
    report_path = tmp_path / "monthly-report.md"
    report_path.write_text("# Synthetic monthly report\n", encoding="utf-8")

    use_case_result = RunMonitorTematicoUseCase(settings=settings).execute(
        RunMonitorTematicoRequest(
            investment_brief_text="Synthetic configurable mandate.",
            monthly_report_path=report_path,
            dry_run=True,
        )
    )

    assert use_case_result.result.status == "succeeded"
    assert use_case_result.payload["llm_provider"] == "static"
    assert use_case_result.payload["search_provider"] == "null"
    json.dumps(use_case_result.payload)


def test_get_portfolio_state_returns_json_ready_read_model(monkeypatch) -> None:
    workspace = make_test_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        metrics = PortfolioMetricsResult(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
            base_currency="EUR",
            position_metrics=pd.DataFrame(
                [
                    {
                        "valuation_date": "2026-01-02",
                        "asset_id": "asset-a",
                        "asset_name": "Asset A",
                        "asset_type": "etf",
                        "isin": "TEST00000001",
                        "quantity": 2.0,
                        "market_value_base": 120.0,
                        "weight": 1.0,
                        "cost_basis_base": 100.0,
                        "unrealized_pnl_base": 20.0,
                        "unrealized_return_pct": 0.2,
                        "valuation_status": "valued",
                    }
                ]
            ),
            portfolio_daily_metrics=pd.DataFrame(
                [
                    {
                        "valuation_date": "2026-01-01",
                        "total_market_value_base": 110.0,
                        "total_unrealized_pnl_base": 10.0,
                        "portfolio_return_pct": 0.1,
                        "drawdown_pct": 0.0,
                        "valuation_coverage_ratio": 1.0,
                        "missing_price_positions_count": 0,
                        "missing_fx_positions_count": 0,
                    },
                    {
                        "valuation_date": "2026-01-02",
                        "total_market_value_base": 120.0,
                        "total_unrealized_pnl_base": 20.0,
                        "portfolio_return_pct": 0.2,
                        "drawdown_pct": -0.01,
                        "valuation_coverage_ratio": 1.0,
                        "missing_price_positions_count": 0,
                        "missing_fx_positions_count": 0,
                    },
                ]
            ),
        )
        snapshots = pd.DataFrame(
            [
                {
                    "snapshot_date": "2026-01-01",
                    "asset_id": "asset-a",
                    "asset_name": "Asset A",
                    "asset_type": "etf",
                    "quantity": 2.0,
                    "market_value_base": 115.0,
                    "unrealized_pnl_base": 15.0,
                }
            ]
        )
        monkeypatch.setattr(
            "src.application.portfolio_state.calculate_portfolio_metrics_from_normalized_degiro",
            lambda **_kwargs: metrics,
        )
        monkeypatch.setattr(
            "src.application.portfolio_state.load_normalized_degiro_snapshots",
            lambda **_kwargs: snapshots,
        )
        monkeypatch.setattr(
            "src.application.portfolio_state.net_external_contributions_until",
            lambda *_args, **_kwargs: 80.0,
        )

        result = GetPortfolioStateUseCase(settings=settings).execute(
            GetPortfolioStateRequest(
                persist=False,
                include_positions=True,
                include_history=True,
                as_of_date="2026-01-02",
            )
        )
        payload = result.to_dict()

        assert result.as_of_date == "2026-01-02"
        assert result.summary["total_market_value_base"] == 120.0
        assert result.summary["net_external_contributions_base"] == 80.0
        assert result.broker_snapshot == {
            "snapshot_date": "2026-01-01",
            "total_market_value_base": 115.0,
        }
        assert result.positions[0]["asset_id"] == "asset-a"
        assert [row["valuation_date"] for row in result.history] == ["2026-01-01", "2026-01-02"]
        assert result.data_quality == {"warnings": []}
        json.dumps(payload, allow_nan=False)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_pending_degiro_import_status_detects_newer_portfolio_csv() -> None:
    workspace = make_test_workspace()
    try:
        repo_root = workspace / "repo"
        incoming_dir = repo_root / "src" / "degiro_exports" / "local" / "incoming"
        normalized_dir = repo_root / "src" / "data" / "local" / "normalized" / "degiro" / "portfolio_snapshots"
        incoming_dir.mkdir(parents=True)
        normalized_dir.mkdir(parents=True)
        (incoming_dir / "portfolio_2026-05-26.csv").write_text("", encoding="utf-8")
        (incoming_dir / "portfolio_2026-06-29.csv").write_text("", encoding="utf-8")
        (normalized_dir / "portfolio_2026-05-26.parquet").write_text("", encoding="utf-8")

        settings = load_settings(repo_root=repo_root, env={}, env_file=repo_root / ".env.missing")
        status = GetPendingDegiroImportStatusUseCase(settings=settings).execute()

        assert status.latest_incoming_portfolio_date == date(2026, 6, 29)
        assert status.latest_normalized_portfolio_date == date(2026, 5, 26)
        assert status.pending_portfolio_files == ["portfolio_2026-06-29.csv"]
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_agent_audit_use_cases_read_persisted_run() -> None:
    workspace = make_test_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        run_dir = settings.data_dir / "agents" / "monthly_pipeline" / "run-001"
        agent_dir = run_dir / "agents" / "monitor_tematico"
        agent_dir.mkdir(parents=True)
        _write_json(
            run_dir / "run_metadata.json",
            {
                "run_id": "run-001",
                "as_of_date": "2026-05-26",
                "generated_at": "2026-05-26T10:00:00+02:00",
                "agents": {
                    "monitor_tematico": {"status": "success"},
                    "analista_activos": {"status": "partial"},
                    "asistente_aportacion_mensual": {"status": "success"},
                },
            },
        )
        _write_json(
            run_dir / "input_payload.json",
            {"inputs": [{"key": "investment_brief", "metadata": {"content": "brief"}}]},
        )
        _write_json(agent_dir / "parsed_output.json", {"status": "success", "metadata": {"agent_plan": ["step"]}})
        (agent_dir / "prompt_rendered.md").write_text("# prompt\n", encoding="utf-8")

        runs = ListAgentRunsUseCase(settings=settings).execute().runs
        audit = GetAgentRunAuditUseCase(settings=settings).execute(GetAgentRunAuditRequest(run_id="run-001"))

        assert len(runs) == 1
        assert runs[0].run_id == "run-001"
        assert runs[0].status == "partial"
        assert audit.preflight == {}
        assert audit.input_payload["inputs"][0]["key"] == "investment_brief"
        assert audit.agents["monitor_tematico"]["parsed_output"]["metadata"]["agent_plan"] == ["step"]
        assert audit.agents["monitor_tematico"]["prompt_rendered"] == "# prompt\n"
        assert audit.agents["monitor_tematico"]["provider"] == {}
        assert audit.agents["monitor_tematico"]["audit_metadata"] == {}
        assert audit.schema_version == 1
        assert audit.is_legacy is True
        assert audit.compatibility_warnings
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_agent_audit_reader_redacts_credentials_defensively() -> None:
    workspace = make_test_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")
        run_dir = settings.data_dir / "agents" / "monthly_pipeline" / "run-secure"
        agent_dir = run_dir / "agents" / "monitor_tematico"
        _write_json(
            run_dir / "run_metadata.json",
            {
                "run_id": "run-secure",
                "schema_version": 2,
                "token": "sentinel-run-token",  # pragma: allowlist secret
            },
        )
        _write_json(
            run_dir / "input_payload.json",
            {
                "metadata": {
                    "apiKey": "sentinel-input-key",  # pragma: allowlist secret
                }
            },
        )
        _write_json(
            agent_dir / "request.json",
            {
                "metadata": {
                    "bearer_token": "sentinel-request-token",  # pragma: allowlist secret
                }
            },
        )
        _write_json(
            agent_dir / "provider.json",
            {"provider": "legacy", "api_key": "sentinel-provider-secret"},  # pragma: allowlist secret
        )
        _write_json(
            agent_dir / "raw_response.json",
            {
                "status": "captured",
                "responses": [
                    {
                        "Authorization": "Bearer sentinel-authorization",  # pragma: allowlist secret
                        "output_text": "safe output",
                    }
                ],
            },
        )

        audit = GetAgentRunAuditUseCase(settings=settings).execute(
            GetAgentRunAuditRequest(run_id="run-secure")
        )

        serialized = json.dumps(
            {
                "run_metadata": audit.run_metadata,
                "input_payload": audit.input_payload,
                "agent": audit.agents["monitor_tematico"],
            }
        )
        assert "sentinel-provider-secret" not in serialized
        assert "sentinel-authorization" not in serialized
        assert "sentinel-run-token" not in serialized
        assert "sentinel-input-key" not in serialized
        assert "sentinel-request-token" not in serialized
        assert serialized.count("[REDACTED]") == 5
        assert audit.schema_version == 2
        assert audit.is_legacy is False
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_import_degiro_use_case_is_idempotent_when_reimporting_same_exports() -> None:
    workspace = make_test_workspace()
    try:
        repo_root = workspace / "repo"
        incoming_dir = repo_root / "src" / "degiro_exports" / "local" / "incoming"
        incoming_dir.mkdir(parents=True)
        _write_transactions_fixture(incoming_dir / "transactions_2025-11-01_2026-04-12.csv")
        _write_account_fixture(incoming_dir / "account_2025-11-01_2026-04-12.csv")
        _write_portfolio_fixture(incoming_dir / "portfolio_2026-04-12.csv")

        settings = load_settings(repo_root=repo_root, env={}, env_file=repo_root / ".env.missing")
        use_case = ImportDegiroUseCase(settings=settings)

        first_result = use_case.execute()
        first_snapshot = _read_warehouse_snapshot(settings.portfolio_db_path)

        second_result = use_case.execute()
        second_snapshot = _read_warehouse_snapshot(settings.portfolio_db_path)

        assert first_result.result.status == "succeeded"
        assert second_result.result.status == "succeeded"
        assert first_result.import_summary.imported_count == 3
        assert second_result.import_summary.imported_count == 3
        assert first_result.warehouse_summary is not None
        assert second_result.warehouse_summary is not None
        assert second_snapshot == first_snapshot
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_run_monthly_agents_use_case_wraps_pipeline(monkeypatch) -> None:
    captured = {}

    def fake_pipeline(**kwargs):
        captured.update(kwargs)
        return MonthlyAgentPipelineResult(
            run_id="run-001",
            as_of_date=date(2026, 5, 26),
            input_refs=(),
            monitor_tematico=AgentResult(status="success", summary="monitor ok"),
            analista_activos=AgentResult(status="partial", summary="analyst partial", warnings=("missing context",)),
            asistente_aportacion_mensual=AgentResult(status="success", summary="assistant ok"),
            output_dir=Path("out"),
        )

    monkeypatch.setattr("src.application.agents.run_monthly_agent_pipeline", fake_pipeline)
    workspace = make_test_workspace()
    settings = load_settings(repo_root=workspace, env={})
    report_path = workspace / "monthly_report_2026-05-26.md"
    report_path.write_text("as_of_date: 2026-05-26\n", encoding="utf-8")
    metrics = _agent_quality_metrics()

    try:
        result = RunMonthlyAgentsUseCase(settings=settings).execute(
            RunMonthlyAgentsRequest(
                investment_brief_text="brief",
                monthly_report_path=report_path,
                metrics=metrics,
                user_satellite_interest="semiconductors",
                llm_provider="static",
                search_provider="null",
                persist=False,
                monthly_budget=1500.0,
                request_scope={"universe": "portfolio"},
                request_parameters={"max_findings": 4},
                request_constraints={"network": "offline"},
                request_metadata={"origin": "test"},
            )
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    assert captured["settings"] == settings
    assert captured["investment_brief_text"] == "brief"
    assert captured["user_satellite_interest"] == "semiconductors"
    assert captured["metrics"] is metrics
    assert captured["persist"] is False
    assert captured["monthly_budget"] == 1500.0
    assert captured["request_scope"] == {"universe": "portfolio"}
    assert captured["request_parameters"] == {"max_findings": 4}
    assert captured["request_constraints"] == {"network": "offline"}
    assert captured["request_metadata"] == {"origin": "test"}
    assert result.result.status == "partial"
    assert result.quality_result.can_run_agents is True
    assert result.result.artifacts["run_id"] == "run-001"
    assert result.result.warnings == ("analista_activos: missing context",)


def test_run_monthly_agents_blocks_before_pipeline_and_persists_attempt(monkeypatch) -> None:
    pipeline_called = False

    def fail_if_called(**_kwargs):
        nonlocal pipeline_called
        pipeline_called = True
        raise AssertionError("Providers must not be reached after a blocking preflight.")

    monkeypatch.setattr("src.application.agents.run_monthly_agent_pipeline", fail_if_called)
    workspace = make_test_workspace()
    settings = load_settings(repo_root=workspace, env={})
    report_path = workspace / "monthly_report_2026-05-26.md"
    report_path.write_text("as_of_date: 2026-05-26\n", encoding="utf-8")
    output_dir = workspace / "blocked-audit"

    try:
        result = RunMonthlyAgentsUseCase(settings=settings).execute(
            RunMonthlyAgentsRequest(
                investment_brief_text="brief",
                monthly_report_path=report_path,
                metrics=_agent_quality_metrics(missing_price_positions_count=1),
                portfolio_metrics_snapshot={"as_of_date": "2026-05-26"},
                persist=True,
                output_dir=output_dir,
            )
        )

        assert pipeline_called is False
        assert result.pipeline_result is None
        assert result.result.status == "failed"
        assert result.result.artifacts["execution_status"] == "blocked"
        assert (output_dir / "preflight.json").exists()
        assert (output_dir / "run_metadata.json").exists()
        assert (output_dir / "input_payload.json").exists()
        assert not (output_dir / "pipeline_result.json").exists()
        assert not (output_dir / "agents").exists()
        metadata = json.loads((output_dir / "run_metadata.json").read_text(encoding="utf-8"))
        assert metadata["execution_status"] == "blocked"
        assert {item["status"] for item in metadata["agents"].values()} == {"not_run"}
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_run_monthly_agents_continues_with_quality_warnings(monkeypatch) -> None:
    def fake_pipeline(**kwargs):
        return MonthlyAgentPipelineResult(
            run_id="run-warning",
            as_of_date=kwargs["metrics"].end_date,
            input_refs=(),
            monitor_tematico=AgentResult(status="success", summary="monitor ok"),
            analista_activos=AgentResult(status="success", summary="analyst ok"),
            asistente_aportacion_mensual=AgentResult(status="success", summary="assistant ok"),
        )

    monkeypatch.setattr("src.application.agents.run_monthly_agent_pipeline", fake_pipeline)
    workspace = make_test_workspace()
    settings = load_settings(repo_root=workspace, env={})
    report_path = workspace / "monthly_report_2026-05-26.md"
    report_path.write_text("as_of_date: 2026-05-26\n", encoding="utf-8")

    try:
        result = RunMonthlyAgentsUseCase(settings=settings).execute(
            RunMonthlyAgentsRequest(
                investment_brief_text="brief",
                monthly_report_path=report_path,
                metrics=_agent_quality_metrics(return_coverage_ratio=0.5),
                portfolio_metrics_snapshot={"as_of_date": "2026-05-26"},
                persist=False,
            )
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    assert result.pipeline_result is not None
    assert result.quality_result.result.status == "partial"
    assert result.result.status == "partial"
    assert result.result.warnings[0].startswith("quality_preflight:")


def test_run_monthly_agents_persists_warning_preflight_on_allowed_run(monkeypatch) -> None:
    workspace = make_test_workspace()
    settings = load_settings(repo_root=workspace, env={})

    def fake_pipeline(**kwargs):
        pipeline_result = MonthlyAgentPipelineResult(
            run_id="run-warning-persisted",
            as_of_date=kwargs["metrics"].end_date,
            input_refs=(),
            monitor_tematico=AgentResult(status="success", summary="monitor ok"),
            analista_activos=AgentResult(status="success", summary="analyst ok"),
            asistente_aportacion_mensual=AgentResult(status="success", summary="assistant ok"),
        )
        output_dir = _persist_pipeline_result(
            pipeline_result,
            settings=kwargs["settings"],
            output_dir=None,
        )
        return replace(pipeline_result, output_dir=output_dir)

    monkeypatch.setattr("src.application.agents.run_monthly_agent_pipeline", fake_pipeline)
    report_path = workspace / "monthly_report_2026-05-26.md"
    report_path.write_text("as_of_date: 2026-05-26\n", encoding="utf-8")

    try:
        result = RunMonthlyAgentsUseCase(settings=settings).execute(
            RunMonthlyAgentsRequest(
                investment_brief_text="brief",
                monthly_report_path=report_path,
                metrics=_agent_quality_metrics(return_coverage_ratio=0.5),
                portfolio_metrics_snapshot={"as_of_date": "2026-05-26"},
                persist=True,
            )
        )
        output_dir = result.pipeline_result.output_dir if result.pipeline_result else None
        assert output_dir is not None
        metadata = json.loads((output_dir / "run_metadata.json").read_text(encoding="utf-8"))
        preflight = json.loads((output_dir / "preflight.json").read_text(encoding="utf-8"))
        audit = GetAgentRunAuditUseCase(settings=settings).execute(
            GetAgentRunAuditRequest(run_id="run-warning-persisted")
        )

        assert result.result.status == "partial"
        assert preflight["status"] == "passed_with_warnings"
        assert metadata["execution_status"] == "partial"
        assert (output_dir / "pipeline_result.json").exists()
        assert audit.preflight["counts"]["warning"] == 1
        assert audit.agents["monitor_tematico"]["parsed_output"]["status"] == "success"
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_run_monthly_agents_blocks_misaligned_dates_before_pipeline(monkeypatch) -> None:
    def fail_if_called(**_kwargs):
        raise AssertionError("The pipeline must not run with misaligned input dates.")

    monkeypatch.setattr("src.application.agents.run_monthly_agent_pipeline", fail_if_called)
    workspace = make_test_workspace()
    settings = load_settings(repo_root=workspace, env={})
    report_path = workspace / "monthly_report_2026-05-25.md"
    report_path.write_text("as_of_date: 2026-05-25\n", encoding="utf-8")

    try:
        result = RunMonthlyAgentsUseCase(settings=settings).execute(
            RunMonthlyAgentsRequest(
                investment_brief_text="brief",
                monthly_report_path=report_path,
                metrics=_agent_quality_metrics(),
                portfolio_metrics_snapshot={"as_of_date": "2026-05-26"},
                persist=False,
            )
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    issue_codes = {issue.code for issue in result.quality_result.report.issues}
    assert result.pipeline_result is None
    assert result.result.status == "failed"
    assert "monthly_report_date_mismatch" in issue_codes
    assert "monthly_report_snapshot_date_mismatch" in issue_codes


def test_run_monthly_agents_blocks_selected_report_without_date(monkeypatch) -> None:
    def fail_if_called(**_kwargs):
        raise AssertionError("The pipeline must not run with an undated selected report.")

    monkeypatch.setattr("src.application.agents.run_monthly_agent_pipeline", fail_if_called)
    workspace = make_test_workspace()
    settings = load_settings(repo_root=workspace, env={})
    report_path = workspace / "monthly_report_without_date.md"
    report_path.write_text("# Monthly report without metadata\n", encoding="utf-8")

    try:
        result = RunMonthlyAgentsUseCase(settings=settings).execute(
            RunMonthlyAgentsRequest(
                investment_brief_text="brief",
                monthly_report_path=report_path,
                metrics=_agent_quality_metrics(),
                portfolio_metrics_snapshot={"as_of_date": "2026-05-26"},
                persist=False,
            )
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    assert result.pipeline_result is None
    assert "monthly_report_date_missing" in {
        issue.code for issue in result.quality_result.report.issues
    }


def test_run_monthly_agents_uses_latest_report_content_date_for_preflight(monkeypatch) -> None:
    def fail_if_called(**_kwargs):
        raise AssertionError("Stale report metadata must not bypass the content date.")

    monkeypatch.setattr("src.application.agents.run_monthly_agent_pipeline", fail_if_called)
    workspace = make_test_workspace()
    settings = load_settings(repo_root=workspace, env={})
    report_path = workspace / "latest_monthly_report.md"
    report_path.write_text("as_of_date: 2026-05-25\n", encoding="utf-8")
    monkeypatch.setattr(
        "src.application.agents.get_latest_monthly_report",
        lambda **_kwargs: SimpleNamespace(
            report_path=report_path,
            as_of_date=date(2026, 5, 26),
        ),
    )

    try:
        result = RunMonthlyAgentsUseCase(settings=settings).execute(
            RunMonthlyAgentsRequest(
                investment_brief_text="brief",
                metrics=_agent_quality_metrics(),
                portfolio_metrics_snapshot={"as_of_date": "2026-05-26"},
                persist=False,
            )
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    assert result.pipeline_result is None
    assert "monthly_report_date_mismatch" in {
        issue.code for issue in result.quality_result.report.issues
    }


def _agent_quality_metrics(
    *,
    return_coverage_ratio: float = 1.0,
    missing_price_positions_count: int = 0,
) -> PortfolioMetricsResult:
    total_positions_count = 1
    valued_positions_count = total_positions_count - missing_price_positions_count
    daily = pd.DataFrame(
        [
            {
                "valuation_date": date(2026, 5, 26),
                "total_positions_count": total_positions_count,
                "valued_positions_count": valued_positions_count,
                "missing_price_positions_count": missing_price_positions_count,
                "missing_fx_positions_count": 0,
                "valuation_coverage_ratio": float(valued_positions_count),
                "return_coverage_ratio": return_coverage_ratio,
                "total_market_value_base": 1000.0,
            }
        ]
    )
    positions = pd.DataFrame(
        [
            {
                "valuation_date": date(2026, 5, 26),
                "asset_id": "asset-1",
                "asset_name": "Asset 1",
                "market_value_base": 1000.0,
            }
        ]
    )
    return PortfolioMetricsResult(
        start_date=date(2026, 5, 26),
        end_date=date(2026, 5, 26),
        base_currency="EUR",
        position_metrics=positions,
        portfolio_daily_metrics=daily,
    )


def _read_warehouse_snapshot(db_path: Path) -> dict[str, object]:
    with duckdb.connect(str(db_path), read_only=True) as connection:
        return {
            "assets_count": connection.execute("SELECT COUNT(*) FROM assets_master").fetchone()[0],
            "transactions_count": connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0],
            "cash_movements_count": connection.execute("SELECT COUNT(*) FROM cash_movements").fetchone()[0],
            "portfolio_snapshots_count": connection.execute("SELECT COUNT(*) FROM portfolio_snapshots").fetchone()[0],
            "transaction_ids": tuple(
                row[0] for row in connection.execute("SELECT transaction_id FROM transactions ORDER BY 1").fetchall()
            ),
            "cash_movement_ids": tuple(
                row[0] for row in connection.execute("SELECT cash_movement_id FROM cash_movements ORDER BY 1").fetchall()
            ),
            "snapshot_ids": tuple(
                row[0] for row in connection.execute("SELECT snapshot_id FROM portfolio_snapshots ORDER BY 1").fetchall()
            ),
            "transaction_net_cash_total": connection.execute(
                "SELECT SUM(net_cash_amount) FROM transactions"
            ).fetchone()[0],
            "cash_movement_amount_total": connection.execute(
                "SELECT SUM(amount) FROM cash_movements"
            ).fetchone()[0],
            "snapshot_market_value_total": connection.execute(
                "SELECT SUM(market_value_base) FROM portfolio_snapshots"
            ).fetchone()[0],
        }


def _write_transactions_fixture(csv_path: Path) -> None:
    rows = [
        [
            "02-04-2026",
            "09:10",
            "STST SPDR MSCI ALL CNTRY WORLD EURH",
            "IE00BF1B7389",
            "XET",
            "XETA",
            "64",
            "23,1800",
            "EUR",
            "-1483,52",
            "EUR",
            "-1483,52",
            "",
            "0,00",
            "-3,00",
            "-1486,52",
            "",
            "1abb5ced-f580-4dfd-8a35-c838518e8ef9",
        ],
    ]
    _write_csv(csv_path, EXPECTED_TRANSACTION_HEADERS, rows)


def _write_account_fixture(csv_path: Path) -> None:
    rows = [
        [
            "15-01-2026",
            "10:00",
            "15-01-2026",
            "",
            "",
            "Ingreso",
            "",
            "EUR",
            "1000,00",
            "EUR",
            "1000,00",
            "",
        ],
    ]
    _write_csv(csv_path, EXPECTED_ACCOUNT_HEADERS, rows)


def _write_portfolio_fixture(csv_path: Path) -> None:
    rows = [["CASH & CASH FUND & FTX CASH (EUR)", "", "", "", "EUR", "1000,00", "1000,00"]]
    _write_csv(csv_path, EXPECTED_PORTFOLIO_HEADERS, rows)


def _write_csv(csv_path: Path, header: list[str], rows: list[list[str]]) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
