from __future__ import annotations

import csv
from datetime import date
import json
from pathlib import Path
import shutil
from uuid import uuid4

import duckdb

from src.application import (
    ApplicationResult,
    GenerateMonthlyReportRequest,
    GenerateMonthlyReportUseCase,
    GetAgentRunAuditRequest,
    GetAgentRunAuditUseCase,
    GetNetExternalContributionsUseCase,
    GetWarehouseCountsUseCase,
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
)
from src.agents import AgentResult, MonthlyAgentPipelineResult
from src.config import default_repo_root, load_settings
from src.degiro_exports.cash_movements import EXPECTED_ACCOUNT_HEADERS
from src.degiro_exports.portfolio_snapshots import EXPECTED_PORTFOLIO_HEADERS
from src.degiro_exports.transactions import EXPECTED_TRANSACTION_HEADERS


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


def test_application_public_use_cases_are_importable() -> None:
    assert ImportDegiroUseCase.name == "import_degiro"
    assert RefreshFxUseCase.name == "refresh_fx"
    assert RefreshMarketDataUseCase.name == "refresh_market_data"
    assert GenerateMonthlyReportUseCase.name == "generate_monthly_report"
    assert RunMonthlyAgentsUseCase.name == "run_monthly_agents"
    assert ListAgentRunsUseCase.name == "list_agent_runs"
    assert GetAgentRunAuditUseCase.name == "get_agent_run_audit"
    assert LoadPortfolioMetricsUseCase.name == "load_portfolio_metrics"
    assert GetWarehouseCountsUseCase.name == "get_warehouse_counts"
    assert GetNetExternalContributionsUseCase.name == "get_net_external_contributions"
    assert ListDashboardReportsUseCase.name == "list_dashboard_reports"


def test_application_request_defaults_are_safe_for_construction() -> None:
    import_request = ImportDegiroRequest()
    fx_request = RefreshFxRequest()
    market_request = RefreshMarketDataRequest()
    report_request = GenerateMonthlyReportRequest()
    metrics_request = LoadPortfolioMetricsRequest()

    assert import_request.load_duckdb is True
    assert fx_request.infer_from_normalized is True
    assert market_request.bootstrap_degiro_assets is True
    assert report_request.persist is True
    assert metrics_request.persist is True


def test_dashboard_read_use_cases_return_safe_defaults() -> None:
    workspace = make_test_workspace()
    try:
        settings = load_settings(repo_root=workspace, env={}, env_file=workspace / ".env.missing")

        brief = ReadInvestmentBriefUseCase(settings=settings).execute()
        targets = ReadTargetWeightsUseCase(settings=settings).execute()
        reports = ListDashboardReportsUseCase(settings=settings).execute()

        assert brief.content == ""
        assert targets.target_weights == {"core": 0.80, "satellite": 0.20}
        assert reports.reports == []
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
        assert audit.input_payload["inputs"][0]["key"] == "investment_brief"
        assert audit.agents["monitor_tematico"]["parsed_output"]["metadata"]["agent_plan"] == ["step"]
        assert audit.agents["monitor_tematico"]["prompt_rendered"] == "# prompt\n"
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

    try:
        result = RunMonthlyAgentsUseCase(settings=settings).execute(
            RunMonthlyAgentsRequest(
                investment_brief_text="brief",
                user_satellite_interest="semiconductors",
                llm_provider="static",
                search_provider="null",
                persist=False,
                monthly_budget=1500.0,
            )
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    assert captured["settings"] == settings
    assert captured["investment_brief_text"] == "brief"
    assert captured["user_satellite_interest"] == "semiconductors"
    assert captured["persist"] is False
    assert captured["monthly_budget"] == 1500.0
    assert result.result.status == "partial"
    assert result.result.artifacts["run_id"] == "run-001"
    assert result.result.warnings == ("analista_activos: missing context",)


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
