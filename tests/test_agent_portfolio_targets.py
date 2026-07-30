from datetime import date
from pathlib import Path
import shutil
from uuid import uuid4

import pandas as pd
import pytest

from src.agents import run_monthly_agent_pipeline
from src.application import GetAgentRunAuditRequest, GetAgentRunAuditUseCase
from src.config import default_repo_root, load_settings
from src.portfolio import PortfolioMetricsResult


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


def test_monthly_agent_pipeline_passes_portfolio_targets_to_contribution_agent(workspace_tmp_path: Path) -> None:
    report_path = workspace_tmp_path / "reports" / "2026-04-30-monthly.md"
    report_path.parent.mkdir(parents=True)
    report_path.write_text("as_of_date: 2026-04-30\n\n## Asignacion actual\n", encoding="utf-8")
    targets_path = workspace_tmp_path / "private" / "portfolio_targets.yaml"
    targets_path.parent.mkdir(parents=True)
    targets_path.write_text(
        "\n".join(
            [
                "base_currency: EUR",
                "monthly_contribution: 900",
                "target_allocation:",
                "  core: 80",
                "  satellite: 20",
                "rebalance_mode: contributions_only",
            ]
        ),
        encoding="utf-8",
    )
    settings = load_settings(
        repo_root=workspace_tmp_path,
        env={"PORTFOLIO_TARGETS_PATH": str(targets_path)},
        env_file=workspace_tmp_path / ".env.missing",
    )

    result = run_monthly_agent_pipeline(
        settings=settings,
        investment_brief_text="Cuenta de largo plazo con core diversificado.",
        monthly_report_path=report_path,
        metrics=_metrics(),
        llm_provider="static",
        search_provider="null",
        persist=True,
        request_scope={"universe": "current_portfolio"},
        request_parameters={"max_findings": 4},
        request_constraints={"network": "offline"},
        request_metadata={"origin": "test"},
    )

    target_ref = next(input_ref for input_ref in result.input_refs if input_ref.key == "target_weights")
    assert target_ref.location == str(targets_path)
    assert target_ref.metadata["weights"] == {"core": 0.80, "satellite": 0.20}
    assert result.asistente_aportacion_mensual.metadata["monthly_budget"] == 900
    assert result.asistente_aportacion_mensual.metadata["target_weights"] == {"core": 0.80, "satellite": 0.20}

    common_input_keys = tuple(input_ref.key for input_ref in result.input_refs)
    expected_input_refs = {
        "monitor_tematico": common_input_keys,
        "analista_activos": (*common_input_keys, "monitor_tematico_result"),
        "asistente_aportacion_mensual": (
            *common_input_keys,
            "monitor_tematico_result",
            "analista_activos_result",
        ),
    }
    for agent_name, request in result.agent_requests.items():
        assert request.scope == {"universe": "current_portfolio"}
        assert request.parameters == {"max_findings": 4}
        assert request.constraints == {"network": "offline"}
        assert request.metadata == {"origin": "test"}
        assert request.input_refs == expected_input_refs[agent_name]
        assert tuple(
            input_ref["key"]
            for input_ref in result.agent_contexts[agent_name]["input_refs"]
        ) == expected_input_refs[agent_name]

    assert result.raw_responses["monitor_tematico"]["status"] == "not_captured"
    assert set(result.raw_responses["monitor_tematico"]["providers"]) == {"llm", "search"}
    assert (
        result.raw_responses["monitor_tematico"]["providers"]["search"]["reason_code"]
        == "deterministic_provider_no_raw_response"
    )
    assert "api_key" not in str(result.provider_configs).lower()

    audit = GetAgentRunAuditUseCase(settings=settings).execute(
        GetAgentRunAuditRequest(run_id=result.run_id)
    )
    assert audit.schema_version == 2
    assert audit.is_legacy is False
    assert audit.run_metadata["input_hash"].startswith("sha256:")
    for agent_name, expected_refs in expected_input_refs.items():
        persisted = audit.agents[agent_name]
        assert persisted["request"]["input_refs"] == list(expected_refs)
        assert persisted["request"]["scope"] == {"universe": "current_portfolio"}
        assert persisted["provider"]["providers"]["llm"]["provider"] == "static_llm"
        assert persisted["audit_metadata"]["hash_projection"] == "semantic-v1"
    monitor_raw = audit.agents["monitor_tematico"]["raw_response"]
    assert set(monitor_raw["providers"]) == {"llm", "search"}


def _metrics() -> PortfolioMetricsResult:
    daily = pd.DataFrame(
        [
            {
                "valuation_date": date(2026, 4, 30),
                "total_market_value_base": 10000.0,
                "total_cost_basis_base": 9500.0,
                "total_unrealized_pnl_base": 500.0,
                "portfolio_return_pct": 0.05263158,
                "drawdown_pct": 0.0,
                "valuation_coverage_ratio": 1.0,
            }
        ]
    )
    positions = pd.DataFrame(
        [
            {
                "valuation_date": date(2026, 4, 30),
                "asset_id": "CORE",
                "asset_name": "Core ETF",
                "asset_type": "etf",
                "isin": "IE00CORE",
                "quantity": 10.0,
                "market_value_base": 8000.0,
                "cost_basis_base": 7600.0,
                "unrealized_pnl_base": 400.0,
                "unrealized_return_pct": 0.05263158,
                "weight": 0.80,
                "valuation_status": "priced",
            },
            {
                "valuation_date": date(2026, 4, 30),
                "asset_id": "SAT",
                "asset_name": "Satellite Stock",
                "asset_type": "stock",
                "isin": "US00SAT",
                "quantity": 5.0,
                "market_value_base": 2000.0,
                "cost_basis_base": 1900.0,
                "unrealized_pnl_base": 100.0,
                "unrealized_return_pct": 0.05263158,
                "weight": 0.20,
                "valuation_status": "priced",
            },
        ]
    )
    return PortfolioMetricsResult(
        start_date=date(2026, 4, 30),
        end_date=date(2026, 4, 30),
        base_currency="EUR",
        position_metrics=positions,
        portfolio_daily_metrics=daily,
    )
