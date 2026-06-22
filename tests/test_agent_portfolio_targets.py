from datetime import date
from pathlib import Path
import shutil
from uuid import uuid4

import pandas as pd
import pytest

from src.agents import run_monthly_agent_pipeline
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
        persist=False,
    )

    target_ref = next(input_ref for input_ref in result.input_refs if input_ref.key == "target_weights")
    assert target_ref.location == str(targets_path)
    assert target_ref.metadata["weights"] == {"core": 0.80, "satellite": 0.20}
    assert result.asistente_aportacion_mensual.metadata["monthly_budget"] == 900
    assert result.asistente_aportacion_mensual.metadata["target_weights"] == {"core": 0.80, "satellite": 0.20}


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
