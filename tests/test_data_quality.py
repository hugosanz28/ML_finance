from datetime import date

import pandas as pd

from src.application import RunAgentQualityChecksRequest, RunAgentQualityChecksUseCase
from src.portfolio import PortfolioMetricsResult
from src.portfolio.data_quality import check_agent_input_quality, check_portfolio_metrics_quality


def test_portfolio_quality_passes_when_latest_metrics_are_complete() -> None:
    metrics = _metrics_result(
        valuation_coverage_ratio=1.0,
        return_coverage_ratio=1.0,
        missing_price_positions_count=0,
        missing_fx_positions_count=0,
    )

    report = check_portfolio_metrics_quality(metrics)

    assert report.can_run_agents is True
    assert report.error_count == 0
    assert [issue.code for issue in report.issues] == ["portfolio_metrics_ready"]


def test_portfolio_quality_blocks_missing_prices_and_fx() -> None:
    metrics = _metrics_result(
        valuation_coverage_ratio=0.5,
        return_coverage_ratio=1.0,
        missing_price_positions_count=1,
        missing_fx_positions_count=1,
    )

    report = check_portfolio_metrics_quality(metrics)

    assert report.can_run_agents is False
    assert {"missing_prices", "missing_fx", "valuation_coverage_below_threshold"} <= {
        issue.code for issue in report.issues
    }


def test_agent_quality_blocks_inconsistent_report_and_snapshot_dates() -> None:
    metrics = _metrics_result(
        valuation_coverage_ratio=1.0,
        return_coverage_ratio=1.0,
        missing_price_positions_count=0,
        missing_fx_positions_count=0,
    )

    report = check_agent_input_quality(
        metrics=metrics,
        monthly_report_date=date(2026, 5, 25),
        portfolio_metrics_snapshot={"as_of_date": "2026-05-26"},
    )

    assert report.can_run_agents is False
    assert "monthly_report_date_mismatch" in {issue.code for issue in report.issues}
    assert "monthly_report_snapshot_date_mismatch" in {issue.code for issue in report.issues}


def test_run_agent_quality_checks_use_case_returns_application_result() -> None:
    metrics = _metrics_result(
        valuation_coverage_ratio=1.0,
        return_coverage_ratio=0.5,
        missing_price_positions_count=0,
        missing_fx_positions_count=0,
    )
    result = RunAgentQualityChecksUseCase().execute(
        RunAgentQualityChecksRequest(
            metrics=metrics,
            monthly_report_date=date(2026, 5, 26),
            portfolio_metrics_snapshot={"as_of_date": "2026-05-26"},
        )
    )

    assert result.can_run_agents is True
    assert result.result.status == "succeeded"
    assert result.report.warning_count == 1
    assert result.result.warnings


def _metrics_result(
    *,
    valuation_coverage_ratio: float,
    return_coverage_ratio: float,
    missing_price_positions_count: int,
    missing_fx_positions_count: int,
) -> PortfolioMetricsResult:
    daily = pd.DataFrame(
        [
            {
                "valuation_date": date(2026, 5, 25),
                "total_positions_count": 1,
                "valued_positions_count": 1,
                "missing_price_positions_count": 0,
                "missing_fx_positions_count": 0,
                "valuation_coverage_ratio": 1.0,
                "return_coverage_ratio": 1.0,
                "total_market_value_base": 900.0,
            },
            {
                "valuation_date": date(2026, 5, 26),
                "total_positions_count": 2,
                "valued_positions_count": 2 - missing_price_positions_count - missing_fx_positions_count,
                "missing_price_positions_count": missing_price_positions_count,
                "missing_fx_positions_count": missing_fx_positions_count,
                "valuation_coverage_ratio": valuation_coverage_ratio,
                "return_coverage_ratio": return_coverage_ratio,
                "total_market_value_base": 1000.0,
            },
        ]
    )
    positions = pd.DataFrame(
        [
            {
                "valuation_date": date(2026, 5, 26),
                "asset_id": "asset_1",
                "asset_name": "Asset 1",
                "market_value_base": 1000.0,
            }
        ]
    )
    return PortfolioMetricsResult(
        start_date=date(2026, 5, 25),
        end_date=date(2026, 5, 26),
        base_currency="EUR",
        position_metrics=positions,
        portfolio_daily_metrics=daily,
    )
