from __future__ import annotations

from datetime import date
from pathlib import Path
import uuid

import pandas as pd

from src.config import load_settings
from src.portfolio import PortfolioMetricsResult
from src.reports import DuckDBReportHistoryRepository, generate_monthly_report, get_latest_monthly_report


def _build_metrics() -> PortfolioMetricsResult:
    position_metrics = pd.DataFrame(
        [
            {
                "valuation_date": "2025-11-19",
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "isin": "ISIN-A",
                "quantity": 5,
                "price_date": "2025-11-19",
                "price_currency": "EUR",
                "close_price": 80,
                "market_value_local": 400,
                "fx_rate_to_base": 1.0,
                "market_value_base": 400,
                "cost_basis_base": 360,
                "unrealized_pnl_base": 40,
                "unrealized_return_pct": 40 / 360,
                "weight": 0.8,
                "valuation_status": "valued",
            },
            {
                "valuation_date": "2025-11-19",
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "asset_type": "stock",
                "isin": "ISIN-B",
                "quantity": 10,
                "price_date": "2025-11-19",
                "price_currency": "EUR",
                "close_price": 10,
                "market_value_local": 100,
                "fx_rate_to_base": 1.0,
                "market_value_base": 100,
                "cost_basis_base": 90,
                "unrealized_pnl_base": 10,
                "unrealized_return_pct": 10 / 90,
                "weight": 0.2,
                "valuation_status": "valued",
            },
            {
                "valuation_date": "2026-01-12",
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "isin": "ISIN-A",
                "quantity": 6,
                "price_date": "2026-01-12",
                "price_currency": "EUR",
                "close_price": 83.33333333,
                "market_value_local": 500,
                "fx_rate_to_base": 1.0,
                "market_value_base": 500,
                "cost_basis_base": 430,
                "unrealized_pnl_base": 70,
                "unrealized_return_pct": 70 / 430,
                "weight": 0.625,
                "valuation_status": "valued",
            },
            {
                "valuation_date": "2026-01-12",
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "asset_type": "stock",
                "isin": "ISIN-B",
                "quantity": 20,
                "price_date": "2026-01-12",
                "price_currency": "EUR",
                "close_price": 15,
                "market_value_local": 300,
                "fx_rate_to_base": 1.0,
                "market_value_base": 300,
                "cost_basis_base": 250,
                "unrealized_pnl_base": 50,
                "unrealized_return_pct": 50 / 250,
                "weight": 0.375,
                "valuation_status": "valued",
            },
            {
                "valuation_date": "2026-03-12",
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "isin": "ISIN-A",
                "quantity": 6,
                "price_date": "2026-03-12",
                "price_currency": "EUR",
                "close_price": 90,
                "market_value_local": 540,
                "fx_rate_to_base": 1.0,
                "market_value_base": 540,
                "cost_basis_base": 430,
                "unrealized_pnl_base": 110,
                "unrealized_return_pct": 110 / 430,
                "weight": 0.6,
                "valuation_status": "valued",
            },
            {
                "valuation_date": "2026-03-12",
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "asset_type": "stock",
                "isin": "ISIN-B",
                "quantity": 20,
                "price_date": "2026-03-12",
                "price_currency": "EUR",
                "close_price": 18,
                "market_value_local": 360,
                "fx_rate_to_base": 1.0,
                "market_value_base": 360,
                "cost_basis_base": 250,
                "unrealized_pnl_base": 110,
                "unrealized_return_pct": 110 / 250,
                "weight": 0.4,
                "valuation_status": "valued",
            },
            {
                "valuation_date": "2026-04-12",
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "isin": "ISIN-A",
                "quantity": 7,
                "price_date": "2026-04-12",
                "price_currency": "EUR",
                "close_price": 90,
                "market_value_local": 630,
                "fx_rate_to_base": 1.0,
                "market_value_base": 630,
                "cost_basis_base": 530,
                "unrealized_pnl_base": 100,
                "unrealized_return_pct": 100 / 530,
                "weight": 0.6,
                "valuation_status": "valued",
            },
            {
                "valuation_date": "2026-04-12",
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "asset_type": "stock",
                "isin": "ISIN-B",
                "quantity": 20,
                "price_date": "2026-04-12",
                "price_currency": "EUR",
                "close_price": 21,
                "market_value_local": 420,
                "fx_rate_to_base": 1.0,
                "market_value_base": 420,
                "cost_basis_base": 250,
                "unrealized_pnl_base": 170,
                "unrealized_return_pct": 170 / 250,
                "weight": 0.4,
                "valuation_status": "valued",
            },
            {
                "valuation_date": "2026-04-12",
                "asset_id": "asset-c",
                "asset_name": "Asset C",
                "asset_type": "stock",
                "isin": "ISIN-C",
                "quantity": 3,
                "price_date": "2026-04-12",
                "price_currency": "USD",
                "close_price": 40,
                "market_value_local": 120,
                "fx_rate_to_base": None,
                "market_value_base": None,
                "cost_basis_base": None,
                "unrealized_pnl_base": None,
                "unrealized_return_pct": None,
                "weight": 0.0,
                "valuation_status": "missing_fx",
            },
        ]
    )
    portfolio_daily_metrics = pd.DataFrame(
        [
            {
                "valuation_date": "2025-11-19",
                "total_positions_count": 2,
                "valued_positions_count": 2,
                "missing_price_positions_count": 0,
                "missing_fx_positions_count": 0,
                "valuation_coverage_ratio": 1.0,
                "return_coverage_ratio": 1.0,
                "total_market_value_base": 500,
                "total_cost_basis_base": 450,
                "total_unrealized_pnl_base": 50,
                "portfolio_return_pct": 50 / 450,
                "daily_change_base": 0,
                "daily_return_pct": 0,
                "running_peak_value_base": 500,
                "drawdown_pct": 0,
            },
            {
                "valuation_date": "2026-01-12",
                "total_positions_count": 2,
                "valued_positions_count": 2,
                "missing_price_positions_count": 0,
                "missing_fx_positions_count": 0,
                "valuation_coverage_ratio": 1.0,
                "return_coverage_ratio": 1.0,
                "total_market_value_base": 800,
                "total_cost_basis_base": 680,
                "total_unrealized_pnl_base": 120,
                "portfolio_return_pct": 120 / 680,
                "daily_change_base": 300,
                "daily_return_pct": 0.6,
                "running_peak_value_base": 800,
                "drawdown_pct": 0,
            },
            {
                "valuation_date": "2026-03-12",
                "total_positions_count": 2,
                "valued_positions_count": 2,
                "missing_price_positions_count": 0,
                "missing_fx_positions_count": 0,
                "valuation_coverage_ratio": 1.0,
                "return_coverage_ratio": 1.0,
                "total_market_value_base": 900,
                "total_cost_basis_base": 680,
                "total_unrealized_pnl_base": 220,
                "portfolio_return_pct": 220 / 680,
                "daily_change_base": 100,
                "daily_return_pct": 0.125,
                "running_peak_value_base": 900,
                "drawdown_pct": 0,
            },
            {
                "valuation_date": "2026-04-12",
                "total_positions_count": 3,
                "valued_positions_count": 2,
                "missing_price_positions_count": 0,
                "missing_fx_positions_count": 1,
                "valuation_coverage_ratio": 2 / 3,
                "return_coverage_ratio": 1.0,
                "total_market_value_base": 1050,
                "total_cost_basis_base": 780,
                "total_unrealized_pnl_base": 270,
                "portfolio_return_pct": 270 / 780,
                "daily_change_base": 150,
                "daily_return_pct": 150 / 900,
                "running_peak_value_base": 1050,
                "drawdown_pct": 0,
            },
        ]
    )
    return PortfolioMetricsResult(
        start_date=date(2025, 11, 19),
        end_date=date(2026, 4, 12),
        base_currency="EUR",
        position_metrics=position_metrics,
        portfolio_daily_metrics=portfolio_daily_metrics,
    )


def _build_transactions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-03-20",
                "transaction_type": "BUY",
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "quantity": 1,
                "gross_amount_base": 90,
                "fees_amount_base": 1,
                "taxes_amount_base": 0,
            },
            {
                "trade_date": "2026-01-15",
                "transaction_type": "SELL",
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "quantity": 2,
                "gross_amount_base": 30,
                "fees_amount_base": 0.5,
                "taxes_amount_base": 0,
            },
        ]
    )


def _build_cash_movements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "movement_date": "2026-03-25",
                "value_date": "2026-03-25",
                "movement_type": "DIVIDEND",
                "asset_name": "Asset B",
                "amount_base": 4.5,
                "amount": 4.5,
            },
            {
                "movement_date": "2026-04-01",
                "value_date": "2026-04-01",
                "movement_type": "CASH_ACCOUNT_TRANSFER_IN",
                "asset_name": None,
                "amount_base": 250,
                "amount": 250,
            },
            {
                "movement_date": "2026-04-05",
                "value_date": "2026-04-05",
                "movement_type": "DIVIDEND",
                "asset_name": "Asset C",
                "amount_base": None,
                "amount": 3.2,
            },
        ]
    )


def _build_test_settings(repo_root: Path):
    pytest_tmp_root = repo_root / ".pytest_tmp"
    pytest_tmp_root.mkdir(parents=True, exist_ok=True)
    return load_settings(
        repo_root=repo_root,
        env={
            "DATA_DIR": "private/data",
            "REPORTS_DIR": "private/reports",
        },
    )


def test_generate_monthly_report_renders_requested_periods() -> None:
    workspace_root = Path.cwd() / ".test_tmp"
    workspace_root.mkdir(parents=True, exist_ok=True)
    workspace = workspace_root / f"monthly-report-{uuid.uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=True)
    settings = _build_test_settings(workspace)

    result = generate_monthly_report(
        settings=settings,
        metrics=_build_metrics(),
        transactions=_build_transactions(),
        cash_movements=_build_cash_movements(),
        as_of_date=date(2026, 4, 12),
        persist=False,
    )

    assert result.output_path is None
    assert result.report_id is None
    assert result.history_entry is None
    assert "report_type: monthly" in result.content
    assert "# Informe mensual de cartera (2026-04-12)" in result.content
    assert "## Asignacion actual" in result.content
    assert "### Ultimo mes" in result.content
    assert "### Ultimos 3 meses" in result.content
    assert "### Ultimos 12 meses" in result.content
    assert "Asset A" in result.content
    assert "Asset C" in result.content
    assert "Cobertura historica incompleta" in result.content
    assert "Hay posiciones con cobertura incompleta de precio o FX" in result.content


def test_generate_monthly_report_writes_markdown_file() -> None:
    workspace_root = Path.cwd() / ".test_tmp"
    workspace_root.mkdir(parents=True, exist_ok=True)
    workspace = workspace_root / f"monthly-report-{uuid.uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=True)
    settings = _build_test_settings(workspace)

    result = generate_monthly_report(
        settings=settings,
        metrics=_build_metrics(),
        transactions=_build_transactions(),
        cash_movements=_build_cash_movements(),
        as_of_date=date(2026, 4, 12),
        persist=True,
    )

    assert result.output_path is not None
    assert result.report_id is not None
    assert result.history_entry is not None
    assert result.output_path.exists()
    assert result.output_path.name.startswith("2026-04-12-monthly-")
    assert result.output_path.suffix == ".md"
    assert result.output_path.read_text(encoding="utf-8") == result.content
    assert result.history_entry.report_id == result.report_id
    assert result.history_entry.report_path == str(result.output_path)
    assert result.history_entry.report_period_end == date(2026, 4, 12)
    assert result.history_entry.report_period_start == date(2025, 11, 19)

    latest = get_latest_monthly_report(settings=settings)
    assert latest is not None
    assert latest.report_id == result.report_id

    repository = DuckDBReportHistoryRepository(settings=settings)
    with repository.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM reports_history").fetchone()[0] == 1


def test_generate_monthly_report_keeps_history_across_multiple_runs() -> None:
    workspace_root = Path.cwd() / ".test_tmp"
    workspace_root.mkdir(parents=True, exist_ok=True)
    workspace = workspace_root / f"monthly-report-{uuid.uuid4().hex[:8]}"
    workspace.mkdir(parents=True, exist_ok=True)
    settings = _build_test_settings(workspace)

    first = generate_monthly_report(
        settings=settings,
        metrics=_build_metrics(),
        transactions=_build_transactions(),
        cash_movements=_build_cash_movements(),
        as_of_date=date(2026, 4, 12),
        persist=True,
    )
    second = generate_monthly_report(
        settings=settings,
        metrics=_build_metrics(),
        transactions=_build_transactions(),
        cash_movements=_build_cash_movements(),
        as_of_date=date(2026, 4, 12),
        persist=True,
    )

    assert first.output_path is not None
    assert second.output_path is not None
    assert first.output_path != second.output_path
    assert first.output_path.exists()
    assert second.output_path.exists()

    repository = DuckDBReportHistoryRepository(settings=settings)
    with repository.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM reports_history").fetchone()[0] == 2

    latest = get_latest_monthly_report(settings=settings)
    assert latest is not None
    assert latest.report_id == second.report_id
