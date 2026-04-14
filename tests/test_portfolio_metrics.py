from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil
from uuid import uuid4

import pandas as pd
import pytest

from src.config import default_repo_root, load_settings
from src.market_data import DailyPriceRecord, DuckDBMarketDataRepository, MarketAsset
from src.portfolio import calculate_portfolio_metrics, calculate_portfolio_metrics_from_normalized_degiro


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


def test_calculate_portfolio_metrics_values_positions_and_computes_drawdown() -> None:
    positions = pd.DataFrame(
        [
            {"position_date": "2026-01-01", "asset_id": "asset-a", "asset_name": "Asset A", "asset_type": "etf", "quantity": 10},
            {"position_date": "2026-01-02", "asset_id": "asset-a", "asset_name": "Asset A", "asset_type": "etf", "quantity": 10},
            {"position_date": "2026-01-03", "asset_id": "asset-a", "asset_name": "Asset A", "asset_type": "etf", "quantity": 10},
        ]
    )
    prices = pd.DataFrame(
        [
            {"asset_id": "asset-a", "price_date": "2026-01-01", "price_currency": "EUR", "close_price": 10},
            {"asset_id": "asset-a", "price_date": "2026-01-02", "price_currency": "EUR", "close_price": 12},
            {"asset_id": "asset-a", "price_date": "2026-01-03", "price_currency": "EUR", "close_price": 9},
        ]
    )
    transactions = pd.DataFrame(
        [
            {
                "asset_id": "asset-a",
                "trade_date": "2026-01-01",
                "transaction_type": "BUY",
                "quantity": 10,
                "gross_amount_base": 100,
                "fees_amount_base": 0,
                "taxes_amount_base": 0,
                "source_row": 1,
            }
        ]
    )

    metrics = calculate_portfolio_metrics(positions, prices, transactions=transactions, base_currency="EUR")

    daily = metrics.portfolio_daily_metrics.copy()
    daily["valuation_date"] = daily["valuation_date"].dt.date

    assert daily["total_market_value_base"].tolist() == [100.0, 120.0, 90.0]
    assert daily["portfolio_return_pct"].tolist() == [0.0, 0.2, -0.1]
    assert daily["daily_return_pct"].fillna(0.0).tolist() == [0.0, 0.2, -0.25]
    assert daily["drawdown_pct"].tolist() == [0.0, 0.0, -0.25]

    positions_metrics = metrics.position_metrics.copy()
    assert positions_metrics["weight"].tolist() == [1.0, 1.0, 1.0]
    assert positions_metrics["valuation_status"].tolist() == ["valued", "valued", "valued"]


def test_calculate_portfolio_metrics_uses_moving_average_cost_basis_after_sell() -> None:
    positions = pd.DataFrame(
        [
            {"position_date": "2026-01-01", "asset_id": "asset-b", "asset_name": "Asset B", "asset_type": "stock", "quantity": 10},
            {"position_date": "2026-01-02", "asset_id": "asset-b", "asset_name": "Asset B", "asset_type": "stock", "quantity": 20},
            {"position_date": "2026-01-03", "asset_id": "asset-b", "asset_name": "Asset B", "asset_type": "stock", "quantity": 15},
        ]
    )
    prices = pd.DataFrame(
        [
            {"asset_id": "asset-b", "price_date": "2026-01-01", "price_currency": "EUR", "close_price": 10},
            {"asset_id": "asset-b", "price_date": "2026-01-02", "price_currency": "EUR", "close_price": 15},
            {"asset_id": "asset-b", "price_date": "2026-01-03", "price_currency": "EUR", "close_price": 20},
        ]
    )
    transactions = pd.DataFrame(
        [
            {"asset_id": "asset-b", "trade_date": "2026-01-01", "transaction_type": "BUY", "quantity": 10, "gross_amount_base": 100, "fees_amount_base": 0, "taxes_amount_base": 0, "source_row": 1},
            {"asset_id": "asset-b", "trade_date": "2026-01-02", "transaction_type": "BUY", "quantity": 10, "gross_amount_base": 200, "fees_amount_base": 0, "taxes_amount_base": 0, "source_row": 2},
            {"asset_id": "asset-b", "trade_date": "2026-01-03", "transaction_type": "SELL", "quantity": 5, "gross_amount_base": 100, "fees_amount_base": 0, "taxes_amount_base": 0, "source_row": 3},
        ]
    )

    metrics = calculate_portfolio_metrics(positions, prices, transactions=transactions, base_currency="EUR")
    positions_metrics = metrics.position_metrics.copy()

    assert positions_metrics["cost_basis_base"].tolist() == [100.0, 300.0, 225.0]
    assert positions_metrics["unrealized_pnl_base"].tolist() == [0.0, 0.0, 75.0]
    assert positions_metrics["unrealized_return_pct"].tolist() == [0.0, 0.0, pytest.approx(75 / 225)]


def test_calculate_portfolio_metrics_marks_missing_fx_without_conversion_rates() -> None:
    positions = pd.DataFrame(
        [
            {"position_date": "2026-01-01", "asset_id": "asset-c", "asset_name": "Asset C", "asset_type": "stock", "quantity": 2},
        ]
    )
    prices = pd.DataFrame(
        [
            {"asset_id": "asset-c", "price_date": "2026-01-01", "price_currency": "USD", "close_price": 25},
        ]
    )
    transactions = pd.DataFrame(
        [
            {"asset_id": "asset-c", "trade_date": "2026-01-01", "transaction_type": "BUY", "quantity": 2, "gross_amount_base": 40, "fees_amount_base": 0, "taxes_amount_base": 0, "source_row": 1},
        ]
    )

    metrics = calculate_portfolio_metrics(positions, prices, transactions=transactions, base_currency="EUR")

    assert metrics.position_metrics.iloc[0]["valuation_status"] == "missing_fx"
    assert pd.isna(metrics.position_metrics.iloc[0]["market_value_base"])
    assert metrics.portfolio_daily_metrics.iloc[0]["missing_fx_positions_count"] == 1
    assert metrics.portfolio_daily_metrics.iloc[0]["valuation_coverage_ratio"] == 0.0


def test_calculate_portfolio_metrics_uses_fx_rates_for_foreign_currency_assets() -> None:
    positions = pd.DataFrame(
        [
            {"position_date": "2026-01-01", "asset_id": "asset-d", "asset_name": "Asset D", "asset_type": "stock", "quantity": 2},
        ]
    )
    prices = pd.DataFrame(
        [
            {"asset_id": "asset-d", "price_date": "2026-01-01", "price_currency": "USD", "close_price": 25},
        ]
    )
    transactions = pd.DataFrame(
        [
            {"asset_id": "asset-d", "trade_date": "2026-01-01", "transaction_type": "BUY", "quantity": 2, "gross_amount_base": 40, "fees_amount_base": 0, "taxes_amount_base": 0, "source_row": 1},
        ]
    )
    fx_rates = pd.DataFrame(
        [
            {"base_currency": "EUR", "quote_currency": "USD", "rate_date": "2026-01-01", "rate": 2.0},
        ]
    )

    metrics = calculate_portfolio_metrics(
        positions,
        prices,
        transactions=transactions,
        fx_rates=fx_rates,
        base_currency="EUR",
    )

    position_metric = metrics.position_metrics.iloc[0]
    assert position_metric["market_value_local"] == 50.0
    assert position_metric["fx_rate_to_base"] == 2.0
    assert position_metric["market_value_base"] == 25.0
    assert position_metric["valuation_status"] == "valued"


def test_calculate_portfolio_metrics_from_normalized_degiro_loads_duckdb_prices(
    workspace_tmp_path: Path,
) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    transactions_dir = settings.normalized_data_dir / "degiro" / "transactions"
    transactions_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "asset_id": "degiro:isin:IE000NDWFGA5",
                "asset_name": "GLOBAL X URANIUM",
                "asset_type": "etf",
                "isin": "IE000NDWFGA5",
                "trade_date": "2026-01-03",
                "transaction_type": "BUY",
                "quantity": 4,
                "source_row": 1,
                "gross_amount_base": 100,
                "fees_amount_base": 0,
                "taxes_amount_base": 0,
            },
            {
                "asset_id": "degiro:isin:IE000NDWFGA5",
                "asset_name": "GLOBAL X URANIUM",
                "asset_type": "etf",
                "isin": "IE000NDWFGA5",
                "trade_date": "2026-01-05",
                "transaction_type": "BUY",
                "quantity": 2,
                "source_row": 2,
                "gross_amount_base": 60,
                "fees_amount_base": 0,
                "taxes_amount_base": 0,
            },
        ]
    ).to_parquet(transactions_dir / "transactions_a.parquet", index=False)

    repository = DuckDBMarketDataRepository(settings=settings)
    repository.upsert_assets(
        [
            MarketAsset(
                asset_id="degiro:isin:IE000NDWFGA5",
                asset_name="GLOBAL X URANIUM",
                asset_type="etf",
                trading_currency="EUR",
                isin="IE000NDWFGA5",
            )
        ]
    )
    repository.upsert_daily_prices(
        asset_id="degiro:isin:IE000NDWFGA5",
        provider_name="yfinance",
        prices=(
            DailyPriceRecord(price_date=date(2026, 1, 3), price_currency="EUR", close_price=25.0),
            DailyPriceRecord(price_date=date(2026, 1, 4), price_currency="EUR", close_price=25.0),
            DailyPriceRecord(price_date=date(2026, 1, 5), price_currency="EUR", close_price=30.0),
        ),
    )

    metrics = calculate_portfolio_metrics_from_normalized_degiro(
        repository=repository,
        settings=settings,
    )

    daily = metrics.portfolio_daily_metrics.copy()
    daily["valuation_date"] = daily["valuation_date"].dt.date

    assert daily["valuation_date"].tolist() == [date(2026, 1, 3), date(2026, 1, 4), date(2026, 1, 5)]
    assert daily["total_market_value_base"].tolist() == [100.0, 100.0, 180.0]
    assert daily["total_cost_basis_base"].tolist() == [100.0, 100.0, 160.0]
