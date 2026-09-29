from datetime import date

import pandas as pd
import pytest

from src.portfolio.cash_history import reconcile_cash_history
from src.portfolio.metrics import calculate_portfolio_metrics
from src.portfolio.performance import calculate_portfolio_performance


def test_deposit_then_purchase_does_not_create_returns_and_sweeps_are_internal():
    positions = pd.DataFrame([
        {"position_date": day, "asset_id": "stock", "asset_type": "stock", "quantity": quantity}
        for day, quantity in [("2026-01-01", 1), ("2026-01-02", 1), ("2026-01-03", 2)]
    ])
    cash = pd.DataFrame([
        {"movement_date": "2026-01-01", "movement_type": "DEPOSIT", "amount": 200, "running_balance": 200},
        {"movement_date": "2026-01-01", "movement_type": "TRADE_SETTLEMENT_BUY", "amount": -100, "running_balance": 100},
        {"movement_date": "2026-01-03", "value_date": "2026-01-02", "movement_type": "DEPOSIT", "amount": 100, "running_balance": 200},
        {"movement_date": "2026-01-03", "movement_type": "TRADE_SETTLEMENT_BUY", "amount": -100, "running_balance": 100},
        {"movement_date": "2026-01-03", "movement_type": "CASH_SWEEP_TRANSFER", "amount": 100, "running_balance": 100},
        {"movement_date": "2026-01-03", "movement_type": "CASH_ACCOUNT_TRANSFER_IN", "amount": 100, "running_balance": 0},
    ]).assign(movement_currency="EUR", amount_base=lambda x: x.amount, base_currency="EUR")
    snapshots = pd.DataFrame([{"asset_id": "degiro:cash:eur", "snapshot_date": "2026-01-03", "quantity": 100}])
    rebuilt = reconcile_cash_history(positions, cash, snapshots)
    assert rebuilt.loc[rebuilt.asset_type == "cash", "quantity"].tolist() == [100, 200, 100]
    prices = pd.DataFrame([{"asset_id": "stock", "price_date": "2026-01-01", "close_price": 100, "price_currency": "EUR"}])
    metrics = calculate_portfolio_metrics(rebuilt, prices)
    result = calculate_portfolio_performance(metrics.portfolio_daily_metrics, cash, base_currency="EUR", as_of_date=date(2026, 1, 3))
    assert [r.return_decimal for r in result.daily_returns] == pytest.approx([0, 0])


def test_snapshot_disagreement_blocks_cash_valuation():
    positions = pd.DataFrame([{"position_date": "2026-01-01", "asset_id": "stock", "quantity": 1}])
    cash = pd.DataFrame([{"movement_date": "2026-01-01", "movement_type": "DEPOSIT", "amount": 100,
                          "movement_currency": "EUR", "running_balance": 100}])
    snapshots = pd.DataFrame([{"snapshot_date": "2026-01-01", "asset_id": "degiro:cash:eur", "quantity": 200}])
    rebuilt = reconcile_cash_history(positions, cash, snapshots)
    metrics = calculate_portfolio_metrics(rebuilt, pd.DataFrame())
    row = metrics.position_metrics.loc[metrics.position_metrics.asset_id == "degiro:cash:eur"].iloc[0]
    assert row.valuation_status == "cash_balance_mismatch"
    assert pd.isna(row.market_value_base)


def test_unknown_zero_cash_is_not_valued():
    positions = pd.DataFrame([{"position_date": "2026-01-01", "asset_id": "degiro:cash:eur",
                               "asset_type": "cash", "quantity": 0, "cash_reconciled": False}])
    metrics = calculate_portfolio_metrics(positions, pd.DataFrame())
    assert metrics.position_metrics.iloc[0].valuation_status == "cash_balance_mismatch"


def test_overlapping_exports_do_not_duplicate_cash_or_external_flows(tmp_path):
    from src.config import load_settings
    from src.portfolio.cash_history import load_cash_movements
    from src.reports.monthly import load_normalized_degiro_cash_movements
    settings = load_settings(repo_root=tmp_path, env_file=tmp_path / "absent.env", env={})
    directory = settings.normalized_data_dir / "degiro" / "cash_movements"
    directory.mkdir(parents=True)
    row = {"movement_date": "2026-01-01", "movement_time": "10:00", "movement_type": "DEPOSIT",
           "amount": 100, "movement_currency": "EUR", "running_balance": 100}
    pd.DataFrame([dict(row, source_file="first")]).to_parquet(directory / "a.parquet")
    pd.DataFrame([dict(row, source_file="second")]).to_parquet(directory / "b.parquet")
    assert len(load_cash_movements(settings)) == 1
    assert len(load_normalized_degiro_cash_movements(settings=settings)) == 1


def test_distinct_assets_and_conversion_legs_survive_cash_deduplication(tmp_path):
    from src.config import load_settings
    from src.portfolio.cash_history import load_cash_movements
    settings = load_settings(repo_root=tmp_path, env_file=tmp_path / "absent.env", env={})
    directory = settings.normalized_data_dir / "degiro" / "cash_movements"
    directory.mkdir(parents=True)
    rows = pd.DataFrame([
        dict(asset_id="rights", description="Venta 10 Fixture@0 EUR"),
        dict(asset_id="rights", description="Compra 10 Fixture - Non tradeable@0 EUR"),
        dict(asset_id="other", description="Venta 10 Other@0 EUR"),
    ]).assign(movement_date="2026-01-01", movement_type="CORPORATE_ACTION_SCRIP_DIVIDEND",
              amount=0, running_balance=100, movement_currency="EUR")
    rows.to_parquet(directory / "a.parquet")
    rows.to_parquet(directory / "overlap.parquet")
    assert len(load_cash_movements(settings)) == 3
