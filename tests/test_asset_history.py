from datetime import date

import pandas as pd
import pytest

from src.portfolio.asset_history import project_asset_history


def test_price_proxy_ignores_quantity_changes_and_preserves_gaps_and_first_buy():
    rows = pd.DataFrame([
        {"asset_id": "a", "asset_name": pd.NA, "asset_type": "etf", "valuation_date": day,
         "quantity": quantity, "market_value_base": value}
        for day, quantity, value in [("2026-01-01", 1, 5), ("2026-01-02", 10, 1100),
                                     ("2026-01-03", 20, 2400), ("2026-01-05", 0, 0),
                                     ("2026-01-06", 10, 1300)]
    ])
    transactions = pd.DataFrame([{"asset_id": "a", "trade_date": "2026-01-02", "transaction_type": "BUY"}])
    result = project_asset_history(rows, transactions, as_of_date=date(2026, 1, 5))
    assert result[0]["asset_name"] == "a"
    assert result[0]["series_kind"] == "valuation_price_proxy"
    assert [point["valuation_date"] for point in result[0]["points"]] == [
        "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05",
    ]
    assert [point["price_change"] for point in result[0]["points"]] == [0, pytest.approx(120 / 110 - 1), None, None]


def test_empty_or_future_history_is_empty():
    assert project_asset_history(pd.DataFrame(), pd.DataFrame(), as_of_date=date(2026, 1, 1)) == []
    rows = pd.DataFrame([{"asset_id": "a", "valuation_date": "2026-01-02"}])
    assert project_asset_history(rows, pd.DataFrame(), as_of_date=date(2026, 1, 1)) == []


def test_distinct_assets_with_same_name_are_not_combined():
    rows = pd.DataFrame([
        {"asset_id": asset, "asset_name": "Same", "valuation_date": "2026-01-01", "quantity": 1, "market_value_base": value}
        for asset, value in [("a", 100), ("b", float("inf"))]
    ])
    result = project_asset_history(rows, pd.DataFrame(), as_of_date=date(2026, 1, 1))
    assert [asset["asset_id"] for asset in result] == ["a", "b"]
    assert result[1]["points"][0]["price_change"] is None
