from datetime import date

import pandas as pd

from src.portfolio.state_projection import (
    build_broker_snapshot_projection,
    latest_broker_snapshot_view,
)


def test_broker_projection_reuses_snapshot_view_and_external_cost_basis() -> None:
    snapshots = pd.DataFrame(
        [
            {
                "snapshot_date": "2026-01-01",
                "asset_id": "old",
                "asset_name": "Old",
                "asset_type": "etf",
                "isin": "OLD",
                "quantity": 1,
                "market_value_base": 50,
                "unrealized_pnl_base": 5,
            },
            {
                "snapshot_date": "2026-01-02",
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "isin": "TESTA",
                "quantity": 1,
                "market_value_base": 120,
                "unrealized_pnl_base": 15,
            },
            {
                "snapshot_date": "2026-01-02",
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "asset_type": "stock",
                "isin": "TESTB",
                "quantity": 2,
                "market_value_base": 100,
                "unrealized_pnl_base": None,
            },
        ]
    )
    position_metrics = pd.DataFrame(
        [
            {
                "valuation_date": "2026-01-02",
                "asset_id": "asset-a",
                "cost_basis_base": 999,
                "unrealized_pnl_base": -879,
                "unrealized_return_pct": -0.88,
            },
            {
                "valuation_date": "2026-01-02",
                "asset_id": "asset-b",
                "cost_basis_base": 80,
                "unrealized_pnl_base": 20,
                "unrealized_return_pct": 0.25,
            },
        ]
    )
    daily_metrics = pd.DataFrame(
        [
            {
                "valuation_date": "2026-01-02",
                "total_cost_basis_base": 180,
            },
            {
                "valuation_date": "2026-01-03",
                "total_cost_basis_base": 170,
            }
        ]
    )

    projection = build_broker_snapshot_projection(
        snapshots,
        position_metrics=position_metrics,
        portfolio_daily_metrics=daily_metrics,
        as_of_date=date(2026, 1, 3),
        include_isin=True,
    )

    assert projection is not None
    assert projection["snapshot_date"] == date(2026, 1, 2)
    assert projection["total_market_value_base"] == 220.0
    assert projection["total_unrealized_pnl_base"] == 40.0
    assert projection["portfolio_return_pct"] == 40.0 / 180.0
    positions = projection["positions"].set_index("asset_id")
    assert positions.loc["asset-a", "cost_basis_base"] == 105.0
    assert positions.loc["asset-a", "unrealized_pnl_base"] == 15.0
    assert positions.loc["asset-b", "cost_basis_base"] == 80.0
    assert positions.loc["asset-b", "unrealized_pnl_base"] == 20.0
    assert positions.loc["asset-b", "isin"] == "TESTB"

    report_projection = build_broker_snapshot_projection(
        snapshots,
        position_metrics=position_metrics,
        portfolio_daily_metrics=daily_metrics,
        as_of_date=date(2026, 1, 3),
        aggregate_cost_basis_date=date(2026, 1, 3),
        include_isin=True,
    )
    assert report_projection is not None
    assert report_projection["total_unrealized_pnl_base"] == 50.0
    assert report_projection["portfolio_return_pct"] == 50.0 / 170.0


def test_latest_broker_snapshot_view_keeps_dashboard_totals() -> None:
    snapshots = pd.DataFrame(
        [
            {
                "snapshot_date": "2026-01-02",
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "quantity": 1,
                "market_value_base": 120,
                "unrealized_pnl_base": 20,
            },
            {
                "snapshot_date": "2026-01-02",
                "asset_id": "asset-b",
                "asset_name": None,
                "asset_type": None,
                "quantity": 2,
                "market_value_base": 80,
                "unrealized_pnl_base": 0,
            },
        ]
    )

    view = latest_broker_snapshot_view(snapshots)

    assert view is not None
    assert view["total_market_value_base"] == 200.0
    assert view["total_unrealized_pnl_base"] == 20.0
    assert view["portfolio_return_pct"] == 20.0 / 180.0
    assert view["positions"]["weight"].tolist() == [0.6, 0.4]
    assert view["positions"]["asset_name"].tolist() == ["Asset A", "asset-b"]
