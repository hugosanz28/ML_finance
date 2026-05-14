from datetime import date

import pandas as pd

from src.portfolio import PortfolioMetricsResult
from src.portfolio.dashboard_transforms import _build_asset_evolution_frame


def test_asset_evolution_starts_on_first_buy_date() -> None:
    metrics = PortfolioMetricsResult(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 3),
        base_currency="EUR",
        position_metrics=pd.DataFrame(
            [
                {
                    "valuation_date": "2026-01-01",
                    "asset_id": "asset-a",
                    "asset_name": "Asset A",
                    "asset_type": "etf",
                    "quantity": 10,
                    "market_value_base": 1000,
                },
                {
                    "valuation_date": "2026-01-02",
                    "asset_id": "asset-a",
                    "asset_name": "Asset A",
                    "asset_type": "etf",
                    "quantity": 10,
                    "market_value_base": 1100,
                },
                {
                    "valuation_date": "2026-01-03",
                    "asset_id": "asset-a",
                    "asset_name": "Asset A",
                    "asset_type": "etf",
                    "quantity": 10,
                    "market_value_base": 1200,
                },
            ]
        ),
        portfolio_daily_metrics=pd.DataFrame(),
    )
    transactions = pd.DataFrame(
        [
            {
                "asset_id": "asset-a",
                "trade_date": "2026-01-02",
                "transaction_type": "BUY",
            }
        ]
    )

    frame = _build_asset_evolution_frame(
        metrics,
        include_cash=False,
        top_n=10,
        transactions=transactions,
    )

    assert frame["valuation_date"].tolist() == [date(2026, 1, 2), date(2026, 1, 3)]
    assert frame["Asset A"].tolist() == [0.0, 9.09090909]
