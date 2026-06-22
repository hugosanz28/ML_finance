"""Shared models and output contracts for portfolio metrics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd


POSITION_METRICS_COLUMNS = [
    "valuation_date",
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "quantity",
    "price_date",
    "price_currency",
    "close_price",
    "market_value_local",
    "fx_rate_to_base",
    "market_value_base",
    "cost_basis_base",
    "unrealized_pnl_base",
    "unrealized_return_pct",
    "weight",
    "valuation_status",
    "pricing_policy",
    "anchor_snapshot_date",
    "anchor_market_price",
    "provider_anchor_price",
    "provider_anchor_price_date",
    "provider_price_age_days",
    "provider_anchor_age_days",
]

PORTFOLIO_DAILY_METRICS_COLUMNS = [
    "valuation_date",
    "total_positions_count",
    "valued_positions_count",
    "missing_price_positions_count",
    "missing_fx_positions_count",
    "valuation_coverage_ratio",
    "return_coverage_ratio",
    "total_market_value_base",
    "total_cost_basis_base",
    "total_unrealized_pnl_base",
    "portfolio_return_pct",
    "daily_change_base",
    "daily_return_pct",
    "running_peak_value_base",
    "drawdown_pct",
]


@dataclass(frozen=True)
class PortfolioMetricsResult:
    """Reusable valuation outputs for reporting and Streamlit."""

    start_date: date
    end_date: date
    base_currency: str
    position_metrics: pd.DataFrame
    portfolio_daily_metrics: pd.DataFrame
    position_metrics_output_path: Path | None = None
    portfolio_daily_output_path: Path | None = None


__all__ = [
    "PORTFOLIO_DAILY_METRICS_COLUMNS",
    "POSITION_METRICS_COLUMNS",
    "PortfolioMetricsResult",
]
