"""Portfolio domain logic, historical reconstruction, and dashboard code."""

from .metrics import (
    PortfolioMetricsResult,
    calculate_portfolio_metrics,
    calculate_portfolio_metrics_from_normalized_degiro,
    load_fx_rates_from_duckdb,
    load_prices_daily_from_duckdb,
    persist_portfolio_metrics,
)
from .positions import (
    ReconstructedPositionHistory,
    load_normalized_degiro_snapshots,
    load_normalized_degiro_transactions,
    persist_reconstructed_positions,
    reconcile_positions_with_snapshots,
    reconstruct_positions_by_date,
    reconstruct_positions_from_normalized_degiro,
)

__all__ = [
    "PortfolioMetricsResult",
    "ReconstructedPositionHistory",
    "calculate_portfolio_metrics",
    "calculate_portfolio_metrics_from_normalized_degiro",
    "load_normalized_degiro_snapshots",
    "load_normalized_degiro_transactions",
    "load_fx_rates_from_duckdb",
    "load_prices_daily_from_duckdb",
    "persist_portfolio_metrics",
    "persist_reconstructed_positions",
    "reconcile_positions_with_snapshots",
    "reconstruct_positions_by_date",
    "reconstruct_positions_from_normalized_degiro",
]
