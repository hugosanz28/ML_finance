"""Portfolio domain logic, historical reconstruction, and dashboard code."""

from .metrics import (
    calculate_portfolio_metrics,
    calculate_portfolio_metrics_from_normalized_degiro,
    load_fx_rates_from_duckdb,
    load_prices_daily_from_duckdb,
    persist_portfolio_metrics,
)
from .metrics_models import (
    PORTFOLIO_DAILY_METRICS_COLUMNS,
    POSITION_METRICS_COLUMNS,
    PortfolioMetricsResult,
)
from .data_quality import (
    DataQualityIssue,
    DataQualityReport,
    check_agent_input_quality,
    check_portfolio_metrics_quality,
    extract_snapshot_as_of_date,
)
from .contributions import net_external_contributions_until
from .positions import (
    ReconstructedPositionHistory,
    load_normalized_degiro_snapshots,
    load_normalized_degiro_transactions,
    persist_reconstructed_positions,
    reconcile_positions_with_snapshots,
    reconstruct_positions_by_date,
    reconstruct_positions_from_normalized_degiro,
)
from .targets import PortfolioTargets, load_portfolio_targets, portfolio_targets_from_mapping

__all__ = [
    "PortfolioMetricsResult",
    "PortfolioTargets",
    "PORTFOLIO_DAILY_METRICS_COLUMNS",
    "POSITION_METRICS_COLUMNS",
    "DataQualityIssue",
    "DataQualityReport",
    "ReconstructedPositionHistory",
    "calculate_portfolio_metrics",
    "calculate_portfolio_metrics_from_normalized_degiro",
    "check_agent_input_quality",
    "check_portfolio_metrics_quality",
    "extract_snapshot_as_of_date",
    "load_normalized_degiro_snapshots",
    "load_normalized_degiro_transactions",
    "load_portfolio_targets",
    "load_fx_rates_from_duckdb",
    "load_prices_daily_from_duckdb",
    "net_external_contributions_until",
    "persist_portfolio_metrics",
    "persist_reconstructed_positions",
    "portfolio_targets_from_mapping",
    "reconcile_positions_with_snapshots",
    "reconstruct_positions_by_date",
    "reconstruct_positions_from_normalized_degiro",
]
