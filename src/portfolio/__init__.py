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
from .contributions import classify_external_cash_flows, net_external_contributions_until
from .performance import (
    DEFAULT_PERFORMANCE_PERIODS,
    calculate_daily_returns,
    calculate_money_weighted_return,
    calculate_portfolio_performance,
    calculate_time_weighted_return,
)
from .performance_models import (
    CashFlowClassificationIssue,
    CashFlowClassificationResult,
    DailyPerformanceObservation,
    ExternalCashFlow,
    PerformanceMetric,
    PerformancePeriodResult,
    PortfolioPerformanceResult,
    PortfolioValuation,
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
from .targets import PortfolioTargets, load_portfolio_targets, portfolio_targets_from_mapping

__all__ = [
    "PortfolioMetricsResult",
    "PortfolioTargets",
    "PORTFOLIO_DAILY_METRICS_COLUMNS",
    "POSITION_METRICS_COLUMNS",
    "DataQualityIssue",
    "DataQualityReport",
    "DEFAULT_PERFORMANCE_PERIODS",
    "CashFlowClassificationIssue",
    "CashFlowClassificationResult",
    "DailyPerformanceObservation",
    "ExternalCashFlow",
    "PerformanceMetric",
    "PerformancePeriodResult",
    "PortfolioPerformanceResult",
    "PortfolioValuation",
    "ReconstructedPositionHistory",
    "calculate_portfolio_metrics",
    "calculate_portfolio_metrics_from_normalized_degiro",
    "calculate_money_weighted_return",
    "calculate_daily_returns",
    "calculate_portfolio_performance",
    "calculate_time_weighted_return",
    "check_agent_input_quality",
    "check_portfolio_metrics_quality",
    "classify_external_cash_flows",
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
