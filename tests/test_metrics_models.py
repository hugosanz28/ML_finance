from src.portfolio import PORTFOLIO_DAILY_METRICS_COLUMNS, POSITION_METRICS_COLUMNS, PortfolioMetricsResult
from src.portfolio import metrics
from src.portfolio.metrics_models import (
    PORTFOLIO_DAILY_METRICS_COLUMNS as MODEL_DAILY_COLUMNS,
    POSITION_METRICS_COLUMNS as MODEL_POSITION_COLUMNS,
    PortfolioMetricsResult as ModelPortfolioMetricsResult,
)


def test_metrics_models_are_public_and_backward_compatible() -> None:
    assert PortfolioMetricsResult is ModelPortfolioMetricsResult
    assert metrics.PortfolioMetricsResult is ModelPortfolioMetricsResult
    assert POSITION_METRICS_COLUMNS == MODEL_POSITION_COLUMNS
    assert PORTFOLIO_DAILY_METRICS_COLUMNS == MODEL_DAILY_COLUMNS
    assert metrics.POSITION_METRICS_COLUMNS == MODEL_POSITION_COLUMNS
    assert metrics.PORTFOLIO_DAILY_METRICS_COLUMNS == MODEL_DAILY_COLUMNS
