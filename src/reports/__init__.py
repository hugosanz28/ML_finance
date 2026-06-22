"""Report generation utilities for portfolio reviews."""

from .history import DuckDBReportHistoryRepository, ReportHistoryEntry
from .monthly import (
    generate_monthly_report,
    get_latest_monthly_report,
    render_monthly_report,
)
from .monthly_models import (
    DEFAULT_MONTHLY_PERIODS,
    MonthlyPeriodSummary,
    MonthlyReportResult,
)

__all__ = [
    "DuckDBReportHistoryRepository",
    "DEFAULT_MONTHLY_PERIODS",
    "MonthlyPeriodSummary",
    "MonthlyReportResult",
    "ReportHistoryEntry",
    "generate_monthly_report",
    "get_latest_monthly_report",
    "render_monthly_report",
]
