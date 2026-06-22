"""Shared models and constants for monthly portfolio reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.reports.history import ReportHistoryEntry


DEFAULT_MONTHLY_PERIODS: tuple[tuple[str, str, int], ...] = (
    ("1m", "Ultimo mes", 1),
    ("3m", "Ultimos 3 meses", 3),
    ("12m", "Ultimos 12 meses", 12),
)

DIVIDEND_MOVEMENT_TYPES = {
    "DIVIDEND",
    "CORPORATE_ACTION_SCRIP_DIVIDEND",
}
CONTRIBUTION_IN_MOVEMENT_TYPES = {
    "DEPOSIT",
    "CASH_ACCOUNT_TRANSFER_IN",
}
CONTRIBUTION_OUT_MOVEMENT_TYPES = {
    "CASH_ACCOUNT_TRANSFER_OUT",
}


@dataclass(frozen=True)
class MonthlyPeriodSummary:
    """Period comparison included in the monthly report."""

    code: str
    label: str
    months: int
    requested_start_date: date
    effective_start_date: date
    end_date: date
    available_coverage_days: int
    total_market_value_start_base: float
    total_market_value_end_base: float
    total_market_value_change_base: float
    total_market_value_change_pct: float | None
    portfolio_return_pct_end: float | None
    drawdown_pct_end: float | None
    valuation_coverage_ratio_end: float
    buy_count: int
    buy_amount_base: float
    sell_count: int
    sell_amount_base: float
    dividend_count: int
    dividend_amount_base: float | None
    dividend_missing_base_count: int
    contribution_in_count: int
    contribution_in_base: float | None
    contribution_out_count: int
    contribution_out_base: float | None
    notable_changes: pd.DataFrame
    notes: tuple[str, ...]


@dataclass(frozen=True)
class MonthlyReportResult:
    """Generated monthly report content plus metadata."""

    report_id: str | None
    as_of_date: date
    generated_at: datetime
    base_currency: str
    output_path: Path | None
    content: str
    current_allocation: pd.DataFrame
    period_summaries: tuple[MonthlyPeriodSummary, ...]
    notes: tuple[str, ...]
    history_entry: ReportHistoryEntry | None


__all__ = [
    "CONTRIBUTION_IN_MOVEMENT_TYPES",
    "CONTRIBUTION_OUT_MOVEMENT_TYPES",
    "DEFAULT_MONTHLY_PERIODS",
    "DIVIDEND_MOVEMENT_TYPES",
    "MonthlyPeriodSummary",
    "MonthlyReportResult",
]
