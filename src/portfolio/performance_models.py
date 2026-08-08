"""Typed contracts for cash-flow-aware portfolio performance."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal


PerformanceStatus = Literal["available", "partial", "unavailable"]


@dataclass(frozen=True)
class PortfolioValuation:
    """One end-of-day portfolio valuation in the configured base currency."""

    valuation_date: date
    market_value_base: float
    coverage_ratio: float = 1.0


@dataclass(frozen=True)
class ExternalCashFlow:
    """Capital entering (positive) or leaving (negative) the portfolio."""

    flow_date: date
    amount_base: float
    movement_type: str
    source_file: str | None = None
    source_row: int | None = None


@dataclass(frozen=True)
class CashFlowClassificationIssue:
    """Stable explanation for a movement excluded from performance inputs."""

    code: str
    message: str
    flow_date: date | None = None
    source_file: str | None = None
    source_row: int | None = None


@dataclass(frozen=True)
class CashFlowClassificationResult:
    """External flows plus explicit exclusions that may reduce confidence."""

    cash_flows: tuple[ExternalCashFlow, ...]
    ignored_internal_count: int
    issues: tuple[CashFlowClassificationIssue, ...]


@dataclass(frozen=True)
class DailyPerformanceObservation:
    """Cash-flow-adjusted return between two consecutive daily valuations."""

    previous_valuation_date: date
    valuation_date: date
    opening_value_base: float
    closing_value_base: float
    net_external_flow_base: float
    return_decimal: float | None
    coverage_ratio: float
    status: PerformanceStatus
    reason_code: str


@dataclass(frozen=True)
class PerformanceMetric:
    """One performance value with its validity and coverage metadata."""

    metric_id: str
    value: float | None
    unit: str
    period_start: date
    period_end: date
    observations: int
    coverage_ratio: float
    status: PerformanceStatus
    reason_code: str


@dataclass(frozen=True)
class PerformancePeriodResult:
    """TWR and MWR calculated over one requested horizon."""

    period_id: str
    requested_start: date | None
    actual_start: date
    end_date: date
    valuation_count: int
    external_flow_count: int
    net_external_flow_base: float
    twr: PerformanceMetric
    mwr: PerformanceMetric
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class PortfolioPerformanceResult:
    """Cash-flow-aware performance horizons for one aligned valuation date."""

    base_currency: str
    as_of_date: date
    daily_returns: tuple[DailyPerformanceObservation, ...]
    periods: tuple[PerformancePeriodResult, ...]
    cash_flow_issues: tuple[CashFlowClassificationIssue, ...] = ()
    warnings: tuple[str, ...] = ()


__all__ = [
    "CashFlowClassificationIssue",
    "CashFlowClassificationResult",
    "DailyPerformanceObservation",
    "ExternalCashFlow",
    "PerformanceMetric",
    "PerformancePeriodResult",
    "PerformanceStatus",
    "PortfolioPerformanceResult",
    "PortfolioValuation",
]
