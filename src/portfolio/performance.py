"""Cash-flow-aware TWR and MWR calculations for portfolio valuations."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import exp, isfinite, log1p
from typing import Iterable, Sequence

import pandas as pd

from src.portfolio.contributions import classify_external_cash_flows
from src.portfolio.performance_models import (
    CashFlowClassificationIssue,
    DailyPerformanceObservation,
    ExternalCashFlow,
    PerformanceMetric,
    PerformancePeriodResult,
    PortfolioPerformanceResult,
    PortfolioValuation,
)


DEFAULT_PERFORMANCE_PERIODS: tuple[tuple[str, int | None], ...] = (
    ("last_month", 1),
    ("last_quarter", 3),
    ("last_year", 12),
    ("since_inception", None),
)


def calculate_portfolio_performance(
    portfolio_daily_metrics: pd.DataFrame,
    cash_movements: pd.DataFrame,
    *,
    base_currency: str,
    as_of_date: date | None = None,
) -> PortfolioPerformanceResult:
    """Calculate standard performance horizons from valued portfolio history."""
    valuations, invalid_valuation_count, warnings = _normalize_valuations(portfolio_daily_metrics)
    if not valuations:
        raise ValueError("portfolio_daily_metrics does not contain usable valuations")

    resolved_as_of_date = valuations[-1].valuation_date if as_of_date is None else as_of_date
    if resolved_as_of_date not in {valuation.valuation_date for valuation in valuations}:
        raise ValueError(f"No portfolio valuation is available for as_of_date={resolved_as_of_date.isoformat()}")
    valuations = tuple(valuation for valuation in valuations if valuation.valuation_date <= resolved_as_of_date)

    cash_flow_result = classify_external_cash_flows(cash_movements, base_currency=base_currency)
    relevant_cash_flows = tuple(
        flow for flow in cash_flow_result.cash_flows if flow.flow_date <= resolved_as_of_date
    )
    relevant_cash_flow_issues = tuple(
        issue
        for issue in cash_flow_result.issues
        if issue.flow_date is None or issue.flow_date <= resolved_as_of_date
    )
    periods = tuple(
        _calculate_period(
            period_id=period_id,
            months=months,
            valuations=valuations,
            cash_flows=relevant_cash_flows,
            as_of_date=resolved_as_of_date,
            invalid_valuation_count=invalid_valuation_count,
            cash_flow_issues=relevant_cash_flow_issues,
        )
        for period_id, months in DEFAULT_PERFORMANCE_PERIODS
    )
    daily_reason_codes = (
        ("cash_flow_classification_incomplete",)
        if _has_relevant_cash_flow_issue(
            relevant_cash_flow_issues,
            start_date=valuations[0].valuation_date,
            end_date=resolved_as_of_date,
        )
        else ()
    )
    return PortfolioPerformanceResult(
        base_currency=base_currency.strip().upper(),
        as_of_date=resolved_as_of_date,
        daily_returns=calculate_daily_returns(
            valuations,
            relevant_cash_flows,
            reason_codes=daily_reason_codes,
        ),
        periods=periods,
        cash_flow_issues=relevant_cash_flow_issues,
        warnings=warnings,
    )


def calculate_daily_returns(
    valuations: Sequence[PortfolioValuation],
    cash_flows: Sequence[ExternalCashFlow] = (),
    *,
    reason_codes: Sequence[str] = (),
) -> tuple[DailyPerformanceObservation, ...]:
    """Return cash-flow-adjusted observations for consecutive valuations."""
    ordered = _ordered_valuations(valuations)
    observations: list[DailyPerformanceObservation] = []
    for previous, current in zip(ordered[:-1], ordered[1:], strict=True):
        coverage_ratio = _coverage_ratio((previous, current))
        net_flow = sum(
            flow.amount_base
            for flow in cash_flows
            if previous.valuation_date < flow.flow_date <= current.valuation_date
        )
        observation_reason_codes = list(reason_codes)
        if coverage_ratio < 1.0 and "partial_valuation_coverage" not in observation_reason_codes:
            observation_reason_codes.insert(0, "partial_valuation_coverage")

        return_decimal: float | None = None
        status = "partial" if observation_reason_codes else "available"
        reason_code = observation_reason_codes[0] if observation_reason_codes else "ok"
        if previous.market_value_base <= 0 or not isfinite(previous.market_value_base):
            status = "unavailable"
            reason_code = "non_positive_opening_valuation"
        else:
            adjusted_closing_value = current.market_value_base - net_flow
            if not isfinite(adjusted_closing_value) or adjusted_closing_value < 0:
                status = "unavailable"
                reason_code = "invalid_flow_adjusted_valuation"
            else:
                return_decimal = round(
                    (adjusted_closing_value / previous.market_value_base) - 1.0,
                    12,
                )

        observations.append(
            DailyPerformanceObservation(
                previous_valuation_date=previous.valuation_date,
                valuation_date=current.valuation_date,
                opening_value_base=previous.market_value_base,
                closing_value_base=current.market_value_base,
                net_external_flow_base=net_flow,
                return_decimal=return_decimal,
                coverage_ratio=round(coverage_ratio, 8),
                status=status,
                reason_code=reason_code,
            )
        )
    return tuple(observations)


def calculate_time_weighted_return(
    valuations: Sequence[PortfolioValuation],
    cash_flows: Sequence[ExternalCashFlow] = (),
    *,
    reason_codes: Sequence[str] = (),
) -> PerformanceMetric:
    """Chain daily subperiod returns after removing dated external flows."""
    ordered = _ordered_valuations(valuations)
    if not ordered:
        raise ValueError("At least one valuation is required")
    coverage_ratio = _coverage_ratio(ordered)
    if len(ordered) < 2:
        return _unavailable_metric(
            metric_id="twr",
            period_start=ordered[0].valuation_date,
            period_end=ordered[-1].valuation_date,
            observations=0,
            coverage_ratio=coverage_ratio,
            reason_code="insufficient_valuations",
        )

    daily_returns = calculate_daily_returns(ordered, cash_flows, reason_codes=reason_codes)
    for observation in daily_returns:
        if observation.return_decimal is None:
            return _unavailable_metric(
                metric_id="twr",
                period_start=ordered[0].valuation_date,
                period_end=ordered[-1].valuation_date,
                observations=len(daily_returns),
                coverage_ratio=coverage_ratio,
                reason_code=observation.reason_code,
            )

    chained_factor = 1.0
    for observation in daily_returns:
        chained_factor *= 1.0 + float(observation.return_decimal)

    return _available_metric(
        metric_id="twr",
        value=chained_factor - 1.0,
        period_start=ordered[0].valuation_date,
        period_end=ordered[-1].valuation_date,
        observations=len(daily_returns),
        coverage_ratio=coverage_ratio,
        reason_codes=reason_codes,
    )


def calculate_money_weighted_return(
    valuations: Sequence[PortfolioValuation],
    cash_flows: Sequence[ExternalCashFlow] = (),
    *,
    reason_codes: Sequence[str] = (),
) -> PerformanceMetric:
    """Calculate annualized MWR using investor-perspective dated cash flows."""
    ordered = _ordered_valuations(valuations)
    if not ordered:
        raise ValueError("At least one valuation is required")
    coverage_ratio = _coverage_ratio(ordered)
    if len(ordered) < 2 or ordered[0].valuation_date == ordered[-1].valuation_date:
        return _unavailable_metric(
            metric_id="mwr_xirr",
            period_start=ordered[0].valuation_date,
            period_end=ordered[-1].valuation_date,
            observations=0,
            coverage_ratio=coverage_ratio,
            reason_code="insufficient_valuations",
        )
    if ordered[0].market_value_base <= 0:
        return _unavailable_metric(
            metric_id="mwr_xirr",
            period_start=ordered[0].valuation_date,
            period_end=ordered[-1].valuation_date,
            observations=0,
            coverage_ratio=coverage_ratio,
            reason_code="non_positive_opening_valuation",
        )
    if ordered[-1].market_value_base <= 0:
        return _unavailable_metric(
            metric_id="mwr_xirr",
            period_start=ordered[0].valuation_date,
            period_end=ordered[-1].valuation_date,
            observations=0,
            coverage_ratio=coverage_ratio,
            reason_code="non_positive_terminal_valuation",
        )

    investor_cash_flows: list[tuple[date, float]] = [
        (ordered[0].valuation_date, -ordered[0].market_value_base)
    ]
    investor_cash_flows.extend(
        (flow.flow_date, -flow.amount_base)
        for flow in cash_flows
        if ordered[0].valuation_date < flow.flow_date <= ordered[-1].valuation_date
    )
    investor_cash_flows.append((ordered[-1].valuation_date, ordered[-1].market_value_base))

    xirr, failure_reason, dated_observations = _solve_xirr(investor_cash_flows)
    if xirr is None:
        return _unavailable_metric(
            metric_id="mwr_xirr",
            period_start=ordered[0].valuation_date,
            period_end=ordered[-1].valuation_date,
            observations=dated_observations,
            coverage_ratio=coverage_ratio,
            reason_code=failure_reason,
        )
    return _available_metric(
        metric_id="mwr_xirr",
        value=xirr,
        period_start=ordered[0].valuation_date,
        period_end=ordered[-1].valuation_date,
        observations=dated_observations,
        coverage_ratio=coverage_ratio,
        reason_codes=reason_codes,
    )


def _calculate_period(
    *,
    period_id: str,
    months: int | None,
    valuations: Sequence[PortfolioValuation],
    cash_flows: Sequence[ExternalCashFlow],
    as_of_date: date,
    invalid_valuation_count: int,
    cash_flow_issues: Sequence[CashFlowClassificationIssue],
) -> PerformancePeriodResult:
    requested_start = _subtract_months(as_of_date, months) if months is not None else None
    if requested_start is None:
        actual_start = valuations[0].valuation_date
        history_shorter = False
    else:
        baselines = [valuation.valuation_date for valuation in valuations if valuation.valuation_date <= requested_start]
        actual_start = baselines[-1] if baselines else valuations[0].valuation_date
        history_shorter = not baselines and actual_start > requested_start

    period_valuations = tuple(
        valuation for valuation in valuations if actual_start <= valuation.valuation_date <= as_of_date
    )
    period_flows = tuple(
        flow for flow in cash_flows if actual_start < flow.flow_date <= as_of_date
    )
    reason_codes: list[str] = []
    if history_shorter:
        reason_codes.append("history_shorter_than_requested_period")
    if invalid_valuation_count:
        reason_codes.append("invalid_valuation_rows_excluded")
    if _coverage_ratio(period_valuations) < 1.0:
        reason_codes.append("partial_valuation_coverage")
    if _has_relevant_cash_flow_issue(
        cash_flow_issues,
        start_date=actual_start,
        end_date=as_of_date,
    ):
        reason_codes.append("cash_flow_classification_incomplete")

    twr = calculate_time_weighted_return(period_valuations, period_flows, reason_codes=reason_codes)
    mwr = calculate_money_weighted_return(period_valuations, period_flows, reason_codes=reason_codes)
    return PerformancePeriodResult(
        period_id=period_id,
        requested_start=requested_start,
        actual_start=actual_start,
        end_date=as_of_date,
        valuation_count=len(period_valuations),
        external_flow_count=len(period_flows),
        net_external_flow_base=sum(flow.amount_base for flow in period_flows),
        twr=twr,
        mwr=mwr,
        reason_codes=tuple(reason_codes),
    )


def _normalize_valuations(
    portfolio_daily_metrics: pd.DataFrame,
) -> tuple[tuple[PortfolioValuation, ...], int, tuple[str, ...]]:
    required_columns = {"valuation_date", "total_market_value_base"}
    missing_columns = sorted(required_columns - set(portfolio_daily_metrics.columns))
    if missing_columns:
        raise ValueError(f"portfolio_daily_metrics missing required columns: {', '.join(missing_columns)}")

    frame = portfolio_daily_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"], errors="coerce").dt.date
    frame["total_market_value_base"] = pd.to_numeric(frame["total_market_value_base"], errors="coerce")
    if "valuation_coverage_ratio" in frame.columns:
        frame["valuation_coverage_ratio"] = pd.to_numeric(frame["valuation_coverage_ratio"], errors="coerce")
    else:
        frame["valuation_coverage_ratio"] = 1.0

    valid_mask = (
        frame["valuation_date"].notna()
        & frame["total_market_value_base"].map(lambda value: isfinite(float(value)) if pd.notna(value) else False)
        & (frame["total_market_value_base"] >= 0)
    )
    invalid_count = int((~valid_mask).sum())
    frame = frame.loc[valid_mask].sort_values("valuation_date")
    if frame["valuation_date"].duplicated().any():
        raise ValueError("portfolio_daily_metrics contains duplicate valuation dates")

    valuations = tuple(
        PortfolioValuation(
            valuation_date=row.valuation_date,
            market_value_base=float(row.total_market_value_base),
            coverage_ratio=_clean_coverage(row.valuation_coverage_ratio),
        )
        for row in frame.itertuples(index=False)
    )
    warnings = (f"Excluded {invalid_count} invalid valuation row(s).",) if invalid_count else ()
    return valuations, invalid_count, warnings


def _solve_xirr(cash_flows: Iterable[tuple[date, float]]) -> tuple[float | None, str, int]:
    grouped: dict[date, float] = defaultdict(float)
    for flow_date, amount in cash_flows:
        if isfinite(amount):
            grouped[flow_date] += amount
    dated_cash_flows = tuple(sorted((flow_date, amount) for flow_date, amount in grouped.items() if abs(amount) > 1e-12))
    if len(dated_cash_flows) < 2 or dated_cash_flows[0][0] == dated_cash_flows[-1][0]:
        return None, "xirr_insufficient_dated_cash_flows", len(dated_cash_flows)
    amounts = [amount for _, amount in dated_cash_flows]
    if not any(amount < 0 for amount in amounts) or not any(amount > 0 for amount in amounts):
        return None, "xirr_missing_sign_change", len(dated_cash_flows)

    first_date = dated_cash_flows[0][0]
    tolerance = max(sum(abs(amount) for amount in amounts) * 1e-12, 1e-10)
    min_y = -12.0
    max_y = log1p(1_000_000.0)
    step = 0.125
    points = [min_y + index * step for index in range(int((max_y - min_y) / step) + 1)]
    if points[-1] < max_y:
        points.append(max_y)
    values = [_xnpv_y(point, dated_cash_flows, first_date) for point in points]

    roots: list[float] = []
    for point, value in zip(points, values, strict=True):
        if isfinite(value) and abs(value) <= tolerance:
            roots.append(point)
    for left_y, right_y, left_value, right_value in zip(
        points[:-1], points[1:], values[:-1], values[1:], strict=True
    ):
        if not isfinite(left_value) or not isfinite(right_value) or left_value * right_value >= 0:
            continue
        roots.append(
            _bisect_xirr_root(
                left_y,
                right_y,
                dated_cash_flows=dated_cash_flows,
                first_date=first_date,
                tolerance=tolerance,
            )
        )

    distinct_roots: list[float] = []
    for root in sorted(roots):
        if not distinct_roots or abs(root - distinct_roots[-1]) > 1e-7:
            distinct_roots.append(root)
    if not distinct_roots:
        return None, "xirr_no_solution", len(dated_cash_flows)
    if len(distinct_roots) > 1:
        return None, "xirr_multiple_solutions", len(dated_cash_flows)
    return exp(distinct_roots[0]) - 1.0, "ok", len(dated_cash_flows)


def _bisect_xirr_root(
    left_y: float,
    right_y: float,
    *,
    dated_cash_flows: Sequence[tuple[date, float]],
    first_date: date,
    tolerance: float,
) -> float:
    left_value = _xnpv_y(left_y, dated_cash_flows, first_date)
    for _ in range(200):
        midpoint = (left_y + right_y) / 2.0
        midpoint_value = _xnpv_y(midpoint, dated_cash_flows, first_date)
        if abs(midpoint_value) <= tolerance or abs(right_y - left_y) <= 1e-12:
            return midpoint
        if left_value * midpoint_value <= 0:
            right_y = midpoint
        else:
            left_y = midpoint
            left_value = midpoint_value
    return (left_y + right_y) / 2.0


def _xnpv_y(yield_log: float, cash_flows: Sequence[tuple[date, float]], first_date: date) -> float:
    return sum(
        amount * exp(-yield_log * ((flow_date - first_date).days / 365.0))
        for flow_date, amount in cash_flows
    )


def _available_metric(
    *,
    metric_id: str,
    value: float,
    period_start: date,
    period_end: date,
    observations: int,
    coverage_ratio: float,
    reason_codes: Sequence[str],
) -> PerformanceMetric:
    effective_reason_codes = list(reason_codes)
    if coverage_ratio < 1.0 and "partial_valuation_coverage" not in effective_reason_codes:
        effective_reason_codes.insert(0, "partial_valuation_coverage")
    status = "partial" if effective_reason_codes else "available"
    return PerformanceMetric(
        metric_id=metric_id,
        value=round(float(value), 12),
        unit="decimal",
        period_start=period_start,
        period_end=period_end,
        observations=observations,
        coverage_ratio=round(coverage_ratio, 8),
        status=status,
        reason_code=effective_reason_codes[0] if effective_reason_codes else "ok",
    )


def _unavailable_metric(
    *,
    metric_id: str,
    period_start: date,
    period_end: date,
    observations: int,
    coverage_ratio: float,
    reason_code: str,
) -> PerformanceMetric:
    return PerformanceMetric(
        metric_id=metric_id,
        value=None,
        unit="decimal",
        period_start=period_start,
        period_end=period_end,
        observations=observations,
        coverage_ratio=round(coverage_ratio, 8),
        status="unavailable",
        reason_code=reason_code,
    )


def _coverage_ratio(valuations: Sequence[PortfolioValuation]) -> float:
    if not valuations:
        return 0.0
    return min(_clean_coverage(valuation.coverage_ratio) for valuation in valuations)


def _has_relevant_cash_flow_issue(
    issues: Sequence[CashFlowClassificationIssue],
    *,
    start_date: date,
    end_date: date,
) -> bool:
    for issue in issues:
        issue_date = issue.flow_date
        if issue_date is None or start_date < issue_date <= end_date:
            return True
    return False


def _ordered_valuations(
    valuations: Sequence[PortfolioValuation],
) -> tuple[PortfolioValuation, ...]:
    ordered = tuple(sorted(valuations, key=lambda item: item.valuation_date))
    dates = [valuation.valuation_date for valuation in ordered]
    if len(dates) != len(set(dates)):
        raise ValueError("valuations contains duplicate valuation dates")
    return ordered


def _clean_coverage(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not isfinite(parsed):
        return 0.0
    return min(max(parsed, 0.0), 1.0)


def _subtract_months(value: date, months: int) -> date:
    return (pd.Timestamp(value) - pd.DateOffset(months=months)).date()


__all__ = [
    "DEFAULT_PERFORMANCE_PERIODS",
    "calculate_daily_returns",
    "calculate_money_weighted_return",
    "calculate_portfolio_performance",
    "calculate_time_weighted_return",
]
