"""Offline risk diagnostics over cash-flow-adjusted calendar-day returns."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isfinite, sqrt
from statistics import covariance, mean, stdev, variance
from typing import Mapping, Sequence

from src.portfolio.performance_models import DailyPerformanceObservation, PerformanceMetric


MIN_OBSERVATIONS = 30
PERIODS_PER_YEAR = 365


@dataclass(frozen=True)
class RiskResult:
    metrics: tuple[PerformanceMetric, ...]
    periods_per_year: int = PERIODS_PER_YEAR
    risk_free_rate_annual: float | None = None


def _metric(
    metric_id: str, value: float | None, unit: str, start: date, end: date,
    count: int, coverage: float, reason: str = "ok",
) -> PerformanceMetric:
    if value is not None and not isfinite(value):
        value, reason = None, "non_finite_result"
    return PerformanceMetric(
        metric_id, None if value is None else round(value, 12), unit, start, end,
        count, round(coverage, 8),
        "unavailable" if value is None else ("available" if reason == "ok" else "partial"), reason,
    )


def _daily_map(
    observations: Sequence[DailyPerformanceObservation], start: date, end: date,
) -> dict[date, DailyPerformanceObservation]:
    result: dict[date, DailyPerformanceObservation] = {}
    for row in observations:
        if row.valuation_date <= start or row.valuation_date > end:
            continue
        if row.valuation_date in result:
            raise ValueError("Duplicate return dates")
        if (row.valuation_date - row.previous_valuation_date).days != 1:
            raise ValueError("Risk analytics require calendar-day returns; do not annualize multi-day intervals as daily")
        result[row.valuation_date] = row
    return result


def _usable(row: DailyPerformanceObservation) -> bool:
    return (
        row.return_decimal is not None and isfinite(row.return_decimal)
        and row.return_decimal >= -1 and row.status != "unavailable"
        and isfinite(row.coverage_ratio) and 0 < row.coverage_ratio <= 1
    )


def calculate_risk(
    observations: Sequence[DailyPerformanceObservation], *, start_date: date, end_date: date,
    risk_free_rate_annual: float | None = None,
) -> RiskResult:
    """Use 365 calendar days; require 30 returns for annualized statistics."""
    if end_date <= start_date:
        raise ValueError("end_date must be after start_date")
    if risk_free_rate_annual is not None and (
        not isfinite(risk_free_rate_annual) or risk_free_rate_annual <= -1
    ):
        raise ValueError("risk_free_rate_annual must be finite and greater than -1")
    rows = _daily_map(observations, start_date, end_date)
    usable = [row for _, row in sorted(rows.items()) if _usable(row)]
    values = [float(row.return_decimal) for row in usable]
    n = len(values)
    expected = (end_date - start_date).days
    coverage = sum(row.coverage_ratio for row in usable) / expected
    partial = coverage < 1 or any(row.status != "available" for row in usable)
    reason = "partial_return_coverage" if partial else "ok"
    metrics: list[PerformanceMetric] = []

    def add(metric_id: str, value: float | None, unit: str, failure: str | None = None) -> None:
        metrics.append(_metric(metric_id, value, unit, start_date, end_date, n, coverage, failure or reason))

    enough = n >= MIN_OBSERVATIONS
    vol = stdev(values) * sqrt(PERIODS_PER_YEAR) if enough else None
    downside = sqrt(sum(min(value, 0) ** 2 for value in values) / n * PERIODS_PER_YEAR) if enough else None
    add("volatility_annualized", vol, "decimal_annualized", None if enough else "insufficient_observations")
    add("downside_volatility_annualized", downside, "decimal_annualized", None if enough else "insufficient_observations")

    # Ratios must disclose their reference rate instead of silently assuming zero.
    for metric_id in ("sharpe", "sortino"):
        failure = None
        value = None
        if not enough:
            failure = "insufficient_observations"
        elif risk_free_rate_annual is None:
            failure = "risk_free_rate_missing"
        else:
            daily_rate = (1 + risk_free_rate_annual) ** (1 / PERIODS_PER_YEAR) - 1
            excess = [item - daily_rate for item in values]
            denominator = vol if metric_id == "sharpe" else sqrt(
                sum(min(item, 0) ** 2 for item in excess) / n * PERIODS_PER_YEAR
            )
            if denominator is None or denominator <= 1e-15:
                failure = "zero_volatility" if metric_id == "sharpe" else "zero_downside_deviation"
            else:
                value = mean(excess) * PERIODS_PER_YEAR / denominator
        add(metric_id, value, "ratio", failure)

    # Missing intervals invalidate a path: joining the remaining returns fabricates a drawdown.
    maximum = duration = annual_return = None
    path_failure = "incomplete_return_path" if n != expected else None
    if path_failure is None:
        level = peak = 1.0
        peak_date = start_date
        maximum = 0.0
        duration = 0
        for row in usable:
            was_underwater = level < peak * (1 - 1e-12)
            level *= 1 + float(row.return_decimal)
            if level >= peak * (1 - 1e-12):
                if level < peak:
                    level = peak
                if was_underwater:
                    duration = max(duration, (row.valuation_date - peak_date).days)
                peak, peak_date = level, row.valuation_date
            else:
                maximum = min(maximum, level / peak - 1)
                duration = max(duration, (row.valuation_date - peak_date).days)
        annual_return = level ** (PERIODS_PER_YEAR / expected) - 1
    add("max_drawdown", maximum, "decimal", path_failure)
    add("drawdown_duration_days", duration, "days", path_failure)
    calmar_failure = path_failure or ("insufficient_observations" if not enough else None)
    if calmar_failure is None and maximum == 0:
        calmar_failure = "zero_drawdown"
    add("calmar", None if calmar_failure else annual_return / abs(maximum), "ratio", calmar_failure)
    return RiskResult(tuple(metrics), risk_free_rate_annual=risk_free_rate_annual)


@dataclass(frozen=True)
class PositionExposure:
    asset_id: str
    market_value_base: float | None
    bucket: str | None = None
    currency: str | None = None
    asset_type: str | None = None
    sector: str | None = None
    sector_source: str | None = None


@dataclass(frozen=True)
class ConcentrationGroup:
    dimension: str
    group: str
    weight: PerformanceMetric


@dataclass(frozen=True)
class ConcentrationResult:
    groups: tuple[ConcentrationGroup, ...]
    hhi_by_dimension: Mapping[str, PerformanceMetric]


def calculate_concentration(
    positions: Sequence[PositionExposure], *, as_of_date: date,
) -> ConcentrationResult:
    """Long-only weights over valued positions; missing metadata stays unclassified."""
    if len({row.asset_id for row in positions}) != len(positions):
        raise ValueError("Duplicate exposure asset IDs")
    if any(not row.asset_id.strip() for row in positions):
        raise ValueError("asset_id cannot be blank")
    if any(row.market_value_base is not None and row.market_value_base < 0 for row in positions):
        raise ValueError("Concentration supports long-only non-negative positions")
    valid = [row for row in positions if row.market_value_base is not None and isfinite(row.market_value_base)]
    total = sum(row.market_value_base for row in valid)
    valuation_coverage = len(valid) / len(positions) if positions else 0.0
    groups: list[ConcentrationGroup] = []
    hhis: dict[str, PerformanceMetric] = {}
    for dimension in ("asset_id", "bucket", "currency", "asset_type", "sector"):
        amounts: dict[str, float] = {}
        unknown = 0.0
        for row in valid:
            name = getattr(row, dimension)
            if dimension == "sector" and not (row.sector_source and row.sector_source.strip()):
                name = None
            name = name.strip() if name and name.strip() else "unclassified"
            if name == "unclassified":
                unknown += row.market_value_base
            amounts[name] = amounts.get(name, 0.0) + row.market_value_base
        coverage = valuation_coverage * (1 - unknown / total) if total > 0 else 0.0
        reason = "partial_exposure_coverage" if coverage < 1 else "ok"
        for name, amount in sorted(amounts.items()):
            groups.append(ConcentrationGroup(dimension, name, _metric(
                "exposure_weight", amount / total if total > 0 else None, "decimal",
                as_of_date, as_of_date, len(valid), coverage, reason if total > 0 else "no_valued_exposure",
            )))
        hhis[dimension] = _metric(
            "concentration_hhi", sum((amount / total) ** 2 for amount in amounts.values())
            if total > 0 and unknown == 0 else None,
            "coefficient", as_of_date, as_of_date, len(valid), coverage,
            "no_valued_exposure" if total <= 0 else ("classification_missing" if unknown else reason),
        )
    return ConcentrationResult(tuple(groups), hhis)


@dataclass(frozen=True)
class AssetCorrelation:
    left_asset_id: str
    right_asset_id: str
    metric: PerformanceMetric


@dataclass(frozen=True)
class DiversificationResult:
    correlations: tuple[AssetCorrelation, ...]
    risk_contributions: Mapping[str, PerformanceMetric]


def calculate_diversification(
    asset_returns: Mapping[str, Sequence[DailyPerformanceObservation]], weights: Mapping[str, float],
    *, start_date: date, end_date: date,
) -> DiversificationResult:
    """Use price/total returns in one base currency, never changes in position quantities."""
    if end_date <= start_date:
        raise ValueError("end_date must be after start_date")
    if not weights or any(not isfinite(w) or w < 0 for w in weights.values()) or abs(sum(weights.values()) - 1) > 1e-9:
        raise ValueError("Long-only weights must be finite and sum to 1")
    identifiers = sorted(set(asset_returns) | set(weights))
    rows = {key: _daily_map(asset_returns.get(key, ()), start_date, end_date) for key in identifiers}
    usable = {key: {day: row for day, row in data.items() if _usable(row)} for key, data in rows.items()}
    expected = (end_date - start_date).days
    correlations: list[AssetCorrelation] = []
    for index, left in enumerate(identifiers):
        for right in identifiers[index:]:
            dates = sorted(set(usable[left]) & set(usable[right]))
            x = [float(usable[left][day].return_decimal) for day in dates]
            y = [float(usable[right][day].return_decimal) for day in dates]
            coverage = sum(min(usable[left][d].coverage_ratio, usable[right][d].coverage_ratio) for d in dates) / expected
            partial = coverage < 1 or any(usable[k][d].status != "available" for k in (left, right) for d in dates)
            reason = "partial_return_coverage" if partial else "ok"
            value = None
            if len(dates) < MIN_OBSERVATIONS:
                reason = "insufficient_observations"
            elif variance(x) <= 1e-30 or variance(y) <= 1e-30:
                reason = "zero_return_variance"
            else:
                value = max(-1.0, min(1.0, covariance(x, y) / (stdev(x) * stdev(y))))
            metric = _metric("asset_correlation", value, "coefficient", start_date, end_date, len(dates), coverage, reason)
            correlations.append(AssetCorrelation(left, right, metric))
            if left != right:
                correlations.append(AssetCorrelation(right, left, metric))

    active = [key for key in weights if weights[key] > 0]
    dates = sorted(set.intersection(*(set(usable[key]) for key in active)))
    coverage = sum(min(usable[key][day].coverage_ratio for key in active) for day in dates) / expected
    reason = "partial_return_coverage" if coverage < 1 or any(
        usable[key][day].status != "available" for key in active for day in dates
    ) else "ok"
    contributions: dict[str, PerformanceMetric] = {}
    portfolio = [sum(weights[key] * float(usable[key][day].return_decimal) for key in active) for day in dates]
    enough = len(dates) >= MIN_OBSERVATIONS
    portfolio_variance = variance(portfolio) if enough else 0
    for key, weight in weights.items():
        failure = "insufficient_observations" if not enough else ("zero_portfolio_variance" if portfolio_variance <= 1e-30 else None)
        value = None
        if failure is None:
            # Euler variance shares sum to one; negative shares are possible for hedges.
            value = weight * covariance(
                [float(usable[key][day].return_decimal) for day in dates], portfolio,
            ) / portfolio_variance if weight else 0.0
        contributions[key] = _metric(
            "risk_contribution_share", value, "decimal", start_date, end_date, len(dates), coverage, failure or reason,
        )
    return DiversificationResult(tuple(correlations), contributions)
