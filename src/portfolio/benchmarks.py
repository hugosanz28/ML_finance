"""Benchmark selection, composite construction, and portfolio comparisons."""

from __future__ import annotations

from datetime import date
import json
from math import isclose, sqrt
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.config import Settings, get_settings
from src.market_data.benchmarks import (
    DEFAULT_BENCHMARK_CATALOG,
    BenchmarkDefinition,
    BenchmarkProvider,
    BenchmarkReturnObservation,
    BenchmarkReturnSeries,
)
from src.portfolio.benchmark_models import (
    BenchmarkComparison,
    BenchmarkComparisonMetric,
    BenchmarkSelection,
    GrowthComparisonPoint,
    PortfolioBenchmarkComparisonResult,
)
from src.portfolio.performance_models import (
    DailyPerformanceObservation,
    PerformanceStatus,
    PortfolioPerformanceResult,
)


MIN_STATISTICAL_OBSERVATIONS = 30
TRADING_DAYS_PER_YEAR = 252


def load_benchmark_selection(
    *,
    settings: Settings | None = None,
    path: str | Path | None = None,
    catalog: Mapping[str, BenchmarkDefinition] = DEFAULT_BENCHMARK_CATALOG,
    required: bool = False,
) -> BenchmarkSelection | None:
    """Load benchmark selection from a strict JSON object."""
    resolved_settings = get_settings() if settings is None else settings
    selection_path = (
        resolved_settings.benchmark_selection_path
        if path is None
        else Path(path).expanduser().resolve()
    )
    if not selection_path.exists():
        if required:
            raise FileNotFoundError(f"Benchmark selection config not found: {selection_path}")
        return None
    raw = selection_path.read_text(encoding="utf-8")
    if not raw.strip():
        raise ValueError(f"Benchmark selection config is empty: {selection_path}")
    payload = json.loads(raw)
    if not isinstance(payload, Mapping):
        raise ValueError("Benchmark selection config must be a JSON object")
    return benchmark_selection_from_mapping(payload, catalog=catalog)


def benchmark_selection_from_mapping(
    payload: Mapping[str, Any],
    *,
    catalog: Mapping[str, BenchmarkDefinition] = DEFAULT_BENCHMARK_CATALOG,
) -> BenchmarkSelection:
    """Validate primary, secondary, and composite benchmark configuration."""
    primary_id = _required_identifier(payload.get("primary_benchmark_id"), field_name="primary_benchmark_id")
    raw_secondary = payload.get("secondary_benchmark_ids", [])
    if isinstance(raw_secondary, (str, bytes)) or not isinstance(raw_secondary, Sequence):
        raise ValueError("secondary_benchmark_ids must be an array")
    secondary_ids = tuple(
        _required_identifier(value, field_name="secondary_benchmark_ids")
        for value in raw_secondary
    )
    selected_ids = (primary_id, *secondary_ids)
    if len(selected_ids) != len(set(selected_ids)):
        raise ValueError("Selected benchmark identifiers must be unique")
    for benchmark_id in selected_ids:
        definition = catalog.get(benchmark_id)
        if definition is None:
            raise ValueError(f"Unknown benchmark_id: {benchmark_id}")
        if not definition.selectable:
            raise ValueError(f"Benchmark is not user-selectable: {benchmark_id}")

    raw_composite_weights = payload.get("composite_weights", {})
    if not isinstance(raw_composite_weights, Mapping):
        raise ValueError("composite_weights must be an object")
    composite_weights: dict[str, dict[str, float]] = {}
    for raw_benchmark_id, raw_weights in raw_composite_weights.items():
        benchmark_id = _required_identifier(raw_benchmark_id, field_name="composite_weights")
        if benchmark_id not in selected_ids:
            raise ValueError(f"Composite override requires the benchmark to be selected: {benchmark_id}")
        definition = catalog.get(benchmark_id)
        if definition is None or not definition.is_composite:
            raise ValueError(f"Composite override references a non-composite benchmark: {benchmark_id}")
        if not isinstance(raw_weights, Mapping):
            raise ValueError(f"composite_weights.{benchmark_id} must be an object")
        normalized_weights = _normalize_composite_weights(raw_weights)
        expected_components = {component.benchmark_id for component in definition.components}
        if set(normalized_weights) != expected_components:
            raise ValueError(
                f"composite_weights.{benchmark_id} must define exactly: {sorted(expected_components)}"
            )
        composite_weights[benchmark_id] = normalized_weights

    return BenchmarkSelection(
        primary_benchmark_id=primary_id,
        secondary_benchmark_ids=secondary_ids,
        composite_weights=composite_weights,
    )


def build_benchmark_return_series(
    benchmark_id: str,
    *,
    provider: BenchmarkProvider,
    start_date: date,
    end_date: date,
    base_currency: str,
    catalog: Mapping[str, BenchmarkDefinition] = DEFAULT_BENCHMARK_CATALOG,
    component_weights: Mapping[str, float] | None = None,
) -> BenchmarkReturnSeries:
    """Fetch a simple series or build an explicitly rebalanced composite."""
    definition = catalog.get(benchmark_id)
    if definition is None:
        raise ValueError(f"Unknown benchmark_id: {benchmark_id}")
    if not definition.is_composite:
        if component_weights is not None:
            raise ValueError(f"Component weights are only valid for composite benchmarks: {benchmark_id}")
        return provider.fetch_daily_returns(
            definition,
            start_date=start_date,
            end_date=end_date,
            base_currency=base_currency,
        )

    weights = (
        {component.benchmark_id: component.target_weight for component in definition.components}
        if component_weights is None
        else _normalize_composite_weights(component_weights)
    )
    expected_components = {component.benchmark_id for component in definition.components}
    if set(weights) != expected_components:
        raise ValueError(f"Composite {benchmark_id} must define exactly: {sorted(expected_components)}")
    component_series = tuple(
        build_benchmark_return_series(
            component_id,
            provider=provider,
            start_date=start_date,
            end_date=end_date,
            base_currency=base_currency,
            catalog=catalog,
        )
        for component_id in weights
    )
    return _compose_benchmark_series(
        definition,
        component_series=component_series,
        weights=weights,
        base_currency=base_currency,
        provider_name=provider.name,
    )


def compare_portfolio_to_benchmarks(
    performance: PortfolioPerformanceResult,
    selection: BenchmarkSelection,
    *,
    provider: BenchmarkProvider,
    catalog: Mapping[str, BenchmarkDefinition] = DEFAULT_BENCHMARK_CATALOG,
) -> PortfolioBenchmarkComparisonResult:
    """Compare cash-flow-adjusted portfolio returns with all selected references."""
    if not performance.daily_returns:
        raise ValueError("Portfolio performance requires daily returns for benchmark comparison")
    start_date = performance.daily_returns[0].valuation_date
    comparisons: list[BenchmarkComparison] = []
    for index, benchmark_id in enumerate(selection.selected_ids):
        series = build_benchmark_return_series(
            benchmark_id,
            provider=provider,
            start_date=start_date,
            end_date=performance.as_of_date,
            base_currency=performance.base_currency,
            catalog=catalog,
            component_weights=selection.composite_weights.get(benchmark_id),
        )
        comparisons.append(
            _compare_one_benchmark(
                performance.daily_returns,
                definition=catalog[benchmark_id],
                series=series,
                role="primary" if index == 0 else "secondary",
                base_currency=performance.base_currency,
            )
        )
    return PortfolioBenchmarkComparisonResult(
        base_currency=performance.base_currency,
        as_of_date=performance.as_of_date,
        primary_benchmark_id=selection.primary_benchmark_id,
        comparisons=tuple(comparisons),
    )


def _compose_benchmark_series(
    definition: BenchmarkDefinition,
    *,
    component_series: Sequence[BenchmarkReturnSeries],
    weights: Mapping[str, float],
    base_currency: str,
    provider_name: str,
) -> BenchmarkReturnSeries:
    component_maps = {
        series.benchmark_id: {
            (observation.previous_observation_date, observation.observation_date): observation.return_decimal
            for observation in series.observations
        }
        for series in component_series
    }
    common_intervals = set.intersection(*(set(values) for values in component_maps.values())) if component_maps else set()
    ordered_intervals = sorted(common_intervals, key=lambda interval: (interval[1], interval[0]))
    current_weights = dict(weights)
    observations: list[BenchmarkReturnObservation] = []
    last_observation_date: date | None = None
    for previous_date, observation_date in ordered_intervals:
        if (
            definition.rebalance_frequency == "monthly"
            and last_observation_date is not None
            and (observation_date.year, observation_date.month)
            != (last_observation_date.year, last_observation_date.month)
        ):
            current_weights = dict(weights)
        component_returns = {
            component_id: component_maps[component_id][(previous_date, observation_date)]
            for component_id in current_weights
        }
        composite_return = sum(
            current_weights[component_id] * component_return
            for component_id, component_return in component_returns.items()
        )
        observations.append(
            BenchmarkReturnObservation(
                previous_observation_date=previous_date,
                observation_date=observation_date,
                return_decimal=round(composite_return, 12),
            )
        )
        total_factor = 1.0 + composite_return
        if total_factor > 0:
            current_weights = {
                component_id: current_weights[component_id] * (1.0 + component_return) / total_factor
                for component_id, component_return in component_returns.items()
            }
        last_observation_date = observation_date

    component_coverages = [series.coverage_ratio for series in component_series]
    overlap_coverage = min(
        (
            len(common_intervals) / len(series.observations)
            if series.observations
            else 0.0
        )
        for series in component_series
    ) if component_series else 0.0
    coverage = min([overlap_coverage, *component_coverages], default=0.0)
    if not observations:
        status = "unavailable"
        reason_code = "component_series_unavailable"
    elif coverage < 1.0 or any(series.status != "available" for series in component_series):
        status = "partial"
        reason_code = "partial_component_coverage"
    else:
        status = "available"
        reason_code = "ok"
    return BenchmarkReturnSeries(
        benchmark_id=definition.benchmark_id,
        provider_name=provider_name,
        source_currency=definition.native_currency,
        currency=base_currency.strip().upper(),
        series_kind=definition.series_kind,
        observations=tuple(observations),
        coverage_ratio=round(coverage, 8),
        status=status,
        reason_code=reason_code,
    )


def _compare_one_benchmark(
    portfolio_observations: Sequence[DailyPerformanceObservation],
    *,
    definition: BenchmarkDefinition,
    series: BenchmarkReturnSeries,
    role: str,
    base_currency: str,
) -> BenchmarkComparison:
    portfolio_by_start = {
        observation.previous_valuation_date: observation
        for observation in portfolio_observations
    }
    aligned: list[tuple[date, date, float, float, float, PerformanceStatus]] = []
    for benchmark_observation in series.observations:
        aggregated = _aggregate_portfolio_interval(
            portfolio_by_start,
            start_date=benchmark_observation.previous_observation_date,
            end_date=benchmark_observation.observation_date,
        )
        if aggregated is None:
            continue
        portfolio_return, portfolio_coverage, portfolio_status = aggregated
        aligned.append(
            (
                benchmark_observation.previous_observation_date,
                benchmark_observation.observation_date,
                portfolio_return,
                benchmark_observation.return_decimal,
                portfolio_coverage,
                portfolio_status,
            )
        )

    alignment_coverage = len(aligned) / len(series.observations) if series.observations else 0.0
    portfolio_coverage = min((row[4] for row in aligned), default=0.0)
    coverage = min(series.coverage_ratio, alignment_coverage, portfolio_coverage)
    reason_codes: list[str] = []
    if series.reason_code != "ok":
        reason_codes.append(series.reason_code)
    if alignment_coverage < 1.0:
        reason_codes.append("incomplete_calendar_alignment")
    if portfolio_coverage < 1.0 and aligned:
        reason_codes.append("partial_portfolio_coverage")
    if any(row[5] != "available" for row in aligned):
        reason_codes.append("partial_portfolio_returns")
    reason_codes = list(dict.fromkeys(reason_codes))

    if not aligned:
        metrics = _unavailable_metrics("no_aligned_observations", coverage_ratio=coverage)
        status: PerformanceStatus = "unavailable"
        if not reason_codes:
            reason_codes.append("no_aligned_observations")
        growth: tuple[GrowthComparisonPoint, ...] = ()
        period_start = None
        period_end = None
    else:
        portfolio_returns = [row[2] for row in aligned]
        benchmark_returns = [row[3] for row in aligned]
        metric_status: PerformanceStatus = "partial" if reason_codes else "available"
        metric_reason = reason_codes[0] if reason_codes else "ok"
        growth = _growth_points(aligned)
        metrics = _comparison_metrics(
            portfolio_returns,
            benchmark_returns,
            coverage_ratio=coverage,
            status=metric_status,
            reason_code=metric_reason,
        )
        status = metric_status
        period_start = aligned[0][0]
        period_end = aligned[-1][1]

    return BenchmarkComparison(
        benchmark_id=definition.benchmark_id,
        benchmark_name=definition.name,
        role=role,
        source_reference=definition.source_reference,
        source_currency=series.source_currency,
        base_currency=base_currency,
        series_kind=definition.series_kind,
        provider_name=series.provider_name,
        period_start=period_start,
        period_end=period_end,
        observations=len(aligned),
        coverage_ratio=round(coverage, 8),
        status=status,
        reason_codes=tuple(reason_codes),
        growth=growth,
        metrics=metrics,
    )


def _aggregate_portfolio_interval(
    observations_by_start: Mapping[date, DailyPerformanceObservation],
    *,
    start_date: date,
    end_date: date,
) -> tuple[float, float, PerformanceStatus] | None:
    current_date = start_date
    factor = 1.0
    coverages: list[float] = []
    statuses: list[PerformanceStatus] = []
    visited: set[date] = set()
    while current_date < end_date:
        if current_date in visited:
            return None
        visited.add(current_date)
        observation = observations_by_start.get(current_date)
        if (
            observation is None
            or observation.valuation_date > end_date
            or observation.return_decimal is None
        ):
            return None
        factor *= 1.0 + observation.return_decimal
        coverages.append(observation.coverage_ratio)
        statuses.append(observation.status)
        current_date = observation.valuation_date
    if current_date != end_date or not coverages:
        return None
    status: PerformanceStatus = "available" if all(value == "available" for value in statuses) else "partial"
    return factor - 1.0, min(coverages), status


def _growth_points(
    aligned: Sequence[tuple[date, date, float, float, float, PerformanceStatus]],
) -> tuple[GrowthComparisonPoint, ...]:
    portfolio_index = 100.0
    benchmark_index = 100.0
    portfolio_peak = 100.0
    benchmark_peak = 100.0
    points = [
        GrowthComparisonPoint(
            observation_date=aligned[0][0],
            portfolio_index=100.0,
            benchmark_index=100.0,
            portfolio_drawdown=0.0,
            benchmark_drawdown=0.0,
        )
    ]
    for _, observation_date, portfolio_return, benchmark_return, _, _ in aligned:
        portfolio_index *= 1.0 + portfolio_return
        benchmark_index *= 1.0 + benchmark_return
        portfolio_peak = max(portfolio_peak, portfolio_index)
        benchmark_peak = max(benchmark_peak, benchmark_index)
        points.append(
            GrowthComparisonPoint(
                observation_date=observation_date,
                portfolio_index=round(portfolio_index, 8),
                benchmark_index=round(benchmark_index, 8),
                portfolio_drawdown=round(portfolio_index / portfolio_peak - 1.0, 12),
                benchmark_drawdown=round(benchmark_index / benchmark_peak - 1.0, 12),
            )
        )
    return tuple(points)


def _comparison_metrics(
    portfolio_returns: Sequence[float],
    benchmark_returns: Sequence[float],
    *,
    coverage_ratio: float,
    status: PerformanceStatus,
    reason_code: str,
) -> tuple[BenchmarkComparisonMetric, ...]:
    observations = len(portfolio_returns)
    portfolio_total = _compound(portfolio_returns)
    benchmark_total = _compound(benchmark_returns)
    relative_return = (1.0 + portfolio_total) / (1.0 + benchmark_total) - 1.0
    portfolio_drawdown = _max_drawdown(portfolio_returns)
    benchmark_drawdown = _max_drawdown(benchmark_returns)
    metrics = [
        _metric("portfolio_total_return", portfolio_total, "decimal", observations, coverage_ratio, status, reason_code),
        _metric("benchmark_total_return", benchmark_total, "decimal", observations, coverage_ratio, status, reason_code),
        _metric("relative_return", relative_return, "decimal", observations, coverage_ratio, status, reason_code),
        _metric("portfolio_max_drawdown", portfolio_drawdown, "decimal", observations, coverage_ratio, status, reason_code),
        _metric("benchmark_max_drawdown", benchmark_drawdown, "decimal", observations, coverage_ratio, status, reason_code),
        _metric(
            "drawdown_difference",
            portfolio_drawdown - benchmark_drawdown,
            "decimal",
            observations,
            coverage_ratio,
            status,
            reason_code,
        ),
    ]
    if observations < MIN_STATISTICAL_OBSERVATIONS:
        metrics.extend(
            _metric(metric_id, None, unit, observations, coverage_ratio, "unavailable", "insufficient_observations")
            for metric_id, unit in (
                ("correlation", "coefficient"),
                ("tracking_error_annualized", "decimal_annualized"),
                ("beta", "coefficient"),
                ("alpha_annualized", "decimal_annualized"),
            )
        )
        return tuple(metrics)

    portfolio_variance = _sample_variance(portfolio_returns)
    benchmark_variance = _sample_variance(benchmark_returns)
    covariance = _sample_covariance(portfolio_returns, benchmark_returns)
    active_returns = [portfolio - benchmark for portfolio, benchmark in zip(portfolio_returns, benchmark_returns, strict=True)]
    tracking_error = sqrt(_sample_variance(active_returns)) * sqrt(TRADING_DAYS_PER_YEAR)
    correlation = (
        covariance / sqrt(portfolio_variance * benchmark_variance)
        if portfolio_variance > 0 and benchmark_variance > 0
        else None
    )
    beta = covariance / benchmark_variance if benchmark_variance > 0 else None
    alpha = (
        (_mean(portfolio_returns) - beta * _mean(benchmark_returns)) * TRADING_DAYS_PER_YEAR
        if beta is not None
        else None
    )
    metrics.append(
        _metric(
            "correlation",
            correlation,
            "coefficient",
            observations,
            coverage_ratio,
            status if correlation is not None else "unavailable",
            reason_code if correlation is not None else "zero_return_variance",
        )
    )
    metrics.append(
        _metric(
            "tracking_error_annualized",
            tracking_error,
            "decimal_annualized",
            observations,
            coverage_ratio,
            status,
            reason_code,
        )
    )
    for metric_id, value, unit in (
        ("beta", beta, "coefficient"),
        ("alpha_annualized", alpha, "decimal_annualized"),
    ):
        metrics.append(
            _metric(
                metric_id,
                value,
                unit,
                observations,
                coverage_ratio,
                status if value is not None else "unavailable",
                reason_code if value is not None else "benchmark_variance_zero",
            )
        )
    return tuple(metrics)


def _unavailable_metrics(reason_code: str, *, coverage_ratio: float) -> tuple[BenchmarkComparisonMetric, ...]:
    return tuple(
        _metric(metric_id, None, unit, 0, coverage_ratio, "unavailable", reason_code)
        for metric_id, unit in (
            ("portfolio_total_return", "decimal"),
            ("benchmark_total_return", "decimal"),
            ("relative_return", "decimal"),
            ("portfolio_max_drawdown", "decimal"),
            ("benchmark_max_drawdown", "decimal"),
            ("drawdown_difference", "decimal"),
            ("correlation", "coefficient"),
            ("tracking_error_annualized", "decimal_annualized"),
            ("beta", "coefficient"),
            ("alpha_annualized", "decimal_annualized"),
        )
    )


def _metric(
    metric_id: str,
    value: float | None,
    unit: str,
    observations: int,
    coverage_ratio: float,
    status: PerformanceStatus,
    reason_code: str,
) -> BenchmarkComparisonMetric:
    return BenchmarkComparisonMetric(
        metric_id=metric_id,
        value=None if value is None else round(float(value), 12),
        unit=unit,
        observations=observations,
        coverage_ratio=round(coverage_ratio, 8),
        status=status,
        reason_code=reason_code,
    )


def _normalize_composite_weights(raw_weights: Mapping[object, object]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for raw_id, raw_weight in raw_weights.items():
        component_id = _required_identifier(raw_id, field_name="component benchmark_id")
        if isinstance(raw_weight, bool):
            raise ValueError(f"Weight for {component_id} must be numeric")
        try:
            weight = float(raw_weight)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Weight for {component_id} must be numeric") from exc
        if not 0 <= weight <= 1:
            raise ValueError(f"Weight for {component_id} must be between 0 and 1")
        weights[component_id] = weight
    if not weights or not isclose(sum(weights.values()), 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError("Composite benchmark weights must sum to 1.0")
    return weights


def _required_identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _compound(returns: Sequence[float]) -> float:
    factor = 1.0
    for value in returns:
        factor *= 1.0 + value
    return factor - 1.0


def _max_drawdown(returns: Sequence[float]) -> float:
    index = 1.0
    peak = 1.0
    maximum_drawdown = 0.0
    for value in returns:
        index *= 1.0 + value
        peak = max(peak, index)
        maximum_drawdown = min(maximum_drawdown, index / peak - 1.0)
    return maximum_drawdown


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _sample_variance(values: Sequence[float]) -> float:
    mean = _mean(values)
    return sum((value - mean) ** 2 for value in values) / (len(values) - 1)


def _sample_covariance(left: Sequence[float], right: Sequence[float]) -> float:
    left_mean = _mean(left)
    right_mean = _mean(right)
    return sum(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(left, right, strict=True)
    ) / (len(left) - 1)


__all__ = [
    "MIN_STATISTICAL_OBSERVATIONS",
    "TRADING_DAYS_PER_YEAR",
    "benchmark_selection_from_mapping",
    "build_benchmark_return_series",
    "compare_portfolio_to_benchmarks",
    "load_benchmark_selection",
]
