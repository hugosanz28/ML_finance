"""Typed contracts for benchmark selection and portfolio comparison."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Mapping

from src.portfolio.performance_models import PerformanceStatus


@dataclass(frozen=True)
class BenchmarkSelection:
    """Primary/secondary benchmark configuration with optional composite weights."""

    primary_benchmark_id: str
    secondary_benchmark_ids: tuple[str, ...] = ()
    composite_weights: Mapping[str, Mapping[str, float]] = field(default_factory=dict)

    @property
    def selected_ids(self) -> tuple[str, ...]:
        return (self.primary_benchmark_id, *self.secondary_benchmark_ids)


@dataclass(frozen=True)
class BenchmarkComparisonMetric:
    """One comparison value with explicit availability metadata."""

    metric_id: str
    value: float | None
    unit: str
    observations: int
    coverage_ratio: float
    status: PerformanceStatus
    reason_code: str


@dataclass(frozen=True)
class GrowthComparisonPoint:
    """Indexed growth and drawdown for one aligned date."""

    observation_date: date
    portfolio_index: float
    benchmark_index: float
    portfolio_drawdown: float
    benchmark_drawdown: float


@dataclass(frozen=True)
class BenchmarkComparison:
    """Portfolio comparison against one configured benchmark."""

    benchmark_id: str
    benchmark_name: str
    role: str
    source_reference: str
    source_currency: str
    base_currency: str
    series_kind: str
    provider_name: str
    period_start: date | None
    period_end: date | None
    observations: int
    coverage_ratio: float
    status: PerformanceStatus
    reason_codes: tuple[str, ...]
    growth: tuple[GrowthComparisonPoint, ...]
    metrics: tuple[BenchmarkComparisonMetric, ...]


@dataclass(frozen=True)
class PortfolioBenchmarkComparisonResult:
    """All primary and secondary comparisons for one portfolio date."""

    base_currency: str
    as_of_date: date
    primary_benchmark_id: str
    comparisons: tuple[BenchmarkComparison, ...]


__all__ = [
    "BenchmarkComparison",
    "BenchmarkComparisonMetric",
    "BenchmarkSelection",
    "GrowthComparisonPoint",
    "PortfolioBenchmarkComparisonResult",
]
