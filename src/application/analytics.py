"""Read-only, JSON-ready analytics boundary shared by future interfaces."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date
from math import isfinite
from typing import Any, Literal, TypeAlias

import pandas as pd

from src.analytics import calculate_risk, metric_catalog
from src.analytics.portfolio import analyze_positions
from src.application.portfolio_targets import ReadPortfolioTargetsUseCase
from src.application.serialization import json_ready_value
from src.config import Settings, get_settings
from src.market_data.benchmarks import BenchmarkProvider, SyntheticBenchmarkProvider, selectable_benchmarks
from src.market_data.repository import DuckDBMarketDataRepository
from src.portfolio.benchmarks import (
    benchmark_selection_from_mapping, compare_portfolio_to_benchmarks, load_benchmark_selection,
)
from src.portfolio.metrics import calculate_portfolio_metrics_from_normalized_degiro
from src.portfolio.metrics_models import PortfolioDataUnavailableError
from src.portfolio.performance import DEFAULT_PERFORMANCE_PERIODS, calculate_portfolio_performance
from src.reports.monthly import load_normalized_degiro_cash_movements


JsonValue: TypeAlias = str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True)
class AnalyticsRequest:
    period: str = "since_inception"
    as_of_date: str | None = None
    benchmark_id: str | None = None
    risk_free_rate_annual: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AnalyticsResult:
    section: str
    status: Literal["available", "partial", "unavailable"]
    reason_code: str
    base_currency: str
    period: dict[str, JsonValue]
    warnings: list[str]
    data: dict[str, JsonValue]
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MetricDefinitionsResult:
    definitions: list[dict[str, str]]
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class GetMetricDefinitionsUseCase:
    name = "get_metric_definitions"

    def execute(self) -> MetricDefinitionsResult:
        return MetricDefinitionsResult([asdict(item) for item in metric_catalog()])


class _AnalyticsUseCase:
    section = "summary"

    def __init__(self, *, settings: Settings | None = None, benchmark_provider: BenchmarkProvider | None = None) -> None:
        self.settings = get_settings() if settings is None else settings
        # Providers are trusted backend dependencies, never accepted from query parameters.
        self.benchmark_provider = benchmark_provider

    def execute(self, request: AnalyticsRequest | None = None) -> AnalyticsResult:
        request = request or AnalyticsRequest()
        requested_date = _validate_request(request)
        warnings: list[str] = []
        period = {"period_id": request.period, "requested_start": None, "actual_start": None, "end_date": request.as_of_date}
        repository = DuckDBMarketDataRepository(settings=self.settings, read_only=True)
        if not self.settings.portfolio_db_path.is_file():
            return self._result(period, {}, ["portfolio_data_unavailable"])
        try:
            metrics = calculate_portfolio_metrics_from_normalized_degiro(
                settings=self.settings, repository=repository, end_date=requested_date, persist=False,
            )
        except (FileNotFoundError, PortfolioDataUnavailableError):
            return self._result(period, {}, ["portfolio_data_unavailable"])
        if metrics.portfolio_daily_metrics.empty:
            return self._result(period, {}, ["portfolio_data_unavailable"])
        cash = load_normalized_degiro_cash_movements(settings=self.settings)
        missing_cash = cash.empty
        if missing_cash:
            cash = pd.DataFrame(columns=["movement_date", "movement_type", "amount_base"])
            warnings.append("cash_flow_data_missing")
        performance = calculate_portfolio_performance(
            metrics.portfolio_daily_metrics, cash, base_currency=metrics.base_currency, as_of_date=requested_date,
        )
        selected = next(item for item in performance.periods if item.period_id == request.period)
        rows = tuple(row for row in performance.daily_returns if selected.actual_start < row.valuation_date <= selected.end_date)
        if missing_cash:
            # Missing account exports must not silently mean zero external flows.
            selected = replace(selected, twr=replace(selected.twr, value=None, status="unavailable", reason_code="cash_flow_data_missing"),
                               mwr=replace(selected.mwr, value=None, status="unavailable", reason_code="cash_flow_data_missing"))
            rows = tuple(replace(row, return_decimal=None, status="unavailable", reason_code="cash_flow_data_missing") for row in rows)
        performance = replace(performance, periods=(selected,), daily_returns=rows)
        period = {key: json_ready_value(getattr(selected, key)) for key in ("period_id", "requested_start", "actual_start", "end_date")}
        warnings.extend(performance.warnings)
        warnings.extend(selected.reason_codes)
        warnings.extend(issue.code for issue in performance.cash_flow_issues)
        data = {}
        if self.section in {"summary", "performance"}:
            data["performance"] = {
                "period": json_ready_value(asdict(selected)),
                "daily_returns": json_ready_value([asdict(row) for row in rows]),
                # Keep classifications but do not expose private source filenames.
                "cash_flow_issues": [{"code": item.code, "flow_date": json_ready_value(item.flow_date)} for item in performance.cash_flow_issues],
            }
        if self.section in {"summary", "risk"}:
            daily = tuple(row for row in rows if (row.valuation_date - row.previous_valuation_date).days == 1)
            if len(daily) != len(rows):
                warnings.append("multiday_returns_excluded_from_risk")
            risk = calculate_risk(
                daily, start_date=selected.actual_start, end_date=selected.end_date,
                risk_free_rate_annual=request.risk_free_rate_annual,
            ) if selected.actual_start < selected.end_date else None
            targets = ReadPortfolioTargetsUseCase(settings=self.settings).execute()
            if targets.validation_error:
                warnings.append("invalid_portfolio_targets")
            mapping = (targets.portfolio_targets or {}).get("asset_bucket_mapping", {})
            positions = analyze_positions(
                metrics.position_metrics, start_date=selected.actual_start, end_date=selected.end_date,
                bucket_mapping=mapping, risk_free_rate_annual=request.risk_free_rate_annual,
            )
            data["risk"] = {
                "portfolio": json_ready_value(asdict(risk)) if risk else {"status": "unavailable", "reason_code": "insufficient_observations"},
                "positions": json_ready_value(asdict(positions)),
            }
            if positions.diversification is None:
                warnings.append(positions.diversification_reason_code)
            if positions.asset_risk:
                warnings.append("valuation_price_proxy")
        if self.section in {"summary", "benchmarks"}:
            selection = benchmark_selection_from_mapping({"primary_benchmark_id": request.benchmark_id}) if request.benchmark_id else load_benchmark_selection(settings=self.settings)
            provider = self.benchmark_provider
            if provider is None and self.settings.price_provider == "synthetic":
                provider = SyntheticBenchmarkProvider()
            failure = "benchmark_not_selected" if selection is None else "benchmark_provider_unavailable" if provider is None else "insufficient_observations" if not rows else None
            comparison = compare_portfolio_to_benchmarks(performance, selection, provider=provider) if failure is None else None
            data["benchmarks"] = {
                "selection": json_ready_value(asdict(selection)) if selection else None,
                "catalog": json_ready_value([asdict(item) for item in selectable_benchmarks()]),
                "comparison": json_ready_value(asdict(comparison)) if comparison else None,
                "reason_code": failure or "ok",
            }
            if failure:
                warnings.append(failure)
        return self._result(period, data, warnings, base_currency=metrics.base_currency)

    def _result(self, period, data, warnings, *, base_currency=None) -> AnalyticsResult:
        states, reasons = _availability(data)
        warnings = list(dict.fromkeys([*warnings, *reasons]))
        status = "unavailable" if not states or all(s == "unavailable" for s in states) else "partial" if warnings or any(s != "available" for s in states) else "available"
        return AnalyticsResult(
            self.section, status, "ok" if status == "available" else warnings[0] if warnings else "analytics_unavailable",
            base_currency or self.settings.default_currency, period, warnings, data,
        )


def _validate_request(request: AnalyticsRequest) -> date | None:
    if request.period not in {item[0] for item in DEFAULT_PERFORMANCE_PERIODS}:
        raise ValueError("Unknown analytics period")
    as_of_date = None
    if request.as_of_date is not None:
        as_of_date = date.fromisoformat(request.as_of_date)
        if as_of_date.isoformat() != request.as_of_date:
            raise ValueError("as_of_date must use YYYY-MM-DD")
        if as_of_date > date.today():
            raise ValueError("as_of_date cannot be in the future")
    if request.benchmark_id is not None:
        benchmark_selection_from_mapping({"primary_benchmark_id": request.benchmark_id})
    if request.risk_free_rate_annual is not None and (not isfinite(request.risk_free_rate_annual) or request.risk_free_rate_annual <= -1):
        raise ValueError("risk_free_rate_annual must be finite and greater than -1")
    return as_of_date


def _availability(value):
    states, reasons = [], []
    if isinstance(value, dict):
        if value.get("status") in {"available", "partial", "unavailable"}:
            states.append(value["status"])
            if value.get("reason_code", "ok") != "ok":
                reasons.append(value["reason_code"])
            reasons.extend(code for code in value.get("reason_codes", []) if code != "ok")
        for item in value.values():
            child_states, child_reasons = _availability(item)
            states.extend(child_states)
            reasons.extend(child_reasons)
    elif isinstance(value, list):
        for item in value:
            child_states, child_reasons = _availability(item)
            states.extend(child_states)
            reasons.extend(child_reasons)
    return states, reasons


class GetAnalyticsSummaryUseCase(_AnalyticsUseCase):
    name = "get_analytics_summary"


class GetPortfolioPerformanceUseCase(_AnalyticsUseCase):
    name = "get_portfolio_performance"
    section = "performance"


class GetPortfolioRiskUseCase(_AnalyticsUseCase):
    name = "get_portfolio_risk"
    section = "risk"


class GetBenchmarkComparisonUseCase(_AnalyticsUseCase):
    name = "get_benchmark_comparison"
    section = "benchmarks"
