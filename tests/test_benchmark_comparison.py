from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.config import load_settings
from src.market_data import LoadedBenchmarkProvider
from src.portfolio import (
    BenchmarkSelection,
    DailyPerformanceObservation,
    PortfolioPerformanceResult,
    benchmark_selection_from_mapping,
    compare_portfolio_to_benchmarks,
    load_benchmark_selection,
)


def test_benchmark_selection_supports_primary_secondary_and_composite_override() -> None:
    selection = benchmark_selection_from_mapping(
        {
            "primary_benchmark_id": "msci_world",
            "secondary_benchmark_ids": ["sp500", "portfolio_60_40", "estr_cash"],
            "composite_weights": {
                "portfolio_60_40": {
                    "msci_world": 0.65,
                    "global_aggregate_bonds_eur_hedged": 0.35,
                }
            },
        }
    )

    assert selection.selected_ids == (
        "msci_world",
        "sp500",
        "portfolio_60_40",
        "estr_cash",
    )
    assert selection.composite_weights["portfolio_60_40"]["msci_world"] == 0.65


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ({"primary_benchmark_id": "unknown"}, "Unknown benchmark_id"),
        (
            {"primary_benchmark_id": "msci_world", "secondary_benchmark_ids": ["msci_world"]},
            "must be unique",
        ),
        (
            {
                "primary_benchmark_id": "portfolio_60_40",
                "composite_weights": {
                    "portfolio_60_40": {
                        "msci_world": 0.7,
                        "global_aggregate_bonds_eur_hedged": 0.4,
                    }
                },
            },
            "sum to 1.0",
        ),
    ],
)
def test_benchmark_selection_rejects_invalid_configuration(
    payload: dict[str, object],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        benchmark_selection_from_mapping(payload)


def test_demo_selection_exposes_all_four_offline_options() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    settings = load_settings(
        repo_root=repo_root,
        env_file=repo_root / "demo" / "synthetic_config" / ".env.demo",
        env={},
    )

    selection = load_benchmark_selection(settings=settings, required=True)

    assert selection is not None
    assert selection.selected_ids == (
        "msci_world",
        "sp500",
        "portfolio_60_40",
        "estr_cash",
    )


def test_comparison_calculates_growth_relative_risk_and_regression_metrics() -> None:
    intervals = _business_intervals(date(2026, 1, 2), 35)
    benchmark_returns = [0.004 if index % 2 == 0 else -0.002 for index in range(len(intervals))]
    portfolio_returns = [0.001 + 1.5 * value for value in benchmark_returns]
    performance = _performance(intervals, portfolio_returns)
    provider = LoadedBenchmarkProvider(_benchmark_frame("msci_world", intervals, benchmark_returns))

    result = compare_portfolio_to_benchmarks(
        performance,
        BenchmarkSelection("msci_world"),
        provider=provider,
    )
    comparison = result.comparisons[0]
    metrics = {metric.metric_id: metric for metric in comparison.metrics}

    assert comparison.status == "available"
    assert comparison.observations == 35
    assert comparison.coverage_ratio == 1.0
    assert len(comparison.growth) == 36
    assert metrics["correlation"].value == pytest.approx(1.0)
    assert metrics["beta"].value == pytest.approx(1.5)
    assert metrics["alpha_annualized"].value == pytest.approx(0.252)
    assert metrics["tracking_error_annualized"].value is not None
    expected_relative = (
        (1.0 + metrics["portfolio_total_return"].value)
        / (1.0 + metrics["benchmark_total_return"].value)
        - 1.0
    )
    assert metrics["relative_return"].value == pytest.approx(expected_relative)


def test_comparison_does_not_invent_statistics_with_too_few_observations() -> None:
    intervals = _business_intervals(date(2026, 1, 2), 5)
    returns = [0.001, 0.002, -0.001, 0.003, 0.0]
    performance = _performance(intervals, returns)
    provider = LoadedBenchmarkProvider(_benchmark_frame("msci_world", intervals, returns))

    comparison = compare_portfolio_to_benchmarks(
        performance,
        BenchmarkSelection("msci_world"),
        provider=provider,
    ).comparisons[0]
    metrics = {metric.metric_id: metric for metric in comparison.metrics}

    for metric_id in ("correlation", "tracking_error_annualized", "beta", "alpha_annualized"):
        assert metrics[metric_id].value is None
        assert metrics[metric_id].status == "unavailable"
        assert metrics[metric_id].reason_code == "insufficient_observations"


def test_comparison_chains_portfolio_weekend_returns_to_benchmark_calendar() -> None:
    portfolio_intervals = tuple(
        (date(2026, 1, 1) + timedelta(days=index), date(2026, 1, 2) + timedelta(days=index))
        for index in range(4)
    )
    performance = _performance(portfolio_intervals, [0.01, 0.02, -0.01, 0.03])
    benchmark_intervals = (
        (date(2026, 1, 1), date(2026, 1, 2)),
        (date(2026, 1, 2), date(2026, 1, 5)),
    )
    provider = LoadedBenchmarkProvider(
        _benchmark_frame("msci_world", benchmark_intervals, [0.01, 0.04])
    )

    comparison = compare_portfolio_to_benchmarks(
        performance,
        BenchmarkSelection("msci_world"),
        provider=provider,
    ).comparisons[0]

    assert comparison.observations == 2
    assert comparison.coverage_ratio == 1.0
    metrics = {metric.metric_id: metric for metric in comparison.metrics}
    assert metrics["portfolio_total_return"].value == pytest.approx(
        (1.01 * 1.02 * 0.99 * 1.03) - 1.0
    )


def test_missing_portfolio_interval_reports_partial_alignment() -> None:
    portfolio_intervals = (
        (date(2026, 1, 1), date(2026, 1, 2)),
        (date(2026, 1, 5), date(2026, 1, 6)),
    )
    benchmark_intervals = (
        (date(2026, 1, 1), date(2026, 1, 2)),
        (date(2026, 1, 2), date(2026, 1, 5)),
        (date(2026, 1, 5), date(2026, 1, 6)),
    )
    performance = _performance(portfolio_intervals, [0.01, 0.02])
    provider = LoadedBenchmarkProvider(
        _benchmark_frame("msci_world", benchmark_intervals, [0.01, 0.0, 0.02])
    )

    comparison = compare_portfolio_to_benchmarks(
        performance,
        BenchmarkSelection("msci_world"),
        provider=provider,
    ).comparisons[0]

    assert comparison.status == "partial"
    assert comparison.coverage_ratio == pytest.approx(2 / 3)
    assert "incomplete_calendar_alignment" in comparison.reason_codes


def _performance(
    intervals: tuple[tuple[date, date], ...],
    returns: list[float],
) -> PortfolioPerformanceResult:
    value = 100.0
    observations: list[DailyPerformanceObservation] = []
    for (previous_date, observation_date), return_decimal in zip(intervals, returns, strict=True):
        opening = value
        value *= 1.0 + return_decimal
        observations.append(
            DailyPerformanceObservation(
                previous_valuation_date=previous_date,
                valuation_date=observation_date,
                opening_value_base=opening,
                closing_value_base=value,
                net_external_flow_base=0.0,
                return_decimal=return_decimal,
                coverage_ratio=1.0,
                status="available",
                reason_code="ok",
            )
        )
    return PortfolioPerformanceResult(
        base_currency="EUR",
        as_of_date=intervals[-1][1],
        daily_returns=tuple(observations),
        periods=(),
    )


def _benchmark_frame(
    benchmark_id: str,
    intervals: tuple[tuple[date, date], ...],
    returns: list[float],
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "benchmark_id": benchmark_id,
                "previous_observation_date": previous_date,
                "observation_date": observation_date,
                "return_decimal": return_decimal,
                "currency": "EUR",
            }
            for (previous_date, observation_date), return_decimal in zip(intervals, returns, strict=True)
        ]
    )


def _business_intervals(start_date: date, count: int) -> tuple[tuple[date, date], ...]:
    intervals: list[tuple[date, date]] = []
    current_date = start_date
    previous_business_date = start_date - timedelta(days=1)
    while previous_business_date.weekday() >= 5:
        previous_business_date -= timedelta(days=1)
    while len(intervals) < count:
        if current_date.weekday() < 5:
            intervals.append((previous_business_date, current_date))
            previous_business_date = current_date
        current_date += timedelta(days=1)
    return tuple(intervals)
