from datetime import date

import pandas as pd
import pytest

from src.market_data import (
    DEFAULT_BENCHMARK_CATALOG,
    LoadedBenchmarkProvider,
    SyntheticBenchmarkProvider,
    selectable_benchmarks,
)
from src.portfolio import build_benchmark_return_series


def test_default_catalog_exposes_the_four_user_selectable_benchmarks() -> None:
    assert [definition.benchmark_id for definition in selectable_benchmarks()] == [
        "msci_world",
        "sp500",
        "portfolio_60_40",
        "estr_cash",
    ]
    assert DEFAULT_BENCHMARK_CATALOG["msci_world"].series_kind == "total_return"
    assert DEFAULT_BENCHMARK_CATALOG["sp500"].native_currency == "USD"
    assert DEFAULT_BENCHMARK_CATALOG["estr_cash"].series_kind == "cash_return"


@pytest.mark.parametrize(
    "benchmark_id",
    ["msci_world", "sp500", "portfolio_60_40", "estr_cash"],
)
def test_synthetic_provider_builds_every_demo_option_without_network(benchmark_id: str) -> None:
    series = build_benchmark_return_series(
        benchmark_id,
        provider=SyntheticBenchmarkProvider(),
        start_date=date(2026, 1, 2),
        end_date=date(2026, 2, 2),
        base_currency="EUR",
    )

    assert series.status == "available"
    assert series.reason_code == "ok"
    assert series.currency == "EUR"
    assert series.observations
    assert series.coverage_ratio == 1.0


def test_synthetic_provider_does_not_invent_weekend_observations() -> None:
    series = SyntheticBenchmarkProvider().fetch_daily_returns(
        DEFAULT_BENCHMARK_CATALOG["msci_world"],
        start_date=date(2026, 1, 3),
        end_date=date(2026, 1, 4),
        base_currency="EUR",
    )

    assert series.observations == ()
    assert series.status == "unavailable"
    assert series.reason_code == "benchmark_series_unavailable"


def test_loaded_provider_converts_usd_returns_to_eur_with_repository_fx_convention() -> None:
    provider = LoadedBenchmarkProvider(
        pd.DataFrame(
            [
                {
                    "benchmark_id": "sp500",
                    "previous_observation_date": "2026-01-01",
                    "observation_date": "2026-01-02",
                    "return_decimal": 0.10,
                    "currency": "USD",
                }
            ]
        ),
        fx_rates=pd.DataFrame(
            [
                {"base_currency": "EUR", "quote_currency": "USD", "rate_date": "2026-01-01", "rate": 1.20},
                {"base_currency": "EUR", "quote_currency": "USD", "rate_date": "2026-01-02", "rate": 1.10},
            ]
        ),
    )

    series = provider.fetch_daily_returns(
        DEFAULT_BENCHMARK_CATALOG["sp500"],
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 2),
        base_currency="EUR",
    )

    assert series.status == "available"
    assert series.source_currency == "USD"
    assert series.currency == "EUR"
    assert series.observations[0].return_decimal == pytest.approx(0.20)


def test_loaded_provider_accepts_inverse_fx_pair() -> None:
    provider = LoadedBenchmarkProvider(
        pd.DataFrame(
            [
                {
                    "benchmark_id": "sp500",
                    "previous_observation_date": "2026-01-01",
                    "observation_date": "2026-01-02",
                    "return_decimal": 0.10,
                    "currency": "USD",
                }
            ]
        ),
        fx_rates=pd.DataFrame(
            [
                {"base_currency": "USD", "quote_currency": "EUR", "rate_date": "2026-01-01", "rate": 1 / 1.20},
                {"base_currency": "USD", "quote_currency": "EUR", "rate_date": "2026-01-02", "rate": 1 / 1.10},
            ]
        ),
    )

    series = provider.fetch_daily_returns(
        DEFAULT_BENCHMARK_CATALOG["sp500"],
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 2),
        base_currency="EUR",
    )

    assert series.observations[0].return_decimal == pytest.approx(0.20)


def test_loaded_provider_marks_calendar_holes_as_partial() -> None:
    provider = LoadedBenchmarkProvider(
        pd.DataFrame(
            [
                _return_row("msci_world", "2025-12-31", "2026-01-01", 0.01),
                _return_row("msci_world", "2026-01-02", "2026-01-05", 0.02),
            ]
        )
    )

    series = provider.fetch_daily_returns(
        DEFAULT_BENCHMARK_CATALOG["msci_world"],
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 5),
        base_currency="EUR",
    )

    assert series.status == "partial"
    assert series.reason_code == "partial_benchmark_coverage"
    assert series.coverage_ratio == pytest.approx(2 / 3)


def test_composite_60_40_rebalances_on_first_observation_of_new_month() -> None:
    provider = LoadedBenchmarkProvider(
        pd.DataFrame(
            [
                _return_row("msci_world", "2026-01-29", "2026-01-30", 0.10),
                _return_row("global_aggregate_bonds_eur_hedged", "2026-01-29", "2026-01-30", 0.00),
                _return_row("msci_world", "2026-01-30", "2026-02-02", 0.00),
                _return_row("global_aggregate_bonds_eur_hedged", "2026-01-30", "2026-02-02", 0.10),
            ]
        )
    )

    series = build_benchmark_return_series(
        "portfolio_60_40",
        provider=provider,
        start_date=date(2026, 1, 30),
        end_date=date(2026, 2, 2),
        base_currency="EUR",
        component_weights={
            "msci_world": 0.60,
            "global_aggregate_bonds_eur_hedged": 0.40,
        },
    )

    assert [observation.return_decimal for observation in series.observations] == pytest.approx([0.06, 0.04])
    assert series.status == "available"


def _return_row(
    benchmark_id: str,
    previous_date: str,
    observation_date: str,
    value: float,
) -> dict[str, object]:
    return {
        "benchmark_id": benchmark_id,
        "previous_observation_date": previous_date,
        "observation_date": observation_date,
        "return_decimal": value,
        "currency": "EUR",
    }
