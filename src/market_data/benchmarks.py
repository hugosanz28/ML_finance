"""Benchmark catalog and offline/loadable daily-return providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, timedelta
from math import isfinite, sin
from types import MappingProxyType
from typing import Literal, Mapping

import pandas as pd


BenchmarkSeriesKind = Literal[
    "price_return",
    "total_return",
    "cash_return",
    "composite_total_return",
]
BenchmarkSeriesStatus = Literal["available", "partial", "unavailable"]


@dataclass(frozen=True)
class BenchmarkComponent:
    """One target-weighted component of a composite benchmark."""

    benchmark_id: str
    target_weight: float


@dataclass(frozen=True)
class BenchmarkDefinition:
    """Stable benchmark metadata independent from any UI label or provider."""

    benchmark_id: str
    name: str
    description: str
    native_currency: str
    series_kind: BenchmarkSeriesKind
    source_reference: str
    selectable: bool = True
    components: tuple[BenchmarkComponent, ...] = ()
    rebalance_frequency: Literal["none", "monthly"] = "none"

    @property
    def is_composite(self) -> bool:
        return bool(self.components)


@dataclass(frozen=True)
class BenchmarkReturnObservation:
    """Return between two consecutive benchmark observation dates."""

    previous_observation_date: date
    observation_date: date
    return_decimal: float


@dataclass(frozen=True)
class BenchmarkReturnSeries:
    """Provider result normalized to the requested comparison currency."""

    benchmark_id: str
    provider_name: str
    source_currency: str
    currency: str
    series_kind: BenchmarkSeriesKind
    observations: tuple[BenchmarkReturnObservation, ...]
    coverage_ratio: float
    status: BenchmarkSeriesStatus
    reason_code: str


_CATALOG = {
    "msci_world": BenchmarkDefinition(
        benchmark_id="msci_world",
        name="MSCI World",
        description="Renta variable de mercados desarrollados; referencia amplia, no recomendacion universal.",
        native_currency="EUR",
        series_kind="total_return",
        source_reference="MSCI World Net Total Return EUR",
    ),
    "sp500": BenchmarkDefinition(
        benchmark_id="sp500",
        name="S&P 500",
        description="Renta variable estadounidense de gran capitalizacion.",
        native_currency="USD",
        series_kind="total_return",
        source_reference="S&P 500 Total Return Index USD",
    ),
    "portfolio_60_40": BenchmarkDefinition(
        benchmark_id="portfolio_60_40",
        name="Cartera 60/40",
        description="60 % renta variable global y 40 % bonos globales aggregate cubiertos a EUR.",
        native_currency="EUR",
        series_kind="composite_total_return",
        source_reference="Composite defined by ML_finance",
        components=(
            BenchmarkComponent("msci_world", 0.60),
            BenchmarkComponent("global_aggregate_bonds_eur_hedged", 0.40),
        ),
        rebalance_frequency="monthly",
    ),
    "estr_cash": BenchmarkDefinition(
        benchmark_id="estr_cash",
        name="EUR cash (€STR)",
        description="Referencia defensiva de efectivo en euros basada en una tasa €STR compuesta.",
        native_currency="EUR",
        series_kind="cash_return",
        source_reference="€STR compounded cash return",
    ),
    "global_aggregate_bonds_eur_hedged": BenchmarkDefinition(
        benchmark_id="global_aggregate_bonds_eur_hedged",
        name="Global Aggregate Bonds EUR Hedged",
        description="Bonos globales aggregate con cobertura de divisa a EUR para el benchmark 60/40.",
        native_currency="EUR",
        series_kind="total_return",
        source_reference="Global Aggregate Bond Total Return EUR Hedged",
        selectable=False,
    ),
}

DEFAULT_BENCHMARK_CATALOG: Mapping[str, BenchmarkDefinition] = MappingProxyType(_CATALOG)


def selectable_benchmarks(
    catalog: Mapping[str, BenchmarkDefinition] = DEFAULT_BENCHMARK_CATALOG,
) -> tuple[BenchmarkDefinition, ...]:
    """Return user-selectable definitions in stable catalog order."""
    return tuple(definition for definition in catalog.values() if definition.selectable)


class BenchmarkProvider(ABC):
    """Provider boundary for normalized benchmark daily returns."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable provider identifier."""

    @abstractmethod
    def fetch_daily_returns(
        self,
        definition: BenchmarkDefinition,
        *,
        start_date: date,
        end_date: date,
        base_currency: str,
    ) -> BenchmarkReturnSeries:
        """Return one non-composite benchmark converted to base currency."""


class SyntheticBenchmarkProvider(BenchmarkProvider):
    """Deterministic offline provider used by tests and the public demo."""

    @property
    def name(self) -> str:
        return "synthetic"

    def fetch_daily_returns(
        self,
        definition: BenchmarkDefinition,
        *,
        start_date: date,
        end_date: date,
        base_currency: str,
    ) -> BenchmarkReturnSeries:
        if definition.is_composite:
            raise ValueError("Composite benchmarks must be built from their component series")
        _validate_date_window(start_date=start_date, end_date=end_date)
        normalized_base = _normalize_currency(base_currency)
        native_observations = tuple(
            BenchmarkReturnObservation(
                previous_observation_date=previous_date,
                observation_date=current_date,
                return_decimal=_synthetic_return(definition.benchmark_id, current_date),
            )
            for previous_date, current_date in _business_intervals(start_date, end_date)
        )
        if not native_observations:
            return BenchmarkReturnSeries(
                benchmark_id=definition.benchmark_id,
                provider_name=self.name,
                source_currency=definition.native_currency,
                currency=normalized_base,
                series_kind=definition.series_kind,
                observations=(),
                coverage_ratio=0.0,
                status="unavailable",
                reason_code="benchmark_series_unavailable",
            )
        observations, missing_fx = _convert_observations(
            native_observations,
            source_currency=definition.native_currency,
            base_currency=normalized_base,
            fx_rate_lookup=_synthetic_fx_lookup(
                base_currency=normalized_base,
                quote_currency=definition.native_currency,
                dates={
                    point_date
                    for observation in native_observations
                    for point_date in (
                        observation.previous_observation_date,
                        observation.observation_date,
                    )
                },
            ),
        )
        status: BenchmarkSeriesStatus = "partial" if missing_fx else "available"
        return BenchmarkReturnSeries(
            benchmark_id=definition.benchmark_id,
            provider_name=self.name,
            source_currency=definition.native_currency,
            currency=normalized_base,
            series_kind=definition.series_kind,
            observations=observations,
            coverage_ratio=1.0 if not missing_fx else _ratio(len(observations), len(native_observations)),
            status=status,
            reason_code="missing_fx_rates" if missing_fx else "ok",
        )


class LoadedBenchmarkProvider(BenchmarkProvider):
    """Provider for benchmark returns and FX rates already loaded into data frames."""

    def __init__(
        self,
        benchmark_returns: pd.DataFrame,
        *,
        fx_rates: pd.DataFrame | None = None,
        provider_name: str = "loaded",
    ) -> None:
        required = {
            "benchmark_id",
            "previous_observation_date",
            "observation_date",
            "return_decimal",
            "currency",
        }
        missing = sorted(required - set(benchmark_returns.columns))
        if missing:
            raise ValueError(f"benchmark_returns missing required columns: {', '.join(missing)}")
        self._benchmark_returns = benchmark_returns.copy()
        self._fx_rates = pd.DataFrame() if fx_rates is None else fx_rates.copy()
        self._name = provider_name.strip() or "loaded"

    @property
    def name(self) -> str:
        return self._name

    def fetch_daily_returns(
        self,
        definition: BenchmarkDefinition,
        *,
        start_date: date,
        end_date: date,
        base_currency: str,
    ) -> BenchmarkReturnSeries:
        if definition.is_composite:
            raise ValueError("Composite benchmarks must be built from their component series")
        _validate_date_window(start_date=start_date, end_date=end_date)
        normalized_base = _normalize_currency(base_currency)
        frame = self._benchmark_returns.loc[
            self._benchmark_returns["benchmark_id"].astype(str) == definition.benchmark_id
        ].copy()
        frame["previous_observation_date"] = pd.to_datetime(
            frame["previous_observation_date"], errors="coerce"
        ).dt.date
        frame["observation_date"] = pd.to_datetime(frame["observation_date"], errors="coerce").dt.date
        frame["return_decimal"] = pd.to_numeric(frame["return_decimal"], errors="coerce")
        frame["currency"] = frame["currency"].astype(str).str.strip().str.upper()
        frame = frame.loc[
            frame["previous_observation_date"].notna()
            & frame["observation_date"].notna()
            & frame["return_decimal"].map(_is_finite)
            & (frame["return_decimal"] > -1.0)
            & (frame["observation_date"] >= start_date)
            & (frame["observation_date"] <= end_date)
        ].sort_values(["observation_date", "previous_observation_date"])
        if frame["observation_date"].duplicated().any():
            raise ValueError(f"Duplicate benchmark observation dates for {definition.benchmark_id}")
        currencies = set(frame["currency"])
        if currencies and currencies != {definition.native_currency}:
            raise ValueError(
                f"Loaded currency for {definition.benchmark_id} does not match catalog: {sorted(currencies)}"
            )

        native_observations = tuple(
            BenchmarkReturnObservation(
                previous_observation_date=row.previous_observation_date,
                observation_date=row.observation_date,
                return_decimal=float(row.return_decimal),
            )
            for row in frame.itertuples(index=False)
        )
        expected_observations = len(tuple(_business_dates(start_date, end_date)))
        source_coverage = _ratio(len(native_observations), expected_observations)
        observations, missing_fx = _convert_observations(
            native_observations,
            source_currency=definition.native_currency,
            base_currency=normalized_base,
            fx_rate_lookup=_loaded_fx_lookup(
                self._fx_rates,
                base_currency=normalized_base,
                quote_currency=definition.native_currency,
            ),
        )
        coverage = min(source_coverage, _ratio(len(observations), len(native_observations)))
        if not observations:
            status: BenchmarkSeriesStatus = "unavailable"
            reason_code = "benchmark_series_unavailable" if not native_observations else "missing_fx_rates"
        elif coverage < 1.0:
            status = "partial"
            reason_code = "missing_fx_rates" if missing_fx else "partial_benchmark_coverage"
        else:
            status = "available"
            reason_code = "ok"
        return BenchmarkReturnSeries(
            benchmark_id=definition.benchmark_id,
            provider_name=self.name,
            source_currency=definition.native_currency,
            currency=normalized_base,
            series_kind=definition.series_kind,
            observations=observations,
            coverage_ratio=round(coverage, 8),
            status=status,
            reason_code=reason_code,
        )


def _convert_observations(
    observations: tuple[BenchmarkReturnObservation, ...],
    *,
    source_currency: str,
    base_currency: str,
    fx_rate_lookup: Mapping[date, float],
) -> tuple[tuple[BenchmarkReturnObservation, ...], bool]:
    normalized_source = _normalize_currency(source_currency)
    normalized_base = _normalize_currency(base_currency)
    if normalized_source == normalized_base:
        return observations, False

    converted: list[BenchmarkReturnObservation] = []
    missing_fx = False
    for observation in observations:
        previous_rate = fx_rate_lookup.get(observation.previous_observation_date)
        current_rate = fx_rate_lookup.get(observation.observation_date)
        if previous_rate is None or current_rate is None or previous_rate <= 0 or current_rate <= 0:
            missing_fx = True
            continue
        # FX rates follow the repository convention: quote units per one base unit.
        converted_return = (1.0 + observation.return_decimal) * (previous_rate / current_rate) - 1.0
        converted.append(
            BenchmarkReturnObservation(
                previous_observation_date=observation.previous_observation_date,
                observation_date=observation.observation_date,
                return_decimal=round(converted_return, 12),
            )
        )
    return tuple(converted), missing_fx


def _loaded_fx_lookup(
    fx_rates: pd.DataFrame,
    *,
    base_currency: str,
    quote_currency: str,
) -> dict[date, float]:
    if base_currency == quote_currency:
        return {}
    required = {"base_currency", "quote_currency", "rate_date", "rate"}
    missing = sorted(required - set(fx_rates.columns))
    if missing:
        return {}
    frame = fx_rates.copy()
    frame["base_currency"] = frame["base_currency"].astype(str).str.strip().str.upper()
    frame["quote_currency"] = frame["quote_currency"].astype(str).str.strip().str.upper()
    frame["rate_date"] = pd.to_datetime(frame["rate_date"], errors="coerce").dt.date
    frame["rate"] = pd.to_numeric(frame["rate"], errors="coerce")
    direct = frame.loc[
        (frame["base_currency"] == base_currency)
        & (frame["quote_currency"] == quote_currency)
        & frame["rate_date"].notna()
        & frame["rate"].map(_is_positive_finite)
    ].sort_values("rate_date")
    if not direct.empty:
        return {
            row.rate_date: float(row.rate)
            for row in direct.drop_duplicates("rate_date", keep="last").itertuples(index=False)
        }
    inverse = frame.loc[
        (frame["base_currency"] == quote_currency)
        & (frame["quote_currency"] == base_currency)
        & frame["rate_date"].notna()
        & frame["rate"].map(_is_positive_finite)
    ].sort_values("rate_date")
    return {
        row.rate_date: 1.0 / float(row.rate)
        for row in inverse.drop_duplicates("rate_date", keep="last").itertuples(index=False)
    }


def _synthetic_fx_lookup(
    *,
    base_currency: str,
    quote_currency: str,
    dates: set[date],
) -> dict[date, float]:
    if base_currency == quote_currency:
        return {}
    pair_seed = sum(ord(character) for character in f"{base_currency}/{quote_currency}")
    return {
        rate_date: 1.05 + (pair_seed % 17) / 100.0 + 0.025 * sin((rate_date.toordinal() + pair_seed) / 23.0)
        for rate_date in dates
    }


def _synthetic_return(benchmark_id: str, observation_date: date) -> float:
    ordinal = observation_date.toordinal()
    if benchmark_id == "msci_world":
        return 0.00028 + 0.0040 * sin(ordinal / 9.0)
    if benchmark_id == "sp500":
        return 0.00034 + 0.0050 * sin((ordinal + 7) / 8.0)
    if benchmark_id == "global_aggregate_bonds_eur_hedged":
        return 0.00010 + 0.0012 * sin((ordinal + 19) / 14.0)
    if benchmark_id == "estr_cash":
        return (1.0 + 0.025) ** (1.0 / 365.0) - 1.0
    raise ValueError(f"Synthetic benchmark series is not defined for {benchmark_id}")


def _business_intervals(start_date: date, end_date: date) -> tuple[tuple[date, date], ...]:
    intervals: list[tuple[date, date]] = []
    for current_date in _business_dates(start_date, end_date):
        previous_date = current_date - timedelta(days=1)
        while previous_date.weekday() >= 5:
            previous_date -= timedelta(days=1)
        intervals.append((previous_date, current_date))
    return tuple(intervals)


def _business_dates(start_date: date, end_date: date) -> tuple[date, ...]:
    current_date = start_date
    dates: list[date] = []
    while current_date <= end_date:
        if current_date.weekday() < 5:
            dates.append(current_date)
        current_date += timedelta(days=1)
    return tuple(dates)


def _validate_date_window(*, start_date: date, end_date: date) -> None:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")


def _normalize_currency(value: str) -> str:
    normalized = value.strip().upper()
    if len(normalized) != 3 or not normalized.isascii() or not normalized.isalpha():
        raise ValueError("currency must be a three-letter ISO currency code")
    return normalized


def _is_finite(value: object) -> bool:
    try:
        return isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _is_positive_finite(value: object) -> bool:
    return _is_finite(value) and float(value) > 0


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return min(max(numerator / denominator, 0.0), 1.0)


__all__ = [
    "DEFAULT_BENCHMARK_CATALOG",
    "BenchmarkComponent",
    "BenchmarkDefinition",
    "BenchmarkProvider",
    "BenchmarkReturnObservation",
    "BenchmarkReturnSeries",
    "BenchmarkSeriesKind",
    "BenchmarkSeriesStatus",
    "LoadedBenchmarkProvider",
    "SyntheticBenchmarkProvider",
    "selectable_benchmarks",
]
