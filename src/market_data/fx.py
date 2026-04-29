"""FX provider contracts, DEGIRO currency inference, and refresh service."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Sequence

import pandas as pd
import yfinance as yf

from src.config import Settings, get_settings
from src.market_data.models import FxFetchResult, FxRateRecord, FxRefreshOutcome, FxRefreshSummary
from src.market_data.repository import DuckDBMarketDataRepository


class FxProviderError(RuntimeError):
    """Base error raised by FX providers."""


class FxDataNotFoundError(FxProviderError):
    """Raised when a provider cannot return FX data for a pair."""


class UnknownFxProviderError(ValueError):
    """Raised when the configured FX provider name is unsupported."""


@dataclass(frozen=True)
class FxPairRequirement:
    """Currency pair and date window needed by normalized portfolio data."""

    base_currency: str
    quote_currency: str
    start_date: date
    end_date: date
    source_rows: int
    missing_base_rows: int = 0

    @property
    def pair(self) -> str:
        return f"{self.base_currency}/{self.quote_currency}"


class FxProvider(ABC):
    """Simple interface for external daily FX providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable provider identifier stored in the database."""

    @abstractmethod
    def fetch_fx_rates(
        self,
        *,
        base_currency: str,
        quote_currency: str,
        start_date: date,
        end_date: date,
    ) -> FxFetchResult:
        """Fetch daily FX rates for the requested pair and date range."""


class YFinanceFxProvider(FxProvider):
    """Daily FX provider backed by `yfinance` ticker pairs such as `EURUSD=X`."""

    def __init__(self, *, cache_dir: str | Path | None = None) -> None:
        self.cache_dir = None if cache_dir is None else Path(cache_dir).expanduser().resolve()
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            yf.set_tz_cache_location(str(self.cache_dir))

    @property
    def name(self) -> str:
        return "yfinance"

    def fetch_fx_rates(
        self,
        *,
        base_currency: str,
        quote_currency: str,
        start_date: date,
        end_date: date,
    ) -> FxFetchResult:
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")

        normalized_base = _normalize_currency(base_currency)
        normalized_quote = _normalize_currency(quote_currency)
        if normalized_base == normalized_quote:
            return FxFetchResult(
                provider_name=self.name,
                base_currency=normalized_base,
                quote_currency=normalized_quote,
                rate_records=tuple(
                    FxRateRecord(rate_date=day, rate=1.0)
                    for day in _iter_dates(start_date=start_date, end_date=end_date)
                ),
            )

        symbol = f"{normalized_base}{normalized_quote}=X"
        history = self._download_history(symbol, start_date=start_date, end_date=end_date)
        records = self._records_from_history(history)
        if not records:
            raise FxDataNotFoundError(
                f"No FX rates available in {self.name} for {normalized_base}/{normalized_quote}; tried: {symbol}"
            )

        return FxFetchResult(
            provider_name=self.name,
            base_currency=normalized_base,
            quote_currency=normalized_quote,
            rate_records=tuple(records),
        )

    def _download_history(
        self,
        symbol: str,
        *,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        ticker = yf.Ticker(symbol)
        history = ticker.history(
            start=start_date.isoformat(),
            end=(end_date + timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
            actions=False,
        )
        if not isinstance(history, pd.DataFrame):
            return pd.DataFrame()
        return history

    def _records_from_history(self, history: pd.DataFrame) -> list[FxRateRecord]:
        if history.empty:
            return []

        records: list[FxRateRecord] = []
        for index_value, row in history.sort_index().iterrows():
            close_rate = _optional_float(row.get("Close"))
            if close_rate is None:
                continue
            records.append(FxRateRecord(rate_date=pd.Timestamp(index_value).date(), rate=close_rate))
        return records


class FxRefreshService:
    """Coordinate FX provider fetches and DuckDB persistence."""

    def __init__(
        self,
        *,
        repository: DuckDBMarketDataRepository | None = None,
        provider: FxProvider | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = get_settings() if settings is None else settings
        self.repository = repository or DuckDBMarketDataRepository(settings=self.settings)
        self.provider = provider or build_fx_provider(
            self.settings.price_provider,
            cache_dir=self.settings.market_data_dir / "yfinance_cache",
        )

    def refresh_rates(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        pairs: Sequence[tuple[str, str]] | None = None,
        infer_from_normalized: bool = True,
        only_missing_base: bool = False,
    ) -> FxRefreshSummary:
        """Refresh daily FX rates for explicit or inferred currency pairs."""
        requirements = (
            infer_fx_requirements_from_normalized_degiro(
                settings=self.settings,
                only_missing_base=only_missing_base,
            )
            if infer_from_normalized
            else []
        )
        selected_pairs = _resolve_pairs(pairs=pairs, requirements=requirements)
        outcomes: list[FxRefreshOutcome] = []

        for base_currency, quote_currency in selected_pairs:
            normalized_base = _normalize_currency(base_currency)
            normalized_quote = _normalize_currency(quote_currency)
            window_start, window_end = _resolve_window(
                base_currency=normalized_base,
                quote_currency=normalized_quote,
                start_date=start_date,
                end_date=end_date,
                requirements=requirements,
            )
            try:
                fetch_result = self.provider.fetch_fx_rates(
                    base_currency=normalized_base,
                    quote_currency=normalized_quote,
                    start_date=window_start,
                    end_date=window_end,
                )
            except FxDataNotFoundError as exc:
                outcomes.append(
                    FxRefreshOutcome(
                        base_currency=normalized_base,
                        quote_currency=normalized_quote,
                        provider_name=self.provider.name,
                        status="skipped",
                        records_written=0,
                        message=str(exc),
                    )
                )
                continue

            written_records = self.repository.upsert_fx_rates(
                base_currency=fetch_result.base_currency,
                quote_currency=fetch_result.quote_currency,
                provider_name=fetch_result.provider_name,
                rates=fetch_result.rate_records,
            )
            outcomes.append(
                FxRefreshOutcome(
                    base_currency=fetch_result.base_currency,
                    quote_currency=fetch_result.quote_currency,
                    provider_name=fetch_result.provider_name,
                    status="updated",
                    records_written=written_records,
                )
            )

        return FxRefreshSummary(provider_name=self.provider.name, outcomes=tuple(outcomes))


def build_fx_provider(
    provider_name: str,
    *,
    cache_dir: str | Path | None = None,
) -> FxProvider:
    """Build the configured FX provider."""
    normalized_name = provider_name.strip().lower()
    if normalized_name == "yfinance":
        return YFinanceFxProvider(cache_dir=cache_dir)
    raise UnknownFxProviderError(f"Unsupported FX provider: {provider_name}")


def infer_fx_requirements_from_normalized_degiro(
    *,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
    only_missing_base: bool = False,
) -> list[FxPairRequirement]:
    """Infer needed FX pairs from normalized DEGIRO parquet datasets."""
    resolved_settings = get_settings() if settings is None else settings
    base_dir = (
        resolved_settings.normalized_data_dir / "degiro"
        if normalized_degiro_dir is None
        else Path(normalized_degiro_dir).expanduser().resolve()
    )
    pair_rows: dict[tuple[str, str], list[tuple[date, bool]]] = {}

    _collect_pair_rows(
        pair_rows,
        directory=base_dir / "transactions",
        currency_column="transaction_currency",
        date_column="trade_date",
        base_amount_columns=("gross_amount_base", "net_cash_amount_base"),
        only_missing_base=only_missing_base,
    )
    _collect_pair_rows(
        pair_rows,
        directory=base_dir / "cash_movements",
        currency_column="movement_currency",
        date_column="movement_date",
        base_amount_columns=("amount_base",),
        only_missing_base=only_missing_base,
    )
    _collect_pair_rows(
        pair_rows,
        directory=base_dir / "portfolio_snapshots",
        currency_column="position_currency",
        date_column="snapshot_date",
        base_amount_columns=("market_value_base",),
        only_missing_base=only_missing_base,
    )

    requirements: list[FxPairRequirement] = []
    for (base_currency, quote_currency), rows in sorted(pair_rows.items()):
        dates = [row_date for row_date, _ in rows]
        requirements.append(
            FxPairRequirement(
                base_currency=base_currency,
                quote_currency=quote_currency,
                start_date=min(dates),
                end_date=max(dates),
                source_rows=len(rows),
                missing_base_rows=sum(1 for _, missing_base in rows if missing_base),
            )
        )
    return requirements


def _collect_pair_rows(
    pair_rows: dict[tuple[str, str], list[tuple[date, bool]]],
    *,
    directory: Path,
    currency_column: str,
    date_column: str,
    base_amount_columns: tuple[str, ...],
    only_missing_base: bool,
) -> None:
    for parquet_path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        frame = pd.read_parquet(parquet_path)
        required_columns = {currency_column, "base_currency", date_column}
        if frame.empty or not required_columns.issubset(frame.columns):
            continue

        current = frame.copy()
        current[currency_column] = current[currency_column].map(_normalize_optional_currency)
        current["base_currency"] = current["base_currency"].map(_normalize_optional_currency)
        current[date_column] = pd.to_datetime(current[date_column], errors="coerce").dt.date
        current = current.loc[
            current[currency_column].notna()
            & current["base_currency"].notna()
            & current[date_column].notna()
            & (current[currency_column] != current["base_currency"])
        ].copy()
        if current.empty:
            continue

        present_base_amount_columns = [column for column in base_amount_columns if column in current.columns]
        if present_base_amount_columns:
            missing_base = current[present_base_amount_columns].isna().any(axis=1)
        else:
            missing_base = pd.Series([True] * len(current), index=current.index)

        if only_missing_base:
            current = current.loc[missing_base].copy()
            missing_base = missing_base.loc[current.index]
            if current.empty:
                continue

        for row_index, row in current.iterrows():
            pair = (str(row["base_currency"]), str(row[currency_column]))
            pair_rows.setdefault(pair, []).append((row[date_column], bool(missing_base.loc[row_index])))


def _resolve_pairs(
    *,
    pairs: Sequence[tuple[str, str]] | None,
    requirements: Sequence[FxPairRequirement],
) -> list[tuple[str, str]]:
    if pairs:
        normalized_pairs = [(_normalize_currency(base), _normalize_currency(quote)) for base, quote in pairs]
    else:
        normalized_pairs = [(requirement.base_currency, requirement.quote_currency) for requirement in requirements]
    return sorted(set(normalized_pairs))


def _resolve_window(
    *,
    base_currency: str,
    quote_currency: str,
    start_date: date | None,
    end_date: date | None,
    requirements: Sequence[FxPairRequirement],
) -> tuple[date, date]:
    matching = [
        requirement
        for requirement in requirements
        if requirement.base_currency == base_currency and requirement.quote_currency == quote_currency
    ]
    inferred_start = min((requirement.start_date for requirement in matching), default=date.today())
    inferred_end = max((requirement.end_date for requirement in matching), default=date.today())
    resolved_start = start_date or inferred_start
    resolved_end = end_date or inferred_end
    if resolved_end < resolved_start:
        raise ValueError("end_date must be on or after start_date")
    return resolved_start, resolved_end


def _iter_dates(*, start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current = current + timedelta(days=1)


def _normalize_currency(value: str) -> str:
    normalized = str(value).strip().upper()
    if len(normalized) != 3:
        raise ValueError(f"Currency code must have three letters: {value}")
    return normalized


def _normalize_optional_currency(value: object | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    return text if len(text) == 3 else None


def _optional_float(value: object | None) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)
