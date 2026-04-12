"""Price provider contracts and concrete implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

from src.market_data.models import DailyPriceRecord, MarketAsset, PriceFetchResult


class PriceProviderError(RuntimeError):
    """Base error raised by price providers."""


class PriceDataNotFoundError(PriceProviderError):
    """Raised when a provider cannot resolve or return price data."""


class UnknownPriceProviderError(ValueError):
    """Raised when the configured provider name is unsupported."""


class PriceProvider(ABC):
    """Simple interface for external daily price providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable provider identifier stored in the database."""

    @abstractmethod
    def fetch_daily_prices(
        self,
        asset: MarketAsset,
        *,
        start_date: date,
        end_date: date,
    ) -> PriceFetchResult:
        """Fetch daily prices for the requested asset and date range."""


class YFinancePriceProvider(PriceProvider):
    """Daily price provider backed by `yfinance`."""

    def __init__(self, *, cache_dir: str | Path | None = None) -> None:
        self.cache_dir = None if cache_dir is None else Path(cache_dir).expanduser().resolve()
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            yf.set_tz_cache_location(str(self.cache_dir))

    @property
    def name(self) -> str:
        return "yfinance"

    def fetch_daily_prices(
        self,
        asset: MarketAsset,
        *,
        start_date: date,
        end_date: date,
    ) -> PriceFetchResult:
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")

        candidate_symbols = asset.candidate_symbols
        if not candidate_symbols:
            raise PriceDataNotFoundError(f"No symbol candidates available for asset {asset.asset_id}")

        attempted_symbols: list[str] = []
        candidate_errors: list[str] = []
        for symbol in candidate_symbols:
            attempted_symbols.append(symbol)
            try:
                history = self._download_history(symbol, start_date=start_date, end_date=end_date)
                records = self._records_from_history(history, fallback_currency=asset.trading_currency)
            except Exception as exc:
                candidate_errors.append(f"{symbol}: {exc}")
                continue

            if records:
                return PriceFetchResult(
                    provider_name=self.name,
                    resolved_symbol=symbol,
                    price_records=tuple(records),
                )

        attempted_display = ", ".join(attempted_symbols)
        error_suffix = ""
        if candidate_errors:
            error_suffix = f" | errors: {'; '.join(candidate_errors)}"
        raise PriceDataNotFoundError(
            f"No daily prices available in {self.name} for asset {asset.asset_id}; tried: {attempted_display}{error_suffix}"
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

    def _records_from_history(
        self,
        history: pd.DataFrame,
        *,
        fallback_currency: str,
    ) -> list[DailyPriceRecord]:
        if history.empty:
            return []

        currency = self._resolve_currency(history, fallback_currency=fallback_currency)
        records: list[DailyPriceRecord] = []
        ordered_history = history.sort_index()

        for index_value, row in ordered_history.iterrows():
            close_price = self._optional_float(row.get("Close"))
            if close_price is None:
                continue

            records.append(
                DailyPriceRecord(
                    price_date=pd.Timestamp(index_value).date(),
                    price_currency=currency,
                    open_price=self._optional_float(row.get("Open")),
                    high_price=self._optional_float(row.get("High")),
                    low_price=self._optional_float(row.get("Low")),
                    close_price=close_price,
                    adjusted_close_price=self._optional_float(row.get("Adj Close")),
                    volume=self._optional_int(row.get("Volume")),
                )
            )

        return records

    def _resolve_currency(self, history: pd.DataFrame, *, fallback_currency: str) -> str:
        raw_currency = history.attrs.get("currency") if hasattr(history, "attrs") else None
        currency = str(raw_currency or fallback_currency).strip().upper()
        if not currency:
            raise PriceDataNotFoundError("Missing price currency in provider response")
        return currency

    @staticmethod
    def _optional_float(value: object | None) -> float | None:
        if value is None or pd.isna(value):
            return None
        return float(value)

    @staticmethod
    def _optional_int(value: object | None) -> int | None:
        if value is None or pd.isna(value):
            return None
        return int(value)


def build_price_provider(
    provider_name: str,
    *,
    cache_dir: str | Path | None = None,
) -> PriceProvider:
    """Build the configured price provider."""
    normalized_name = provider_name.strip().lower()
    if normalized_name == "yfinance":
        return YFinancePriceProvider(cache_dir=cache_dir)
    raise UnknownPriceProviderError(f"Unsupported price provider: {provider_name}")
