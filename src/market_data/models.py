"""Domain models for market data refresh workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping


def _clean_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class MarketAsset:
    """Asset metadata required to resolve and persist market prices."""

    asset_id: str
    asset_name: str
    asset_type: str
    trading_currency: str
    asset_similar: str | None = None
    isin: str | None = None
    ticker: str | None = None
    broker_symbol: str | None = None
    exchange_mic: str | None = None
    first_seen_date: date | None = None
    last_seen_date: date | None = None
    is_active: bool = True

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> "MarketAsset":
        """Build an asset model from a DuckDB row or dict-like object."""
        return cls(
            asset_id=str(values["asset_id"]),
            asset_name=str(values["asset_name"]),
            asset_type=str(values["asset_type"]),
            trading_currency=str(values["trading_currency"]).upper(),
            asset_similar=_clean_text(values.get("asset_similar")),
            isin=_clean_text(values.get("isin")),
            ticker=_clean_text(values.get("ticker")),
            broker_symbol=_clean_text(values.get("broker_symbol")),
            exchange_mic=_clean_text(values.get("exchange_mic")),
            first_seen_date=values.get("first_seen_date"),
            last_seen_date=values.get("last_seen_date"),
            is_active=bool(values.get("is_active", True)),
        )

    @property
    def candidate_symbols(self) -> tuple[str, ...]:
        """Return de-duplicated symbol candidates for provider lookup."""
        seen: set[str] = set()
        ordered: list[str] = []

        for raw_value in (self.ticker, self.broker_symbol, self.isin):
            if raw_value is None:
                continue
            normalized = raw_value.strip().upper()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)

        return tuple(ordered)


@dataclass(frozen=True)
class DailyPriceRecord:
    """Single daily bar persisted into `prices_daily`."""

    price_date: date
    price_currency: str
    close_price: float
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    adjusted_close_price: float | None = None
    volume: int | None = None
    source_updated_at: datetime | None = None


@dataclass(frozen=True)
class PriceFetchResult:
    """Successful provider response for one resolved symbol."""

    provider_name: str
    resolved_symbol: str
    price_records: tuple[DailyPriceRecord, ...]


@dataclass(frozen=True)
class PriceRefreshOutcome:
    """Result for one requested asset refresh."""

    asset_id: str
    provider_name: str
    status: str
    records_written: int
    resolved_symbol: str | None = None
    resolved_asset_id: str | None = None
    message: str | None = None

    @property
    def used_proxy(self) -> bool:
        return self.resolved_asset_id is not None and self.resolved_asset_id != self.asset_id


@dataclass(frozen=True)
class PriceRefreshSummary:
    """Aggregate result for a refresh run."""

    provider_name: str
    outcomes: tuple[PriceRefreshOutcome, ...]

    @property
    def total_records(self) -> int:
        return sum(outcome.records_written for outcome in self.outcomes)

    @property
    def updated_assets(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == "updated")

    @property
    def skipped_assets(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == "skipped")


@dataclass(frozen=True)
class FxRateRecord:
    """Single FX rate persisted into `fx_rates`."""

    rate_date: date
    rate: float
    source_updated_at: datetime | None = None


@dataclass(frozen=True)
class FxFetchResult:
    """Successful FX provider response for one currency pair."""

    provider_name: str
    base_currency: str
    quote_currency: str
    rate_records: tuple[FxRateRecord, ...]


@dataclass(frozen=True)
class FxRefreshOutcome:
    """Refresh result for one currency pair."""

    base_currency: str
    quote_currency: str
    provider_name: str
    status: str
    records_written: int
    message: str | None = None

    @property
    def pair(self) -> str:
        return f"{self.base_currency}/{self.quote_currency}"


@dataclass(frozen=True)
class FxRefreshSummary:
    """Aggregate result for an FX refresh run."""

    provider_name: str
    outcomes: tuple[FxRefreshOutcome, ...]

    @property
    def total_records(self) -> int:
        return sum(outcome.records_written for outcome in self.outcomes)

    @property
    def updated_pairs(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == "updated")

    @property
    def skipped_pairs(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == "skipped")
