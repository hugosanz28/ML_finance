"""Market data ingestion, provider contracts, and daily price refresh utilities."""

from src.market_data.degiro import (
    load_asset_overrides_frame,
    load_market_assets_from_normalized_degiro,
    sync_market_assets_from_normalized_degiro,
    write_asset_overrides_template,
)
from src.market_data.models import (
    DailyPriceRecord,
    MarketAsset,
    PriceFetchResult,
    PriceRefreshOutcome,
    PriceRefreshSummary,
)
from src.market_data.providers import (
    PriceDataNotFoundError,
    PriceProvider,
    PriceProviderError,
    UnknownPriceProviderError,
    YFinancePriceProvider,
    build_price_provider,
)
from src.market_data.repository import DuckDBMarketDataRepository
from src.market_data.service import PriceRefreshService

__all__ = [
    "DailyPriceRecord",
    "DuckDBMarketDataRepository",
    "MarketAsset",
    "load_asset_overrides_frame",
    "load_market_assets_from_normalized_degiro",
    "PriceDataNotFoundError",
    "PriceFetchResult",
    "PriceProvider",
    "PriceProviderError",
    "PriceRefreshOutcome",
    "PriceRefreshService",
    "PriceRefreshSummary",
    "UnknownPriceProviderError",
    "YFinancePriceProvider",
    "build_price_provider",
    "sync_market_assets_from_normalized_degiro",
    "write_asset_overrides_template",
]
