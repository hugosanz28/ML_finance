"""Market data ingestion, provider contracts, and daily price refresh utilities."""

from src.market_data.degiro import (
    load_asset_overrides_frame,
    load_market_assets_from_normalized_degiro,
    sync_market_assets_from_normalized_degiro,
    write_asset_overrides_template,
)
from src.market_data.fx import (
    FxDataNotFoundError,
    FxPairRequirement,
    FxProvider,
    FxProviderError,
    FxRefreshService,
    UnknownFxProviderError,
    YFinanceFxProvider,
    build_fx_provider,
    infer_fx_requirements_from_normalized_degiro,
)
from src.market_data.models import (
    DailyPriceRecord,
    FxFetchResult,
    FxRateRecord,
    FxRefreshOutcome,
    FxRefreshSummary,
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
    "FxDataNotFoundError",
    "FxFetchResult",
    "FxPairRequirement",
    "FxProvider",
    "FxProviderError",
    "FxRateRecord",
    "FxRefreshOutcome",
    "FxRefreshService",
    "FxRefreshSummary",
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
    "UnknownFxProviderError",
    "UnknownPriceProviderError",
    "YFinanceFxProvider",
    "YFinancePriceProvider",
    "build_fx_provider",
    "build_price_provider",
    "infer_fx_requirements_from_normalized_degiro",
    "sync_market_assets_from_normalized_degiro",
    "write_asset_overrides_template",
]
