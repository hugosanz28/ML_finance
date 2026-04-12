"""Orchestration layer for daily market data refreshes."""

from __future__ import annotations

from datetime import date
from typing import Sequence

from src.config import Settings, get_settings
from src.market_data.degiro import load_market_assets_from_normalized_degiro, sync_market_assets_from_normalized_degiro
from src.market_data.models import MarketAsset, PriceFetchResult, PriceRefreshOutcome, PriceRefreshSummary
from src.market_data.providers import PriceDataNotFoundError, PriceProvider, build_price_provider
from src.market_data.repository import DuckDBMarketDataRepository


class PriceRefreshService:
    """Coordinate provider fetches, fallback resolution, and DuckDB persistence."""

    def __init__(
        self,
        *,
        repository: DuckDBMarketDataRepository | None = None,
        provider: PriceProvider | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = get_settings() if settings is None else settings
        self.repository = repository or DuckDBMarketDataRepository(settings=self.settings)
        self.provider = provider or build_price_provider(
            self.settings.price_provider,
            cache_dir=self.settings.market_data_dir / "yfinance_cache",
        )

    def refresh_prices(
        self,
        *,
        start_date: date,
        end_date: date,
        asset_ids: Sequence[str] | None = None,
        active_only: bool = True,
        bootstrap_degiro_assets: bool = True,
    ) -> PriceRefreshSummary:
        """Refresh daily prices for selected assets."""
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")

        if bootstrap_degiro_assets:
            sync_market_assets_from_normalized_degiro(
                repository=self.repository,
                settings=self.settings,
            )

        assets, asset_lookup = self._load_refresh_assets(asset_ids=asset_ids, active_only=active_only)
        outcomes: list[PriceRefreshOutcome] = []

        for asset in assets:
            try:
                fetch_result, resolved_asset = self._fetch_with_fallback(
                    asset,
                    start_date=start_date,
                    end_date=end_date,
                    asset_lookup=asset_lookup,
                    visited_asset_ids=set(),
                )
            except PriceDataNotFoundError as exc:
                outcomes.append(
                    PriceRefreshOutcome(
                        asset_id=asset.asset_id,
                        provider_name=self.provider.name,
                        status="skipped",
                        records_written=0,
                        message=str(exc),
                    )
                )
                continue

            written_records = self.repository.upsert_daily_prices(
                asset_id=asset.asset_id,
                provider_name=fetch_result.provider_name,
                prices=fetch_result.price_records,
            )
            used_proxy = resolved_asset.asset_id != asset.asset_id
            message = None
            if used_proxy:
                message = f"Fetched via asset_similar {resolved_asset.asset_id}"

            outcomes.append(
                PriceRefreshOutcome(
                    asset_id=asset.asset_id,
                    provider_name=fetch_result.provider_name,
                    status="updated",
                    records_written=written_records,
                    resolved_symbol=fetch_result.resolved_symbol,
                    resolved_asset_id=resolved_asset.asset_id,
                    message=message,
                )
            )

        return PriceRefreshSummary(provider_name=self.provider.name, outcomes=tuple(outcomes))

    def _fetch_with_fallback(
        self,
        asset: MarketAsset,
        *,
        start_date: date,
        end_date: date,
        asset_lookup: dict[str, MarketAsset],
        visited_asset_ids: set[str],
    ) -> tuple[PriceFetchResult, MarketAsset]:
        if asset.asset_id in visited_asset_ids:
            raise PriceDataNotFoundError(f"Detected asset_similar cycle while resolving {asset.asset_id}")

        visited_asset_ids.add(asset.asset_id)

        try:
            return (
                self.provider.fetch_daily_prices(
                    asset,
                    start_date=start_date,
                    end_date=end_date,
                ),
                asset,
            )
        except PriceDataNotFoundError as exc:
            if asset.asset_similar is None:
                raise exc

            similar_asset = asset_lookup.get(asset.asset_similar) or self.repository.get_asset(asset.asset_similar)
            if similar_asset is None:
                raise PriceDataNotFoundError(
                    f"{exc}. Missing asset_similar target: {asset.asset_similar}"
                ) from exc

            return self._fetch_with_fallback(
                similar_asset,
                start_date=start_date,
                end_date=end_date,
                asset_lookup=asset_lookup,
                visited_asset_ids=visited_asset_ids,
            )

    def _load_refresh_assets(
        self,
        *,
        asset_ids: Sequence[str] | None,
        active_only: bool,
    ) -> tuple[list[MarketAsset], dict[str, MarketAsset]]:
        normalized_assets = load_market_assets_from_normalized_degiro(settings=self.settings)
        db_assets = self.repository.list_assets(asset_ids=asset_ids, active_only=False)

        merged_assets: dict[str, MarketAsset] = {asset.asset_id: asset for asset in db_assets}
        merged_assets.update({asset.asset_id: asset for asset in normalized_assets})

        assets = list(merged_assets.values())
        if asset_ids:
            selected = set(asset_ids)
            assets = [asset for asset in assets if asset.asset_id in selected]
        if active_only:
            assets = [asset for asset in assets if asset.is_active]
        assets.sort(key=lambda asset: asset.asset_id)

        return assets, {asset.asset_id: asset for asset in merged_assets.values()}
