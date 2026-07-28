"""Application use cases for market data refresh workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Sequence

from src.application.types import ApplicationResult
from src.config import Settings, get_settings
from src.market_data import (
    DuckDBMarketDataRepository,
    FxRefreshService,
    FxRefreshSummary,
    MarketAsset,
    PriceRefreshService,
    PriceRefreshSummary,
    build_fx_provider,
    build_price_provider,
    infer_fx_requirements_from_normalized_degiro,
    sync_market_assets_from_normalized_degiro,
    write_asset_overrides_template,
)


@dataclass(frozen=True)
class RefreshFxRequest:
    start_date: date | None = None
    end_date: date | None = None
    pairs: tuple[tuple[str, str], ...] = ()
    provider: str | None = None
    infer_from_normalized: bool = True
    only_missing_base: bool = False


@dataclass(frozen=True)
class RefreshFxResult:
    result: ApplicationResult
    summary: FxRefreshSummary


@dataclass(frozen=True)
class InferFxRequirementsRequest:
    only_missing_base: bool = False


@dataclass(frozen=True)
class FxRequirementView:
    pair: str
    start_date: date
    end_date: date
    source_rows: int
    missing_base_rows: int

    def to_dict(self) -> dict[str, str | int]:
        """Return JSON primitives while preserving ISO date semantics."""
        return {
            "pair": self.pair,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "source_rows": self.source_rows,
            "missing_base_rows": self.missing_base_rows,
        }


@dataclass(frozen=True)
class InferFxRequirementsResult:
    requirements: tuple[FxRequirementView, ...]

    def to_dict(self) -> dict[str, list[dict[str, str | int]]]:
        """Return a JSON-serializable representation for interface adapters."""
        return {"requirements": [requirement.to_dict() for requirement in self.requirements]}


@dataclass(frozen=True)
class RefreshMarketDataRequest:
    start_date: date | None = None
    end_date: date | None = None
    asset_ids: tuple[str, ...] = ()
    provider: str | None = None
    bootstrap_degiro_assets: bool = True
    include_inactive: bool = False
    write_overrides_template: bool = True


@dataclass(frozen=True)
class RefreshMarketDataResult:
    result: ApplicationResult
    summary: PriceRefreshSummary | None
    synced_assets: int = 0
    override_template_path: Path | None = None


class RefreshFxUseCase:
    """Refresh FX rates from explicit pairs or inferred DEGIRO requirements."""

    name = "refresh_fx"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: RefreshFxRequest | None = None) -> RefreshFxResult:
        resolved_request = request or RefreshFxRequest()
        provider = None
        if resolved_request.provider:
            provider = build_fx_provider(
                resolved_request.provider,
                cache_dir=self.settings.market_data_dir / "yfinance_cache",
            )
        service = FxRefreshService(
            repository=DuckDBMarketDataRepository(settings=self.settings),
            provider=provider,
            settings=self.settings,
        )
        summary = service.refresh_rates(
            start_date=resolved_request.start_date,
            end_date=resolved_request.end_date,
            pairs=resolved_request.pairs or None,
            infer_from_normalized=resolved_request.infer_from_normalized,
            only_missing_base=resolved_request.only_missing_base,
        )
        status = "partial" if summary.skipped_pairs else "succeeded"
        message = f"FX refresh wrote {summary.total_records} row(s)."
        return RefreshFxResult(
            result=ApplicationResult(
                name=self.name,
                status=status,
                message=message,
                artifacts={
                    "provider": summary.provider_name,
                    "updated_pairs": summary.updated_pairs,
                    "skipped_pairs": summary.skipped_pairs,
                    "rows_written": summary.total_records,
                },
            ),
            summary=summary,
        )


class InferFxRequirementsUseCase:
    """Return a serializable view of FX requirements inferred from DEGIRO data."""

    name = "infer_fx_requirements"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(
        self,
        request: InferFxRequirementsRequest | None = None,
    ) -> InferFxRequirementsResult:
        resolved_request = request or InferFxRequirementsRequest()
        requirements = infer_fx_requirements_from_normalized_degiro(
            settings=self.settings,
            only_missing_base=resolved_request.only_missing_base,
        )
        return InferFxRequirementsResult(
            requirements=tuple(
                FxRequirementView(
                    pair=requirement.pair,
                    start_date=requirement.start_date,
                    end_date=requirement.end_date,
                    source_rows=requirement.source_rows,
                    missing_base_rows=requirement.missing_base_rows,
                )
                for requirement in requirements
            )
        )


class RefreshMarketDataUseCase:
    """Refresh market prices for assets in the local warehouse."""

    name = "refresh_market_data"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: RefreshMarketDataRequest | None = None) -> RefreshMarketDataResult:
        resolved_request = request or RefreshMarketDataRequest()
        repository = DuckDBMarketDataRepository(settings=self.settings)
        synced_assets = 0
        if resolved_request.bootstrap_degiro_assets:
            synced_assets = sync_market_assets_from_normalized_degiro(
                repository=repository,
                settings=self.settings,
            )

        assets = repository.list_assets(
            asset_ids=resolved_request.asset_ids or None,
            active_only=not resolved_request.include_inactive,
        )
        if not assets:
            return RefreshMarketDataResult(
                result=ApplicationResult(
                    name=self.name,
                    status="failed",
                    message="No assets available for refresh.",
                    artifacts={"synced_assets": synced_assets},
                ),
                summary=None,
                synced_assets=synced_assets,
            )

        start_date = resolved_request.start_date or _derive_start_date(assets)
        end_date = resolved_request.end_date or date.today()
        provider = None
        if resolved_request.provider:
            provider = build_price_provider(
                resolved_request.provider,
                cache_dir=self.settings.market_data_dir / "yfinance_cache",
            )
        service = PriceRefreshService(repository=repository, provider=provider, settings=self.settings)
        summary = service.refresh_prices(
            start_date=start_date,
            end_date=end_date,
            asset_ids=resolved_request.asset_ids or None,
            active_only=not resolved_request.include_inactive,
            bootstrap_degiro_assets=False,
        )

        override_template_path = None
        skipped_asset_ids = tuple(outcome.asset_id for outcome in summary.outcomes if outcome.status == "skipped")
        if skipped_asset_ids and resolved_request.write_overrides_template:
            override_template_path = write_asset_overrides_template(
                skipped_asset_ids,
                repository=repository,
                settings=self.settings,
            )

        status = "partial" if summary.skipped_assets else "succeeded"
        return RefreshMarketDataResult(
            result=ApplicationResult(
                name=self.name,
                status=status,
                message=f"Market data refresh wrote {summary.total_records} row(s).",
                artifacts={
                    "provider": summary.provider_name,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "synced_assets": synced_assets,
                    "updated_assets": summary.updated_assets,
                    "skipped_assets": summary.skipped_assets,
                    "rows_written": summary.total_records,
                    "override_template_path": override_template_path,
                },
            ),
            summary=summary,
            synced_assets=synced_assets,
            override_template_path=override_template_path,
        )


def _derive_start_date(assets: Sequence[MarketAsset]) -> date:
    dated_assets = [asset.first_seen_date for asset in assets if asset.first_seen_date is not None]
    if dated_assets:
        return min(dated_assets)
    return date.today()


__all__ = [
    "FxRequirementView",
    "InferFxRequirementsRequest",
    "InferFxRequirementsResult",
    "InferFxRequirementsUseCase",
    "RefreshFxRequest",
    "RefreshFxResult",
    "RefreshFxUseCase",
    "RefreshMarketDataRequest",
    "RefreshMarketDataResult",
    "RefreshMarketDataUseCase",
]
