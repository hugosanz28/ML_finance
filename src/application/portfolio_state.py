"""Serializable application boundary for the main portfolio read model."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from src.application.serialization import json_ready_value
from src.config import Settings, get_settings
from src.market_data.repository import DuckDBMarketDataRepository
from src.portfolio.metrics_models import PortfolioDataUnavailableError
from src.portfolio import (
    calculate_portfolio_metrics_from_normalized_degiro,
    load_normalized_degiro_snapshots,
)
from src.portfolio.contributions import net_external_contributions_until
from src.portfolio.state_projection import project_portfolio_state


@dataclass(frozen=True)
class GetPortfolioStateRequest:
    persist: bool = True
    include_positions: bool = True
    include_history: bool = False
    as_of_date: str | None = None

    def to_dict(self) -> dict[str, bool | str | None]:
        """Return primitives suitable for query or message adapters."""
        return asdict(self)


@dataclass(frozen=True)
class GetPortfolioStateResult:
    as_of_date: str
    base_currency: str
    summary: dict[str, float | None]
    broker_snapshot: dict[str, str | float | None] | None
    positions: list[dict[str, str | int | float | bool | None]]
    history: list[dict[str, str | int | float | bool | None]]
    data_quality: dict[str, list[str]]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation for HTTP or CLI adapters."""
        return asdict(self)


class PortfolioStateUnavailableError(ValueError):
    """The configured workspace has no readable portfolio history."""


class GetPortfolioStateUseCase:
    name = "get_portfolio_state"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: GetPortfolioStateRequest | None = None) -> GetPortfolioStateResult:
        resolved_request = request or GetPortfolioStateRequest()
        requested_date = _parse_as_of_date(resolved_request.as_of_date)
        # Disabling persistence also prevents implicit DuckDB/schema creation on GET.
        repository = DuckDBMarketDataRepository(settings=self.settings, read_only=not resolved_request.persist)
        try:
            metrics = calculate_portfolio_metrics_from_normalized_degiro(
                settings=self.settings,
                repository=repository,
                persist=resolved_request.persist,
            )
        except (FileNotFoundError, PortfolioDataUnavailableError) as exc:
            raise PortfolioStateUnavailableError("Portfolio data is unavailable") from exc
        snapshots = load_normalized_degiro_snapshots(settings=self.settings)
        projection = project_portfolio_state(
            position_metrics=metrics.position_metrics,
            portfolio_daily_metrics=metrics.portfolio_daily_metrics,
            snapshots=snapshots,
            as_of_date=requested_date,
            include_positions=resolved_request.include_positions,
            include_history=resolved_request.include_history,
        )
        summary = dict(projection.summary)
        summary["net_external_contributions_base"] = net_external_contributions_until(
            self.settings,
            as_of_date=projection.as_of_date,
            repository=repository,
        )

        return GetPortfolioStateResult(
            as_of_date=projection.as_of_date.isoformat(),
            base_currency=metrics.base_currency,
            summary=json_ready_value(summary),
            broker_snapshot=json_ready_value(projection.broker_snapshot),
            positions=json_ready_value(list(projection.positions)),
            history=json_ready_value(list(projection.history)),
            data_quality={"warnings": list(projection.warnings)},
        )


def _parse_as_of_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("as_of_date must use ISO format YYYY-MM-DD.") from exc


__all__ = [
    "GetPortfolioStateRequest",
    "GetPortfolioStateResult",
    "GetPortfolioStateUseCase",
]
