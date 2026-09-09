"""Adapt validated valuation history to the existing risk calculations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import Mapping

import pandas as pd

from src.analytics.risk import (
    ConcentrationResult, DiversificationResult, PositionExposure, RiskResult,
    calculate_concentration, calculate_diversification, calculate_risk,
)
from src.portfolio.performance import calculate_daily_returns
from src.portfolio.performance_models import PortfolioValuation


@dataclass(frozen=True)
class PositionAnalytics:
    concentration: ConcentrationResult
    asset_risk: Mapping[str, RiskResult]
    diversification: DiversificationResult | None
    diversification_reason_code: str
    series_kind: str = "valuation_price_proxy"


def analyze_positions(
    position_metrics: pd.DataFrame, *, start_date: date, end_date: date,
    bucket_mapping: Mapping[str, str], risk_free_rate_annual: float | None = None,
) -> PositionAnalytics:
    """Keep price/FX returns independent of trade quantities; never infer metadata."""
    frame = position_metrics.copy()
    if not frame.empty:
        frame["valuation_date"] = pd.to_datetime(frame["valuation_date"]).dt.date
        frame = frame.loc[frame["valuation_date"].between(start_date, end_date)]
        if frame.duplicated(["asset_id", "valuation_date"]).any():
            raise ValueError("Duplicate asset valuation dates")
    current = frame.loc[frame["valuation_date"] == end_date] if not frame.empty else frame
    exposures = []
    for row in current.to_dict("records"):
        asset_id = str(row["asset_id"])
        asset_bucket = bucket_mapping.get(asset_id)
        isin_bucket = bucket_mapping.get(_text(row.get("isin")))
        # Conflicting exact mappings are unknown, not an arbitrary preference.
        bucket = None if asset_bucket and isin_bucket and asset_bucket != isin_bucket else asset_bucket or isin_bucket
        exposures.append(PositionExposure(
            asset_id, _number(row.get("market_value_base")), bucket,
            _text(row.get("price_currency")), _text(row.get("asset_type")),
            _text(row.get("sector")), _text(row.get("sector_source")),
        ))
    concentration = calculate_concentration(exposures, as_of_date=end_date)
    asset_returns = {}
    asset_risk = {}
    for exposure in exposures:
        history = frame.loc[frame["asset_id"] == exposure.asset_id].sort_values("valuation_date")
        valuations = []
        for row in history.to_dict("records"):
            price, fx = _number(row.get("close_price")), _number(row.get("fx_rate_to_base"))
            if price is None or fx is None or price <= 0 or fx <= 0:
                continue
            if not str(row.get("valuation_status", "")).startswith("valued"):
                continue
            # FX follows the warehouse convention: quote units per base unit.
            valuations.append(PortfolioValuation(row["valuation_date"], price / fx))
        observations = calculate_daily_returns(valuations, reason_codes=("valuation_price_proxy",))
        # Do not annualize a multi-day interval as one daily observation.
        daily = tuple(row for row in observations if (row.valuation_date - row.previous_valuation_date).days == 1)
        asset_returns[exposure.asset_id] = daily
        if start_date < end_date:
            asset_risk[exposure.asset_id] = calculate_risk(
                daily, start_date=start_date, end_date=end_date,
                risk_free_rate_annual=risk_free_rate_annual,
            )
    total = sum(item.market_value_base or 0 for item in exposures)
    complete = bool(exposures) and all(item.market_value_base is not None for item in exposures)
    diversification = None
    reason = "incomplete_position_valuation"
    if complete and total > 0 and start_date < end_date:
        diversification = calculate_diversification(
            asset_returns, {item.asset_id: item.market_value_base / total for item in exposures},
            start_date=start_date, end_date=end_date,
        )
        reason = "valuation_price_proxy"
    elif start_date == end_date:
        reason = "insufficient_observations"
    return PositionAnalytics(concentration, asset_risk, diversification, reason)


def _number(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    return number if isfinite(number) else None


def _text(value: object) -> str | None:
    return None if value is None or pd.isna(value) else str(value).strip() or None
