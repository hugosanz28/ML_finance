from dataclasses import asdict, replace
from datetime import date, timedelta
import json
from math import sqrt
from statistics import stdev

import pytest

from src.analytics.catalog import metric_catalog
from src.analytics.risk import (
    PositionExposure, calculate_concentration, calculate_diversification, calculate_risk,
)
from src.portfolio.performance_models import DailyPerformanceObservation


START = date(2026, 1, 1)
END = START + timedelta(days=40)


def returns(values):
    return tuple(DailyPerformanceObservation(
        START + timedelta(days=i), START + timedelta(days=i + 1),
        100, 100 * (1 + value), 0, value, 1, "available", "ok",
    ) for i, value in enumerate(values))


def risk(values, **kwargs):
    return {m.metric_id: m for m in calculate_risk(
        returns(values), start_date=START, end_date=START + timedelta(days=len(values)), **kwargs,
    ).metrics}


def test_known_volatility_downside_sharpe_sortino():
    values = [-0.01, 0.02] * 20
    result = risk(values, risk_free_rate_annual=0)
    vol = stdev(values) * sqrt(365)
    downside = sqrt(0.0001 / 2 * 365)
    assert result["volatility_annualized"].value == pytest.approx(vol)
    assert result["downside_volatility_annualized"].value == pytest.approx(downside)
    assert result["sharpe"].value == pytest.approx(0.005 * 365 / vol)
    assert result["sortino"].value == pytest.approx(0.005 * 365 / downside)
    assert result["max_drawdown"].value == pytest.approx(-0.01)
    assert result["drawdown_duration_days"].value == 2
    annual_return = ((0.99 * 1.02) ** 20) ** (365 / 40) - 1
    assert result["calmar"].value == pytest.approx(annual_return / .01)


def test_constant_series_has_real_zero_volatility_but_undefined_ratios():
    result = risk([0.0] * 40, risk_free_rate_annual=0)
    assert result["volatility_annualized"].value == 0
    assert result["max_drawdown"].value == 0
    assert result["drawdown_duration_days"].value == 0
    assert result["sharpe"].value is None
    assert result["sharpe"].reason_code == "zero_volatility"
    assert result["sortino"].reason_code == "zero_downside_deviation"
    assert result["calmar"].reason_code == "zero_drawdown"


def test_missing_reference_rate_and_short_sample_are_explicit():
    assert risk([.01, -.01] * 20)["sharpe"].reason_code == "risk_free_rate_missing"
    result = risk([.01, -.01])
    assert result["volatility_annualized"].value is None
    assert result["volatility_annualized"].reason_code == "insufficient_observations"
    assert result["max_drawdown"].value == pytest.approx(-.01)


def test_gaps_prevent_path_metrics_and_reduce_statistical_coverage():
    observations = returns([-.01, .02] * 20)
    result = calculate_risk(observations[:10] + observations[11:], start_date=START, end_date=END)
    metrics = {m.metric_id: m for m in result.metrics}
    assert metrics["volatility_annualized"].status == "partial"
    assert metrics["volatility_annualized"].coverage_ratio == .975
    assert metrics["max_drawdown"].value is None
    assert metrics["max_drawdown"].reason_code == "incomplete_return_path"
    json.dumps(asdict(result), allow_nan=False, default=str)


def test_unrecovered_drawdown_duration_and_new_peaks():
    assert risk([.01] * 40)["drawdown_duration_days"].value == 0
    assert risk([-.1] + [0] * 39)["drawdown_duration_days"].value == 40
    result = risk([-.5, 1.0])
    assert result["drawdown_duration_days"].value == 2
    assert result["max_drawdown"].value == -.5


def test_multiday_and_duplicate_returns_are_rejected():
    observations = returns([.01] * 40)
    with pytest.raises(ValueError, match="calendar-day"):
        calculate_risk((replace(observations[1], previous_valuation_date=START),), start_date=START, end_date=END)
    with pytest.raises(ValueError, match="Duplicate"):
        calculate_risk(observations + (observations[0],), start_date=START, end_date=END)


def test_concentration_known_weights_and_sector_provenance():
    result = calculate_concentration([
        PositionExposure("a", 80, "core", "EUR", "ETF", "Technology", None),
        PositionExposure("b", 20, "satellite", "USD", "stock", "Energy", "issuer report"),
    ], as_of_date=END)
    assert result.hhi_by_dimension["asset_id"].value == pytest.approx(.68)
    assert result.hhi_by_dimension["sector"].value is None
    assert result.hhi_by_dimension["sector"].coverage_ratio == .2
    unknown = next(row for row in result.groups if row.dimension == "sector" and row.group == "unclassified")
    assert unknown.weight.value == .8
    assert unknown.weight.status == "partial"


def test_missing_valuation_never_means_zero():
    result = calculate_concentration([PositionExposure("a", 100), PositionExposure("b", None)], as_of_date=END)
    assert result.hhi_by_dimension["asset_id"].value == 1
    assert result.hhi_by_dimension["asset_id"].status == "partial"
    assert result.hhi_by_dimension["asset_id"].coverage_ratio == .5
    assert calculate_concentration([], as_of_date=END).hhi_by_dimension["asset_id"].value is None


def test_correlation_and_risk_shares_known_proportional_assets():
    x = [-.01, .02] * 20
    result = calculate_diversification(
        {"a": returns(x), "b": returns([2*v for v in x])}, {"a": .5, "b": .5},
        start_date=START, end_date=END,
    )
    assert all(row.metric.value == 1 for row in result.correlations)
    assert result.risk_contributions["a"].value == pytest.approx(1/3)
    assert result.risk_contributions["b"].value == pytest.approx(2/3)


def test_correlations_constant_and_missing_active_asset():
    result = calculate_diversification(
        {"a": returns([0] * 40)}, {"a": .5, "b": .5}, start_date=START, end_date=END,
    )
    assert result.correlations[0].metric.reason_code == "zero_return_variance"
    assert all(m.value is None for m in result.risk_contributions.values())
    assert result.risk_contributions["a"].coverage_ratio == 0


def test_diversification_gaps_and_partial_source_status():
    x = returns([-.01, .02] * 20)
    result = calculate_diversification(
        {"a": x, "b": x[1:]}, {"a": .5, "b": .5}, start_date=START, end_date=END,
    )
    assert result.risk_contributions["a"].coverage_ratio == .975
    assert result.risk_contributions["a"].status == "partial"


def test_catalog_has_complete_explanations_and_covers_risk_outputs():
    catalog = {row.metric_id: row for row in metric_catalog()}
    assert len(catalog) == len(metric_catalog())
    assert set(risk([.01] * 40)) <= set(catalog)
    assert {"asset_correlation", "risk_contribution_share", "concentration_hhi", "beta", "relative_return"} <= set(catalog)
    assert all(all(asdict(row).values()) for row in catalog.values())


@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), -1.01])
def test_invalid_returns_reduce_coverage_without_publishing_nan(invalid):
    observations = list(returns([-.01, .02] * 20))
    observations[0] = replace(observations[0], return_decimal=invalid)
    result = calculate_risk(observations, start_date=START, end_date=END)
    metrics = {row.metric_id: row for row in result.metrics}
    assert metrics["volatility_annualized"].observations == 39
    assert metrics["volatility_annualized"].coverage_ratio == .975
    assert metrics["max_drawdown"].value is None
    json.dumps(asdict(result), allow_nan=False, default=str)


def test_partial_sources_propagate_even_without_missing_dates():
    observations = list(returns([-.01, .02] * 20))
    observations[0] = replace(observations[0], status="partial", coverage_ratio=.5)
    result = calculate_risk(observations, start_date=START, end_date=END)
    assert all(row.status == "partial" for row in result.metrics if row.value is not None)
    result = calculate_diversification(
        {"a": observations}, {"a": 1}, start_date=START, end_date=END,
    )
    assert result.risk_contributions["a"].status == "partial"
    assert result.risk_contributions["a"].coverage_ratio == .9875


def test_negative_risk_contributions_and_zero_weight_missing_series():
    x = [-.01, .02] * 20
    result = calculate_diversification(
        {"a": returns(x), "b": returns([-v for v in x])},
        {"a": .75, "b": .25, "unused": 0}, start_date=START, end_date=END,
    )
    assert result.risk_contributions["a"].value == 1.5
    assert result.risk_contributions["b"].value == -.5
    assert result.risk_contributions["unused"].value == 0


def test_minimum_sample_boundary_and_invalid_inputs():
    assert risk([-.01, .02] * 15)["volatility_annualized"].status == "available"
    assert risk([-.01, .02] * 14 + [.01])["volatility_annualized"].value is None
    with pytest.raises(ValueError, match="end_date"):
        calculate_risk([], start_date=END, end_date=START)
    with pytest.raises(ValueError, match="risk_free_rate"):
        risk([.01] * 40, risk_free_rate_annual=float("nan"))
    with pytest.raises(ValueError, match="long-only"):
        calculate_concentration([PositionExposure("a", -1)], as_of_date=END)
    with pytest.raises(ValueError, match="sum to 1"):
        calculate_diversification({}, {"a": .5}, start_date=START, end_date=END)
