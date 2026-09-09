"""Compact analytics contract, validation and deterministic interpretation signals."""

from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
from math import isfinite
from typing import Any, Mapping

from src.portfolio.data_quality import DataQualityIssue


ANALYTICS_INPUT_KEY = "portfolio_analytics_snapshot"
MAX_SNAPSHOT_BYTES = 65536
STATISTICAL_METRICS = frozenset({
    "volatility_annualized", "downside_volatility_annualized", "sharpe", "sortino", "calmar",
    "asset_correlation", "risk_contribution_share", "correlation", "beta", "alpha_annualized", "tracking_error_annualized",
})


def analytics_quality_issues(snapshot: Mapping[str, Any], *, as_of_date: date, base_currency: str) -> tuple[DataQualityIssue, ...]:
    """Reject invalid contracts; incomplete but honestly labelled data is a warning."""
    def issue(code, severity="error"):
        return DataQualityIssue(code, severity, code.replace("_", " "))

    try:
        _check_json_tree(snapshot)
        encoded = json.dumps(snapshot, allow_nan=False, sort_keys=True)
        if len(encoded.encode("utf-8")) > MAX_SNAPSHOT_BYTES:
            return (issue("analytics_snapshot_too_large"),)
        if type(snapshot.get("schema_version")) is not int or snapshot.get("schema_version") != 1 or snapshot.get("status") not in {"available", "partial", "unavailable"}:
            return (issue("analytics_invalid_contract"),)
        period = snapshot["period"]
        if snapshot["as_of_date"] != as_of_date.isoformat() or period["end_date"] != as_of_date.isoformat():
            return (issue("analytics_date_mismatch"),)
        if snapshot["base_currency"] != base_currency:
            return (issue("analytics_currency_mismatch"),)
        start = date.fromisoformat(period["actual_start"]) if period.get("actual_start") else None
        if start is not None and start > as_of_date:
            return (issue("analytics_invalid_period"),)
        if not isinstance(snapshot["portfolio"], dict) or not isinstance(snapshot["assets"], list) or not isinstance(snapshot["correlations"], list):
            return (issue("analytics_invalid_contract"),)
        if not isinstance(snapshot["warnings"], list) or not all(isinstance(item, str) for item in snapshot["warnings"]):
            return (issue("analytics_invalid_contract"),)
        issues = []
        for metric in _metrics(snapshot):
            status, value = metric["status"], metric["value"]
            count, coverage = metric["observations"], metric["coverage_ratio"]
            if status not in {"available", "partial", "unavailable"} or not isinstance(metric["reason_code"], str):
                raise ValueError("Invalid metric status")
            if type(count) is not int or count < 0 or type(coverage) not in (int, float) or not 0 <= coverage <= 1:
                raise ValueError("Invalid sample/coverage")
            if (status == "unavailable") != (value is None):
                raise ValueError("Invalid metric availability")
            if value is not None and (type(value) not in (int, float) or not isfinite(value)):
                raise ValueError("Invalid metric number")
            if status == "available" and coverage == 0:
                raise ValueError("A metric cannot be available without coverage")
            if metric.get("period_end") and date.fromisoformat(metric["period_end"]) > as_of_date:
                return (issue("analytics_date_mismatch"),)
            if metric.get("period_start") and start and date.fromisoformat(metric["period_start"]) < start:
                return (issue("analytics_date_mismatch"),)
            if metric["metric_id"] in STATISTICAL_METRICS and count < 30:
                if value is not None:
                    return (issue("analytics_insufficient_sample_value"),)
                issues.append(issue("analytics_insufficient_sample", "warning"))
            if coverage < .8 or status != "available":
                issues.append(issue("analytics_partial_coverage", "warning"))
        if snapshot["status"] != "available" or snapshot.get("warnings"):
            issues.append(issue("analytics_" + snapshot["status"], "warning"))
        return tuple({item.code: item for item in issues}.values())
    except (TypeError, ValueError, KeyError, OverflowError, RecursionError):
        return (issue("analytics_invalid_contract"),)


def _check_json_tree(value, depth=0):
    if depth > 30:
        raise ValueError("Analytics tree is too deep")
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("Analytics keys must be strings")
            _check_json_tree(item, depth + 1)
    elif type(value) is list:
        for item in value:
            _check_json_tree(item, depth + 1)
    elif value is not None and type(value) not in (str, bool, int, float):
        raise ValueError("Analytics values must be JSON primitives")


def _metrics(value):
    if isinstance(value, dict):
        if "metric_id" in value:
            yield value
        else:
            for item in value.values():
                yield from _metrics(item)
    elif isinstance(value, list):
        for item in value:
            yield from _metrics(item)


def analytics_for_agent(snapshot: Mapping[str, Any], agent_name: str) -> dict[str, Any]:
    """Send only relevant sections; monitor has no analytics input at all."""
    if agent_name == "monitor_tematico":
        return {}
    result = deepcopy(dict(snapshot))
    if agent_name == "asistente_aportacion_mensual":
        result.pop("assets", None)
        result.pop("correlations", None)
    elif agent_name == "analista_activos":
        result["portfolio"].pop("performance", None)
        result["portfolio"].pop("target_deviations", None)
    return result


def context_analytics(context) -> dict[str, Any]:
    if not context.has_input(ANALYTICS_INPUT_KEY):
        return {}
    return dict(context.get_input(ANALYTICS_INPUT_KEY).metadata["snapshot"])


def analytics_reason_codes(snapshot: Mapping[str, Any] | None, *, asset_id: str | None = None) -> tuple[str, ...]:
    """Interpret precomputed values, not financial formulas or trading signals."""
    if not snapshot:
        return ()
    reasons = ["analytics_" + str(snapshot["status"])]
    portfolio = snapshot.get("portfolio", {})
    metrics = portfolio.get("risk", {})
    if asset_id is not None:
        asset = next((item for item in snapshot.get("assets", []) if item["asset_id"] == asset_id), None)
        if asset is None:
            return (*reasons, "analytics_asset_unavailable")
        metrics = asset.get("metrics", {})
    drawdown = metrics.get("max_drawdown", {})
    if drawdown.get("value") is not None and drawdown.get("coverage_ratio", 0) >= .8:
        reasons.append("analytics_drawdown_observed" if drawdown["value"] < 0 else "analytics_no_drawdown_observed")
    if any(item.get("deviation", 0) > 0 for item in portfolio.get("target_deviations", []) if item.get("deviation") is not None):
        reasons.append("analytics_above_target")
    twr = portfolio.get("performance", {}).get("twr") or {}
    if snapshot["status"] == "unavailable" or twr.get("status") == "unavailable" or twr.get("coverage_ratio", 1) < .8:
        reasons.append("analytics_manual_review_required")
    return tuple(reasons)
