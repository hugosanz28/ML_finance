"""Build a bounded agent input through the public analytics application boundary."""

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from src.application.analytics import AnalyticsRequest, GetAnalyticsSummaryUseCase
from src.application.portfolio_targets import ReadPortfolioTargetsUseCase
from src.analytics.portfolio import target_deviations
from src.config import Settings, get_settings


METRIC_FIELDS = ("metric_id", "value", "unit", "period_start", "period_end", "observations", "coverage_ratio", "status", "reason_code")


@dataclass(frozen=True)
class BuildPortfolioAnalyticsSnapshotRequest:
    as_of_date: str


@dataclass(frozen=True)
class BuildPortfolioAnalyticsSnapshotResult:
    snapshot: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self.snapshot)


class BuildPortfolioAnalyticsSnapshotUseCase:
    name = "build_portfolio_analytics_snapshot"

    def __init__(self, *, settings: Settings | None = None):
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: BuildPortfolioAnalyticsSnapshotRequest) -> BuildPortfolioAnalyticsSnapshotResult:
        summary = GetAnalyticsSummaryUseCase(settings=self.settings).execute(AnalyticsRequest(
            as_of_date=request.as_of_date, period="last_year",
        ))
        data = summary.data
        performance = data.get("performance", {}).get("period", {})
        risk = data.get("risk", {})
        positions = risk.get("positions", {})
        concentration = positions.get("concentration", {})
        diversification = positions.get("diversification") or {}
        weights = {row["group"]: row["weight"].get("value") or 0 for row in concentration.get("groups", []) if row["dimension"] == "asset_id"}
        all_assets = positions.get("asset_risk", {})
        selected_ids = sorted(all_assets, key=lambda key: (-weights.get(key, 0), key))[:12]
        assets = [{"asset_id": key, "metrics": _metric_map(all_assets[key].get("metrics", [])),
                   "risk_contribution": _metric(diversification.get("risk_contributions", {}).get(key))} for key in selected_ids]
        correlations = [row for row in diversification.get("correlations", [])
                        if row["left_asset_id"] < row["right_asset_id"] and row["left_asset_id"] in selected_ids and row["right_asset_id"] in selected_ids]
        correlations.sort(key=lambda row: (-abs(row["metric"].get("value") or 0), row["left_asset_id"], row["right_asset_id"]))
        comparisons = (data.get("benchmarks", {}).get("comparison") or {}).get("comparisons", [])
        benchmarks = [{key: item[key] for key in (
            "benchmark_id", "role", "series_kind", "source_currency", "base_currency", "provider_name", "source_reference",
            "period_start", "period_end", "observations", "coverage_ratio", "status", "reason_codes",
        )} | {"metrics": _metric_map(item["metrics"])} for item in comparisons[:2]]
        targets = ReadPortfolioTargetsUseCase(settings=self.settings).execute()
        target_weights = (targets.portfolio_targets or {}).get("target_allocation", {})
        groups = [{"dimension": row["dimension"], "group": row["group"], "weight": _metric(row["weight"])}
                  for row in concentration.get("groups", [])]
        # Preserve aggregate dimensions before individual holdings in a bounded monthly view.
        dimension_order = {name: index for index, name in enumerate(("bucket", "currency", "asset_type", "sector", "asset_id"))}
        groups.sort(key=lambda row: (dimension_order.get(row["dimension"], 99), -(row["weight"].get("value") or 0), row["group"]))
        omitted = {"assets": max(0, len(all_assets) - len(assets)), "correlations": max(0, len(correlations) - 24),
                   "benchmarks": max(0, len(comparisons) - len(benchmarks)), "groups": max(0, len(groups) - 48)}
        warnings = list(summary.warnings)
        if any(omitted.values()):
            warnings.append("analytics_snapshot_truncated")
        period = dict(summary.period)
        if period.get("end_date") not in (None, request.as_of_date):
            raise ValueError("Analytics summary cutoff differs from requested cutoff")
        # An unavailable history still records the requested cutoff, not a invented start.
        period["end_date"] = request.as_of_date
        return BuildPortfolioAnalyticsSnapshotResult({
            "schema_version": 1, "as_of_date": request.as_of_date, "base_currency": summary.base_currency,
            "period": period, "status": "partial" if any(omitted.values()) and summary.status == "available" else summary.status,
            "warnings": warnings, "omitted": omitted, "asset_series_kind": positions.get("series_kind"),
            "portfolio": {"performance": {key: _metric(performance.get(key)) for key in ("twr", "mwr")},
                          "risk": _metric_map(risk.get("portfolio", {}).get("metrics", [])),
                          "concentration": {"groups": groups[:48], "hhi": {key: _metric(value) for key, value in concentration.get("hhi_by_dimension", {}).items()}},
                          "benchmarks": benchmarks,
                          "target_deviations": target_deviations(groups, target_weights)},
            "assets": assets,
            "correlations": [{"left_asset_id": row["left_asset_id"], "right_asset_id": row["right_asset_id"], "metric": _metric(row["metric"])} for row in correlations[:24]],
        })


def _metric(value):
    return {key: value[key] for key in METRIC_FIELDS if key in value} if value else None


def _metric_map(values):
    return {item["metric_id"]: _metric(item) for item in values}
