"""Application use cases for Streamlit/dashboard read models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import re
from typing import Any

import pandas as pd

from src.agents import load_investment_brief
from src.agents import build_portfolio_metrics_snapshot
from src.agents.pipeline import extract_monthly_report_as_of_date, prepare_agent_metrics_snapshot
from src.config import Settings, get_settings
from src.market_data import DuckDBMarketDataRepository
from src.portfolio import (
    PortfolioMetricsResult,
    calculate_portfolio_metrics_from_normalized_degiro,
    load_normalized_degiro_snapshots,
    load_normalized_degiro_transactions,
    load_portfolio_targets,
)
from src.reports import get_latest_monthly_report


@dataclass(frozen=True)
class LoadPortfolioMetricsRequest:
    persist: bool = True


@dataclass(frozen=True)
class LoadPortfolioMetricsResult:
    metrics: PortfolioMetricsResult


@dataclass(frozen=True)
class LoadPortfolioSnapshotsResult:
    snapshots: pd.DataFrame


@dataclass(frozen=True)
class LoadPortfolioTransactionsResult:
    transactions: pd.DataFrame


@dataclass(frozen=True)
class GetWarehouseCountsResult:
    counts: dict[str, int]


@dataclass(frozen=True)
class GetNetExternalContributionsRequest:
    as_of_date: date


@dataclass(frozen=True)
class GetNetExternalContributionsResult:
    net_external: float | None


@dataclass(frozen=True)
class ListDashboardReportsResult:
    reports: list[dict[str, Any]]


@dataclass(frozen=True)
class ReadInvestmentBriefResult:
    content: str


@dataclass(frozen=True)
class ReadTargetWeightsResult:
    target_weights: dict[str, Any]


@dataclass(frozen=True)
class BuildAgentDashboardSnapshotRequest:
    metrics: PortfolioMetricsResult
    snapshots: pd.DataFrame
    as_of_date: date


@dataclass(frozen=True)
class BuildAgentDashboardSnapshotResult:
    snapshot: dict[str, Any]


class LoadPortfolioMetricsUseCase:
    name = "load_portfolio_metrics"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: LoadPortfolioMetricsRequest | None = None) -> LoadPortfolioMetricsResult:
        resolved_request = request or LoadPortfolioMetricsRequest()
        return LoadPortfolioMetricsResult(
            metrics=calculate_portfolio_metrics_from_normalized_degiro(
                settings=self.settings,
                persist=resolved_request.persist,
            )
        )


class LoadPortfolioSnapshotsUseCase:
    name = "load_portfolio_snapshots"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> LoadPortfolioSnapshotsResult:
        return LoadPortfolioSnapshotsResult(snapshots=load_normalized_degiro_snapshots(settings=self.settings))


class LoadPortfolioTransactionsUseCase:
    name = "load_portfolio_transactions"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> LoadPortfolioTransactionsResult:
        return LoadPortfolioTransactionsResult(transactions=load_normalized_degiro_transactions(settings=self.settings))


class GetWarehouseCountsUseCase:
    name = "get_warehouse_counts"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> GetWarehouseCountsResult:
        repository = DuckDBMarketDataRepository(settings=self.settings)
        tables = ("assets_master", "transactions", "cash_movements", "portfolio_snapshots", "prices_daily", "fx_rates")
        counts: dict[str, int] = {}
        with repository.connection() as connection:
            for table in tables:
                counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return GetWarehouseCountsResult(counts=counts)


class GetNetExternalContributionsUseCase:
    name = "get_net_external_contributions"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: GetNetExternalContributionsRequest) -> GetNetExternalContributionsResult:
        return GetNetExternalContributionsResult(
            net_external=net_external_contributions_until(self.settings, as_of_date=request.as_of_date)
        )


class ListDashboardReportsUseCase:
    name = "list_dashboard_reports"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> ListDashboardReportsResult:
        return ListDashboardReportsResult(reports=list_dashboard_reports(self.settings))


class ReadInvestmentBriefUseCase:
    name = "read_investment_brief"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> ReadInvestmentBriefResult:
        try:
            content = load_investment_brief(settings=self.settings)
        except FileNotFoundError:
            content = ""
        return ReadInvestmentBriefResult(content=content)


class ReadTargetWeightsUseCase:
    name = "read_target_weights"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> ReadTargetWeightsResult:
        try:
            targets = load_portfolio_targets(settings=self.settings)
        except ValueError:
            return ReadTargetWeightsResult(target_weights={"core": 0.80, "satellite": 0.20})
        if targets is None:
            return ReadTargetWeightsResult(target_weights={"core": 0.80, "satellite": 0.20})
        return ReadTargetWeightsResult(target_weights=targets.target_weights())


class BuildAgentDashboardSnapshotUseCase:
    name = "build_agent_dashboard_snapshot"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: BuildAgentDashboardSnapshotRequest) -> BuildAgentDashboardSnapshotResult:
        snapshot = build_agent_dashboard_snapshot(
            request.metrics,
            snapshots=request.snapshots,
            as_of_date=request.as_of_date,
        )
        return BuildAgentDashboardSnapshotResult(
            snapshot=prepare_agent_metrics_snapshot(snapshot, settings=self.settings)
        )


def net_external_contributions_until(settings: Settings, *, as_of_date: date) -> float | None:
    repository = DuckDBMarketDataRepository(settings=settings)
    query = """
        SELECT SUM(
            CASE
                WHEN UPPER(movement_type) = 'DEPOSIT' THEN ABS(amount_base)
                WHEN UPPER(movement_type) = 'WITHDRAWAL' THEN -ABS(amount_base)
                ELSE 0
            END
        ) AS net_external
        FROM cash_movements
        WHERE amount_base IS NOT NULL
          AND UPPER(movement_type) IN ('DEPOSIT', 'WITHDRAWAL')
          AND COALESCE(value_date, movement_date) <= ?
    """
    with repository.connection() as connection:
        row = connection.execute(query, [as_of_date]).fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def build_agent_dashboard_snapshot(
    metrics: PortfolioMetricsResult,
    *,
    snapshots: pd.DataFrame,
    as_of_date: date,
) -> dict[str, Any]:
    base_snapshot = build_portfolio_metrics_snapshot(metrics, as_of_date=as_of_date)
    broker = _broker_snapshot_view_for_date(snapshots, as_of_date=as_of_date)
    if broker is None:
        return base_snapshot

    positions = _overlay_external_cost_metrics(
        broker["positions"],
        metrics,
        target_date=broker["snapshot_date"],
    )
    total_value = float(broker["total_market_value_base"])
    total_unrealized, total_return = _derive_broker_pnl_with_external_cost_basis(
        _daily_metrics(metrics),
        target_date=broker["snapshot_date"],
        total_market_value_base=total_value,
    )
    if total_unrealized is None or total_return is None:
        total_unrealized, total_return = _derive_totals_from_positions(
            positions,
            total_market_value_base=total_value,
        )

    daily_payload = dict(base_snapshot.get("daily") or {})
    daily_payload["valuation_date"] = broker["snapshot_date"].isoformat()
    daily_payload["total_market_value_base"] = round(total_value, 8)
    if total_unrealized is not None:
        daily_payload["total_unrealized_pnl_base"] = round(float(total_unrealized), 8)
    if total_return is not None:
        daily_payload["portfolio_return_pct"] = round(float(total_return), 8)

    selected_columns = [
        "asset_id",
        "asset_name",
        "asset_type",
        "isin",
        "quantity",
        "market_value_base",
        "cost_basis_base",
        "unrealized_pnl_base",
        "unrealized_return_pct",
        "weight",
        "valuation_status",
    ]
    for column in selected_columns:
        if column not in positions.columns:
            positions[column] = pd.NA
    positions_ready = positions.loc[:, selected_columns].sort_values(["weight", "asset_name"], ascending=[False, True])

    return {
        "as_of_date": broker["snapshot_date"].isoformat(),
        "base_currency": metrics.base_currency,
        "daily": _json_ready_value(daily_payload),
        "positions": _json_ready_value(positions_ready.to_dict(orient="records")),
    }


def _daily_metrics(metrics: PortfolioMetricsResult) -> pd.DataFrame:
    frame = metrics.portfolio_daily_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"]).dt.date
    return frame.sort_values("valuation_date")


def _latest_broker_snapshot_view(snapshots: pd.DataFrame) -> dict[str, Any] | None:
    if snapshots is None or snapshots.empty:
        return None
    frame = snapshots.copy()
    frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"], errors="coerce").dt.date
    frame["market_value_base"] = pd.to_numeric(frame["market_value_base"], errors="coerce")
    frame["unrealized_pnl_base"] = pd.to_numeric(frame.get("unrealized_pnl_base"), errors="coerce")
    frame["quantity"] = pd.to_numeric(frame.get("quantity"), errors="coerce")
    frame["asset_name"] = frame["asset_name"].fillna(frame["asset_id"]).astype("string")
    frame["asset_type"] = frame["asset_type"].fillna("unknown").astype("string")
    frame = frame.dropna(subset=["snapshot_date", "market_value_base"])
    if frame.empty:
        return None

    latest_date = max(frame["snapshot_date"])
    latest = frame.loc[frame["snapshot_date"] == latest_date].copy()
    total_value = float(latest["market_value_base"].sum())
    has_snapshot_unrealized = latest["unrealized_pnl_base"].notna().any()
    if has_snapshot_unrealized:
        total_unrealized = float(latest["unrealized_pnl_base"].fillna(0.0).sum())
        total_cost = total_value - total_unrealized
        portfolio_return_pct = None if abs(total_cost) < 1e-9 else total_unrealized / total_cost
    else:
        total_unrealized = None
        portfolio_return_pct = None

    latest["weight"] = 0.0 if abs(total_value) < 1e-9 else latest["market_value_base"] / total_value
    latest["cost_basis_base"] = pd.NA
    rows_with_unrealized = latest["unrealized_pnl_base"].notna()
    latest.loc[rows_with_unrealized, "cost_basis_base"] = (
        latest.loc[rows_with_unrealized, "market_value_base"] - latest.loc[rows_with_unrealized, "unrealized_pnl_base"]
    )
    latest["unrealized_return_pct"] = pd.to_numeric(
        latest["unrealized_pnl_base"] / pd.to_numeric(latest["cost_basis_base"], errors="coerce").replace(0, pd.NA),
        errors="coerce",
    )
    latest["valuation_status"] = "broker_snapshot"
    positions = latest.loc[
        :,
        [
            "asset_id",
            "asset_name",
            "asset_type",
            "quantity",
            "market_value_base",
            "weight",
            "cost_basis_base",
            "unrealized_pnl_base",
            "unrealized_return_pct",
            "valuation_status",
        ],
    ].copy()

    return {
        "snapshot_date": latest_date,
        "positions": positions,
        "total_market_value_base": total_value,
        "total_unrealized_pnl_base": total_unrealized,
        "portfolio_return_pct": portfolio_return_pct,
    }


def _broker_snapshot_view_for_date(snapshots: pd.DataFrame, *, as_of_date: date) -> dict[str, Any] | None:
    if snapshots is None or snapshots.empty:
        return None
    frame = snapshots.copy()
    frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["snapshot_date"])
    eligible = frame.loc[frame["snapshot_date"] <= as_of_date].copy()
    if eligible.empty:
        return None
    return _latest_broker_snapshot_view(eligible)


def _external_positions_for_date(metrics: PortfolioMetricsResult, *, target_date: date) -> pd.DataFrame:
    frame = metrics.position_metrics.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"], errors="coerce").dt.date
    frame["cost_basis_base"] = pd.to_numeric(frame["cost_basis_base"], errors="coerce")
    frame["unrealized_pnl_base"] = pd.to_numeric(frame["unrealized_pnl_base"], errors="coerce")
    frame["unrealized_return_pct"] = pd.to_numeric(frame["unrealized_return_pct"], errors="coerce")
    frame = frame.dropna(subset=["valuation_date", "asset_id"])
    if frame.empty:
        return pd.DataFrame(columns=["asset_id", "cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"])
    dates = sorted(frame["valuation_date"].dropna().unique().tolist())
    if not dates:
        return pd.DataFrame(columns=["asset_id", "cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"])
    chosen_date = target_date if target_date in set(dates) else max([date_value for date_value in dates if date_value <= target_date], default=None)
    if chosen_date is None:
        return pd.DataFrame(columns=["asset_id", "cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"])
    return frame.loc[frame["valuation_date"] == chosen_date, ["asset_id", "cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"]].copy()


def _overlay_external_cost_metrics(
    broker_positions: pd.DataFrame,
    metrics: PortfolioMetricsResult,
    *,
    target_date: date,
) -> pd.DataFrame:
    if broker_positions.empty:
        return broker_positions
    enriched = broker_positions.copy()
    for column in ("cost_basis_base", "unrealized_pnl_base", "unrealized_return_pct"):
        if column not in enriched.columns:
            enriched[column] = pd.NA
    external = _external_positions_for_date(metrics, target_date=target_date)
    if external.empty:
        return enriched
    merged = enriched.merge(
        external.rename(
            columns={
                "cost_basis_base": "cost_basis_external",
                "unrealized_pnl_base": "unrealized_external",
                "unrealized_return_pct": "return_external",
            }
        ),
        on="asset_id",
        how="left",
    )
    merged["cost_basis_base"] = pd.to_numeric(merged["cost_basis_base"], errors="coerce").fillna(pd.to_numeric(merged["cost_basis_external"], errors="coerce"))
    merged["unrealized_pnl_base"] = pd.to_numeric(merged["unrealized_pnl_base"], errors="coerce").fillna(pd.to_numeric(merged["unrealized_external"], errors="coerce"))
    merged["unrealized_return_pct"] = pd.to_numeric(merged["unrealized_return_pct"], errors="coerce").fillna(pd.to_numeric(merged["return_external"], errors="coerce"))
    return merged.drop(columns=["cost_basis_external", "unrealized_external", "return_external"])


def _derive_totals_from_positions(positions: pd.DataFrame, *, total_market_value_base: float) -> tuple[float | None, float | None]:
    if positions.empty:
        return None, None
    cost = pd.to_numeric(positions.get("cost_basis_base"), errors="coerce")
    if cost is None or cost.notna().sum() == 0:
        return None, None
    total_cost = float(cost.fillna(0.0).sum())
    total_unrealized = float(total_market_value_base - total_cost)
    total_return = None if abs(total_cost) < 1e-9 else total_unrealized / total_cost
    return total_unrealized, total_return


def _derive_broker_pnl_with_external_cost_basis(
    daily: pd.DataFrame,
    *,
    target_date: date,
    total_market_value_base: float,
) -> tuple[float | None, float | None]:
    frame = daily.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"], errors="coerce").dt.date
    frame["total_cost_basis_base"] = pd.to_numeric(frame["total_cost_basis_base"], errors="coerce")
    frame = frame.dropna(subset=["valuation_date"]).sort_values("valuation_date")
    if frame.empty:
        return None, None
    if target_date in set(frame["valuation_date"].tolist()):
        row = frame.loc[frame["valuation_date"] == target_date].iloc[-1]
    else:
        candidates = frame.loc[frame["valuation_date"] <= target_date]
        if candidates.empty:
            return None, None
        row = candidates.iloc[-1]
    total_cost = float(row["total_cost_basis_base"]) if pd.notna(row["total_cost_basis_base"]) else None
    if total_cost is None or abs(total_cost) < 1e-9:
        return None, None
    total_unrealized = float(total_market_value_base - total_cost)
    total_return = total_unrealized / total_cost
    return total_unrealized, total_return


def _json_ready_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_ready_value(item) for item in value]
    if isinstance(value, (date, pd.Timestamp)):
        return value.isoformat()
    if value is None or pd.isna(value):
        return None
    return value


def list_dashboard_reports(settings: Settings) -> list[dict[str, Any]]:
    by_path: dict[Path, dict[str, Any]] = {}
    latest = get_latest_monthly_report(settings=settings)
    if latest is not None:
        path = Path(latest.report_path).expanduser().resolve()
        if path.exists():
            by_path[path] = {
                "label": "history latest",
                "path": path,
                "as_of_date": latest.as_of_date,
                "source": "history",
            }

    if settings.reports_dir.exists():
        for path in sorted(settings.reports_dir.glob("*.md"), reverse=True):
            resolved = path.resolve()
            as_of_date = extract_report_as_of_date_from_path(resolved)
            if resolved in by_path:
                if by_path[resolved].get("as_of_date") is None:
                    by_path[resolved]["as_of_date"] = as_of_date
                continue
            by_path[resolved] = {
                "label": "file",
                "path": resolved,
                "as_of_date": as_of_date,
                "source": "reports_dir",
            }

    reports = list(by_path.values())
    reports.sort(
        key=lambda item: (
            item.get("as_of_date") or date.min,
            item["path"].stat().st_mtime if item.get("path") and item["path"].exists() else 0.0,
        ),
        reverse=True,
    )
    for item in reports:
        as_of = item.get("as_of_date")
        as_of_label = as_of.isoformat() if isinstance(as_of, date) else "sin_fecha"
        item["label"] = f"{as_of_label} | {item.get('source', 'file')}"
    return reports


def extract_report_as_of_date_from_path(path: Path) -> date | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", path.stem)
    if match is None:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


__all__ = [
    "BuildAgentDashboardSnapshotRequest",
    "BuildAgentDashboardSnapshotResult",
    "BuildAgentDashboardSnapshotUseCase",
    "GetNetExternalContributionsRequest",
    "GetNetExternalContributionsResult",
    "GetNetExternalContributionsUseCase",
    "GetWarehouseCountsResult",
    "GetWarehouseCountsUseCase",
    "ListDashboardReportsResult",
    "ListDashboardReportsUseCase",
    "LoadPortfolioMetricsRequest",
    "LoadPortfolioMetricsResult",
    "LoadPortfolioMetricsUseCase",
    "LoadPortfolioSnapshotsResult",
    "LoadPortfolioSnapshotsUseCase",
    "LoadPortfolioTransactionsResult",
    "LoadPortfolioTransactionsUseCase",
    "ReadInvestmentBriefResult",
    "ReadInvestmentBriefUseCase",
    "ReadTargetWeightsResult",
    "ReadTargetWeightsUseCase",
    "extract_report_as_of_date_from_path",
    "extract_monthly_report_as_of_date",
    "list_dashboard_reports",
    "net_external_contributions_until",
]
