"""Application use cases for Streamlit/dashboard read models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import re
from typing import Any

import pandas as pd

from src.agents import build_portfolio_metrics_snapshot
from src.agents.pipeline import extract_monthly_report_as_of_date, prepare_agent_metrics_snapshot
from src.application.portfolio_targets import (
    DEFAULT_TARGET_WEIGHTS,
    ReadPortfolioTargetsUseCase,
)
from src.application.serialization import json_ready_value
from src.application.settings import ReadInvestmentBriefResult, ReadInvestmentBriefUseCase
from src.config import Settings, get_settings
from src.market_data import DuckDBMarketDataRepository
from src.portfolio import (
    PortfolioMetricsResult,
    calculate_portfolio_metrics_from_normalized_degiro,
    load_normalized_degiro_snapshots,
    load_normalized_degiro_transactions,
)
from src.portfolio.contributions import net_external_contributions_until
from src.portfolio.state_projection import build_broker_snapshot_projection
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
class GetPendingDegiroImportStatusResult:
    latest_incoming_portfolio_date: date | None
    latest_normalized_portfolio_date: date | None
    pending_portfolio_files: list[str]


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


class GetPendingDegiroImportStatusUseCase:
    name = "get_pending_degiro_import_status"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> GetPendingDegiroImportStatusResult:
        incoming_dir = self.settings.degiro_exports_dir / "incoming"
        normalized_dir = self.settings.normalized_data_dir / "degiro" / "portfolio_snapshots"
        incoming = _portfolio_export_dates(incoming_dir, suffix=".csv")
        normalized = _portfolio_export_dates(normalized_dir, suffix=".parquet")

        latest_normalized = max(normalized.values(), default=None)
        pending_files = [
            name
            for name, snapshot_date in sorted(incoming.items())
            if latest_normalized is None or snapshot_date > latest_normalized
        ]
        return GetPendingDegiroImportStatusResult(
            latest_incoming_portfolio_date=max(incoming.values(), default=None),
            latest_normalized_portfolio_date=latest_normalized,
            pending_portfolio_files=pending_files,
        )


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


class ReadTargetWeightsUseCase:
    name = "read_target_weights"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> ReadTargetWeightsResult:
        targets = ReadPortfolioTargetsUseCase(settings=self.settings).execute()
        return ReadTargetWeightsResult(
            target_weights=targets.target_weights or dict(DEFAULT_TARGET_WEIGHTS)
        )


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


def build_agent_dashboard_snapshot(
    metrics: PortfolioMetricsResult,
    *,
    snapshots: pd.DataFrame,
    as_of_date: date,
) -> dict[str, Any]:
    base_snapshot = build_portfolio_metrics_snapshot(metrics, as_of_date=as_of_date)
    broker = build_broker_snapshot_projection(
        snapshots,
        position_metrics=metrics.position_metrics,
        portfolio_daily_metrics=metrics.portfolio_daily_metrics,
        as_of_date=as_of_date,
    )
    if broker is None:
        return base_snapshot

    positions = broker["positions"]
    total_value = float(broker["total_market_value_base"])
    total_unrealized = broker["total_unrealized_pnl_base"]
    total_return = broker["portfolio_return_pct"]

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
        "daily": json_ready_value(daily_payload),
        "positions": json_ready_value(positions_ready.to_dict(orient="records")),
    }


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


def _portfolio_export_dates(directory: Path, *, suffix: str) -> dict[str, date]:
    if not directory.exists():
        return {}

    dates: dict[str, date] = {}
    for path in sorted(directory.glob(f"portfolio_*{suffix}")):
        match = re.fullmatch(r"portfolio_(\d{4}-\d{2}-\d{2})" + re.escape(suffix), path.name)
        if match is None:
            continue
        try:
            dates[path.name] = date.fromisoformat(match.group(1))
        except ValueError:
            continue
    return dates


__all__ = [
    "BuildAgentDashboardSnapshotRequest",
    "BuildAgentDashboardSnapshotResult",
    "BuildAgentDashboardSnapshotUseCase",
    "GetNetExternalContributionsRequest",
    "GetNetExternalContributionsResult",
    "GetNetExternalContributionsUseCase",
    "GetPendingDegiroImportStatusResult",
    "GetPendingDegiroImportStatusUseCase",
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
