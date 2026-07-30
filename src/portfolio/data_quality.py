"""Portfolio data quality checks used before reporting and agent runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal, Mapping

import pandas as pd

from src.portfolio.metrics import PortfolioMetricsResult


QualitySeverity = Literal["info", "warning", "error"]


@dataclass(frozen=True)
class DataQualityIssue:
    """One deterministic quality finding."""

    code: str
    severity: QualitySeverity
    message: str
    details: Mapping[str, Any] | None = None

    @property
    def blocks_agents(self) -> bool:
        return self.severity == "error"


@dataclass(frozen=True)
class DataQualityReport:
    """Aggregated quality report for a portfolio state."""

    as_of_date: date | None
    issues: tuple[DataQualityIssue, ...]

    @property
    def error_count(self) -> int:
        return sum(issue.severity == "error" for issue in self.issues)

    @property
    def warning_count(self) -> int:
        return sum(issue.severity == "warning" for issue in self.issues)

    @property
    def info_count(self) -> int:
        return sum(issue.severity == "info" for issue in self.issues)

    @property
    def can_run_agents(self) -> bool:
        return self.error_count == 0


def check_portfolio_metrics_quality(
    metrics: PortfolioMetricsResult,
    *,
    expected_as_of_date: date | None = None,
    min_valuation_coverage_ratio: float = 1.0,
    min_return_coverage_ratio: float = 0.8,
) -> DataQualityReport:
    """Check portfolio metrics for conditions that can invalidate agent inputs."""
    issues: list[DataQualityIssue] = []
    if metrics.portfolio_daily_metrics.empty:
        return DataQualityReport(
            as_of_date=None,
            issues=(
                DataQualityIssue(
                    code="portfolio_daily_metrics_empty",
                    severity="error",
                    message="No portfolio daily metrics are available.",
                ),
            ),
        )

    daily = metrics.portfolio_daily_metrics.copy()
    daily["valuation_date"] = pd.to_datetime(daily["valuation_date"], errors="coerce").dt.date
    daily = daily.dropna(subset=["valuation_date"]).sort_values("valuation_date")
    if daily.empty:
        return DataQualityReport(
            as_of_date=None,
            issues=(
                DataQualityIssue(
                    code="portfolio_daily_metrics_missing_dates",
                    severity="error",
                    message="Portfolio daily metrics do not contain valid valuation dates.",
                ),
            ),
        )

    latest = daily.iloc[-1]
    as_of_date = latest["valuation_date"]
    if expected_as_of_date is not None and as_of_date != expected_as_of_date:
        issues.append(
            DataQualityIssue(
                code="metrics_date_mismatch",
                severity="error",
                message=(
                    "Portfolio metrics are not aligned with the expected date: "
                    f"metrics={as_of_date.isoformat()} expected={expected_as_of_date.isoformat()}."
                ),
                details={"metrics_date": as_of_date.isoformat(), "expected_date": expected_as_of_date.isoformat()},
            )
        )

    total_positions = _int_value(latest.get("total_positions_count"))
    valued_positions = _int_value(latest.get("valued_positions_count"))
    missing_price = _int_value(latest.get("missing_price_positions_count"))
    missing_fx = _int_value(latest.get("missing_fx_positions_count"))
    valuation_coverage = _float_value(latest.get("valuation_coverage_ratio"))
    return_coverage = _float_value(latest.get("return_coverage_ratio"))
    total_value = _float_value(latest.get("total_market_value_base"))

    if total_positions <= 0:
        issues.append(
            DataQualityIssue(
                code="no_open_positions",
                severity="error",
                message="No open positions are available on the latest valuation date.",
                details={"as_of_date": as_of_date.isoformat()},
            )
        )
    if valued_positions <= 0:
        issues.append(
            DataQualityIssue(
                code="no_valued_positions",
                severity="error",
                message="No positions have a usable valuation on the latest valuation date.",
                details={"as_of_date": as_of_date.isoformat()},
            )
        )
    if missing_price > 0:
        issues.append(
            DataQualityIssue(
                code="missing_prices",
                severity="error",
                message=f"Missing prices for {missing_price} position(s).",
                details={"missing_price_positions_count": missing_price},
            )
        )
    if missing_fx > 0:
        issues.append(
            DataQualityIssue(
                code="missing_fx",
                severity="error",
                message=f"Missing FX rates for {missing_fx} position(s).",
                details={"missing_fx_positions_count": missing_fx},
            )
        )
    if valuation_coverage < min_valuation_coverage_ratio:
        issues.append(
            DataQualityIssue(
                code="valuation_coverage_below_threshold",
                severity="error",
                message=(
                    "Valuation coverage is below threshold: "
                    f"{valuation_coverage:.2%} < {min_valuation_coverage_ratio:.2%}."
                ),
                details={"valuation_coverage_ratio": valuation_coverage},
            )
        )
    if return_coverage < min_return_coverage_ratio:
        issues.append(
            DataQualityIssue(
                code="return_coverage_below_threshold",
                severity="warning",
                message=(
                    "Return coverage is below threshold: "
                    f"{return_coverage:.2%} < {min_return_coverage_ratio:.2%}."
                ),
                details={"return_coverage_ratio": return_coverage},
            )
        )
    if total_value <= 0:
        issues.append(
            DataQualityIssue(
                code="non_positive_portfolio_value",
                severity="error",
                message="Portfolio market value is zero or negative.",
                details={"total_market_value_base": total_value},
            )
        )

    if not issues:
        issues.append(
            DataQualityIssue(
                code="portfolio_metrics_ready",
                severity="info",
                message="Portfolio metrics passed agent input quality checks.",
                details={"as_of_date": as_of_date.isoformat()},
            )
        )

    return DataQualityReport(as_of_date=as_of_date, issues=tuple(issues))


def check_agent_input_quality(
    *,
    metrics: PortfolioMetricsResult,
    monthly_report_date: date | None = None,
    require_monthly_report_date: bool = False,
    portfolio_metrics_snapshot: Mapping[str, Any] | None = None,
    min_valuation_coverage_ratio: float = 1.0,
    min_return_coverage_ratio: float = 0.8,
) -> DataQualityReport:
    """Check the portfolio and date alignment expected by monthly agents."""
    metrics_report = check_portfolio_metrics_quality(
        metrics,
        min_valuation_coverage_ratio=min_valuation_coverage_ratio,
        min_return_coverage_ratio=min_return_coverage_ratio,
    )
    issues = list(metrics_report.issues)
    as_of_date = metrics_report.as_of_date
    snapshot_date = extract_snapshot_as_of_date(portfolio_metrics_snapshot)

    if require_monthly_report_date and monthly_report_date is None:
        issues.append(
            DataQualityIssue(
                code="monthly_report_date_missing",
                severity="error",
                message="The selected monthly report does not contain a valid as_of_date.",
            )
        )
    if portfolio_metrics_snapshot is not None and snapshot_date is None:
        issues.append(
            DataQualityIssue(
                code="snapshot_date_missing",
                severity="error",
                message=(
                    "portfolio_metrics_snapshot must include a valid "
                    "`as_of_date` or `daily.valuation_date`."
                ),
            )
        )
    if snapshot_date is not None and as_of_date is not None and snapshot_date != as_of_date:
        issues.append(
            DataQualityIssue(
                code="snapshot_metrics_date_mismatch",
                severity="error",
                message=(
                    "Portfolio metrics snapshot is not aligned with calculated metrics: "
                    f"snapshot={snapshot_date.isoformat()} metrics={as_of_date.isoformat()}."
                ),
                details={"snapshot_date": snapshot_date.isoformat(), "metrics_date": as_of_date.isoformat()},
            )
        )
    if monthly_report_date is not None and as_of_date is not None and monthly_report_date != as_of_date:
        issues.append(
            DataQualityIssue(
                code="monthly_report_date_mismatch",
                severity="error",
                message=(
                    "Monthly report is not aligned with calculated metrics: "
                    f"report={monthly_report_date.isoformat()} metrics={as_of_date.isoformat()}."
                ),
                details={"monthly_report_date": monthly_report_date.isoformat(), "metrics_date": as_of_date.isoformat()},
            )
        )
    if monthly_report_date is not None and snapshot_date is not None and monthly_report_date != snapshot_date:
        issues.append(
            DataQualityIssue(
                code="monthly_report_snapshot_date_mismatch",
                severity="error",
                message=(
                    "Monthly report and portfolio metrics snapshot have different dates: "
                    f"report={monthly_report_date.isoformat()} snapshot={snapshot_date.isoformat()}."
                ),
                details={
                    "monthly_report_date": monthly_report_date.isoformat(),
                    "snapshot_date": snapshot_date.isoformat(),
                },
            )
        )

    return DataQualityReport(as_of_date=as_of_date, issues=tuple(_dedupe_issues(issues)))


def extract_snapshot_as_of_date(snapshot: Mapping[str, Any] | None) -> date | None:
    """Extract `as_of_date` from an agent metrics snapshot-like mapping."""
    if snapshot is None:
        return None
    raw_date = snapshot.get("as_of_date")
    if raw_date is None:
        daily = snapshot.get("daily")
        if isinstance(daily, Mapping):
            raw_date = daily.get("valuation_date")
    if isinstance(raw_date, datetime):
        return raw_date.date()
    if isinstance(raw_date, date):
        return raw_date
    if isinstance(raw_date, str) and raw_date.strip():
        try:
            return date.fromisoformat(raw_date[:10])
        except ValueError:
            return None
    return None


def _dedupe_issues(issues: list[DataQualityIssue]) -> tuple[DataQualityIssue, ...]:
    seen: set[tuple[str, str]] = set()
    deduped: list[DataQualityIssue] = []
    for issue in issues:
        key = (issue.code, issue.message)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)
    return tuple(deduped)


def _int_value(value: Any) -> int:
    if pd.isna(value):
        return 0
    return int(value)


def _float_value(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    return float(value)


__all__ = [
    "DataQualityIssue",
    "DataQualityReport",
    "QualitySeverity",
    "check_agent_input_quality",
    "check_portfolio_metrics_quality",
    "extract_snapshot_as_of_date",
]
