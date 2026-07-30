"""Application use cases for portfolio and agent input quality checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping

from src.application.types import ApplicationResult
from src.config import Settings, get_settings
from src.portfolio import PortfolioMetricsResult, calculate_portfolio_metrics_from_normalized_degiro
from src.portfolio.data_quality import (
    DataQualityReport,
    check_agent_input_quality,
    extract_snapshot_as_of_date,
)


@dataclass(frozen=True)
class RunAgentQualityChecksRequest:
    metrics: PortfolioMetricsResult | None = None
    monthly_report_date: date | None = None
    require_monthly_report_date: bool = False
    portfolio_metrics_snapshot: Mapping[str, Any] | None = None
    min_valuation_coverage_ratio: float = 1.0
    min_return_coverage_ratio: float = 0.8


@dataclass(frozen=True)
class RunAgentQualityChecksResult:
    result: ApplicationResult
    report: DataQualityReport
    request: RunAgentQualityChecksRequest = field(default_factory=RunAgentQualityChecksRequest)

    @property
    def can_run_agents(self) -> bool:
        return self.report.can_run_agents

    def to_dict(self) -> dict[str, Any]:
        """Return the stable preflight payload used by UI and audit trails."""
        snapshot_date = extract_snapshot_as_of_date(self.request.portfolio_metrics_snapshot)
        if not self.report.can_run_agents:
            status = "blocked"
        elif self.report.warning_count:
            status = "passed_with_warnings"
        else:
            status = "passed"
        return {
            "schema_version": 1,
            "status": status,
            "can_run_agents": self.report.can_run_agents,
            "as_of_date": self.report.as_of_date.isoformat() if self.report.as_of_date else None,
            "counts": {
                "error": self.report.error_count,
                "warning": self.report.warning_count,
                "info": self.report.info_count,
            },
            "issues": [
                {
                    "code": issue.code,
                    "severity": issue.severity,
                    "message": issue.message,
                    "details": dict(issue.details or {}),
                }
                for issue in self.report.issues
            ],
            "inputs": {
                "monthly_report_date": (
                    self.request.monthly_report_date.isoformat()
                    if self.request.monthly_report_date
                    else None
                ),
                "require_monthly_report_date": self.request.require_monthly_report_date,
                "snapshot_as_of_date": snapshot_date.isoformat() if snapshot_date else None,
                "min_valuation_coverage_ratio": self.request.min_valuation_coverage_ratio,
                "min_return_coverage_ratio": self.request.min_return_coverage_ratio,
            },
        }


class RunAgentQualityChecksUseCase:
    """Run deterministic quality checks before monthly agents."""

    name = "run_agent_quality_checks"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: RunAgentQualityChecksRequest | None = None) -> RunAgentQualityChecksResult:
        resolved_request = request or RunAgentQualityChecksRequest()
        metrics = resolved_request.metrics or calculate_portfolio_metrics_from_normalized_degiro(
            settings=self.settings,
            persist=False,
        )
        report = check_agent_input_quality(
            metrics=metrics,
            monthly_report_date=resolved_request.monthly_report_date,
            require_monthly_report_date=resolved_request.require_monthly_report_date,
            portfolio_metrics_snapshot=resolved_request.portfolio_metrics_snapshot,
            min_valuation_coverage_ratio=resolved_request.min_valuation_coverage_ratio,
            min_return_coverage_ratio=resolved_request.min_return_coverage_ratio,
        )
        if not report.can_run_agents:
            status = "failed"
            message = f"Agent input quality checks failed with {report.error_count} blocking issue(s)."
        elif report.warning_count:
            status = "partial"
            message = f"Agent input quality checks passed with {report.warning_count} warning(s)."
        else:
            status = "succeeded"
            message = "Agent input quality checks passed."
        return RunAgentQualityChecksResult(
            result=ApplicationResult(
                name=self.name,
                status=status,
                message=message,
                warnings=tuple(issue.message for issue in report.issues if issue.severity == "warning"),
                artifacts={
                    "as_of_date": report.as_of_date.isoformat() if report.as_of_date else None,
                    "error_count": report.error_count,
                    "warning_count": report.warning_count,
                    "info_count": report.info_count,
                    "can_run_agents": report.can_run_agents,
                },
            ),
            report=report,
            request=resolved_request,
        )


__all__ = [
    "RunAgentQualityChecksRequest",
    "RunAgentQualityChecksResult",
    "RunAgentQualityChecksUseCase",
]
