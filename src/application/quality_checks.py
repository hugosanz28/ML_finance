"""Application use cases for portfolio and agent input quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

from src.application.types import ApplicationResult
from src.config import Settings, get_settings
from src.portfolio import PortfolioMetricsResult, calculate_portfolio_metrics_from_normalized_degiro
from src.portfolio.data_quality import DataQualityReport, check_agent_input_quality


@dataclass(frozen=True)
class RunAgentQualityChecksRequest:
    metrics: PortfolioMetricsResult | None = None
    monthly_report_date: date | None = None
    portfolio_metrics_snapshot: Mapping[str, Any] | None = None
    min_valuation_coverage_ratio: float = 1.0
    min_return_coverage_ratio: float = 0.8


@dataclass(frozen=True)
class RunAgentQualityChecksResult:
    result: ApplicationResult
    report: DataQualityReport

    @property
    def can_run_agents(self) -> bool:
        return self.report.can_run_agents


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
            portfolio_metrics_snapshot=resolved_request.portfolio_metrics_snapshot,
            min_valuation_coverage_ratio=resolved_request.min_valuation_coverage_ratio,
            min_return_coverage_ratio=resolved_request.min_return_coverage_ratio,
        )
        status = "succeeded" if report.can_run_agents else "failed"
        message = (
            "Agent input quality checks passed."
            if report.can_run_agents
            else f"Agent input quality checks failed with {report.error_count} blocking issue(s)."
        )
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
        )


__all__ = [
    "RunAgentQualityChecksRequest",
    "RunAgentQualityChecksResult",
    "RunAgentQualityChecksUseCase",
]
