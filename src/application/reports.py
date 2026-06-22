"""Application use cases for report workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from src.application.types import ApplicationResult
from src.config import Settings, get_settings
from src.reports import MonthlyReportResult, ReportHistoryEntry, generate_monthly_report, get_latest_monthly_report


@dataclass(frozen=True)
class GenerateMonthlyReportRequest:
    as_of_date: date | None = None
    output_dir: Path | None = None
    normalized_degiro_dir: Path | None = None
    persist: bool = True


@dataclass(frozen=True)
class GenerateMonthlyReportResult:
    result: ApplicationResult
    report: MonthlyReportResult


@dataclass(frozen=True)
class GetLatestMonthlyReportResult:
    result: ApplicationResult
    report: ReportHistoryEntry | None


class GenerateMonthlyReportUseCase:
    """Generate and optionally persist the monthly Markdown report."""

    name = "generate_monthly_report"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self, request: GenerateMonthlyReportRequest | None = None) -> GenerateMonthlyReportResult:
        resolved_request = request or GenerateMonthlyReportRequest()
        report = generate_monthly_report(
            settings=self.settings,
            as_of_date=resolved_request.as_of_date,
            output_dir=resolved_request.output_dir,
            normalized_degiro_dir=resolved_request.normalized_degiro_dir,
            persist=resolved_request.persist,
        )
        return GenerateMonthlyReportResult(
            result=ApplicationResult(
                name=self.name,
                status="succeeded",
                message=f"Monthly report generated for {report.as_of_date.isoformat()}.",
                artifacts={
                    "as_of_date": report.as_of_date.isoformat(),
                    "output_path": report.output_path,
                    "base_currency": report.base_currency,
                },
            ),
            report=report,
        )


class GetLatestMonthlyReportUseCase:
    """Read latest persisted monthly report metadata."""

    name = "get_latest_monthly_report"

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = get_settings() if settings is None else settings

    def execute(self) -> GetLatestMonthlyReportResult:
        report = get_latest_monthly_report(settings=self.settings)
        if report is None:
            result = ApplicationResult(
                name=self.name,
                status="skipped",
                message="No monthly report metadata found.",
            )
        else:
            result = ApplicationResult(
                name=self.name,
                status="succeeded",
                message=f"Latest monthly report: {report.report_id}.",
                artifacts={
                    "report_id": report.report_id,
                    "as_of_date": report.as_of_date.isoformat(),
                    "report_path": report.report_path,
                },
            )
        return GetLatestMonthlyReportResult(result=result, report=report)


__all__ = [
    "GenerateMonthlyReportRequest",
    "GenerateMonthlyReportResult",
    "GenerateMonthlyReportUseCase",
    "GetLatestMonthlyReportResult",
    "GetLatestMonthlyReportUseCase",
]
