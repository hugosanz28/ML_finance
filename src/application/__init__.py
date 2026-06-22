"""Reusable application use cases for scripts, Streamlit, and future UIs."""

from src.application.degiro import ImportDegiroRequest, ImportDegiroResult, ImportDegiroUseCase
from src.application.market_data import (
    RefreshFxRequest,
    RefreshFxResult,
    RefreshFxUseCase,
    RefreshMarketDataRequest,
    RefreshMarketDataResult,
    RefreshMarketDataUseCase,
)
from src.application.agents import RunMonthlyAgentsRequest, RunMonthlyAgentsResult, RunMonthlyAgentsUseCase
from src.application.quality_checks import (
    RunAgentQualityChecksRequest,
    RunAgentQualityChecksResult,
    RunAgentQualityChecksUseCase,
)
from src.application.reports import (
    GenerateMonthlyReportRequest,
    GenerateMonthlyReportResult,
    GenerateMonthlyReportUseCase,
    GetLatestMonthlyReportResult,
    GetLatestMonthlyReportUseCase,
)
from src.application.types import ApplicationResult, ApplicationStatus

__all__ = [
    "ApplicationResult",
    "ApplicationStatus",
    "GenerateMonthlyReportRequest",
    "GenerateMonthlyReportResult",
    "GenerateMonthlyReportUseCase",
    "GetLatestMonthlyReportResult",
    "GetLatestMonthlyReportUseCase",
    "ImportDegiroRequest",
    "ImportDegiroResult",
    "ImportDegiroUseCase",
    "RefreshFxRequest",
    "RefreshFxResult",
    "RefreshFxUseCase",
    "RefreshMarketDataRequest",
    "RefreshMarketDataResult",
    "RefreshMarketDataUseCase",
    "RunMonthlyAgentsRequest",
    "RunMonthlyAgentsResult",
    "RunMonthlyAgentsUseCase",
    "RunAgentQualityChecksRequest",
    "RunAgentQualityChecksResult",
    "RunAgentQualityChecksUseCase",
]
