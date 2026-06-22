from src.reports import DEFAULT_MONTHLY_PERIODS, MonthlyPeriodSummary, MonthlyReportResult
from src.reports import monthly
from src.reports.monthly_models import (
    DEFAULT_MONTHLY_PERIODS as MODEL_DEFAULT_MONTHLY_PERIODS,
    MonthlyPeriodSummary as ModelMonthlyPeriodSummary,
    MonthlyReportResult as ModelMonthlyReportResult,
)


def test_monthly_report_models_are_public_and_backward_compatible() -> None:
    assert MonthlyPeriodSummary is ModelMonthlyPeriodSummary
    assert MonthlyReportResult is ModelMonthlyReportResult
    assert monthly.MonthlyPeriodSummary is ModelMonthlyPeriodSummary
    assert monthly.MonthlyReportResult is ModelMonthlyReportResult
    assert DEFAULT_MONTHLY_PERIODS == MODEL_DEFAULT_MONTHLY_PERIODS
    assert monthly.DEFAULT_MONTHLY_PERIODS == MODEL_DEFAULT_MONTHLY_PERIODS
