"""HTTP schemas; financial computation remains in application/domain services."""

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


class Schema(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class DateQuery(Schema):
    as_of_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")

    @field_validator("as_of_date")
    @classmethod
    def valid_date(cls, value: str | None) -> str | None:
        if value is not None and date.fromisoformat(value) > date.today():
            raise ValueError("Future dates are not supported")
        return value


class PortfolioQuery(DateQuery):
    include_positions: bool = True
    include_history: bool = False


class AnalyticsQuery(DateQuery):
    period: Literal["last_month", "last_quarter", "last_year", "since_inception"] = "since_inception"
    benchmark_id: str | None = Field(default=None, max_length=80)
    risk_free_rate_annual: float | None = Field(default=None, gt=-1, allow_inf_nan=False)


class ListQuery(Schema):
    limit: int = Field(default=20, ge=1, le=100)


class ErrorDetail(Schema):
    code: str
    message: str


class ErrorResponse(Schema):
    error: ErrorDetail


class HealthResponse(Schema):
    status: Literal["ok"] = "ok"
    api_version: Literal["v1"] = "v1"
    mode: Literal["read_only", "operations"] = "read_only"


class PortfolioResponse(Schema):
    as_of_date: date
    base_currency: str
    summary: dict[str, float | None]
    broker_snapshot: dict[str, str | float | None] | None
    positions: list[dict[str, JsonValue]]
    history: list[dict[str, JsonValue]]
    data_quality: dict[str, list[str]]


class AnalyticsPeriod(Schema):
    period_id: str
    requested_start: date | None
    actual_start: date | None
    end_date: date | None


class AnalyticsResponse(Schema):
    schema_version: Literal[1]
    section: Literal["summary", "performance", "risk", "benchmarks"]
    status: Literal["available", "partial", "unavailable"]
    reason_code: str
    base_currency: str
    period: AnalyticsPeriod
    warnings: list[str]
    data: dict[str, JsonValue] = Field(description="Application analytics sections; metrics preserve status, coverage and reason_code.")


class MetricDefinition(Schema):
    metric_id: str
    name: str
    description: str
    formula: str
    unit: str
    interpretation: str
    limitations: str
    required_data: str
    validity_conditions: str


class MetricDefinitionsResponse(Schema):
    schema_version: Literal[1]
    definitions: list[MetricDefinition]


class ReportSummary(Schema):
    report_id: str


class ReportsResponse(Schema):
    reports: list[ReportSummary]


class ReportResponse(ReportSummary):
    content_markdown: str


class RunSummary(Schema):
    run_id: str
    as_of_date: str | None
    generated_at: str | None
    status: str
    agent_statuses: dict[str, str]


class RunsResponse(Schema):
    runs: list[RunSummary]


class AuditResponse(Schema):
    run_id: str
    schema_version: int
    is_legacy: bool
    compatibility_warnings: list[str]
    run_metadata: dict[str, JsonValue]
    input_payload: dict[str, JsonValue]
    preflight: dict[str, JsonValue]
    agents: dict[str, dict[str, JsonValue]]
