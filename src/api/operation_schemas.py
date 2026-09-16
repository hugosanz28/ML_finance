"""Bounded write contracts: paths, credentials and arbitrary provider options are forbidden."""

from datetime import date
from typing import Literal

from pydantic import Field, JsonValue, model_validator

from src.api.schemas import DateQuery, Schema


class OperationBody(Schema):
    workspace_mode: Literal["demo", "real"]
    confirm: Literal[True]


class UploadItem(Schema):
    filename: str = Field(min_length=1, max_length=200)
    content_base64: str = Field(min_length=1, max_length=6990508)


class UploadsBody(OperationBody):
    uploaded_at: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    uploads: list[UploadItem] = Field(min_length=1, max_length=5)

    @model_validator(mode="after")
    def upload_date(self):
        if date.fromisoformat(self.uploaded_at) > date.today():
            raise ValueError("Future upload date")
        return self


class RefreshBody(OperationBody):
    scope: Literal["both", "fx", "prices"] = "both"
    only_missing_base: bool = False
    fx_provider: Literal["synthetic", "yfinance"]
    price_provider: Literal["synthetic", "yfinance"]
    start_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")

    @model_validator(mode="after")
    def dates(self):
        start = date.fromisoformat(self.start_date) if self.start_date else None
        end = date.fromisoformat(self.end_date) if self.end_date else date.today()
        if end > date.today() or start and start > end:
            raise ValueError("Invalid date range")
        return self


class ReportBody(OperationBody, DateQuery):
    pass


class BenchmarkRefreshBody(OperationBody):
    provider: Literal["yfinance_ecb"]
    start_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")

    @model_validator(mode="after")
    def dates(self):
        if not date(2000, 1, 1) <= date.fromisoformat(self.start_date) < date.fromisoformat(self.end_date) < date.today():
            raise ValueError("Invalid benchmark date range; use completed days")
        return self


class SimulationBody(ReportBody):
    contribution_amount: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    allow_fractional_units: bool = False
    minimum_order_value: float = Field(default=0, ge=0, allow_inf_nan=False)
    max_orders: int = Field(default=4, ge=1, le=100)


class AgentsBody(OperationBody):
    llm_provider: Literal["static", "openai"] = "static"
    search_provider: Literal["null", "static", "tavily", "duckduckgo"] = "null"
    monthly_budget: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    user_satellite_interest: str | None = Field(default=None, max_length=2000)
    report_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,159}$")
    investment_brief_text: str | None = Field(default=None, max_length=65536)
    target_weights: dict[str, float] | None = None

    @model_validator(mode="after")
    def weights(self):
        if self.target_weights is not None:
            if not self.target_weights:
                raise ValueError("Use null for saved targets, or provide explicit weights")
            if len(self.target_weights) > 100 or any(
                not key.strip() or len(key) > 100 or not 0 <= value <= 1
                for key, value in self.target_weights.items()
            ):
                raise ValueError("Invalid target weights")
            if abs(sum(self.target_weights.values()) - 1) > 1e-6:
                raise ValueError("Target weights must sum to one")
        return self


class BriefBody(OperationBody):
    content: str = Field(max_length=65536)
    expected_previous_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")


class TargetsBody(OperationBody):
    portfolio_targets: dict[str, JsonValue]
    expected_previous_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")


class JobResponse(Schema):
    job_id: str
    operation: str
    state: Literal["pending", "running", "succeeded", "partial", "failed"]
    progress: int = Field(ge=0, le=100)
    phase: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    warnings: list[str]
    error_code: str | None
    result: dict[str, JsonValue] | None


class JobsResponse(Schema):
    jobs: list[JobResponse]


class BriefResponse(Schema):
    content: str
    exists: bool
    content_hash: str


class TargetsResponse(Schema):
    portfolio_targets: dict[str, JsonValue] | None
    target_weights: dict[str, float]
    exists: bool
    content_hash: str
    validation_error: str | None
