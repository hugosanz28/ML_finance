"""Application boundary for private monthly planning data."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from src.config import Settings, get_settings
from src.application.portfolio_state import (
    GetPortfolioStateRequest, GetPortfolioStateUseCase, PortfolioStateUnavailableError,
)
from src.personal_finance.model import PersonalPlan
from src.personal_finance.store import PersonalPlanStore


@dataclass(frozen=True)
class ReadPersonalPlanRequest:
    as_of_date: date | None = None


@dataclass(frozen=True)
class UpdatePersonalPlanRequest:
    plan: PersonalPlan
    expected_previous_hash: str


@dataclass(frozen=True)
class PreviewPersonalPlanRequest:
    plan: PersonalPlan
    as_of_date: date | None = None


class PreviewPersonalPlanUseCase:
    def __init__(self, *, settings: Settings | None = None):
        self.settings = settings or get_settings()

    def execute(self, request: PreviewPersonalPlanRequest) -> dict[str, object]:
        return _summary(request.plan, request.as_of_date or date.today(), self.settings)


def _summary(plan: PersonalPlan, as_of_date: date, settings: Settings) -> dict[str, object]:
    # Only a selected goal receives the whole DEGIRO valuation; other goals stay independent.
    valuation = None
    valuation_date = None
    warnings: list[str] = []
    if any(goal.source == "degiro" for goal in plan.goals):
        try:
            portfolio = GetPortfolioStateUseCase(settings=settings).execute(GetPortfolioStateRequest(
                persist=False, include_positions=False, include_history=False,
            ))
            raw_value = portfolio.summary.get("total_market_value_base")
            if raw_value is not None:
                candidate = int((Decimal(str(raw_value)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
                valuation = candidate if candidate >= 0 else None
            valuation_date = portfolio.as_of_date
            warnings = portfolio.data_quality["warnings"]
        except PortfolioStateUnavailableError:
            warnings = ["portfolio_data_unavailable"]
    return {
        **asdict(plan.calculate(as_of_date, valuation)),
        "portfolio_value_cents": valuation,
        "portfolio_as_of_date": valuation_date,
        "portfolio_warnings": warnings,
    }


class ReadPersonalPlanUseCase:
    def __init__(self, *, settings: Settings | None = None):
        self.settings = settings or get_settings()

    def execute(self, request: ReadPersonalPlanRequest) -> dict[str, object]:
        plan, content_hash = PersonalPlanStore(self.settings.data_dir).read()
        return {
            "configured": plan is not None,
            "plan": plan.model_dump(mode="json") if plan else None,
            "content_hash": content_hash,
            "summary": _summary(plan, request.as_of_date or date.today(), self.settings) if plan else None,
        }


class UpdatePersonalPlanUseCase:
    def __init__(self, *, settings: Settings | None = None):
        self.settings = settings or get_settings()

    def execute(self, request: UpdatePersonalPlanRequest) -> dict[str, object]:
        updated_hash = PersonalPlanStore(self.settings.data_dir).write(request.plan, request.expected_previous_hash)
        return {"status": "succeeded", "warnings": [], "content_hash": updated_hash}
