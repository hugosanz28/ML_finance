"""Validated editable inputs for one local plan with configurable goals."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.personal_finance.planner import GoalRequest, MonthlyExpense, PlanRequest, PlanResult, calculate_plan


CentAmount = Field(ge=0, le=1_000_000_000_000, strict=True)


class PlanModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExpenseCategory(PlanModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,39}$")
    name: str = Field(min_length=1, max_length=80)
    kind: Literal["fixed", "variable", "irregular"]
    monthly_cents: int = CentAmount
    due_day: int | None = Field(default=None, ge=1, le=31, strict=True)


class ActualExpense(PlanModel):
    id: str = Field(pattern=r"^[a-f0-9]{32}$")
    date: date
    category_id: str
    amount_cents: int = CentAmount
    note: str = Field(default="", max_length=200)


class ActualIncome(PlanModel):
    id: str = Field(pattern=r"^[a-f0-9]{32}$")
    date: date
    amount_cents: int = CentAmount


class GoalAllocation(PlanModel):
    id: str = Field(pattern=r"^[a-f0-9]{32}$")
    date: date
    amount_cents: int = Field(ge=-1_000_000_000_000, le=1_000_000_000_000, strict=True)
    note: str = Field(default="", max_length=200)


class SavingsGoal(PlanModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,39}$")
    name: str = Field(min_length=1, max_length=80)
    source: Literal["bank", "degiro"]
    target_cents: int = CentAmount
    due_date: date | None = None
    contribution_mode: Literal["deadline", "manual"]
    manual_monthly_cents: int = CentAmount
    opening_reserved_cents: int = CentAmount
    allocations: list[GoalAllocation] = Field(default_factory=list, max_length=10000)

    @model_validator(mode="after")
    def coherent(self):
        if self.contribution_mode == "deadline" and self.due_date is None:
            raise ValueError("Deadline goals need a due date")
        if self.source == "degiro" and self.opening_reserved_cents:
            raise ValueError("DEGIRO valuation comes from the portfolio, not a manual opening reserve")
        if self.source == "bank" and self.reserved_cents < 0:
            raise ValueError("Goal reserve cannot be negative")
        return self

    @property
    def reserved_cents(self) -> int:
        return self.opening_reserved_cents + sum(item.amount_cents for item in self.allocations)


class PersonalPlan(PlanModel):
    payday_day: int = Field(ge=1, le=31, strict=True)
    net_salary_cents: int = CentAmount
    bank_name: str = Field(default="Cuenta bancaria", min_length=1, max_length=80)
    bank_balance_cents: int | None = Field(default=None, ge=0, le=1_000_000_000_000, strict=True)
    bank_balance_date: date | None = None
    emergency_reserved_cents: int = CentAmount
    immediate_commitments_cents: int = CentAmount
    goals: list[SavingsGoal] = Field(default_factory=list, max_length=100)
    expenses: list[ExpenseCategory] = Field(default_factory=list, max_length=100)
    actual_expenses: list[ActualExpense] = Field(default_factory=list, max_length=10000)
    actual_income: list[ActualIncome] = Field(default_factory=list, max_length=10000)

    @model_validator(mode="after")
    def coherent(self):
        if (self.bank_balance_cents is None) != (self.bank_balance_date is None):
            raise ValueError("Bank balance and date must be provided together")
        categories = {item.id for item in self.expenses}
        if len(categories) != len(self.expenses):
            raise ValueError("Expense category IDs must be unique")
        if any(item.category_id not in categories for item in self.actual_expenses):
            raise ValueError("Expense category is missing")
        if len({goal.id for goal in self.goals}) != len(self.goals):
            raise ValueError("Goal IDs must be unique")
        if sum(goal.source == "degiro" for goal in self.goals) > 1:
            raise ValueError("Only one goal can use the full DEGIRO portfolio")
        events = [*self.actual_expenses, *self.actual_income, *(event for goal in self.goals for event in goal.allocations)]
        if len({item.id for item in events}) != len(events):
            raise ValueError("Event IDs must be unique")
        return self

    def calculate(self, as_of_date: date, portfolio_value_cents: int | None = None) -> PlanResult:
        if portfolio_value_cents is not None and (not isinstance(portfolio_value_cents, int) or portfolio_value_cents < 0):
            raise ValueError("Portfolio valuation must be nonnegative integer cents")
        goals = tuple(GoalRequest(
            id=goal.id,
            name=goal.name,
            source=goal.source,
            target_cents=goal.target_cents,
            due_date=goal.due_date,
            current_cents=goal.reserved_cents if goal.source == "bank" else portfolio_value_cents,
            contribution_mode=goal.contribution_mode,
            manual_monthly_cents=goal.manual_monthly_cents,
        ) for goal in self.goals)
        return calculate_plan(PlanRequest(
            as_of_date=as_of_date,
            payday_day=self.payday_day,
            net_salary_cents=self.net_salary_cents,
            expenses=tuple(MonthlyExpense(item.name, item.monthly_cents) for item in self.expenses),
            bank_balance_cents=self.bank_balance_cents,
            emergency_reserved_cents=self.emergency_reserved_cents,
            immediate_commitments_cents=self.immediate_commitments_cents,
            goals=goals,
        ))
