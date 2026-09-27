"""Deterministic monthly planning for user-defined goals in integer EUR cents."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from typing import Literal


@dataclass(frozen=True)
class MonthlyExpense:
    name: str
    amount_cents: int


@dataclass(frozen=True)
class GoalRequest:
    id: str
    name: str
    source: Literal["bank", "degiro"]
    target_cents: int
    due_date: date | None
    current_cents: int | None
    contribution_mode: Literal["deadline", "manual"]
    manual_monthly_cents: int


@dataclass(frozen=True)
class PlanRequest:
    as_of_date: date
    payday_day: int
    net_salary_cents: int
    expenses: tuple[MonthlyExpense, ...]
    bank_balance_cents: int | None
    emergency_reserved_cents: int
    immediate_commitments_cents: int
    goals: tuple[GoalRequest, ...]


@dataclass(frozen=True)
class GoalResult:
    id: str
    current_cents: int | None
    remaining_cents: int | None
    paydays_remaining: int | None
    required_monthly_cents: int | None
    planned_monthly_cents: int | None
    status: str


@dataclass(frozen=True)
class PlanResult:
    unallocated_bank_cents: int | None
    bank_reserve_shortfall_cents: int | None
    monthly_expenses_cents: int
    monthly_margin_cents: int
    goals_planned_monthly_cents: int | None
    after_goals_cents: int | None
    status: str
    goals: tuple[GoalResult, ...]


def _payday(year: int, month: int, day: int) -> date:
    # A salary set for day 31 lands on the last day of a shorter month.
    return date(year, month, min(day, monthrange(year, month)[1]))


def remaining_paydays(as_of_date: date, due_date: date, payday_day: int) -> int:
    """Count paydays from today through the target date, inclusive."""
    if not 1 <= payday_day <= 31:
        raise ValueError("payday_day must be between 1 and 31")
    if due_date < as_of_date:
        return 0
    months = (due_date.year - as_of_date.year) * 12 + due_date.month - as_of_date.month + 1
    if _payday(as_of_date.year, as_of_date.month, payday_day) < as_of_date:
        months -= 1
    if _payday(due_date.year, due_date.month, payday_day) > due_date:
        months -= 1
    return max(0, months)


def _valid_cents(value: int | None) -> bool:
    return value is None or (isinstance(value, int) and not isinstance(value, bool) and value >= 0)


def _calculate_goal(goal: GoalRequest, as_of_date: date, payday_day: int) -> GoalResult:
    if goal.source not in {"bank", "degiro"} or goal.contribution_mode not in {"deadline", "manual"}:
        raise ValueError("invalid goal source or contribution mode")
    if not goal.name.strip() or not all(_valid_cents(value) for value in (
        goal.target_cents, goal.current_cents, goal.manual_monthly_cents,
    )):
        raise ValueError("invalid goal name or amount")
    if goal.contribution_mode == "deadline" and goal.due_date is None:
        raise ValueError("deadline goals need a due date")

    remaining = max(0, goal.target_cents - goal.current_cents) if goal.current_cents is not None else None
    paydays = remaining_paydays(as_of_date, goal.due_date, payday_day) if goal.due_date else None
    required = None
    if remaining is not None and paydays is not None:
        required = (remaining + paydays - 1) // paydays if paydays and remaining else (0 if not remaining else None)

    if goal.target_cents == 0:
        status = "no_target"
    elif remaining == 0:
        status = "funded"
    elif goal.due_date and goal.due_date < as_of_date:
        status = "overdue"
    elif goal.contribution_mode == "deadline" and paydays == 0:
        status = "no_paydays_before_due"
    elif goal.current_cents is None:
        status = "valuation_unavailable"
    elif goal.contribution_mode == "manual" and required is not None and goal.manual_monthly_cents < required:
        status = "behind_plan"
    else:
        status = "feasible"

    planned = goal.manual_monthly_cents if goal.contribution_mode == "manual" else required
    return GoalResult(goal.id, goal.current_cents, remaining, paydays, required, planned, status)


def calculate_plan(request: PlanRequest) -> PlanResult:
    """Calculate every goal without treating bank envelopes as spending."""
    amounts = (
        request.net_salary_cents, request.bank_balance_cents,
        request.emergency_reserved_cents, request.immediate_commitments_cents,
        *(expense.amount_cents for expense in request.expenses),
    )
    if not all(_valid_cents(value) for value in amounts):
        raise ValueError("amounts must be nonnegative integer cents")
    if any(not expense.name.strip() for expense in request.expenses):
        raise ValueError("expense names cannot be empty")

    goals = tuple(_calculate_goal(goal, request.as_of_date, request.payday_day) for goal in request.goals)
    monthly_expenses = sum(expense.amount_cents for expense in request.expenses)
    margin = request.net_salary_cents - monthly_expenses
    planned = (sum(goal.planned_monthly_cents or 0 for goal in goals) if all(
        goal.planned_monthly_cents is not None for goal in goals
    ) else None)
    after_goals = margin - planned if planned is not None else None

    available = None
    shortfall = None
    if request.bank_balance_cents is not None:
        bank_reserved = sum(goal.current_cents or 0 for goal in request.goals if goal.source == "bank")
        available = request.bank_balance_cents - bank_reserved - request.emergency_reserved_cents - request.immediate_commitments_cents
        shortfall = max(0, -available)

    if shortfall:
        status = "bank_reserves_exceed_balance"
    elif after_goals is not None and after_goals < 0:
        status = "goals_infeasible"
    elif any(goal.status in {"overdue", "no_paydays_before_due", "valuation_unavailable", "behind_plan"} for goal in goals):
        status = "goals_need_attention"
    else:
        status = "feasible"
    return PlanResult(available, shortfall, monthly_expenses, margin, planned, after_goals, status, goals)
