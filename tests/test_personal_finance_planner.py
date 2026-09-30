"""Configurable goals must not conflate bank balance, envelopes and spending."""

from dataclasses import replace
from datetime import date

import pytest

from src.personal_finance.planner import GoalRequest, MonthlyExpense, PlanRequest, calculate_plan, remaining_paydays


def goal(**overrides):
    values = dict(
        id="car", name="Coche", source="bank", target_cents=1_000_000,
        due_date=date(2027, 9, 1), current_cents=400_000,
        contribution_mode="deadline", manual_monthly_cents=0,
    )
    return GoalRequest(**{**values, **overrides})


def request(**overrides):
    values = dict(
        as_of_date=date(2026, 10, 1), payday_day=1,
        net_salary_cents=200_000,
        expenses=(MonthlyExpense("Casa y comida", 60_000), MonthlyExpense("Gasolina", 10_000)),
        bank_balance_cents=500_000,
        emergency_reserved_cents=0, immediate_commitments_cents=0,
        goals=(goal(),),
    )
    return PlanRequest(**{**values, **overrides})


def test_multiple_bank_goals_reserve_once_and_share_monthly_margin():
    holiday = goal(id="holiday", name="Vacaciones", target_cents=120_000,
                   current_cents=50_000, contribution_mode="manual", manual_monthly_cents=10_000)
    result = calculate_plan(request(goals=(goal(), holiday)))
    assert result.unallocated_bank_cents == 50_000
    assert result.monthly_expenses_cents == 70_000
    assert result.monthly_margin_cents == 130_000
    assert result.goals[0].required_monthly_cents == 50_000
    assert result.goals[1].planned_monthly_cents == 10_000
    assert result.goals_planned_monthly_cents == 60_000
    assert result.after_goals_cents == 70_000
    assert result.status == "feasible"


def test_rounds_up_and_counts_only_pending_paydays():
    result = calculate_plan(request(goals=(goal(current_cents=0),)))
    assert result.goals[0].paydays_remaining == 12
    assert result.goals[0].required_monthly_cents == 83_334
    assert 12 * result.goals[0].required_monthly_cents >= 1_000_000
    assert remaining_paydays(date(2026, 10, 2), date(2027, 9, 1), 1) == 11
    assert remaining_paydays(date(2026, 1, 31), date(2026, 2, 28), 31) == 2


def test_infeasible_and_bank_shortage_are_visible():
    result = calculate_plan(request(net_salary_cents=90_000, bank_balance_cents=350_000))
    assert result.status == "bank_reserves_exceed_balance"
    assert result.after_goals_cents == -30_000
    assert result.bank_reserve_shortfall_cents == 50_000


def test_goal_funded_or_due_before_next_payday():
    assert calculate_plan(request(goals=(goal(current_cents=1_000_000),))).goals[0].status == "funded"
    near = goal(due_date=date(2026, 10, 2))
    result = calculate_plan(request(as_of_date=date(2026, 10, 2), goals=(near,)))
    assert result.goals[0].status == "no_paydays_before_due"
    assert result.goals[0].required_monthly_cents is None
    assert calculate_plan(request(goals=(replace(near, due_date=date(2026, 9, 30)),))).goals[0].status == "overdue"


def test_degiro_goal_without_valuation_does_not_block_bank_goal():
    home = goal(id="home", name="Vivienda", source="degiro", target_cents=3_000_000,
                due_date=None, current_cents=None, contribution_mode="manual", manual_monthly_cents=40_000)
    result = calculate_plan(request(goals=(goal(), home)))
    assert result.goals[1].current_cents is None
    assert result.goals[1].planned_monthly_cents == 40_000
    assert result.unallocated_bank_cents == 100_000
    assert result.after_goals_cents == 40_000
    assert result.goals[1].status == "valuation_unavailable"
    assert result.status == "goals_need_attention"


@pytest.mark.parametrize("value", [-1, 1.5, True])
def test_rejects_invalid_money(value):
    with pytest.raises(ValueError):
        calculate_plan(request(goals=(goal(current_cents=value),)))
