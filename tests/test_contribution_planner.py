from __future__ import annotations

from dataclasses import replace
import json
import math

import pytest

from src.portfolio.contribution_planner import (
    ContributionConstraints,
    ContributionPosition,
    plan_contribution,
)


def _position(
    asset_id: str,
    bucket: str,
    value: float,
    *,
    price: float | None = 100.0,
    asset_type: str = "etf",
) -> ContributionPosition:
    quantity = value / price if price else 0.0
    return ContributionPosition(
        asset_id=asset_id,
        isin=f"ISIN-{asset_id}",
        bucket=bucket,
        quantity=quantity,
        market_value_base=value,
        unit_price_base=price,
        asset_type=asset_type,
    )


def _portfolio() -> tuple[ContributionPosition, ...]:
    return (
        _position("core", "core", 800.0),
        _position("satellite", "satellite", 100.0),
        _position("cash", "cash", 100.0, price=None, asset_type="cash"),
    )


def _targets() -> dict[str, float]:
    return {"core": 0.60, "satellite": 0.30, "cash": 0.10}


def test_whole_unit_plan_improves_deviation_and_preserves_residual_cash() -> None:
    positions = _portfolio()

    plan = plan_contribution(
        positions,
        _targets(),
        ContributionConstraints(contribution_amount=250.0, max_orders=2),
    )

    assert plan.invested_amount == 200.0
    assert plan.remaining_cash == 50.0
    assert plan.deviation_after < plan.deviation_before
    assert len(plan.orders) == 1
    assert plan.orders[0].asset_id == "satellite"
    assert plan.orders[0].quantity == 2.0
    assert plan.orders[0].amount_base == 200.0
    assert all(order.quantity > 0 for order in plan.orders)
    assert sum(order.amount_base for order in plan.orders) <= plan.budget
    assert positions == _portfolio()
    json.dumps(plan.to_dict(), allow_nan=False)


def test_fractional_plan_can_use_the_complete_budget() -> None:
    plan = plan_contribution(
        _portfolio(),
        _targets(),
        ContributionConstraints(
            contribution_amount=175.0,
            allow_fractional_units=True,
        ),
    )

    assert plan.invested_amount == 175.0
    assert plan.remaining_cash == 0.0
    assert plan.orders[0].quantity == 1.75
    assert plan.orders[0].amount_base == 175.0


def test_minimum_order_and_whole_units_can_leave_all_budget_unspent() -> None:
    plan = plan_contribution(
        _portfolio(),
        _targets(),
        ContributionConstraints(
            contribution_amount=80.0,
            minimum_order_value=50.0,
        ),
    )

    assert plan.orders == ()
    assert plan.invested_amount == 0.0
    assert plan.remaining_cash == 80.0
    assert plan.deviation_after == plan.deviation_before
    assert "whole_units_unaffordable:satellite" in plan.constraint_events


def test_max_orders_uses_stable_bucket_and_identifier_tiebreaks() -> None:
    positions = (
        _position("core", "core", 800.0, price=50.0),
        _position("asset-beta", "beta", 100.0, price=50.0),
        _position("asset-alpha", "alpha", 100.0, price=50.0),
    )

    first = plan_contribution(
        positions,
        {"core": 0.60, "alpha": 0.20, "beta": 0.20},
        ContributionConstraints(contribution_amount=200.0, max_orders=1),
    )
    second = plan_contribution(
        tuple(reversed(positions)),
        {"beta": 0.20, "alpha": 0.20, "core": 0.60},
        ContributionConstraints(contribution_amount=200.0, max_orders=1),
    )

    assert [order.to_dict() for order in first.orders] == [
        order.to_dict() for order in second.orders
    ]
    assert first.orders[0].asset_id == "asset-alpha"
    assert "max_orders_reached" in first.constraint_events


def test_single_asset_cap_is_never_exceeded() -> None:
    plan = plan_contribution(
        _portfolio(),
        _targets(),
        ContributionConstraints(
            contribution_amount=200.0,
            allow_fractional_units=True,
            max_single_asset_weight=0.15,
        ),
    )

    satellite_order = plan.orders[0]
    final_total = 1_000.0 + plan.invested_amount
    assert (100.0 + satellite_order.amount_base) / final_total <= 0.15 + 1e-9
    assert plan.remaining_cash > 0
    assert "max_single_asset_weight:satellite" in plan.constraint_events


def test_multiple_assets_in_one_bucket_are_planned_without_mutation() -> None:
    positions = (
        _position("core", "core", 800.0),
        _position("satellite-a", "satellite", 50.0, price=25.0),
        _position("satellite-b", "satellite", 50.0, price=25.0),
        _position("cash", "cash", 100.0, price=None, asset_type="cash"),
    )
    original = tuple(positions)

    plan = plan_contribution(
        positions,
        _targets(),
        ContributionConstraints(
            contribution_amount=200.0,
            allow_fractional_units=True,
            max_orders=2,
            max_single_asset_weight=0.15,
        ),
    )

    assert plan.invested_amount == 200.0
    assert {order.asset_id for order in plan.orders} == {
        "satellite-a",
        "satellite-b",
    }
    assert plan.deviation_after < plan.deviation_before
    assert positions == original


def test_no_purchase_is_forced_when_current_weights_match_targets() -> None:
    positions = (
        _position("core", "core", 600.0),
        _position("satellite", "satellite", 300.0),
        _position("cash", "cash", 100.0, price=None, asset_type="cash"),
    )

    plan = plan_contribution(
        positions,
        _targets(),
        ContributionConstraints(contribution_amount=200.0),
    )

    assert plan.orders == ()
    assert plan.deviation_before == 0.0
    assert plan.deviation_after == 0.0
    assert plan.remaining_cash == 200.0
    assert "no_improving_purchase" in plan.warnings


def test_unusable_unit_price_is_reported_and_never_bought() -> None:
    positions = (
        _position("core", "core", 800.0),
        replace(_position("satellite", "satellite", 100.0), unit_price_base=None),
        _position("cash", "cash", 100.0, price=None, asset_type="cash"),
    )

    plan = plan_contribution(
        positions,
        _targets(),
        ContributionConstraints(contribution_amount=200.0),
    )

    assert plan.orders == ()
    assert "asset_not_buyable:satellite" in plan.constraint_events
    assert plan.deviation_after == plan.deviation_before


@pytest.mark.parametrize(
    ("targets", "constraints", "error"),
    [
        (
            {"core": 1.0},
            ContributionConstraints(contribution_amount=float("nan")),
            "contribution_amount",
        ),
        (
            {"core": 0.9},
            ContributionConstraints(contribution_amount=100.0),
            "sum to 1.0",
        ),
        (
            {"core": 1.0},
            ContributionConstraints(contribution_amount=100.0, max_orders=0),
            "max_orders",
        ),
        (
            {"core": 1.0},
            ContributionConstraints(
                contribution_amount=100.0,
                max_single_asset_weight=1.1,
            ),
            "max_single_asset_weight",
        ),
    ],
)
def test_invalid_inputs_fail_before_planning(
    targets: dict[str, float],
    constraints: ContributionConstraints,
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        plan_contribution(
            (_position("core", "core", 100.0),),
            targets,
            constraints,
        )


def test_aggregate_values_must_remain_json_finite() -> None:
    positions = (
        _position("core-a", "core", 1e308),
        _position("core-b", "core", 1e308),
    )

    with pytest.raises(ValueError, match="must remain finite"):
        plan_contribution(
            positions,
            {"core": 1.0},
            ContributionConstraints(contribution_amount=100.0),
        )


def test_whole_order_never_uses_epsilon_to_exceed_budget_or_cap() -> None:
    budget = 99.9999999995
    cap = (100.0 + budget) / (1_000.0 + budget)

    plan = plan_contribution(
        _portfolio(),
        _targets(),
        ContributionConstraints(
            contribution_amount=budget,
            max_single_asset_weight=cap,
        ),
    )

    assert sum(order.amount_base for order in plan.orders) <= budget
    assert plan.invested_amount <= budget
    for order in plan.orders:
        final_weight = (
            100.0 + order.amount_base
        ) / (1_000.0 + plan.invested_amount)
        assert final_weight <= cap


def test_fractional_order_must_meet_minimum_exactly() -> None:
    budget = 99.9999999995

    plan = plan_contribution(
        _portfolio(),
        _targets(),
        ContributionConstraints(
            contribution_amount=budget,
            allow_fractional_units=True,
            minimum_order_value=100.0,
        ),
    )

    assert plan.orders == ()
    assert plan.invested_amount == 0.0
    assert "minimum_order_not_met:satellite" in plan.constraint_events


def test_extreme_finite_inputs_cannot_emit_infinite_quantities() -> None:
    positions = (
        _position("large", "core", 1e308, price=1e307),
        _position("tiny-price", "satellite", 0.0, price=1e-9),
    )

    plan = plan_contribution(
        positions,
        {"core": 0.10, "satellite": 0.90},
        ContributionConstraints(
            contribution_amount=1e307,
            allow_fractional_units=True,
        ),
    )

    assert plan.orders == ()
    assert "non_finite_quantity:tiny-price" in plan.constraint_events
    json.dumps(plan.to_dict(), allow_nan=False)


def test_constraint_event_order_is_independent_of_position_order() -> None:
    positions = (
        _position("b", "satellite", 100.0, price=None),
        _position("a", "core", 900.0, price=None),
    )
    constraints = ContributionConstraints(contribution_amount=100.0)
    targets = {"core": 0.50, "satellite": 0.50}

    first = plan_contribution(positions, targets, constraints)
    second = plan_contribution(tuple(reversed(positions)), targets, constraints)

    assert first.to_dict() == second.to_dict()


def test_fractional_accumulation_never_rounds_above_budget() -> None:
    positions = (
        ContributionPosition("a", "a", 57.19, 571.9, 10.0),
        ContributionPosition("b", "b", 61.58, 615.8, 10.0),
        ContributionPosition("c", "c", 89.66, 896.6, 10.0),
    )
    budget = 4_529.39

    plan = plan_contribution(
        positions,
        {"a": 0.45, "b": 0.05, "c": 0.50},
        ContributionConstraints(
            contribution_amount=budget,
            allow_fractional_units=True,
            max_orders=3,
        ),
    )

    assert plan.invested_amount <= budget
    assert sum(order.amount_base for order in plan.orders) <= budget


def test_canonical_cash_id_is_never_treated_as_buyable() -> None:
    positions = (
        _position("core", "core", 900.0),
        _position(
            "degiro:cash:eur",
            "cash",
            100.0,
            price=1.0,
            asset_type="unknown",
        ),
    )

    plan = plan_contribution(
        positions,
        {"core": 0.50, "cash": 0.50},
        ContributionConstraints(contribution_amount=200.0),
    )

    assert plan.orders == ()


def test_extreme_minimum_order_ratio_does_not_overflow() -> None:
    positions = (
        _position("core", "core", 1e299, price=1e299),
        _position("satellite", "satellite", 1.0, price=1e-9),
    )

    plan = plan_contribution(
        positions,
        {"core": 0.10, "satellite": 0.90},
        ContributionConstraints(
            contribution_amount=1e299,
            minimum_order_value=1e308,
        ),
    )

    assert plan.orders == ()
    json.dumps(plan.to_dict(), allow_nan=False)


def test_fractional_quantity_accumulation_stays_json_finite() -> None:
    scale = 1e296
    positions = (
        ContributionPosition("a", "a", 1.0, 571.9 * scale, 1e-9),
        ContributionPosition("b", "b", 1.0, 615.8 * scale, 1e-9),
        ContributionPosition("c", "c", 1.0, 896.6 * scale, 1e-9),
    )

    plan = plan_contribution(
        positions,
        {"a": 0.45, "b": 0.05, "c": 0.50},
        ContributionConstraints(
            contribution_amount=4_529.39 * scale,
            allow_fractional_units=True,
            max_orders=3,
        ),
    )

    assert all(math.isfinite(order.quantity) for order in plan.orders)
    json.dumps(plan.to_dict(), allow_nan=False)
