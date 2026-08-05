"""Pure deterministic planner for contributions-only portfolio simulations."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping, Sequence


_EPSILON = 1e-9
_MAX_PLANNING_ROUNDS = 512


@dataclass(frozen=True)
class ContributionPosition:
    """Current valued position adapted at the application boundary."""

    asset_id: str
    bucket: str
    quantity: float
    market_value_base: float
    unit_price_base: float | None
    isin: str | None = None
    asset_type: str | None = None


@dataclass(frozen=True)
class ContributionConstraints:
    """User and target constraints accepted by the deterministic planner."""

    contribution_amount: float
    allow_fractional_units: bool = False
    minimum_order_value: float = 0.0
    max_orders: int = 10
    max_single_asset_weight: float | None = None


@dataclass(frozen=True)
class ContributionOrder:
    asset_id: str
    isin: str | None
    bucket: str
    quantity: float
    unit_price_base: float
    amount_base: float
    weight_before: float
    weight_after: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "isin": self.isin,
            "bucket": self.bucket,
            "quantity": _clean(self.quantity),
            "unit_price_base": _clean(self.unit_price_base),
            "amount_base": _clean(self.amount_base),
            "weight_before": _clean(self.weight_before),
            "weight_after": _clean(self.weight_after),
        }


@dataclass(frozen=True)
class BucketAllocation:
    bucket: str
    target_weight: float
    value_before: float
    value_after: float
    weight_before: float
    weight_after: float
    deviation_before: float
    deviation_after: float
    allocated_amount: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "target_weight": _clean(self.target_weight),
            "value_before": _clean(self.value_before),
            "value_after": _clean(self.value_after),
            "weight_before": _clean(self.weight_before),
            "weight_after": _clean(self.weight_after),
            "deviation_before": _clean(self.deviation_before),
            "deviation_after": _clean(self.deviation_after),
            "allocated_amount": _clean(self.allocated_amount),
        }


@dataclass(frozen=True)
class ContributionPlan:
    budget: float
    invested_amount: float
    remaining_cash: float
    deviation_before: float
    deviation_after: float
    orders: tuple[ContributionOrder, ...]
    bucket_allocations: tuple[BucketAllocation, ...]
    constraint_events: tuple[str, ...]
    warnings: tuple[str, ...]
    assumptions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return primitives only; adapters can serialize this directly."""
        return {
            "budget": _clean(self.budget),
            "invested_amount": _clean(self.invested_amount),
            "remaining_cash": _clean(self.remaining_cash),
            "deviation_before": _clean(self.deviation_before),
            "deviation_after": _clean(self.deviation_after),
            "orders": [order.to_dict() for order in self.orders],
            "bucket_allocations": [
                allocation.to_dict() for allocation in self.bucket_allocations
            ],
            "constraint_events": list(self.constraint_events),
            "warnings": list(self.warnings),
            "assumptions": list(self.assumptions),
        }


@dataclass(frozen=True)
class _Candidate:
    position: ContributionPosition
    amount: float
    quantity: float
    deviation_after: float


def plan_contribution(
    positions: Sequence[ContributionPosition],
    target_weights: Mapping[str, float],
    constraints: ContributionConstraints,
) -> ContributionPlan:
    """Propose deterministic buys that strictly improve bucket deviation."""
    normalized_positions = tuple(
        sorted(positions, key=lambda position: (position.bucket, position.asset_id))
    )
    normalized_targets = _validate_inputs(
        normalized_positions,
        target_weights,
        constraints,
    )
    budget = float(constraints.contribution_amount)
    current_total = sum(position.market_value_base for position in normalized_positions)
    if not math.isfinite(current_total) or not math.isfinite(current_total + budget):
        raise ValueError("Portfolio value plus contribution must remain finite.")
    current_by_bucket = _sum_by_bucket(normalized_positions)
    current_by_asset = {
        position.asset_id: position.market_value_base
        for position in normalized_positions
    }
    position_by_asset = {
        position.asset_id: position for position in normalized_positions
    }
    allocated_by_asset: dict[str, float] = {}
    quantity_by_asset: dict[str, float] = {}
    events: list[str] = []
    warnings: list[str] = []

    buyable_positions: list[ContributionPosition] = []
    for position in normalized_positions:
        if _is_cash_position(position):
            continue
        if position.unit_price_base is None:
            events.append(f"asset_not_buyable:{position.asset_id}")
            continue
        buyable_positions.append(position)

    if not normalized_positions:
        warnings.append("no_current_positions")
    elif not buyable_positions:
        warnings.append("no_buyable_positions")

    deviation_before = _portfolio_deviation(
        bucket_values=current_by_bucket,
        total_value=current_total,
        target_weights=normalized_targets,
    )
    current_deviation = deviation_before

    for _round in range(_MAX_PLANNING_ROUNDS):
        invested = math.fsum(allocated_by_asset.values())
        remaining = budget - invested
        if remaining <= _EPSILON:
            break
        if invested > 0.0:
            # Leave one representable step of headroom so adding a later
            # fractional allocation cannot round above the hard budget.
            remaining = math.nextafter(remaining, 0.0)

        candidate = _best_candidate(
            positions=buyable_positions,
            target_weights=normalized_targets,
            current_by_bucket=current_by_bucket,
            current_by_asset=current_by_asset,
            allocated_by_asset=allocated_by_asset,
            current_total=current_total,
            remaining_budget=remaining,
            current_deviation=current_deviation,
            constraints=constraints,
            events=events,
        )
        if candidate is None:
            break

        asset_id = candidate.position.asset_id
        proposed_asset_amount = math.fsum(
            (allocated_by_asset.get(asset_id, 0.0), candidate.amount)
        )
        proposed_invested = math.fsum(
            (
                *(
                    amount
                    for existing_asset_id, amount in allocated_by_asset.items()
                    if existing_asset_id != asset_id
                ),
                proposed_asset_amount,
            )
        )
        if proposed_invested > budget:
            events.append(f"budget_precision_limit:{asset_id}")
            break
        try:
            proposed_quantity = math.fsum(
                (quantity_by_asset.get(asset_id, 0.0), candidate.quantity)
            )
        except OverflowError:
            events.append(f"non_finite_quantity:{asset_id}")
            break
        if not math.isfinite(proposed_quantity):
            events.append(f"non_finite_quantity:{asset_id}")
            break
        allocated_by_asset[asset_id] = proposed_asset_amount
        quantity_by_asset[asset_id] = proposed_quantity
        current_deviation = candidate.deviation_after
    else:
        warnings.append("planning_round_limit_reached")

    invested_amount = math.fsum(allocated_by_asset.values())
    remaining_cash = max(0.0, budget - invested_amount)
    if remaining_cash > _EPSILON:
        events.append(f"budget_residual:{_clean(remaining_cash)}")
    if not allocated_by_asset and buyable_positions:
        warnings.append("no_improving_purchase")

    final_total = current_total + invested_amount
    allocated_by_bucket: dict[str, float] = {}
    for asset_id, amount in allocated_by_asset.items():
        bucket = position_by_asset[asset_id].bucket
        allocated_by_bucket[bucket] = allocated_by_bucket.get(bucket, 0.0) + amount

    deviation_after = _portfolio_deviation(
        bucket_values={
            bucket: current_by_bucket.get(bucket, 0.0)
            + allocated_by_bucket.get(bucket, 0.0)
            for bucket in normalized_targets
        },
        total_value=final_total,
        target_weights=normalized_targets,
    )
    if deviation_after > deviation_before + _EPSILON:
        raise RuntimeError("Contribution planner produced a worsening allocation.")

    orders = tuple(
        ContributionOrder(
            asset_id=asset_id,
            isin=position_by_asset[asset_id].isin,
            bucket=position_by_asset[asset_id].bucket,
            quantity=quantity_by_asset[asset_id],
            unit_price_base=position_by_asset[asset_id].unit_price_base or 0.0,
            amount_base=amount,
            weight_before=(
                current_by_asset[asset_id] / current_total
                if current_total > _EPSILON
                else 0.0
            ),
            weight_after=(
                (current_by_asset[asset_id] + amount) / final_total
                if final_total > _EPSILON
                else 0.0
            ),
        )
        for asset_id, amount in sorted(allocated_by_asset.items())
    )
    bucket_allocations = tuple(
        _build_bucket_allocation(
            bucket=bucket,
            target_weight=normalized_targets[bucket],
            current_value=current_by_bucket.get(bucket, 0.0),
            allocated_amount=allocated_by_bucket.get(bucket, 0.0),
            current_total=current_total,
            final_total=final_total,
        )
        for bucket in sorted(normalized_targets)
    )
    assumptions = (
        "contributions_only_no_sales",
        "current_positions_only",
        "remaining_cash_excluded_from_after_weights",
        "deviation_is_half_l1_bucket_distance",
        (
            "fractional_units_allowed"
            if constraints.allow_fractional_units
            else "whole_units_only"
        ),
    )
    return ContributionPlan(
        budget=_clean(budget),
        invested_amount=_clean(invested_amount),
        remaining_cash=_clean(remaining_cash),
        deviation_before=_clean(deviation_before),
        deviation_after=_clean(deviation_after),
        orders=orders,
        bucket_allocations=bucket_allocations,
        constraint_events=tuple(dict.fromkeys(events)),
        warnings=tuple(dict.fromkeys(warnings)),
        assumptions=assumptions,
    )


def _best_candidate(
    *,
    positions: Sequence[ContributionPosition],
    target_weights: Mapping[str, float],
    current_by_bucket: Mapping[str, float],
    current_by_asset: Mapping[str, float],
    allocated_by_asset: Mapping[str, float],
    current_total: float,
    remaining_budget: float,
    current_deviation: float,
    constraints: ContributionConstraints,
    events: list[str],
) -> _Candidate | None:
    invested = sum(allocated_by_asset.values())
    simulated_total = current_total + invested
    allocated_by_bucket: dict[str, float] = {}
    for position in positions:
        allocated = allocated_by_asset.get(position.asset_id, 0.0)
        allocated_by_bucket[position.bucket] = (
            allocated_by_bucket.get(position.bucket, 0.0) + allocated
        )

    candidates: list[_Candidate] = []
    unique_orders = len(allocated_by_asset)
    for position in sorted(positions, key=lambda item: (item.bucket, item.asset_id)):
        asset_id = position.asset_id
        is_new_order = asset_id not in allocated_by_asset
        if is_new_order and unique_orders >= constraints.max_orders:
            events.append("max_orders_reached")
            continue

        bucket_value = (
            current_by_bucket.get(position.bucket, 0.0)
            + allocated_by_bucket.get(position.bucket, 0.0)
        )
        target_weight = target_weights[position.bucket]
        current_bucket_weight = (
            bucket_value / simulated_total
            if simulated_total > _EPSILON
            else 0.0
        )
        if current_bucket_weight >= target_weight - _EPSILON:
            continue

        target_headroom = _target_headroom(
            bucket_value=bucket_value,
            total_value=simulated_total,
            target_weight=target_weight,
        )
        cap_headroom = _asset_cap_headroom(
            asset_value=(
                current_by_asset[asset_id]
                + allocated_by_asset.get(asset_id, 0.0)
            ),
            total_value=simulated_total,
            cap=constraints.max_single_asset_weight,
        )
        if cap_headroom <= _EPSILON:
            events.append(f"max_single_asset_weight:{asset_id}")
            continue

        candidate = _candidate_for_position(
            position=position,
            is_new_order=is_new_order,
            target_headroom=target_headroom,
            cap_headroom=cap_headroom,
            asset_value_before_candidate=(
                current_by_asset[asset_id]
                + allocated_by_asset.get(asset_id, 0.0)
            ),
            remaining_budget=remaining_budget,
            current_total=current_total,
            current_by_bucket=current_by_bucket,
            allocated_by_bucket=allocated_by_bucket,
            allocated_by_asset=allocated_by_asset,
            target_weights=target_weights,
            current_deviation=current_deviation,
            constraints=constraints,
            events=events,
        )
        if candidate is not None:
            candidates.append(candidate)

    if not candidates:
        return None
    return min(
        candidates,
        key=lambda candidate: (
            round(candidate.deviation_after, 12),
            -round(candidate.amount, 12),
            candidate.position.bucket,
            candidate.position.asset_id,
        ),
    )


def _candidate_for_position(
    *,
    position: ContributionPosition,
    is_new_order: bool,
    target_headroom: float,
    cap_headroom: float,
    asset_value_before_candidate: float,
    remaining_budget: float,
    current_total: float,
    current_by_bucket: Mapping[str, float],
    allocated_by_bucket: Mapping[str, float],
    allocated_by_asset: Mapping[str, float],
    target_weights: Mapping[str, float],
    current_deviation: float,
    constraints: ContributionConstraints,
    events: list[str],
) -> _Candidate | None:
    unit_price = position.unit_price_base
    assert unit_price is not None
    maximum_amount = min(remaining_budget, cap_headroom)
    desired_amount = min(maximum_amount, target_headroom)
    minimum_amount = constraints.minimum_order_value if is_new_order else 0.0

    if constraints.allow_fractional_units:
        amount = desired_amount
        if amount < minimum_amount:
            amount = minimum_amount
        if amount > maximum_amount or amount <= 0.0:
            events.append(f"minimum_order_not_met:{position.asset_id}")
            return None
        quantity = amount / unit_price
    else:
        maximum_units_value = maximum_amount / unit_price
        desired_units_value = desired_amount / unit_price
        if not math.isfinite(maximum_units_value) or not math.isfinite(
            desired_units_value
        ):
            events.append(f"non_finite_quantity:{position.asset_id}")
            return None
        maximum_units = math.floor(maximum_units_value)
        desired_units = math.floor(desired_units_value)
        minimum_units_value = minimum_amount / unit_price
        if is_new_order and not math.isfinite(minimum_units_value):
            events.append(f"non_finite_quantity:{position.asset_id}")
            return None
        minimum_units = (
            max(1, math.ceil(minimum_units_value))
            if is_new_order
            else 1
        )
        units = desired_units
        if units < minimum_units:
            units = minimum_units
        if units > maximum_units or units <= 0:
            event = (
                "minimum_order_not_met"
                if minimum_amount > remaining_budget + _EPSILON
                else "whole_units_unaffordable"
            )
            events.append(f"{event}:{position.asset_id}")
            return None
        try:
            quantity = float(units)
        except OverflowError:
            events.append(f"non_finite_quantity:{position.asset_id}")
            return None
        amount = quantity * unit_price

    if not math.isfinite(quantity) or not math.isfinite(amount):
        events.append(f"non_finite_quantity:{position.asset_id}")
        return None
    if amount > remaining_budget or amount > cap_headroom:
        events.append(f"hard_limit_exceeded:{position.asset_id}")
        return None
    if is_new_order and amount < constraints.minimum_order_value:
        events.append(f"minimum_order_not_met:{position.asset_id}")
        return None
    if constraints.max_single_asset_weight is not None:
        final_asset_weight = (
            asset_value_before_candidate + amount
        ) / (
            current_total
            + sum(allocated_by_asset.values())
            + amount
        )
        if final_asset_weight > constraints.max_single_asset_weight:
            events.append(f"max_single_asset_weight:{position.asset_id}")
            return None

    candidate_deviation = _deviation_with_candidate(
        position=position,
        amount=amount,
        current_total=current_total,
        current_by_bucket=current_by_bucket,
        allocated_by_bucket=allocated_by_bucket,
        allocated_by_asset=allocated_by_asset,
        target_weights=target_weights,
    )
    if candidate_deviation >= current_deviation - _EPSILON:
        events.append(f"purchase_would_not_improve:{position.asset_id}")
        return None
    return _Candidate(
        position=position,
        amount=amount,
        quantity=quantity,
        deviation_after=candidate_deviation,
    )


def _deviation_with_candidate(
    *,
    position: ContributionPosition,
    amount: float,
    current_total: float,
    current_by_bucket: Mapping[str, float],
    allocated_by_bucket: Mapping[str, float],
    allocated_by_asset: Mapping[str, float],
    target_weights: Mapping[str, float],
) -> float:
    total_allocated = sum(allocated_by_asset.values())
    bucket_values = {
        bucket: current_by_bucket.get(bucket, 0.0)
        + allocated_by_bucket.get(bucket, 0.0)
        for bucket in target_weights
    }
    bucket_values[position.bucket] = (
        bucket_values.get(position.bucket, 0.0)
        + amount
    )
    return _portfolio_deviation(
        bucket_values=bucket_values,
        total_value=current_total + total_allocated + amount,
        target_weights=target_weights,
    )


def _target_headroom(
    *,
    bucket_value: float,
    total_value: float,
    target_weight: float,
) -> float:
    if target_weight >= 1.0 - _EPSILON:
        return math.inf
    return max(
        0.0,
        (target_weight * total_value - bucket_value) / (1.0 - target_weight),
    )


def _asset_cap_headroom(
    *,
    asset_value: float,
    total_value: float,
    cap: float | None,
) -> float:
    if cap is None or cap >= 1.0 - _EPSILON:
        return math.inf
    return max(0.0, (cap * total_value - asset_value) / (1.0 - cap))


def _portfolio_deviation(
    *,
    bucket_values: Mapping[str, float],
    total_value: float,
    target_weights: Mapping[str, float],
) -> float:
    if total_value <= _EPSILON:
        return 0.0
    return 0.5 * sum(
        abs(
            (bucket_values.get(bucket, 0.0) / total_value)
            - target_weights[bucket]
        )
        for bucket in sorted(target_weights)
    )


def _sum_by_bucket(
    positions: Sequence[ContributionPosition],
) -> dict[str, float]:
    values: dict[str, float] = {}
    for position in positions:
        values[position.bucket] = (
            values.get(position.bucket, 0.0) + position.market_value_base
        )
    return values


def _is_cash_position(position: ContributionPosition) -> bool:
    return (
        (position.asset_type or "").casefold() == "cash"
        or position.asset_id.casefold().startswith("degiro:cash:")
    )


def _build_bucket_allocation(
    *,
    bucket: str,
    target_weight: float,
    current_value: float,
    allocated_amount: float,
    current_total: float,
    final_total: float,
) -> BucketAllocation:
    weight_before = (
        current_value / current_total if current_total > _EPSILON else 0.0
    )
    value_after = current_value + allocated_amount
    weight_after = value_after / final_total if final_total > _EPSILON else 0.0
    return BucketAllocation(
        bucket=bucket,
        target_weight=target_weight,
        value_before=current_value,
        value_after=value_after,
        weight_before=weight_before,
        weight_after=weight_after,
        deviation_before=abs(weight_before - target_weight),
        deviation_after=abs(weight_after - target_weight),
        allocated_amount=allocated_amount,
    )


def _validate_inputs(
    positions: Sequence[ContributionPosition],
    target_weights: Mapping[str, float],
    constraints: ContributionConstraints,
) -> dict[str, float]:
    if not isinstance(target_weights, Mapping) or not target_weights:
        raise ValueError("target_weights must be a non-empty mapping.")
    targets: dict[str, float] = {}
    for raw_bucket, raw_weight in target_weights.items():
        bucket = str(raw_bucket).strip()
        if not bucket or bucket in targets:
            raise ValueError("target_weights must contain unique non-empty buckets.")
        targets[bucket] = _finite_number(
            raw_weight,
            field_name=f"target_weights.{bucket}",
            minimum=0.0,
        )
    if not math.isclose(sum(targets.values()), 1.0, abs_tol=1e-9):
        raise ValueError("target_weights must sum to 1.0.")

    _finite_number(
        constraints.contribution_amount,
        field_name="contribution_amount",
        minimum=_EPSILON,
    )
    _finite_number(
        constraints.minimum_order_value,
        field_name="minimum_order_value",
        minimum=0.0,
    )
    if not isinstance(constraints.allow_fractional_units, bool):
        raise ValueError("allow_fractional_units must be boolean.")
    if isinstance(constraints.max_orders, bool) or not isinstance(
        constraints.max_orders, int
    ):
        raise ValueError("max_orders must be an integer.")
    if constraints.max_orders <= 0:
        raise ValueError("max_orders must be greater than zero.")
    if constraints.max_single_asset_weight is not None:
        cap = _finite_number(
            constraints.max_single_asset_weight,
            field_name="max_single_asset_weight",
            minimum=0.0,
        )
        if cap > 1.0:
            raise ValueError("max_single_asset_weight cannot exceed 1.0.")

    seen_assets: set[str] = set()
    for position in positions:
        if not position.asset_id.strip() or position.asset_id in seen_assets:
            raise ValueError("positions must contain unique non-empty asset_id values.")
        seen_assets.add(position.asset_id)
        if position.bucket not in targets:
            raise ValueError(
                f"Position {position.asset_id!r} references an unknown bucket."
            )
        _finite_number(
            position.quantity,
            field_name=f"positions.{position.asset_id}.quantity",
            minimum=0.0,
        )
        _finite_number(
            position.market_value_base,
            field_name=f"positions.{position.asset_id}.market_value_base",
            minimum=0.0,
        )
        if position.unit_price_base is not None:
            _finite_number(
                position.unit_price_base,
                field_name=f"positions.{position.asset_id}.unit_price_base",
                minimum=_EPSILON,
            )
    return targets


def _finite_number(
    value: Any,
    *,
    field_name: str,
    minimum: float,
) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number.")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite number.") from exc
    if not math.isfinite(parsed) or parsed < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}.")
    return parsed


def _clean(value: float) -> float:
    parsed = float(value)
    return 0.0 if parsed == 0.0 else parsed


__all__ = [
    "BucketAllocation",
    "ContributionConstraints",
    "ContributionOrder",
    "ContributionPlan",
    "ContributionPosition",
    "plan_contribution",
]
