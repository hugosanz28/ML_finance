"""Application boundary for deterministic contribution simulations."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Any, Mapping

from src.application.portfolio_state import (
    GetPortfolioStateRequest,
    GetPortfolioStateUseCase,
)
from src.application.portfolio_targets import ReadPortfolioTargetsUseCase
from src.application.serialization import json_ready_value
from src.application.types import ApplicationResult, ApplicationStatus
from src.config import Settings, get_settings
from src.portfolio.contribution_planner import (
    ContributionConstraints,
    ContributionPosition,
    plan_contribution,
)


_BASE_ASSUMPTIONS = (
    "contributions_only_no_sales",
    "current_positions_only",
    "no_fees_taxes_slippage",
    "implied_unit_price_from_market_value",
    "deterministic_identifier_tiebreak",
    "remaining_cash_excluded_from_after_weights",
)


@dataclass(frozen=True)
class SimulateContributionRequest:
    """User-controlled simulation parameters; state and targets stay server-side."""

    contribution_amount: float | None = None
    allow_fractional_units: bool = False
    minimum_order_value: float = 0.0
    max_orders: int = 4
    as_of_date: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SimulateContributionResult:
    result: ApplicationResult
    simulation: dict[str, Any] | None
    assumptions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a strict JSON-ready response envelope."""
        return json_ready_value(
            {
                "status": self.result.status,
                "message": self.result.message,
                "warnings": list(self.result.warnings),
                "artifacts": dict(self.result.artifacts),
                "assumptions": list(self.assumptions),
                "simulation": self.simulation,
            }
        )


@dataclass(frozen=True)
class _PositionAdaptation:
    positions: tuple[ContributionPosition, ...]
    warnings: tuple[str, ...]
    constraint_events: tuple[str, ...]
    incomplete_mapping: bool = False
    error: str | None = None


class SimulateContributionUseCase:
    """Compose validated local state and targets around the pure planner."""

    name = "simulate_contribution"

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        state_use_case: GetPortfolioStateUseCase | None = None,
        targets_use_case: ReadPortfolioTargetsUseCase | None = None,
    ) -> None:
        self.settings = get_settings() if settings is None else settings
        self.state_use_case = state_use_case or GetPortfolioStateUseCase(
            settings=self.settings
        )
        self.targets_use_case = targets_use_case or ReadPortfolioTargetsUseCase(
            settings=self.settings
        )

    def execute(
        self,
        request: SimulateContributionRequest | None = None,
    ) -> SimulateContributionResult:
        resolved_request = request or SimulateContributionRequest()
        request_error = _validate_request(resolved_request)
        if request_error:
            return self._failed(request_error)

        targets_state = self.targets_use_case.execute()
        if targets_state.validation_error:
            return self._failed(
                f"Portfolio targets are invalid: {targets_state.validation_error}"
            )
        targets = targets_state.portfolio_targets
        if targets is None:
            return self._failed("Portfolio targets are not configured.")

        target_weights = targets.get("target_allocation")
        raw_mapping = targets.get("asset_bucket_mapping")
        if not isinstance(target_weights, Mapping) or not target_weights:
            return self._failed("Portfolio targets do not contain target_allocation.")
        if not isinstance(raw_mapping, Mapping) or not raw_mapping:
            return self._failed(
                "Portfolio targets require a non-empty asset_bucket_mapping."
            )

        contribution_amount = (
            resolved_request.contribution_amount
            if resolved_request.contribution_amount is not None
            else targets.get("monthly_contribution")
        )
        contribution_error = _validate_positive_number(
            contribution_amount,
            field_name="contribution_amount",
        )
        if contribution_error:
            return self._failed(contribution_error)
        assert contribution_amount is not None
        budget = float(contribution_amount)

        assumptions = list(_BASE_ASSUMPTIONS)
        rebalance_mode = targets.get("rebalance_mode")
        if rebalance_mode is None:
            assumptions.append("rebalance_mode_defaulted_to_contributions_only")
        elif rebalance_mode != "contributions_only":
            return self._failed(
                "Portfolio targets must use rebalance_mode='contributions_only'."
            )

        try:
            state = self.state_use_case.execute(
                GetPortfolioStateRequest(
                    persist=False,
                    include_positions=True,
                    include_history=False,
                    as_of_date=resolved_request.as_of_date,
                )
            )
        except (FileNotFoundError, ValueError) as exc:
            return self._failed(f"Portfolio state is unavailable: {exc}")

        target_currency = str(targets.get("base_currency") or "").upper()
        if target_currency and target_currency != state.base_currency.upper():
            return self._failed(
                "Portfolio state and targets use different base currencies."
            )

        adaptation = _adapt_positions(
            state.positions,
            asset_bucket_mapping=raw_mapping,
        )
        if adaptation.error:
            return self._failed(adaptation.error)

        constraints_payload = {
            "contribution_amount": budget,
            "allow_fractional_units": resolved_request.allow_fractional_units,
            "minimum_order_value": float(resolved_request.minimum_order_value),
            "max_orders": resolved_request.max_orders,
            "max_single_asset_weight": targets.get("max_single_asset_weight"),
        }
        state_warnings = tuple(
            str(warning)
            for warning in (state.data_quality.get("warnings") or [])
        )
        warnings = tuple(dict.fromkeys((*state_warnings, *adaptation.warnings)))

        if adaptation.incomplete_mapping:
            simulation = _empty_simulation(
                as_of_date=state.as_of_date,
                base_currency=state.base_currency,
                targets_content_hash=targets_state.content_hash,
                budget=budget,
                constraints=constraints_payload,
                constraint_events=adaptation.constraint_events,
            )
            return self._completed(
                status="partial",
                message=(
                    "Contribution simulation skipped because active positions "
                    "are incomplete or not fully mapped."
                ),
                warnings=warnings,
                assumptions=tuple(assumptions),
                simulation=simulation,
                targets_hash=targets_state.content_hash,
            )

        constraints = ContributionConstraints(
            contribution_amount=budget,
            allow_fractional_units=resolved_request.allow_fractional_units,
            minimum_order_value=float(resolved_request.minimum_order_value),
            max_orders=resolved_request.max_orders,
            max_single_asset_weight=_optional_float(
                targets.get("max_single_asset_weight")
            ),
        )
        try:
            plan = plan_contribution(
                adaptation.positions,
                {str(key): float(value) for key, value in target_weights.items()},
                constraints,
            )
        except ValueError as exc:
            return self._failed(f"Contribution simulation is invalid: {exc}")

        plan_payload = plan.to_dict()
        plan_warnings = tuple(
            str(warning) for warning in (plan_payload.pop("warnings", []) or [])
        )
        plan_assumptions = tuple(
            str(assumption)
            for assumption in (plan_payload.pop("assumptions", []) or [])
        )
        combined_warnings = tuple(
            dict.fromkeys((*warnings, *plan_warnings))
        )
        combined_assumptions = tuple(
            dict.fromkeys((*assumptions, *plan_assumptions))
        )
        existing_events = tuple(
            str(event)
            for event in (plan_payload.get("constraint_events") or [])
        )
        plan_payload["constraint_events"] = list(
            dict.fromkeys((*adaptation.constraint_events, *existing_events))
        )
        simulation = json_ready_value(
            {
                "as_of_date": state.as_of_date,
                "base_currency": state.base_currency,
                "targets_content_hash": targets_state.content_hash,
                "constraints": constraints_payload,
                **plan_payload,
            }
        )
        has_incomplete_data = (
            bool(state_warnings)
            or any(
                warning.startswith("unusable_unit_price:")
                for warning in adaptation.warnings
            )
            or bool(
                {"no_current_positions", "no_buyable_positions", "planning_round_limit_reached"}
                & set(plan_warnings)
            )
            or any(
                event.startswith("non_finite_quantity:")
                for event in plan_payload["constraint_events"]
            )
        )
        status: ApplicationStatus = (
            "partial" if has_incomplete_data else "succeeded"
        )
        message = (
            "Contribution simulation completed with data warnings."
            if status == "partial"
            else "Contribution simulation completed."
        )
        return self._completed(
            status=status,
            message=message,
            warnings=combined_warnings,
            assumptions=combined_assumptions,
            simulation=simulation,
            targets_hash=targets_state.content_hash,
        )

    def _completed(
        self,
        *,
        status: ApplicationStatus,
        message: str,
        warnings: tuple[str, ...],
        assumptions: tuple[str, ...],
        simulation: dict[str, Any],
        targets_hash: str,
    ) -> SimulateContributionResult:
        return SimulateContributionResult(
            result=ApplicationResult(
                name=self.name,
                status=status,
                message=message,
                warnings=warnings,
                artifacts={
                    "as_of_date": str(simulation.get("as_of_date") or ""),
                    "budget": _artifact_float(simulation.get("budget")),
                    "invested_amount": _artifact_float(
                        simulation.get("invested_amount")
                    ),
                    "remaining_cash": _artifact_float(
                        simulation.get("remaining_cash")
                    ),
                    "order_count": len(simulation.get("orders") or []),
                    "targets_hash": targets_hash,
                },
            ),
            simulation=simulation,
            assumptions=assumptions,
        )

    def _failed(self, message: str) -> SimulateContributionResult:
        return SimulateContributionResult(
            result=ApplicationResult(
                name=self.name,
                status="failed",
                message=message,
            ),
            simulation=None,
            assumptions=_BASE_ASSUMPTIONS,
        )


def _validate_request(request: SimulateContributionRequest) -> str | None:
    if not isinstance(request.allow_fractional_units, bool):
        return "allow_fractional_units must be boolean."
    minimum_error = _validate_non_negative_number(
        request.minimum_order_value,
        field_name="minimum_order_value",
    )
    if minimum_error:
        return minimum_error
    if isinstance(request.max_orders, bool) or not isinstance(
        request.max_orders, int
    ):
        return "max_orders must be an integer."
    if request.max_orders <= 0:
        return "max_orders must be greater than zero."
    if request.contribution_amount is not None:
        return _validate_positive_number(
            request.contribution_amount,
            field_name="contribution_amount",
        )
    return None


def _validate_positive_number(value: Any, *, field_name: str) -> str | None:
    error = _validate_finite_number(value, field_name=field_name)
    if error:
        return error
    assert value is not None
    if float(value) <= 0:
        return f"{field_name} must be greater than zero."
    return None


def _validate_non_negative_number(value: Any, *, field_name: str) -> str | None:
    error = _validate_finite_number(value, field_name=field_name)
    if error:
        return error
    assert value is not None
    if float(value) < 0:
        return f"{field_name} cannot be negative."
    return None


def _validate_finite_number(value: Any, *, field_name: str) -> str | None:
    if value is None or isinstance(value, bool):
        return f"{field_name} must be a finite number."
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return f"{field_name} must be a finite number."
    if not math.isfinite(parsed):
        return f"{field_name} must be a finite number."
    return None


def _adapt_positions(
    raw_positions: list[dict[str, Any]],
    *,
    asset_bucket_mapping: Mapping[Any, Any],
) -> _PositionAdaptation:
    mapping = {
        str(identifier): str(bucket)
        for identifier, bucket in asset_bucket_mapping.items()
    }
    casefold_mapping: dict[str, list[tuple[str, str]]] = {}
    for identifier, bucket in mapping.items():
        casefold_mapping.setdefault(identifier.casefold(), []).append(
            (identifier, bucket)
        )

    positions: list[ContributionPosition] = []
    warnings: list[str] = []
    constraint_events: list[str] = []
    used_mapping_keys: set[str] = set()
    incomplete_mapping = False

    for raw in [dict(position) for position in raw_positions]:
        asset_id = str(raw.get("asset_id") or "").strip()
        value = _optional_float(raw.get("market_value_base"))
        if not asset_id:
            if value is not None and value > 0:
                warnings.append("missing_asset_id")
                constraint_events.append("position_excluded:missing_asset_id")
                incomplete_mapping = True
            continue
        if value is None or value < 0:
            warnings.append(f"invalid_market_value:{asset_id}")
            constraint_events.append(f"position_excluded:{asset_id}")
            incomplete_mapping = True
            continue
        if value <= 1e-9:
            continue

        isin = str(raw.get("isin") or "").strip() or None
        candidate_entries: list[tuple[str, str]] = []
        if asset_id in mapping:
            candidate_entries.append((asset_id, mapping[asset_id]))
        if isin:
            candidate_entries.extend(casefold_mapping.get(isin.casefold(), []))
        candidate_entries = list(dict.fromkeys(candidate_entries))
        candidate_buckets = {bucket for _, bucket in candidate_entries}
        if len(candidate_buckets) > 1:
            return _PositionAdaptation(
                positions=(),
                warnings=(),
                constraint_events=(),
                error=(
                    f"Conflicting asset_bucket_mapping entries for {asset_id!r}."
                ),
            )
        if not candidate_entries:
            warnings.append(f"missing_asset_bucket_mapping:{asset_id}")
            constraint_events.append(f"position_unmapped:{asset_id}")
            incomplete_mapping = True
            continue
        bucket = candidate_entries[0][1]
        used_mapping_keys.update(key for key, _ in candidate_entries)
        if len(candidate_entries) > 1:
            warnings.append(f"duplicate_mapping_alias:{asset_id}")

        asset_type = str(raw.get("asset_type") or "").strip() or None
        quantity = _optional_float(raw.get("quantity"))
        unit_price: float | None = None
        normalized_quantity = quantity if quantity is not None and quantity >= 0 else 0.0
        is_cash = (
            (asset_type or "").casefold() == "cash"
            or asset_id.casefold().startswith("degiro:cash:")
        )
        if not is_cash:
            if quantity is not None and quantity > 0:
                unit_price = value / quantity
            else:
                warnings.append(f"unusable_unit_price:{asset_id}")
                constraint_events.append(f"asset_not_buyable:{asset_id}")

        positions.append(
            ContributionPosition(
                asset_id=asset_id,
                isin=isin,
                bucket=bucket,
                quantity=normalized_quantity,
                market_value_base=value,
                unit_price_base=unit_price,
                asset_type=asset_type,
            )
        )

    for identifier in sorted(set(mapping) - used_mapping_keys):
        warnings.append(f"unused_mapping_entry:{identifier}")
    return _PositionAdaptation(
        positions=tuple(positions),
        warnings=tuple(warnings),
        constraint_events=tuple(constraint_events),
        incomplete_mapping=incomplete_mapping,
    )


def _empty_simulation(
    *,
    as_of_date: str,
    base_currency: str,
    targets_content_hash: str,
    budget: float,
    constraints: dict[str, Any],
    constraint_events: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "as_of_date": as_of_date,
        "base_currency": base_currency,
        "targets_content_hash": targets_content_hash,
        "budget": budget,
        "invested_amount": 0.0,
        "remaining_cash": budget,
        "deviation_before": None,
        "deviation_after": None,
        "orders": [],
        "bucket_allocations": [],
        "constraints": constraints,
        "constraint_events": list(constraint_events),
    }


def _optional_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _artifact_float(value: Any) -> float | None:
    parsed = _optional_float(value)
    return parsed


__all__ = [
    "SimulateContributionRequest",
    "SimulateContributionResult",
    "SimulateContributionUseCase",
]
