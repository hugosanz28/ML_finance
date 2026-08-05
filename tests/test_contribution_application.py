from __future__ import annotations

from copy import deepcopy
import json
from types import SimpleNamespace
from typing import Any

from src.application import (
    SimulateContributionRequest,
    SimulateContributionUseCase,
)
from src.config import load_settings


class _TargetsUseCase:
    def __init__(self, portfolio_targets: dict[str, Any] | None) -> None:
        self.portfolio_targets = portfolio_targets
        self.calls = 0

    def execute(self) -> SimpleNamespace:
        self.calls += 1
        return SimpleNamespace(
            portfolio_targets=deepcopy(self.portfolio_targets),
            validation_error=None,
            content_hash="sha256:test-targets",
        )


class _StateUseCase:
    def __init__(
        self,
        positions: list[dict[str, Any]],
        *,
        warnings: list[str] | None = None,
    ) -> None:
        self.positions = positions
        self.warnings = warnings or []
        self.requests: list[Any] = []

    def execute(self, request: Any) -> SimpleNamespace:
        self.requests.append(request)
        return SimpleNamespace(
            as_of_date="2026-07-30",
            base_currency="EUR",
            positions=deepcopy(self.positions),
            data_quality={"warnings": list(self.warnings)},
        )


def _targets(
    mapping: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "base_currency": "EUR",
        "target_allocation": {
            "core": 0.60,
            "satellite": 0.30,
            "cash": 0.10,
        },
        "monthly_contribution": 200.0,
        "max_single_asset_weight": 0.80,
        "rebalance_mode": "contributions_only",
        "asset_bucket_mapping": mapping
        or {
            "asset-core": "core",
            "asset-satellite": "satellite",
            "asset-cash": "cash",
        },
    }


def _positions() -> list[dict[str, Any]]:
    return [
        {
            "asset_id": "asset-core",
            "isin": "IE00CORE00001",
            "asset_type": "etf",
            "quantity": 8.0,
            "market_value_base": 800.0,
        },
        {
            "asset_id": "asset-satellite",
            "isin": "IE00SAT000001",
            "asset_type": "etf",
            "quantity": 1.0,
            "market_value_base": 100.0,
        },
        {
            "asset_id": "asset-cash",
            "isin": None,
            "asset_type": "cash",
            "quantity": 100.0,
            "market_value_base": 100.0,
        },
    ]


def _use_case(
    tmp_path: Any,
    *,
    targets: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
) -> tuple[SimulateContributionUseCase, _TargetsUseCase, _StateUseCase]:
    settings = load_settings(
        repo_root=tmp_path,
        env={},
        env_file=tmp_path / ".env.missing",
    )
    targets_use_case = _TargetsUseCase(
        targets if targets is not None else _targets()
    )
    state_use_case = _StateUseCase(
        positions if positions is not None else _positions(),
        warnings=warnings,
    )
    use_case = SimulateContributionUseCase(
        settings=settings,
        targets_use_case=targets_use_case,  # type: ignore[arg-type]
        state_use_case=state_use_case,  # type: ignore[arg-type]
    )
    return use_case, targets_use_case, state_use_case


def test_simulation_composes_server_side_state_and_targets_as_strict_json(
    tmp_path: Any,
) -> None:
    positions = _positions()
    positions_before = deepcopy(positions)
    use_case, _, state_use_case = _use_case(tmp_path, positions=positions)

    result = use_case.execute()
    payload = result.to_dict()

    assert payload["status"] == "succeeded"
    assert payload["simulation"]["invested_amount"] == 200.0
    assert payload["simulation"]["remaining_cash"] == 0.0
    assert payload["simulation"]["orders"] == [
        {
            "asset_id": "asset-satellite",
            "isin": "IE00SAT000001",
            "bucket": "satellite",
            "quantity": 2.0,
            "unit_price_base": 100.0,
            "amount_base": 200.0,
            "weight_before": 0.10,
            "weight_after": 0.25,
        }
    ]
    assert payload["simulation"]["deviation_after"] <= payload["simulation"][
        "deviation_before"
    ]
    assert payload["simulation"]["targets_content_hash"] == "sha256:test-targets"
    assert payload["assumptions"].count(
        "remaining_cash_excluded_from_after_weights"
    ) == 1
    assert state_use_case.requests[0].persist is False
    assert state_use_case.requests[0].include_positions is True
    assert state_use_case.requests[0].include_history is False
    assert positions == positions_before
    json.dumps(payload, allow_nan=False)


def test_simulation_uses_case_insensitive_isin_mapping(tmp_path: Any) -> None:
    mapping = {
        "ie00core00001": "core",
        "ie00sat000001": "satellite",
        "asset-cash": "cash",
    }
    use_case, _, _ = _use_case(tmp_path, targets=_targets(mapping))

    result = use_case.execute(
        SimulateContributionRequest(
            contribution_amount=200,
            allow_fractional_units=True,
        )
    )

    assert result.result.status == "succeeded"
    assert result.simulation is not None
    assert result.simulation["orders"][0]["asset_id"] == "asset-satellite"


def test_simulation_skips_orders_when_an_active_position_is_unmapped(
    tmp_path: Any,
) -> None:
    mapping = {
        "asset-core": "core",
        "asset-cash": "cash",
    }
    use_case, _, _ = _use_case(tmp_path, targets=_targets(mapping))

    result = use_case.execute()

    assert result.result.status == "partial"
    assert result.simulation is not None
    assert result.simulation["orders"] == []
    assert result.simulation["remaining_cash"] == 200.0
    assert "missing_asset_bucket_mapping:asset-satellite" in result.result.warnings


def test_simulation_rejects_conflicting_asset_and_isin_mappings(
    tmp_path: Any,
) -> None:
    mapping = {
        "asset-core": "core",
        "asset-satellite": "satellite",
        "IE00SAT000001": "core",
        "asset-cash": "cash",
    }
    use_case, _, _ = _use_case(tmp_path, targets=_targets(mapping))

    result = use_case.execute()

    assert result.result.status == "failed"
    assert result.simulation is None
    assert "Conflicting asset_bucket_mapping" in result.result.message


def test_simulation_propagates_state_and_stale_mapping_warnings(
    tmp_path: Any,
) -> None:
    targets = _targets()
    targets["asset_bucket_mapping"]["unused-id"] = "core"
    use_case, _, _ = _use_case(
        tmp_path,
        targets=targets,
        warnings=["state_projection_warning"],
    )

    result = use_case.execute()

    assert result.result.status == "partial"
    assert "state_projection_warning" in result.result.warnings
    assert "unused_mapping_entry:unused-id" in result.result.warnings
    assert result.simulation is not None
    assert result.simulation["orders"]


def test_simulation_rejects_invalid_request_before_reading_local_state(
    tmp_path: Any,
) -> None:
    use_case, targets_use_case, state_use_case = _use_case(tmp_path)

    result = use_case.execute(
        SimulateContributionRequest(contribution_amount=float("nan"))
    )

    assert result.result.status == "failed"
    assert result.simulation is None
    assert targets_use_case.calls == 0
    assert state_use_case.requests == []
    json.dumps(result.to_dict(), allow_nan=False)


def test_empty_portfolio_returns_partial_without_orders(tmp_path: Any) -> None:
    use_case, _, _ = _use_case(tmp_path, positions=[])

    result = use_case.execute()

    assert result.result.status == "partial"
    assert "no_current_positions" in result.result.warnings
    assert result.simulation is not None
    assert result.simulation["orders"] == []
    assert result.simulation["remaining_cash"] == 200.0
