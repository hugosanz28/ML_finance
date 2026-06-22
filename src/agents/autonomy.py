"""Helpers for bounded agent autonomy metadata."""

from __future__ import annotations

from typing import Any, Mapping, Sequence


def autonomy_metadata(
    *,
    agent_plan: Sequence[str],
    allowed_actions: Sequence[str],
    selected_actions: Sequence[str] = (),
    skipped_actions: Sequence[Mapping[str, Any]] = (),
    applied_constraints: Sequence[str] = (),
    decision_basis: Sequence[str] = (),
) -> dict[str, Any]:
    """Return the standard metadata block for bounded agent decisions."""
    return {
        "agent_plan": tuple(str(item) for item in agent_plan),
        "allowed_actions": tuple(str(item) for item in allowed_actions),
        "selected_actions": tuple(str(item) for item in selected_actions),
        "skipped_actions": tuple(dict(item) for item in skipped_actions),
        "applied_constraints": tuple(str(item) for item in applied_constraints),
        "decision_basis": tuple(str(item) for item in decision_basis),
    }


def skipped_action(action: str, reason: str) -> dict[str, str]:
    """Build a compact skipped-action record."""
    return {"action": action, "reason": reason}


__all__ = ["autonomy_metadata", "skipped_action"]
