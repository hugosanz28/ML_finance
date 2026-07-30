"""Portfolio target configuration contract and loader."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from pathlib import Path
from typing import Any, Mapping

from src.config import Settings, get_settings


@dataclass(frozen=True)
class PortfolioTargets:
    """Structured portfolio objectives used by reports and agents."""

    base_currency: str
    target_allocation: Mapping[str, float]
    monthly_contribution: float | None = None
    risk_profile: str | None = None
    max_single_asset_weight: float | None = None
    max_sector_weight: float | None = None
    rebalance_mode: str | None = None
    asset_bucket_mapping: Mapping[str, str] = field(default_factory=dict)

    def target_weights(self) -> dict[str, float]:
        """Return target allocation weights normalized to decimal units."""
        return dict(self.target_allocation)

    def to_storage_mapping(self) -> dict[str, Any]:
        """Return the complete canonical mapping used at application boundaries."""
        return {
            "base_currency": self.base_currency,
            "target_allocation": dict(self.target_allocation),
            "monthly_contribution": self.monthly_contribution,
            "risk_profile": self.risk_profile,
            "max_single_asset_weight": self.max_single_asset_weight,
            "max_sector_weight": self.max_sector_weight,
            "rebalance_mode": self.rebalance_mode,
            "asset_bucket_mapping": dict(self.asset_bucket_mapping),
        }

    def to_agent_payload(self) -> dict[str, Any]:
        """Return a JSON-ready payload for agent inputs."""
        payload: dict[str, Any] = {
            "base_currency": self.base_currency,
            "target_allocation": dict(self.target_allocation),
            "target_weights": dict(self.target_allocation),
        }
        for key, value in (
            ("monthly_contribution", self.monthly_contribution),
            ("risk_profile", self.risk_profile),
            ("max_single_asset_weight", self.max_single_asset_weight),
            ("max_sector_weight", self.max_sector_weight),
            ("rebalance_mode", self.rebalance_mode),
        ):
            if value is not None:
                payload[key] = value
        return payload


def load_portfolio_targets(
    *,
    settings: Settings | None = None,
    path: str | Path | None = None,
    required: bool = False,
) -> PortfolioTargets | None:
    """Load portfolio target objectives from JSON or a simple YAML file."""
    resolved_settings = get_settings() if settings is None else settings
    target_path = resolved_settings.portfolio_targets_path if path is None else Path(path).expanduser().resolve()
    if not target_path.exists():
        if required:
            raise FileNotFoundError(f"Portfolio targets config not found: {target_path}")
        return None
    raw = target_path.read_text(encoding="utf-8")
    data = _parse_structured_text(raw, source_path=target_path)
    return portfolio_targets_from_mapping(data, default_base_currency=resolved_settings.default_currency)


def portfolio_targets_from_mapping(
    data: Mapping[str, Any],
    *,
    default_base_currency: str = "EUR",
) -> PortfolioTargets:
    """Validate a mapping into the portfolio targets contract."""
    target_allocation = data.get("target_allocation")
    legacy_target_weights = data.get("target_weights")
    if target_allocation is None:
        target_allocation = legacy_target_weights
    if not isinstance(target_allocation, Mapping) or not target_allocation:
        raise ValueError("Portfolio targets require a non-empty target_allocation mapping.")

    normalized_allocation = _normalize_weights(target_allocation, field_name="target_allocation")
    if legacy_target_weights is not None and data.get("target_allocation") is not None:
        if not isinstance(legacy_target_weights, Mapping):
            raise ValueError("target_weights must be a mapping when provided.")
        normalized_legacy = _normalize_weights(legacy_target_weights, field_name="target_weights")
        if normalized_legacy != normalized_allocation:
            raise ValueError("target_allocation and target_weights must contain the same weights.")
    base_currency = str(data.get("base_currency") or default_base_currency).strip().upper()
    if not base_currency:
        raise ValueError("base_currency cannot be empty.")
    monthly_contribution = _optional_float(
        data.get("monthly_contribution"),
        field_name="monthly_contribution",
    )
    if monthly_contribution is not None and monthly_contribution < 0:
        raise ValueError("monthly_contribution cannot be negative.")

    return PortfolioTargets(
        base_currency=base_currency,
        monthly_contribution=monthly_contribution,
        risk_profile=_optional_str(data.get("risk_profile")),
        target_allocation=normalized_allocation,
        max_single_asset_weight=_optional_weight(data.get("max_single_asset_weight"), field_name="max_single_asset_weight"),
        max_sector_weight=_optional_weight(data.get("max_sector_weight"), field_name="max_sector_weight"),
        rebalance_mode=_optional_str(data.get("rebalance_mode")),
        asset_bucket_mapping=_normalize_asset_bucket_mapping(
            data.get("asset_bucket_mapping"),
            valid_buckets=set(normalized_allocation),
        ),
    )


def _parse_structured_text(raw: str, *, source_path: Path) -> Mapping[str, Any]:
    text = raw.strip()
    if not text:
        raise ValueError(f"Portfolio targets config is empty: {source_path}")
    if source_path.suffix.lower() == ".json" or text.startswith("{"):
        parsed = json.loads(text)
    else:
        parsed = _parse_simple_yaml(text)
    if not isinstance(parsed, Mapping):
        raise ValueError(f"Portfolio targets config must be an object: {source_path}")
    return parsed


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    current_key: str | None = None
    for raw_line in text.splitlines():
        line_without_comment = raw_line.split("#", 1)[0].rstrip()
        if not line_without_comment.strip():
            continue
        if line_without_comment[:1].isspace():
            if current_key is None:
                raise ValueError(f"Nested YAML value without parent key: {raw_line}")
            child_key, child_value = _split_yaml_key_value(line_without_comment.strip())
            nested = result.setdefault(current_key, {})
            if not isinstance(nested, dict):
                raise ValueError(f"YAML key cannot be both scalar and mapping: {current_key}")
            nested[child_key] = _parse_scalar(child_value)
            continue
        key, value = _split_yaml_key_value(line_without_comment)
        if value == "":
            result[key] = {}
            current_key = key
        else:
            result[key] = _parse_scalar(value)
            current_key = None
    return result


def _split_yaml_key_value(line: str) -> tuple[str, str]:
    separator_index = next(
        (
            index
            for index, character in enumerate(line)
            if character == ":" and (index + 1 == len(line) or line[index + 1].isspace())
        ),
        -1,
    )
    # Preserve the permissive legacy ``key:value`` syntax when no YAML-style
    # separator exists, while allowing canonical asset IDs to contain colons.
    if separator_index < 0:
        separator_index = line.find(":")
    if separator_index < 0:
        raise ValueError(f"Invalid YAML line: {line}")
    key, value = line[:separator_index], line[separator_index + 1 :]
    key = key.strip()
    if not key:
        raise ValueError(f"Invalid empty YAML key: {line}")
    return key, value.strip()


def _parse_scalar(value: str) -> Any:
    if value == "":
        return ""
    lowered = value.lower()
    if lowered in {"null", "none"}:
        return None
    if lowered in {"true", "false"}:
        return lowered == "true"
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _normalize_weights(raw_weights: Mapping[str, Any], *, field_name: str) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in raw_weights.items():
        name = str(key).strip()
        if not name:
            raise ValueError(f"{field_name} contains an empty key.")
        if name in weights:
            raise ValueError(f"{field_name} contains duplicate key {name!r} after normalization.")
        parsed = _optional_float(value, field_name=f"{field_name}.{name}")
        if parsed is None:
            raise ValueError(f"{field_name}.{name} must be numeric.")
        if parsed < 0:
            raise ValueError(f"{field_name}.{name} cannot be negative.")
        weights[name] = parsed

    total = sum(weights.values())
    if math.isclose(total, 1.0, rel_tol=0.0, abs_tol=1e-9):
        divisor = 1.0
    elif math.isclose(total, 100.0, rel_tol=0.0, abs_tol=1e-9):
        divisor = 100.0
    else:
        raise ValueError(f"{field_name} weights must sum to 1.0 or 100.0.")
    return {name: value / divisor for name, value in weights.items()}


def _normalize_asset_bucket_mapping(
    raw_mapping: Any,
    *,
    valid_buckets: set[str],
) -> dict[str, str]:
    if raw_mapping is None:
        return {}
    if not isinstance(raw_mapping, Mapping):
        raise ValueError("asset_bucket_mapping must be a mapping.")

    normalized: dict[str, str] = {}
    for raw_identifier, raw_bucket in raw_mapping.items():
        if not isinstance(raw_identifier, str) or not isinstance(raw_bucket, str):
            raise ValueError("asset_bucket_mapping keys and values must be strings.")
        identifier = raw_identifier.strip()
        bucket = raw_bucket.strip()
        if not identifier:
            raise ValueError("asset_bucket_mapping contains an empty identifier.")
        if not bucket:
            raise ValueError(f"asset_bucket_mapping[{identifier!r}] contains an empty bucket.")
        if identifier in normalized:
            raise ValueError(
                f"asset_bucket_mapping contains duplicate identifier {identifier!r} after normalization."
            )
        if bucket not in valid_buckets:
            raise ValueError(
                f"asset_bucket_mapping[{identifier!r}] references unknown target bucket {bucket!r}."
            )
        normalized[identifier] = bucket
    return normalized


def _optional_weight(value: Any, *, field_name: str) -> float | None:
    parsed = _optional_float(value, field_name=field_name)
    if parsed is None:
        return None
    if parsed < 0:
        raise ValueError(f"{field_name} cannot be negative.")
    normalized = parsed / 100.0 if parsed > 1.0 else parsed
    if normalized > 1.0:
        raise ValueError(f"{field_name} cannot exceed 1.0 (or 100).")
    return normalized


def _optional_float(value: Any, *, field_name: str = "value") -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric, not boolean.")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric, got {value!r}.") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be finite.")
    return parsed


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "PortfolioTargets",
    "load_portfolio_targets",
    "portfolio_targets_from_mapping",
]
