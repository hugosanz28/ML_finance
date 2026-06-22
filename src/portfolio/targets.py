"""Portfolio target configuration contract and loader."""

from __future__ import annotations

from dataclasses import dataclass
import json
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

    def target_weights(self) -> dict[str, float]:
        """Return target allocation weights normalized to decimal units."""
        return dict(self.target_allocation)

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
    target_allocation = data.get("target_allocation") or data.get("target_weights")
    if not isinstance(target_allocation, Mapping) or not target_allocation:
        raise ValueError("Portfolio targets require a non-empty target_allocation mapping.")

    return PortfolioTargets(
        base_currency=str(data.get("base_currency") or default_base_currency).upper(),
        monthly_contribution=_optional_float(data.get("monthly_contribution")),
        risk_profile=_optional_str(data.get("risk_profile")),
        target_allocation=_normalize_weights(target_allocation, field_name="target_allocation"),
        max_single_asset_weight=_optional_weight(data.get("max_single_asset_weight"), field_name="max_single_asset_weight"),
        max_sector_weight=_optional_weight(data.get("max_sector_weight"), field_name="max_sector_weight"),
        rebalance_mode=_optional_str(data.get("rebalance_mode")),
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
    if ":" not in line:
        raise ValueError(f"Invalid YAML line: {line}")
    key, value = line.split(":", 1)
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
        weights[name] = _required_weight(value, field_name=f"{field_name}.{name}")
    return weights


def _required_weight(value: Any, *, field_name: str) -> float:
    parsed = _optional_weight(value, field_name=field_name)
    if parsed is None:
        raise ValueError(f"{field_name} must be numeric.")
    return parsed


def _optional_weight(value: Any, *, field_name: str) -> float | None:
    parsed = _optional_float(value)
    if parsed is None:
        return None
    if parsed < 0:
        raise ValueError(f"{field_name} cannot be negative.")
    return parsed / 100.0 if parsed > 1.0 else parsed


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected numeric value, got {value!r}") from exc


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
