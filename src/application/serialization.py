"""Serialization helpers shared by application read models."""

from __future__ import annotations

from datetime import date
from math import isfinite
from typing import Any

import pandas as pd


def json_ready_value(value: Any) -> Any:
    """Recursively convert common domain/dataframe scalars to strict JSON values."""
    if isinstance(value, dict):
        return {str(key): json_ready_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_ready_value(item) for item in value]
    if isinstance(value, (date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, float) and not isfinite(value):
        return None
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item") and callable(value.item):
        return json_ready_value(value.item())
    return value


__all__ = ["json_ready_value"]
