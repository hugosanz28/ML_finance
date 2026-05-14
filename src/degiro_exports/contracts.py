"""Compatibility imports for normalized DEGIRO dataset contracts."""

from src.normalized_contracts import (
    NormalizedDatasetContract,
    NormalizedDatasetContractError,
    validate_normalized_degiro_frame,
)

__all__ = [
    "NormalizedDatasetContract",
    "NormalizedDatasetContractError",
    "validate_normalized_degiro_frame",
]
