"""Shared validation contracts for normalized local datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import pandas as pd


@dataclass(frozen=True)
class NormalizedDatasetContract:
    """Column contract for one normalized dataset."""

    name: str
    required_columns: tuple[str, ...]
    non_null_columns: tuple[str, ...] = ()


NORMALIZED_DEGIRO_CONTRACTS: Mapping[str, NormalizedDatasetContract] = {
    "transactions": NormalizedDatasetContract(
        name="transactions",
        required_columns=(
            "transaction_id",
            "broker",
            "asset_id",
            "asset_name",
            "asset_type",
            "isin",
            "trade_date",
            "transaction_type",
            "quantity",
            "unit_price",
            "gross_amount",
            "transaction_currency",
            "gross_amount_base",
            "base_currency",
            "net_cash_amount_local",
            "net_cash_amount_base",
            "source_file",
            "source_row",
        ),
        non_null_columns=(
            "transaction_id",
            "broker",
            "asset_id",
            "trade_date",
            "transaction_type",
            "quantity",
            "transaction_currency",
            "base_currency",
            "source_file",
            "source_row",
        ),
    ),
    "assets": NormalizedDatasetContract(
        name="assets",
        required_columns=(
            "asset_id",
            "asset_name",
            "asset_type",
            "isin",
            "trading_currency",
            "first_seen_date",
            "last_seen_date",
            "source_file",
        ),
        non_null_columns=("asset_id", "asset_name", "asset_type", "trading_currency", "source_file"),
    ),
    "cash_movements": NormalizedDatasetContract(
        name="cash_movements",
        required_columns=(
            "cash_movement_id",
            "broker",
            "asset_id",
            "asset_name",
            "asset_type",
            "isin",
            "movement_date",
            "movement_type",
            "amount",
            "movement_currency",
            "base_currency",
            "amount_base",
            "source_file",
            "source_row",
        ),
        non_null_columns=(
            "cash_movement_id",
            "broker",
            "movement_date",
            "movement_type",
            "amount",
            "movement_currency",
            "base_currency",
            "source_file",
            "source_row",
        ),
    ),
    "portfolio_snapshots": NormalizedDatasetContract(
        name="portfolio_snapshots",
        required_columns=(
            "snapshot_id",
            "broker",
            "snapshot_date",
            "snapshot_source",
            "asset_id",
            "asset_name",
            "asset_type",
            "isin",
            "quantity",
            "position_currency",
            "base_currency",
            "market_value_base",
            "source_file",
            "source_row",
        ),
        non_null_columns=(
            "snapshot_id",
            "broker",
            "snapshot_date",
            "snapshot_source",
            "asset_id",
            "quantity",
            "position_currency",
            "base_currency",
            "source_file",
            "source_row",
        ),
    ),
}


class NormalizedDatasetContractError(ValueError):
    """Raised when a normalized dataset does not match its contract."""


def validate_normalized_degiro_frame(
    dataset_name: str,
    frame: pd.DataFrame,
    *,
    source: str | None = None,
) -> pd.DataFrame:
    """Validate a normalized DEGIRO frame and return it unchanged."""
    try:
        contract = NORMALIZED_DEGIRO_CONTRACTS[dataset_name]
    except KeyError as exc:
        raise NormalizedDatasetContractError(f"Unknown normalized DEGIRO dataset: {dataset_name}") from exc

    missing_columns = [column for column in contract.required_columns if column not in frame.columns]
    if missing_columns:
        raise NormalizedDatasetContractError(
            _format_contract_error(
                contract.name,
                source=source,
                detail=f"missing required columns: {', '.join(missing_columns)}",
            )
        )

    if not frame.empty:
        null_columns = [
            column
            for column in contract.non_null_columns
            if frame[column].isna().any()
            or frame[column].map(lambda value: isinstance(value, str) and not value.strip()).any()
        ]
        if null_columns:
            raise NormalizedDatasetContractError(
                _format_contract_error(
                    contract.name,
                    source=source,
                    detail=f"null or blank values in required fields: {', '.join(null_columns)}",
                )
            )

    return frame


def _format_contract_error(dataset_name: str, *, source: str | None, detail: str) -> str:
    location = f" in {source}" if source else ""
    return f"Invalid normalized DEGIRO {dataset_name} dataset{location}: {detail}."
