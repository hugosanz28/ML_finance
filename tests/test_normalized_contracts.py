import pandas as pd
import pytest

from src.degiro_exports import NormalizedDatasetContractError, validate_normalized_degiro_frame


def test_validate_normalized_degiro_frame_rejects_missing_required_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "transaction_id": "tx-1",
                "asset_id": "asset-1",
            }
        ]
    )

    with pytest.raises(NormalizedDatasetContractError, match="missing required columns"):
        validate_normalized_degiro_frame("transactions", frame, source="transactions.parquet")


def test_validate_normalized_degiro_frame_rejects_blank_required_values() -> None:
    frame = pd.DataFrame(
        [
            {
                "asset_id": " ",
                "asset_name": "Example ETF",
                "asset_type": "etf",
                "isin": "NL0000000000",
                "trading_currency": "EUR",
                "first_seen_date": "2026-01-01",
                "last_seen_date": "2026-01-01",
                "source_file": "transactions.csv",
            }
        ]
    )

    with pytest.raises(NormalizedDatasetContractError, match="null or blank"):
        validate_normalized_degiro_frame("assets", frame)
