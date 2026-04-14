from __future__ import annotations

from datetime import date
from pathlib import Path
import uuid

import pandas as pd
import pytest

from src.portfolio import reconstruct_positions_by_date, reconstruct_positions_from_normalized_degiro


def test_reconstruct_positions_by_date_builds_daily_history_from_buys_and_sells() -> None:
    transactions = pd.DataFrame(
        [
            {
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "isin": "IE0000000001",
                "trade_date": "2026-01-01",
                "transaction_type": "BUY",
                "quantity": 10,
                "source_row": 1,
            },
            {
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "isin": "IE0000000001",
                "trade_date": "2026-01-03",
                "transaction_type": "BUY",
                "quantity": 5,
                "source_row": 2,
            },
            {
                "asset_id": "asset-a",
                "asset_name": "Asset A",
                "asset_type": "etf",
                "isin": "IE0000000001",
                "trade_date": "2026-01-05",
                "transaction_type": "SELL",
                "quantity": 7,
                "source_row": 3,
            },
        ]
    )

    reconstructed = reconstruct_positions_by_date(transactions)

    positions = reconstructed.positions.copy()
    positions["position_date"] = positions["position_date"].dt.date

    assert reconstructed.start_date == date(2026, 1, 1)
    assert reconstructed.end_date == date(2026, 1, 5)
    assert positions["position_date"].tolist() == [
        date(2026, 1, 1),
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 1, 4),
        date(2026, 1, 5),
    ]
    assert positions["quantity"].tolist() == [10.0, 10.0, 15.0, 15.0, 8.0]
    assert positions["transaction_delta"].tolist() == [10.0, 0.0, 5.0, 0.0, -7.0]
    assert positions["transaction_count"].tolist() == [1, 0, 1, 0, 1]
    assert reconstructed.snapshot_reconciliation.empty


def test_reconstruct_positions_by_date_uses_snapshot_anchor_for_initial_state() -> None:
    transactions = pd.DataFrame(
        [
            {
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "asset_type": "stock",
                "isin": "US0000000002",
                "trade_date": "2026-01-04",
                "transaction_type": "BUY",
                "quantity": 1,
                "source_row": 1,
            },
            {
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "asset_type": "stock",
                "isin": "US0000000002",
                "trade_date": "2026-01-06",
                "transaction_type": "SELL",
                "quantity": 2,
                "source_row": 2,
            },
        ]
    )
    snapshots = pd.DataFrame(
        [
            {
                "asset_id": "asset-b",
                "asset_name": "Asset B",
                "asset_type": "stock",
                "isin": "US0000000002",
                "snapshot_date": "2026-01-02",
                "quantity": 4,
                "snapshot_source": "broker_export",
            }
        ]
    )

    reconstructed = reconstruct_positions_by_date(
        transactions,
        snapshots=snapshots,
        start_date=date(2026, 1, 3),
        end_date=date(2026, 1, 6),
    )

    positions = reconstructed.positions.copy()
    positions["position_date"] = positions["position_date"].dt.date

    assert positions["position_date"].tolist() == [
        date(2026, 1, 3),
        date(2026, 1, 4),
        date(2026, 1, 5),
        date(2026, 1, 6),
    ]
    assert positions["quantity"].tolist() == [4.0, 5.0, 5.0, 3.0]
    assert positions["anchor_snapshot_date"].dt.date.tolist() == [date(2026, 1, 2)] * 4
    assert positions["anchor_snapshot_quantity"].tolist() == [4.0, 4.0, 4.0, 4.0]


def test_reconcile_positions_with_snapshots_marks_matches_and_mismatches() -> None:
    transactions = pd.DataFrame(
        [
            {
                "asset_id": "asset-c",
                "asset_name": "Asset C",
                "asset_type": "etf",
                "isin": "IE0000000003",
                "trade_date": "2026-01-01",
                "transaction_type": "BUY",
                "quantity": 5,
                "source_row": 1,
            }
        ]
    )
    snapshots = pd.DataFrame(
        [
            {
                "asset_id": "asset-c",
                "asset_name": "Asset C",
                "asset_type": "etf",
                "isin": "IE0000000003",
                "snapshot_date": "2026-01-01",
                "quantity": 5,
                "snapshot_source": "broker_export",
            },
            {
                "asset_id": "asset-c",
                "asset_name": "Asset C",
                "asset_type": "etf",
                "isin": "IE0000000003",
                "snapshot_date": "2026-01-02",
                "quantity": 4,
                "snapshot_source": "broker_export",
            },
        ]
    )

    reconstructed = reconstruct_positions_by_date(transactions, snapshots=snapshots)

    reconciliation = reconstructed.snapshot_reconciliation.copy()
    reconciliation["snapshot_date"] = reconciliation["snapshot_date"].dt.date

    assert reconciliation["comparison_status"].tolist() == ["matched", "mismatch"]
    assert reconciliation["quantity_difference"].tolist() == [0.0, 1.0]


def test_reconstruct_positions_by_date_rejects_negative_running_quantities() -> None:
    transactions = pd.DataFrame(
        [
            {
                "asset_id": "asset-d",
                "trade_date": "2026-01-01",
                "transaction_type": "SELL",
                "quantity": 1,
            }
        ]
    )

    with pytest.raises(ValueError, match="negative quantity"):
        reconstruct_positions_by_date(transactions)


def test_reconstruct_positions_from_normalized_degiro_loads_and_persists_parquet_datasets() -> None:
    base_tmp_dir = Path(".test_tmp")
    base_tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = base_tmp_dir / f"positions-{uuid.uuid4().hex[:8]}"
    normalized_dir = tmp_path / "normalized" / "degiro"
    transactions_dir = normalized_dir / "transactions"
    snapshots_dir = normalized_dir / "portfolio_snapshots"
    output_dir = tmp_path / "curated" / "portfolio" / "positions_history"
    transactions_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "asset_id": "degiro:isin:IE000NDWFGA5",
                "asset_name": "GLOBAL X URANIUM",
                "asset_type": "etf",
                "isin": "IE000NDWFGA5",
                "trade_date": "2026-01-03",
                "transaction_type": "BUY",
                "quantity": 4,
                "source_row": 1,
            },
            {
                "asset_id": "degiro:isin:IE000NDWFGA5",
                "asset_name": "GLOBAL X URANIUM",
                "asset_type": "etf",
                "isin": "IE000NDWFGA5",
                "trade_date": "2026-01-05",
                "transaction_type": "BUY",
                "quantity": 2,
                "source_row": 2,
            },
        ]
    ).to_parquet(transactions_dir / "transactions_a.parquet", index=False)

    pd.DataFrame(
        [
            {
                "asset_id": "degiro:isin:IE000NDWFGA5",
                "asset_name": "GLOBAL X URANIUM",
                "asset_type": "etf",
                "isin": "IE000NDWFGA5",
                "snapshot_date": "2026-01-05",
                "quantity": 6,
                "snapshot_source": "broker_export",
            }
        ]
    ).to_parquet(snapshots_dir / "portfolio_a.parquet", index=False)

    reconstructed = reconstruct_positions_from_normalized_degiro(
        normalized_degiro_dir=normalized_dir,
        persist=True,
        output_dir=output_dir,
    )

    assert reconstructed.positions_output_path is not None
    assert reconstructed.reconciliation_output_path is not None
    assert reconstructed.positions_output_path.exists()
    assert reconstructed.reconciliation_output_path.exists()

    stored_positions = pd.read_parquet(reconstructed.positions_output_path)
    stored_reconciliation = pd.read_parquet(reconstructed.reconciliation_output_path)

    assert stored_positions["quantity"].tolist() == [4.0, 4.0, 6.0]
    assert stored_reconciliation["comparison_status"].tolist() == ["matched"]
