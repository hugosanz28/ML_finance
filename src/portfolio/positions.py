"""Historical position reconstruction from normalized transactions and snapshots."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

import pandas as pd

from src.config import Settings, ensure_local_directories, get_settings


SUPPORTED_TRANSACTION_TYPES = {"BUY": 1.0, "SELL": -1.0}

POSITION_HISTORY_COLUMNS = [
    "position_date",
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "quantity",
    "transaction_delta",
    "transaction_count",
    "anchor_snapshot_date",
    "anchor_snapshot_quantity",
    "anchor_snapshot_source",
]

SNAPSHOT_RECONCILIATION_COLUMNS = [
    "snapshot_date",
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "snapshot_source",
    "snapshot_quantity",
    "reconstructed_quantity",
    "quantity_difference",
    "abs_quantity_difference",
    "comparison_status",
]

_TRANSACTION_REQUIRED_COLUMNS = ("asset_id", "trade_date", "transaction_type", "quantity")
_SNAPSHOT_REQUIRED_COLUMNS = ("asset_id", "snapshot_date", "quantity")
_TRANSACTION_OPTIONAL_COLUMNS = ("asset_name", "asset_type", "isin", "source_row")
_SNAPSHOT_OPTIONAL_COLUMNS = ("asset_name", "asset_type", "isin", "snapshot_source")


@dataclass(frozen=True)
class ReconstructedPositionHistory:
    """Reconstructed daily quantities plus optional broker reconciliation."""

    start_date: date
    end_date: date
    positions: pd.DataFrame
    snapshot_reconciliation: pd.DataFrame
    positions_output_path: Path | None = None
    reconciliation_output_path: Path | None = None


def reconstruct_positions_by_date(
    transactions: pd.DataFrame | None,
    *,
    snapshots: pd.DataFrame | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    include_zero_quantity_days: bool = False,
    zero_tolerance: float = 1e-8,
) -> ReconstructedPositionHistory:
    """Build a daily position history per asset from normalized broker events."""
    transactions_frame = _prepare_transactions_frame(transactions)
    snapshots_frame = _prepare_snapshots_frame(snapshots)
    resolved_start_date, resolved_end_date = _resolve_date_window(
        transactions_frame,
        snapshots_frame,
        start_date=start_date,
        end_date=end_date,
    )

    asset_reference = _build_asset_reference(transactions_frame, snapshots_frame)
    daily_transactions = _build_daily_transaction_deltas(transactions_frame)

    rows: list[dict[str, object]] = []
    for asset in asset_reference.to_dict(orient="records"):
        rows.extend(
            _reconstruct_asset_history(
                asset,
                daily_transactions=daily_transactions,
                snapshots_frame=snapshots_frame,
                start_date=resolved_start_date,
                end_date=resolved_end_date,
                include_zero_quantity_days=include_zero_quantity_days,
                zero_tolerance=zero_tolerance,
            )
        )

    positions = pd.DataFrame(rows, columns=POSITION_HISTORY_COLUMNS)
    if not positions.empty:
        positions["position_date"] = pd.to_datetime(positions["position_date"])
        positions["anchor_snapshot_date"] = pd.to_datetime(positions["anchor_snapshot_date"])
        positions = positions.sort_values(["asset_id", "position_date"]).reset_index(drop=True)

    snapshot_reconciliation = reconcile_positions_with_snapshots(
        positions,
        snapshots_frame,
        zero_tolerance=zero_tolerance,
    )

    return ReconstructedPositionHistory(
        start_date=resolved_start_date,
        end_date=resolved_end_date,
        positions=positions,
        snapshot_reconciliation=snapshot_reconciliation,
    )


def reconstruct_positions_from_normalized_degiro(
    *,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    include_zero_quantity_days: bool = False,
    zero_tolerance: float = 1e-8,
    persist: bool = False,
    output_dir: str | Path | None = None,
) -> ReconstructedPositionHistory:
    """Load normalized DEGIRO parquet datasets and reconstruct positions."""
    transactions = load_normalized_degiro_transactions(
        settings=settings,
        normalized_degiro_dir=normalized_degiro_dir,
    )
    snapshots = load_normalized_degiro_snapshots(
        settings=settings,
        normalized_degiro_dir=normalized_degiro_dir,
    )
    reconstructed = reconstruct_positions_by_date(
        transactions,
        snapshots=snapshots,
        start_date=start_date,
        end_date=end_date,
        include_zero_quantity_days=include_zero_quantity_days,
        zero_tolerance=zero_tolerance,
    )
    if not persist:
        return reconstructed
    return persist_reconstructed_positions(
        reconstructed,
        settings=settings,
        output_dir=output_dir,
    )


def persist_reconstructed_positions(
    reconstructed: ReconstructedPositionHistory,
    *,
    settings: Settings | None = None,
    output_dir: str | Path | None = None,
) -> ReconstructedPositionHistory:
    """Persist reconstructed positions and reconciliation datasets as parquet."""
    resolved_settings = get_settings() if settings is None else settings
    ensure_local_directories(resolved_settings)

    base_output_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else resolved_settings.curated_data_dir / "portfolio" / "positions_history"
    )
    base_output_dir.mkdir(parents=True, exist_ok=True)

    suffix = f"{reconstructed.start_date.isoformat()}_{reconstructed.end_date.isoformat()}"
    positions_output_path = (base_output_dir / f"positions_{suffix}.parquet").resolve()
    reconciliation_output_path = (base_output_dir / f"snapshot_reconciliation_{suffix}.parquet").resolve()

    positions_ready = reconstructed.positions.copy()
    if not positions_ready.empty:
        positions_ready["position_date"] = pd.to_datetime(positions_ready["position_date"])
    positions_ready.to_parquet(positions_output_path, index=False)

    reconciliation_ready = reconstructed.snapshot_reconciliation.copy()
    if not reconciliation_ready.empty:
        reconciliation_ready["snapshot_date"] = pd.to_datetime(reconciliation_ready["snapshot_date"])
    reconciliation_ready.to_parquet(reconciliation_output_path, index=False)

    return replace(
        reconstructed,
        positions_output_path=positions_output_path,
        reconciliation_output_path=reconciliation_output_path,
    )


def load_normalized_degiro_transactions(
    *,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Load normalized DEGIRO transactions from parquet datasets."""
    return _load_parquet_collection(
        _resolve_normalized_degiro_dir(settings=settings, normalized_degiro_dir=normalized_degiro_dir) / "transactions"
    )


def load_normalized_degiro_snapshots(
    *,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Load normalized DEGIRO portfolio snapshots from parquet datasets."""
    return _load_parquet_collection(
        _resolve_normalized_degiro_dir(settings=settings, normalized_degiro_dir=normalized_degiro_dir)
        / "portfolio_snapshots"
    )


def reconcile_positions_with_snapshots(
    positions: pd.DataFrame,
    snapshots: pd.DataFrame,
    *,
    zero_tolerance: float = 1e-8,
) -> pd.DataFrame:
    """Compare reconstructed quantities against broker snapshots by asset and date."""
    if snapshots.empty:
        return pd.DataFrame(columns=SNAPSHOT_RECONCILIATION_COLUMNS)

    lookup = pd.DataFrame(columns=["asset_id", "snapshot_date", "reconstructed_quantity"])
    if not positions.empty:
        lookup = positions.loc[:, ["asset_id", "position_date", "quantity"]].rename(
            columns={"position_date": "snapshot_date", "quantity": "reconstructed_quantity"}
        )
        lookup["snapshot_date"] = pd.to_datetime(lookup["snapshot_date"]).dt.date

    current = snapshots.copy()
    current["snapshot_date"] = pd.to_datetime(current["snapshot_date"]).dt.date
    merged = current.merge(lookup, on=["asset_id", "snapshot_date"], how="left")
    merged["reconstructed_quantity"] = pd.to_numeric(
        merged["reconstructed_quantity"],
        errors="coerce",
    ).fillna(0.0)
    merged["snapshot_quantity"] = pd.to_numeric(merged["quantity"], errors="coerce").fillna(0.0)
    merged["quantity_difference"] = (merged["reconstructed_quantity"] - merged["snapshot_quantity"]).round(8)
    merged["abs_quantity_difference"] = merged["quantity_difference"].abs().round(8)
    merged["comparison_status"] = merged["abs_quantity_difference"].map(
        lambda value: "matched" if value <= zero_tolerance else "mismatch"
    )

    ready = merged.loc[
        :,
        [
            "snapshot_date",
            "asset_id",
            "asset_name",
            "asset_type",
            "isin",
            "snapshot_source",
            "snapshot_quantity",
            "reconstructed_quantity",
            "quantity_difference",
            "abs_quantity_difference",
            "comparison_status",
        ],
    ].copy()
    ready["snapshot_date"] = pd.to_datetime(ready["snapshot_date"])
    return ready.sort_values(["snapshot_date", "asset_id"]).reset_index(drop=True)


def _prepare_transactions_frame(transactions: pd.DataFrame | None) -> pd.DataFrame:
    frame = pd.DataFrame(columns=[*_TRANSACTION_REQUIRED_COLUMNS, *_TRANSACTION_OPTIONAL_COLUMNS])
    if transactions is not None and not transactions.empty:
        frame = transactions.copy()

    _require_columns(frame, _TRANSACTION_REQUIRED_COLUMNS, frame_name="transactions")
    for column in _TRANSACTION_OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    if frame.empty:
        return frame.loc[:, [*_TRANSACTION_REQUIRED_COLUMNS, *_TRANSACTION_OPTIONAL_COLUMNS]]

    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
    frame["transaction_type"] = frame["transaction_type"].astype("string").str.upper()
    frame["quantity"] = pd.to_numeric(frame["quantity"], errors="raise")
    if (frame["quantity"] < 0).any():
        raise ValueError("transactions.quantity must be non-negative for reconstruction.")

    unsupported_transaction_types = sorted(set(frame["transaction_type"]) - set(SUPPORTED_TRANSACTION_TYPES))
    if unsupported_transaction_types:
        raise ValueError(
            "Unsupported transaction types for reconstruction: "
            f"{', '.join(unsupported_transaction_types)}"
        )

    frame["source_row"] = pd.to_numeric(frame["source_row"], errors="coerce").fillna(0).astype(int)
    frame["asset_name"] = frame["asset_name"].map(_normalize_optional_text)
    frame["asset_type"] = frame["asset_type"].map(_normalize_optional_text)
    frame["isin"] = frame["isin"].map(_normalize_optional_text)
    return frame.loc[:, [*_TRANSACTION_REQUIRED_COLUMNS, *_TRANSACTION_OPTIONAL_COLUMNS]]


def _prepare_snapshots_frame(snapshots: pd.DataFrame | None) -> pd.DataFrame:
    frame = pd.DataFrame(columns=[*_SNAPSHOT_REQUIRED_COLUMNS, *_SNAPSHOT_OPTIONAL_COLUMNS])
    if snapshots is not None and not snapshots.empty:
        frame = snapshots.copy()

    _require_columns(frame, _SNAPSHOT_REQUIRED_COLUMNS, frame_name="snapshots")
    for column in _SNAPSHOT_OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    if frame.empty:
        return frame.loc[:, [*_SNAPSHOT_REQUIRED_COLUMNS, *_SNAPSHOT_OPTIONAL_COLUMNS]]

    frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"], errors="raise").dt.date
    frame["quantity"] = pd.to_numeric(frame["quantity"], errors="raise")
    frame["asset_name"] = frame["asset_name"].map(_normalize_optional_text)
    frame["asset_type"] = frame["asset_type"].map(_normalize_optional_text)
    frame["isin"] = frame["isin"].map(_normalize_optional_text)
    frame["snapshot_source"] = frame["snapshot_source"].map(_normalize_optional_text).fillna("broker_export")
    return frame.loc[:, [*_SNAPSHOT_REQUIRED_COLUMNS, *_SNAPSHOT_OPTIONAL_COLUMNS]]


def _resolve_date_window(
    transactions: pd.DataFrame,
    snapshots: pd.DataFrame,
    *,
    start_date: date | None,
    end_date: date | None,
) -> tuple[date, date]:
    candidate_start_dates: list[date] = []
    candidate_end_dates: list[date] = []

    if not transactions.empty:
        candidate_start_dates.append(min(transactions["trade_date"]))
        candidate_end_dates.append(max(transactions["trade_date"]))
    if not snapshots.empty:
        candidate_start_dates.append(min(snapshots["snapshot_date"]))
        candidate_end_dates.append(max(snapshots["snapshot_date"]))

    if start_date is None:
        if not candidate_start_dates:
            raise ValueError("Cannot reconstruct positions without transactions or snapshots.")
        resolved_start_date = min(candidate_start_dates)
    else:
        resolved_start_date = start_date

    if end_date is None:
        if not candidate_end_dates:
            raise ValueError("Cannot reconstruct positions without transactions or snapshots.")
        resolved_end_date = max(candidate_end_dates)
    else:
        resolved_end_date = end_date

    if resolved_end_date < resolved_start_date:
        raise ValueError("end_date must be on or after start_date.")
    return resolved_start_date, resolved_end_date


def _build_asset_reference(transactions: pd.DataFrame, snapshots: pd.DataFrame) -> pd.DataFrame:
    reference_frames: list[pd.DataFrame] = []
    if not transactions.empty:
        tx_reference = transactions.loc[:, ["asset_id", "asset_name", "asset_type", "isin", "trade_date"]].rename(
            columns={"trade_date": "activity_date"}
        )
        reference_frames.append(tx_reference)
    if not snapshots.empty:
        snapshot_reference = snapshots.loc[
            :,
            ["asset_id", "asset_name", "asset_type", "isin", "snapshot_date"],
        ].rename(columns={"snapshot_date": "activity_date"})
        reference_frames.append(snapshot_reference)

    combined = pd.concat(reference_frames, ignore_index=True, sort=False)
    combined = combined.sort_values(["asset_id", "activity_date"]).reset_index(drop=True)

    rows: list[dict[str, object]] = []
    for asset_id, group in combined.groupby("asset_id", sort=True):
        rows.append(
            {
                "asset_id": str(asset_id),
                "asset_name": _pick_last_text(group["asset_name"]),
                "asset_type": _pick_last_text(group["asset_type"]),
                "isin": _pick_last_text(group["isin"]),
            }
        )
    return pd.DataFrame(rows, columns=["asset_id", "asset_name", "asset_type", "isin"])


def _build_daily_transaction_deltas(transactions: pd.DataFrame) -> pd.DataFrame:
    if transactions.empty:
        return pd.DataFrame(columns=["asset_id", "trade_date", "transaction_delta", "transaction_count"])

    current = transactions.copy()
    current["transaction_delta"] = current.apply(
        lambda row: float(row["quantity"]) * SUPPORTED_TRANSACTION_TYPES[str(row["transaction_type"])],
        axis=1,
    )
    grouped = (
        current.sort_values(["asset_id", "trade_date", "source_row"])
        .groupby(["asset_id", "trade_date"], as_index=False)
        .agg(
            transaction_delta=("transaction_delta", "sum"),
            transaction_count=("transaction_delta", "size"),
        )
    )
    grouped["transaction_delta"] = grouped["transaction_delta"].round(8)
    return grouped


def _reconstruct_asset_history(
    asset: dict[str, object],
    *,
    daily_transactions: pd.DataFrame,
    snapshots_frame: pd.DataFrame,
    start_date: date,
    end_date: date,
    include_zero_quantity_days: bool,
    zero_tolerance: float,
) -> list[dict[str, object]]:
    asset_id = str(asset["asset_id"])
    asset_transactions = daily_transactions.loc[daily_transactions["asset_id"] == asset_id].copy()
    asset_snapshots = snapshots_frame.loc[snapshots_frame["asset_id"] == asset_id].copy()
    asset_snapshots = asset_snapshots.sort_values("snapshot_date").reset_index(drop=True)

    anchor_snapshot = _select_anchor_snapshot(
        asset_snapshots,
        asset_transactions=asset_transactions,
        start_date=start_date,
        end_date=end_date,
    )
    anchor_date = None
    anchor_quantity = 0.0
    anchor_source = None
    series_start_date = start_date

    if anchor_snapshot is not None:
        anchor_date = anchor_snapshot["snapshot_date"]
        anchor_quantity = round(float(anchor_snapshot["quantity"]), 8)
        anchor_source = anchor_snapshot["snapshot_source"]
        if anchor_date > series_start_date:
            series_start_date = anchor_date

    if series_start_date > end_date:
        return []

    eligible_transactions = asset_transactions.copy()
    if anchor_date is not None:
        eligible_transactions = eligible_transactions.loc[eligible_transactions["trade_date"] > anchor_date]

    pre_start_transactions = eligible_transactions.loc[eligible_transactions["trade_date"] < series_start_date]
    initial_quantity = round(anchor_quantity + float(pre_start_transactions["transaction_delta"].sum()), 8)
    initial_quantity = _normalize_quantity(initial_quantity, zero_tolerance=zero_tolerance)
    _ensure_non_negative_quantity(
        initial_quantity,
        asset_id=asset_id,
        current_date=series_start_date,
        zero_tolerance=zero_tolerance,
    )

    in_range_transactions = eligible_transactions.loc[
        (eligible_transactions["trade_date"] >= series_start_date)
        & (eligible_transactions["trade_date"] <= end_date)
    ].copy()
    if initial_quantity == 0 and in_range_transactions.empty and anchor_date is None:
        return []

    transaction_delta_map = {
        row["trade_date"]: round(float(row["transaction_delta"]), 8)
        for row in in_range_transactions.to_dict(orient="records")
    }
    transaction_count_map = {
        row["trade_date"]: int(row["transaction_count"])
        for row in in_range_transactions.to_dict(orient="records")
    }

    rows: list[dict[str, object]] = []
    running_quantity = initial_quantity
    for position_date in pd.date_range(series_start_date, end_date, freq="D").date:
        transaction_delta = transaction_delta_map.get(position_date, 0.0)
        running_quantity = round(running_quantity + transaction_delta, 8)
        running_quantity = _normalize_quantity(running_quantity, zero_tolerance=zero_tolerance)
        _ensure_non_negative_quantity(
            running_quantity,
            asset_id=asset_id,
            current_date=position_date,
            zero_tolerance=zero_tolerance,
        )

        if not include_zero_quantity_days and running_quantity == 0 and transaction_delta == 0:
            continue

        rows.append(
            {
                "position_date": position_date,
                "asset_id": asset_id,
                "asset_name": asset["asset_name"],
                "asset_type": asset["asset_type"],
                "isin": asset["isin"],
                "quantity": running_quantity,
                "transaction_delta": transaction_delta,
                "transaction_count": transaction_count_map.get(position_date, 0),
                "anchor_snapshot_date": anchor_date,
                "anchor_snapshot_quantity": anchor_quantity if anchor_date is not None else None,
                "anchor_snapshot_source": anchor_source,
            }
        )
    return rows


def _select_anchor_snapshot(
    asset_snapshots: pd.DataFrame,
    *,
    asset_transactions: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> dict[str, object] | None:
    if asset_snapshots.empty:
        return None

    snapshots_on_or_before_start = asset_snapshots.loc[asset_snapshots["snapshot_date"] <= start_date]
    if not snapshots_on_or_before_start.empty:
        return snapshots_on_or_before_start.iloc[-1].to_dict()

    if asset_transactions.empty:
        snapshots_in_range = asset_snapshots.loc[
            (asset_snapshots["snapshot_date"] >= start_date) & (asset_snapshots["snapshot_date"] <= end_date)
        ]
        if not snapshots_in_range.empty:
            return snapshots_in_range.iloc[0].to_dict()
        return None

    first_snapshot_in_range = asset_snapshots.loc[
        (asset_snapshots["snapshot_date"] >= start_date) & (asset_snapshots["snapshot_date"] <= end_date)
    ]
    if first_snapshot_in_range.empty:
        return None

    candidate = first_snapshot_in_range.iloc[0].to_dict()
    if (asset_transactions["trade_date"] < candidate["snapshot_date"]).any():
        return None
    return candidate


def _load_parquet_collection(directory: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for parquet_path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        frame = pd.read_parquet(parquet_path)
        if frame.empty:
            continue
        frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def _resolve_normalized_degiro_dir(
    *,
    settings: Settings | None,
    normalized_degiro_dir: str | Path | None,
) -> Path:
    resolved_settings = get_settings() if settings is None else settings
    return (
        resolved_settings.normalized_data_dir / "degiro"
        if normalized_degiro_dir is None
        else Path(normalized_degiro_dir).expanduser().resolve()
    )


def _require_columns(frame: pd.DataFrame, required_columns: tuple[str, ...], *, frame_name: str) -> None:
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in {frame_name}: {', '.join(missing_columns)}")


def _normalize_optional_text(value: object | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _pick_last_text(series: pd.Series) -> str | None:
    for value in reversed(series.tolist()):
        text = _normalize_optional_text(value)
        if text is not None:
            return text
    return None


def _normalize_quantity(value: float, *, zero_tolerance: float) -> float:
    if abs(value) <= zero_tolerance:
        return 0.0
    return value


def _ensure_non_negative_quantity(
    value: float,
    *,
    asset_id: str,
    current_date: date,
    zero_tolerance: float,
) -> None:
    if value < -zero_tolerance:
        raise ValueError(
            f"Reconstruction produced a negative quantity for {asset_id} on {current_date.isoformat()}."
        )
