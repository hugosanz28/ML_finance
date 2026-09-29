"""Reconstruct broker cash without counting internal cash-account sweeps twice."""

from pathlib import Path

import pandas as pd

from src.config import Settings


INTERNAL_TRANSFERS = {"CASH_ACCOUNT_TRANSFER_IN", "CASH_ACCOUNT_TRANSFER_OUT", "CASH_SWEEP_TRANSFER"}


def load_cash_movements(settings: Settings, normalized_degiro_dir: str | Path | None = None) -> pd.DataFrame:
    root = Path(normalized_degiro_dir) if normalized_degiro_dir else settings.normalized_data_dir / "degiro"
    frames = [pd.read_parquet(path) for path in sorted((root / "cash_movements").glob("*.parquet"))]
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    # Overlapping exports keep their provenance but represent the same ledger entry.
    keys = [key for key in ("account_id", "asset_id", "description", "movement_date", "movement_time", "value_date", "movement_type",
                           "amount", "movement_currency", "running_balance", "external_reference") if key in frame]
    return frame.drop_duplicates(keys).copy() if keys else frame


def reconcile_cash_history(positions: pd.DataFrame, cash: pd.DataFrame, snapshots: pd.DataFrame) -> pd.DataFrame:
    """Use ledger amounts in their own currency; snapshots verify the resulting balance."""
    if cash.empty or positions.empty:
        return positions
    required = {"movement_date", "movement_type", "amount", "movement_currency", "running_balance"}
    if not required.issubset(cash.columns):
        return positions
    ledger = cash.loc[~cash.movement_type.isin(INTERNAL_TRANSFERS)].copy()
    ledger["movement_date"] = pd.to_datetime(ledger.movement_date).dt.normalize()
    if "value_date" in ledger:
        external = ledger.movement_type.isin({"DEPOSIT", "WITHDRAWAL"})
        # The performance engine books external flows on value date. Cash must agree.
        ledger.loc[external, "movement_date"] = pd.to_datetime(ledger.loc[external, "value_date"]).fillna(ledger.loc[external, "movement_date"])
    for key, default in (("movement_time", ""), ("source_row", 0)):
        if key not in ledger:
            ledger[key] = default
    ledger = ledger.sort_values(["movement_date", "movement_time", "source_row"], ascending=[True, True, False], kind="stable")
    start, end = pd.to_datetime(positions.position_date).min(), pd.to_datetime(positions.position_date).max()
    rows = []
    currencies = set()
    for currency, events in ledger.groupby("movement_currency"):
        events = events.loc[events.movement_date <= end]
        if events.empty:
            continue
        currencies.add(f"degiro:cash:{str(currency).lower()}")
        asset_id = f"degiro:cash:{str(currency).lower()}"
        first = events.iloc[0]
        known = pd.notna(first.running_balance) and first.get("running_balance_currency", currency) == currency
        balance = float(first.running_balance) - float(first.amount) if known else 0.0
        daily = events.groupby("movement_date").amount.sum().to_dict()
        anchors = snapshots.loc[snapshots.asset_id == asset_id] if not snapshots.empty else pd.DataFrame()
        anchor_values = {pd.Timestamp(row.snapshot_date): float(row.quantity) for row in anchors.itertuples()}
        # Include zero balances: they prove cash coverage instead of hiding an unknown balance.
        for day in pd.date_range(min(start, events.movement_date.min()), end):
            balance = round(balance + float(daily.get(day, 0)), 8)
            if day in anchor_values:
                known = known and abs(balance - anchor_values[day]) <= 0.02
                if known:
                    # Broker rounding is authoritative within the explicit reconciliation tolerance.
                    balance = anchor_values[day]
            if day >= start:
                rows.append({"position_date": day, "asset_id": asset_id, "asset_name": f"Cash {currency}",
                             "asset_type": "cash", "isin": None, "quantity": balance,
                             "cash_reconciled": known})
    retained = positions.loc[~positions.asset_id.isin(currencies)]
    return pd.concat([retained, pd.DataFrame(rows)], ignore_index=True, sort=False)
