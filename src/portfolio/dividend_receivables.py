"""Reconstruct settled scrip-dividend receivables from the broker ledger."""

import re
from math import isfinite

import pandas as pd


def _units(description, side):
    # Rights are whole units. Unsupported descriptions remain unvalued.
    verb = "Compra|Buy" if side == "buy" else "Venta|Sell"
    match = re.search(rf"\b(?:{verb})\s+(\d+)\s", str(description), re.IGNORECASE)
    return int(match[1]) if match else None


def reconcile_dividend_receivables(positions: pd.DataFrame, cash: pd.DataFrame) -> pd.DataFrame:
    """Use a confirmed gross settlement only after an explicit non-tradeable conversion.

    This is retrospective accounting, not a historical market quote. The
    booking date agrees with cash reconstruction, including a settlement lag.
    Ambiguous/partial conversions and payments beyond the requested end stay unknown.
    """
    required = {"asset_id", "movement_date", "movement_type", "description", "amount", "movement_currency"}
    if positions.empty or not required.issubset(cash.columns):
        return positions
    result = positions.copy()
    result["position_date"] = pd.to_datetime(result.position_date).dt.normalize()
    start, end = result.position_date.min(), result.position_date.max()
    ledger = cash.copy()
    ledger["movement_date"] = pd.to_datetime(ledger.movement_date).dt.normalize()
    ledger = ledger.loc[ledger.movement_date <= end]
    for asset_id, events in ledger.dropna(subset=["asset_id"]).groupby("asset_id"):
        if "account_id" in events and events.account_id.nunique() > 1:
            continue
        conversion = events.loc[events.movement_type == "CORPORATE_ACTION_SCRIP_DIVIDEND"]
        if conversion.empty:
            continue
        locked = conversion.description.str.contains(r"non[ -]+trad(?:e)?able", case=False, na=False, regex=True)
        buys = conversion.loc[locked & conversion.description.map(lambda text: _units(text, "buy") is not None)]
        sells = conversion.loc[~locked & conversion.description.map(lambda text: _units(text, "sell") is not None)]
        if len(buys) != 1 or len(sells) != 1:
            continue
        buy, sell = buys.iloc[0], sells.iloc[0]
        units = _units(buy.description, "buy")
        if (not units or units != _units(sell.description, "sell")
                or buy.movement_date != sell.movement_date or buy.amount != 0 or sell.amount != 0):
            continue
        delistings = events.loc[events.movement_type == "CORPORATE_ACTION_DELISTING"]
        if len(delistings) != 1:
            continue
        delisting = delistings.iloc[0]
        paid_on = delisting.movement_date
        if paid_on <= buy.movement_date or _units(delisting.description, "sell") != units or delisting.amount != 0:
            continue
        payments = events.loc[(events.movement_type == "DIVIDEND") & (events.movement_date == paid_on)]
        if len(payments) != 1 or payments.iloc[0].amount <= 0 or not isfinite(float(payments.iloc[0].amount)):
            continue
        payment = payments.iloc[0]
        currency = str(payment.movement_currency)
        if not re.fullmatch(r"[A-Z]{3}", currency):
            continue
        held = result.loc[result.asset_id == asset_id].sort_values("position_date")
        # Prove a full conversion from the preceding position, never infer quantities from a payout.
        opening = held.loc[held.position_date <= buy.movement_date]
        active = held.loc[(held.position_date >= buy.movement_date) & (held.quantity > 0)]
        if (opening.empty or opening.iloc[-1].quantity != units or active.empty
                or not active.quantity.eq(units).all() or (active.position_date >= paid_on).any()):
            continue
        template = opening.iloc[-1].to_dict()
        rows = []
        for day in pd.date_range(max(start, buy.movement_date), min(end, paid_on - pd.Timedelta(days=1))):
            rows.append({**template, "position_date": day, "quantity": units,
                         "asset_type": "dividend_receivable",
                         "receivable_amount": float(payment.amount), "receivable_currency": currency})
        # Keep the claim until cash is actually booked; remove it on that same day.
        replaced = result.asset_id.eq(asset_id) & result.position_date.between(buy.movement_date, paid_on)
        result = pd.concat([result.loc[~replaced], pd.DataFrame(rows)], ignore_index=True, sort=False)
    return result
