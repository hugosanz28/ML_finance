"""Price-proxy evolution for interfaces, never quantity-driven returns."""

from datetime import date
import math

import pandas as pd


def project_asset_history(positions: pd.DataFrame, transactions: pd.DataFrame, *, as_of_date: date) -> list[dict]:
    if positions.empty:
        return []
    frame = positions.copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"]).dt.date
    frame = frame.loc[frame["valuation_date"] <= as_of_date]
    first_buys = {}
    if not transactions.empty:
        buys = transactions.loc[transactions["transaction_type"].str.upper() == "BUY"].copy()
        buys["trade_date"] = pd.to_datetime(buys["trade_date"]).dt.date
        first_buys = buys.groupby("asset_id")["trade_date"].min().to_dict()
    result = []
    for asset_id, rows in frame.groupby("asset_id", sort=True):
        rows = rows.sort_values("valuation_date")
        first_buy = first_buys.get(asset_id, rows.iloc[0]["valuation_date"])
        rows = rows.loc[rows["valuation_date"] >= first_buy]
        if rows.empty:
            continue
        # A complete calendar keeps the chart from joining missing observations.
        rows = rows.drop_duplicates("valuation_date", keep="last").set_index("valuation_date")
        first_price = None
        points = []
        for day in pd.date_range(rows.index.min(), as_of_date, freq="D"):
            row = rows.loc[day.date()] if day.date() in rows.index else None
            price = None
            if row is not None and pd.notna(row["quantity"]) and row["quantity"] > 0 and pd.notna(row["market_value_base"]):
                candidate = float(row["market_value_base"] / row["quantity"])
                if math.isfinite(candidate) and candidate > 0:
                    price = candidate
            if first_price is None and price is not None:
                first_price = price
            change = price / first_price - 1 if price is not None and first_price is not None else None
            points.append({"valuation_date": day.date().isoformat(),
                           "price_change": change if change is not None and math.isfinite(change) else None})
        latest = rows.iloc[-1]
        name, kind = latest.get("asset_name"), latest.get("asset_type")
        result.append({"asset_id": str(asset_id), "asset_name": str(name) if pd.notna(name) else str(asset_id),
                       "asset_type": str(kind) if pd.notna(kind) else "unknown",
                       "series_kind": "valuation_price_proxy", "points": points})
    return result
