"""Offline closing prices for expired rights, reviewed against exchange bulletins."""

from datetime import date
import json
from math import isfinite
from pathlib import Path
import re
from urllib.parse import urlparse

import pandas as pd

from src.portfolio.metrics_models import PortfolioDataUnavailableError


def apply_reviewed_rights_prices(positions: pd.DataFrame, path: Path) -> pd.DataFrame:
    """Use exact ISIN/date matches; only Friday's close may carry over its weekend.

    This local file is curated evidence, never an automatic web search result.
    Missing sessions stay unknown, and dividend receivables retain their own policy.
    """
    if not path.is_file():
        return positions
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload["schema_version"] != 1 or len(payload["prices"]) > 10000:
            raise ValueError
        lookup = {}
        for item in payload["prices"]:
            isin = item["isin"]
            day = date.fromisoformat(item["date"])
            price = item["close"]
            source = urlparse(item["source_url"])
            if (not re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", isin)
                    or item["instrument_type"] != "subscription_right"
                    or item["currency"] != "EUR" or item["exchange_mic"] != "XMAD"
                    or type(price) not in (float, int) or not isfinite(price) or price <= 0
                    or day.weekday() > 4 or day >= date.today()
                    or source.scheme != "https" or not source.hostname
                    or source.username or source.password or not item["source_note"].strip()
                    or (isin, day) in lookup):
                raise ValueError
            lookup[(isin, day)] = float(price)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        # Never silently discard malformed evidence and continue with misleading totals.
        raise PortfolioDataUnavailableError("Invalid reviewed rights price evidence") from exc
    result = positions.copy()
    result["reviewed_rights_close"] = float("nan")
    result["reviewed_rights_price_date"] = None
    for index, row in result.iterrows():
        if row.get("asset_type") == "dividend_receivable":
            continue
        asset_id = str(row.asset_id)
        if not asset_id.startswith("degiro:isin:"):
            continue
        day = pd.Timestamp(row.position_date).date()
        quote_day = day - pd.Timedelta(days=day.weekday() - 4) if day.weekday() > 4 else day
        price = lookup.get((asset_id.removeprefix("degiro:isin:"), quote_day))
        if price is not None:
            result.at[index, "reviewed_rights_close"] = price
            result.at[index, "reviewed_rights_price_date"] = quote_day
    return result
