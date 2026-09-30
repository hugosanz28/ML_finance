"""Explicit metadata downloads and offline, source-labelled sector reads."""

from datetime import datetime, timezone
import json
from urllib.parse import quote

import pandas as pd
import yfinance as yf

from src.application.local_files import write_text_atomically
from src.application.types import ApplicationResult
from src.market_data.repository import DuckDBMarketDataRepository
from src.portfolio.positions import load_local_asset_aliases, load_normalized_degiro_snapshots


def read_classifications(settings) -> dict:
    path = settings.data_dir / "asset_classifications.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1 or not isinstance(payload.get("assets"), dict):
        raise ValueError("Invalid asset classifications")
    return payload["assets"]


def classified_positions(frame: pd.DataFrame, settings) -> pd.DataFrame:
    metadata = read_classifications(settings)
    result = frame.copy()
    for key in ("sector", "sector_source"):
        result[key] = result.asset_id.map(lambda asset_id, field=key: metadata.get(asset_id, {}).get(field))
    result["asset_type"] = [metadata.get(asset_id, {}).get("asset_type") or old
                            for asset_id, old in zip(result.asset_id, result.asset_type, strict=True)]
    return result


class RefreshAssetClassificationsUseCase:
    def __init__(self, *, settings, downloader=None):
        self.settings = settings
        self.downloader = downloader or (lambda symbol: yf.Ticker(symbol).get_info())

    def execute(self) -> ApplicationResult:
        if self.settings.price_provider == "synthetic":
            raise ValueError("External metadata is forbidden in demo")
        snapshots = load_normalized_degiro_snapshots(settings=self.settings)
        if snapshots.empty:
            return ApplicationResult(name="asset_classifications", status="skipped", message="No broker snapshot available.")
        current = snapshots.loc[snapshots.snapshot_date == snapshots.snapshot_date.max()]
        assets = {asset.asset_id: asset for asset in DuckDBMarketDataRepository(settings=self.settings, read_only=True).list_assets()}
        aliases = load_local_asset_aliases(self.settings)
        metadata = read_classifications(self.settings)
        failures, updated = 0, 0
        types = {"EQUITY": "stock", "ETF": "etf", "CRYPTOCURRENCY": "crypto"}
        for row in current.itertuples():
            if str(row.asset_id).startswith("degiro:cash:") or row.quantity == 0:
                continue
            asset = assets.get(row.asset_id)
            if asset is None or not (asset.ticker or getattr(asset, "isin", None)):
                asset = next((assets[source] for source, target in aliases.items()
                              if target == row.asset_id and source in assets), asset)
            # yfinance resolves official ISINs too, just like the price downloader.
            symbol = (asset.ticker or getattr(asset, "broker_symbol", None) or getattr(asset, "isin", None)) if asset else None
            if not symbol:
                failures += 1
                continue
            try:
                info = self.downloader(symbol)
                if info.get("quoteType") not in types:
                    failures += 1
                    continue
                resolved_symbol = info.get("symbol") or symbol
                source = f"https://finance.yahoo.com/quote/{quote(resolved_symbol, safe='')}/profile/"
                sector = info.get("sector") if info.get("quoteType") == "EQUITY" else None
                metadata[row.asset_id] = dict(asset_type=types[info["quoteType"]],
                                              sector=sector if isinstance(sector, str) else None,
                                              sector_source=source if sector else None,
                                              source=source, fetched_at=datetime.now(timezone.utc).isoformat())
                updated += 1
            except Exception:
                failures += 1
        if updated:
            write_text_atomically(self.settings.data_dir / "asset_classifications.json",
                                  json.dumps(dict(schema_version=1, assets=metadata), ensure_ascii=False, allow_nan=False))
        return ApplicationResult(name="asset_classifications", status="partial" if failures else "succeeded",
                                 message="Asset metadata refreshed.",
                                 warnings=("asset_classifications_partial",) if failures else (),
                                 artifacts={"updated_assets": updated, "skipped_assets": failures})
