from types import SimpleNamespace

import pandas as pd

from src.application import asset_classifications as classifications
from src.config import load_settings


def test_source_labelled_sectors_and_no_etf_lookthrough(tmp_path, monkeypatch):
    settings = load_settings(repo_root=tmp_path, env_file=tmp_path / "absent.env", env={"PRICE_PROVIDER": "yfinance"})
    assets = [SimpleNamespace(asset_id="stock", ticker=None, isin="ISIN_STOCK"), SimpleNamespace(asset_id="fund", ticker="FUND")]
    monkeypatch.setattr(classifications, "DuckDBMarketDataRepository",
                        lambda **kw: SimpleNamespace(list_assets=lambda: assets))
    monkeypatch.setattr(classifications, "load_normalized_degiro_snapshots", lambda **kw: pd.DataFrame([
        {"asset_id": asset.asset_id, "snapshot_date": "2026-01-01", "quantity": 1} for asset in assets]))
    def download(symbol):
        return dict(quoteType="EQUITY" if symbol == "ISIN_STOCK" else "ETF", sector="Technology")
    result = classifications.RefreshAssetClassificationsUseCase(settings=settings, downloader=download).execute()
    assert result.status == "succeeded"
    frame = classifications.classified_positions(pd.DataFrame([
        {"asset_id": asset.asset_id, "asset_type": "unknown"} for asset in assets]), settings)
    assert frame.iloc[0].sector == "Technology"
    assert frame.iloc[0].sector_source.startswith("https://finance.yahoo.com/")
    assert frame.iloc[1].asset_type == "etf"
    assert pd.isna(frame.iloc[1].sector)
    def unavailable(symbol):
        raise RuntimeError("offline")
    result = classifications.RefreshAssetClassificationsUseCase(settings=settings, downloader=unavailable).execute()
    assert result.status == "partial"
    assert classifications.read_classifications(settings)["stock"]["sector"] == "Technology"
