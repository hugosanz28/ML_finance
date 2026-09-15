"""Explicit real-data refresh and immutable, validated offline benchmark reads.

Index IDs remain conceptual references. Accumulating ETF adjusted closes are
labelled proxies, never passed off as the licensed official index history.
"""

from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

from src.market_data.benchmarks import BenchmarkProvider, BenchmarkReturnSeries, LoadedBenchmarkProvider


@dataclass(frozen=True)
class BenchmarkSource:
    source_id: str
    reference: str
    provider: str
    is_proxy: bool
    currency: str = "EUR"


SOURCES = {
    "msci_world": BenchmarkSource("EUNL.DE", "MSCI World proxy: iShares Core MSCI World UCITS ETF Acc (IE00B4L5Y983), Xetra EUR adjusted close", "yfinance", True),
    "sp500": BenchmarkSource("SXR8.DE", "S&P 500 proxy: iShares Core S&P 500 UCITS ETF Acc (IE00B5BMR087), Xetra EUR adjusted close", "yfinance", True),
    "global_aggregate_bonds_eur_hedged": BenchmarkSource("EUNA.DE", "Global aggregate bonds proxy: iShares Core Global Aggregate Bond EUR Hedged Acc (IE00BDBRDM35), Xetra EUR adjusted close", "yfinance", True),
    "estr_cash": BenchmarkSource("EST.B.EU000A2QQF08.CI", "ECB compounded euro short-term rate index (EU000A2QQF08), not a bank deposit return", "ecb", False),
}
SUPPORTED_BASES = {"EUR", "USD", "GBP", "CHF", "JPY"}
MAX_CACHE_BYTES = 8 * 1024 * 1024


class BenchmarkCacheError(ValueError):
    pass


def cache_path(data_dir: Path) -> Path:
    path = data_dir / "benchmark_cache.json"
    if not path.resolve().is_relative_to(data_dir.absolute()):
        raise BenchmarkCacheError("benchmark_cache_path_not_allowed")
    return path


def content_hash(value) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def validate_window(start: date, end: date, currency: str) -> None:
    # Exclude the current, potentially unfinished trading day. No arbitrary tickers/URLs.
    if not date(2000, 1, 1) <= start < end < date.today():
        raise BenchmarkCacheError("invalid_benchmark_date_window")
    if currency not in SUPPORTED_BASES:
        raise BenchmarkCacheError("benchmark_currency_unsupported")


def normalize_levels(frame: pd.DataFrame) -> list[dict]:
    """Keep explicit holes, reject malformed values and never fill prices."""
    if not {"date", "value"} <= set(frame.columns) or len(frame) < 2:
        raise BenchmarkCacheError("benchmark_levels_unavailable")
    frame = frame.loc[:, ["date", "value"]].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    missing = frame["value"].isna()
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    if (frame["date"].isna().any() or frame["date"].duplicated().any()
            or (frame["value"].isna() & ~missing).any() or frame["value"].notna().sum() < 2
            or not frame.loc[frame["value"].notna(), "value"].map(lambda x: 0 < x < float("inf")).all()):
        raise BenchmarkCacheError("invalid_benchmark_levels")
    return [{"date": row.date.isoformat(), "value": float(row.value) if pd.notna(row.value) else None}
            for row in frame.sort_values("date").itertuples(index=False)]


class PublicBenchmarkDownloader:
    """Only refresh calls this adapter. GET never constructs a network client."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir

    def levels(self, source: BenchmarkSource, start: date, end: date) -> list[dict]:
        if source.provider == "ecb":
            return self.ecb_levels(source.source_id, start, end)
        import yfinance as yf

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        yf.set_tz_cache_location(str(self.cache_dir))
        ticker = yf.Ticker(source.source_id)
        history = ticker.history(start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(),
                                 interval="1d", auto_adjust=False, actions=False, keepna=True,
                                 repair=False, timeout=20, raise_errors=True)
        metadata = ticker.get_history_metadata()
        if metadata.get("currency") != source.currency or "Adj Close" not in history:
            raise BenchmarkCacheError("benchmark_currency_or_adjustment_mismatch")
        # Keep exchange-local dates; converting midnight to UTC can shift trading days.
        return normalize_levels(pd.DataFrame({"date": history.index.date, "value": history["Adj Close"].to_numpy()}))

    def ecb_levels(self, key: str, start: date, end: date) -> list[dict]:
        flow, series = key.split(".", 1)
        query = urlencode({"startPeriod": start.isoformat(), "endPeriod": end.isoformat(), "format": "csvdata"})
        with urlopen(f"https://data-api.ecb.europa.eu/service/data/{flow}/{series}?{query}", timeout=30) as response:
            payload = response.read(MAX_CACHE_BYTES + 1)
        if len(payload) > MAX_CACHE_BYTES:
            raise BenchmarkCacheError("benchmark_response_too_large")
        frame = pd.read_csv(io.BytesIO(payload))
        if not {"KEY", "TIME_PERIOD", "OBS_VALUE"} <= set(frame.columns) or set(frame["KEY"]) != {key}:
            raise BenchmarkCacheError("unexpected_ecb_series")
        if "OBS_STATUS" in frame and not frame["OBS_STATUS"].eq("A").all():
            raise BenchmarkCacheError("invalid_ecb_observation_status")
        return normalize_levels(frame.rename(columns={"TIME_PERIOD": "date", "OBS_VALUE": "value"}))


def refresh_benchmark_cache(data_dir: Path, start: date, end: date, currency: str, *, downloader=None) -> dict:
    validate_window(start, end, currency)
    path = cache_path(data_dir)
    downloader = downloader or PublicBenchmarkDownloader(data_dir / "benchmark_yfinance_cache")
    fetched = datetime.now(timezone.utc).isoformat()
    sources = dict(SOURCES)
    if currency != "EUR":
        sources["fx"] = BenchmarkSource(f"EXR.D.{currency}.EUR.SP00.A", f"ECB {currency} per EUR reference FX", "ecb", False, currency)
    entries = {}
    for key, source in sources.items():
        levels = downloader.levels(source, start, end)
        levels = normalize_levels(pd.DataFrame(levels))
        if any(not start.isoformat() <= row["date"] <= end.isoformat() for row in levels):
            raise BenchmarkCacheError("benchmark_observation_outside_request")
        entries[key] = {"source": asdict(source), "fetched_at": fetched, "levels": levels, "content_sha256": content_hash(levels)}
    data = {"schema_version": 1, "base_currency": currency, "requested_start": start.isoformat(),
            "requested_end": end.isoformat(), "fetched_at": fetched, "entries": entries}
    payload = json.dumps({"data": data, "content_sha256": content_hash(data)}, ensure_ascii=False, allow_nan=False).encode()
    if len(payload) > MAX_CACHE_BYTES:
        raise BenchmarkCacheError("benchmark_cache_too_large")
    # Whole requested history is replaced, never spliced across adjusted-price revisions.
    # Failed downloads leave the last successful snapshot untouched.
    data_dir.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=data_dir, prefix=".benchmarks-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return {"content_sha256": content_hash(data), "fetched_at": fetched,
            "observations": {key: len(entry["levels"]) for key, entry in entries.items()}}


def load_benchmark_cache(data_dir: Path) -> dict:
    path = cache_path(data_dir)
    if not path.is_file():
        raise BenchmarkCacheError("benchmark_cache_missing")
    if path.stat().st_size > MAX_CACHE_BYTES:
        raise BenchmarkCacheError("benchmark_cache_invalid")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        data = document["data"]
        if data["schema_version"] != 1 or content_hash(data) != document["content_sha256"]:
            raise ValueError("Invalid cache hash")
        currency = data["base_currency"]
        if currency not in SUPPORTED_BASES:
            raise ValueError("Invalid currency")
        start = date.fromisoformat(data["requested_start"])
        end = date.fromisoformat(data["requested_end"])
        validate_window(start, end, currency)
        if datetime.fromisoformat(data["fetched_at"]).tzinfo is None:
            raise ValueError("Missing fetch timezone")
        expected = dict(SOURCES)
        if currency != "EUR":
            expected["fx"] = BenchmarkSource(f"EXR.D.{currency}.EUR.SP00.A", f"ECB {currency} per EUR reference FX", "ecb", False, currency)
        if set(data["entries"]) != set(expected):
            raise ValueError("Unexpected cache sources")
        for key, source in expected.items():
            entry = data["entries"][key]
            if entry["source"] != asdict(source) or content_hash(entry["levels"]) != entry["content_sha256"]:
                raise ValueError("Invalid source provenance")
            if normalize_levels(pd.DataFrame(entry["levels"])) != entry["levels"]:
                raise ValueError("Invalid levels")
            if entry["fetched_at"] != data["fetched_at"] or any(
                not start.isoformat() <= row["date"] <= end.isoformat() for row in entry["levels"]
            ):
                raise ValueError("Inconsistent cache window")
        return data
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise BenchmarkCacheError("benchmark_cache_invalid") from exc


class CachedBenchmarkProvider(BenchmarkProvider):
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    @property
    def name(self) -> str:
        return "cached_yfinance_ecb"

    def fetch_daily_returns(self, definition, *, start_date, end_date, base_currency):
        reference = SOURCES[definition.benchmark_id].reference
        try:
            data = load_benchmark_cache(self.data_dir)
        except BenchmarkCacheError as exc:
            return BenchmarkReturnSeries(definition.benchmark_id, self.name, "EUR", base_currency, definition.series_kind,
                                         (), 0, "unavailable", str(exc), source_reference=reference)
        if base_currency != data["base_currency"]:
            return BenchmarkReturnSeries(definition.benchmark_id, self.name, "EUR", base_currency, definition.series_kind,
                                         (), 0, "unavailable", "benchmark_cache_currency_mismatch", source_reference=reference)
        entry = data["entries"][definition.benchmark_id]
        levels = entry["levels"]
        rows = [{"benchmark_id": definition.benchmark_id, "previous_observation_date": previous["date"],
                 "observation_date": current["date"], "return_decimal": current["value"] / previous["value"] - 1,
                 "currency": "EUR"} for previous, current in zip(levels, levels[1:], strict=False)
                if previous["value"] is not None and current["value"] is not None]
        fx = None
        if base_currency != "EUR":
            # ECB gives target units per EUR; the existing converter expects EUR per target.
            fx = pd.DataFrame([{"base_currency": base_currency, "quote_currency": "EUR", "rate_date": row["date"],
                                "rate": 1 / row["value"]} for row in data["entries"]["fx"]["levels"] if row["value"] is not None])
        frame = pd.DataFrame(rows, columns=["benchmark_id", "previous_observation_date", "observation_date", "return_decimal", "currency"])
        series = LoadedBenchmarkProvider(frame, fx_rates=fx, provider_name=self.name).fetch_daily_returns(
            replace(definition, native_currency="EUR"), start_date=start_date, end_date=end_date, base_currency=base_currency)
        warnings = ["benchmark_etf_proxy"] if entry["source"]["is_proxy"] else []
        if any(row["value"] is None and start_date.isoformat() <= row["date"] <= end_date.isoformat() for row in levels):
            warnings.append("benchmark_missing_observations")
        if max(date.fromisoformat(row["date"]) for row in levels if row["value"] is not None) < end_date - timedelta(days=7):
            warnings.append("benchmark_cache_stale")
        sources = []
        for key in (definition.benchmark_id, "fx") if fx is not None else (definition.benchmark_id,):
            item = data["entries"][key]
            valid = [row for row in item["levels"] if row["value"] is not None]
            sources.append({**item["source"], "fetched_at": item["fetched_at"], "first_observation": valid[0]["date"],
                            "last_observation": valid[-1]["date"], "content_sha256": item["content_sha256"]})
        return replace(series, source_reference=reference, sources=tuple(sources), warning_codes=tuple(warnings),
                       status="partial" if warnings and series.status == "available" else series.status)
