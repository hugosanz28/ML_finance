"""Bootstrap helpers from normalized DEGIRO datasets into market assets."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import Settings, get_settings
from src.market_data.models import MarketAsset
from src.market_data.repository import DuckDBMarketDataRepository


ASSET_HINT_COLUMNS = [
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "reference_exchange",
    "execution_venue",
    "trading_currency",
    "first_seen_date",
    "last_seen_date",
]

SNAPSHOT_COLUMNS = [
    "asset_id",
    "asset_name",
    "asset_type",
    "isin",
    "broker_symbol",
    "snapshot_date",
    "position_currency",
]

OVERRIDE_COLUMNS = [
    "asset_id",
    "asset_name",
    "asset_type",
    "ticker",
    "broker_symbol",
    "exchange_mic",
    "trading_currency",
    "asset_similar",
    "is_active",
    "notes",
]


def load_market_assets_from_normalized_degiro(
    *,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
) -> list[MarketAsset]:
    """Load market assets from normalized DEGIRO parquet datasets."""
    resolved_settings = get_settings() if settings is None else settings
    base_dir = (
        resolved_settings.normalized_data_dir / "degiro"
        if normalized_degiro_dir is None
        else Path(normalized_degiro_dir).expanduser().resolve()
    )

    frames = [
        _load_transaction_asset_hints(base_dir / "assets"),
        _load_snapshot_assets(base_dir / "portfolio_snapshots"),
    ]
    populated_frames = [frame for frame in frames if not frame.empty]
    if not populated_frames:
        return []

    combined = pd.concat(populated_frames, ignore_index=True, sort=False)
    combined = combined.loc[
        combined["asset_id"].notna()
        & combined["asset_name"].notna()
        & combined["asset_type"].notna()
        & combined["trading_currency"].notna()
    ].copy()
    if combined.empty:
        return []

    overrides = _load_asset_overrides(
        settings=resolved_settings,
        overrides_path=None,
    )
    combined = combined.sort_values(["asset_id", "last_seen_date", "first_seen_date"], na_position="last")

    assets: list[MarketAsset] = []
    for asset_id, group in combined.groupby("asset_id", sort=True):
        override = overrides.get(str(asset_id), {})
        asset = MarketAsset(
            asset_id=str(asset_id),
            asset_name=_override_or_value(override, "asset_name", _pick_last_text(group["asset_name"])),
            asset_type=_override_or_value(override, "asset_type", _pick_last_text(group["asset_type"])),
            asset_similar=_override_or_value(override, "asset_similar", None),
            isin=_pick_last_text(group["isin"]),
            ticker=_override_or_value(override, "ticker", None),
            broker_symbol=_override_or_value(override, "broker_symbol", _pick_last_text(group["broker_symbol"])),
            exchange_mic=_override_or_value(override, "exchange_mic", _pick_last_text(group["exchange_mic"])),
            trading_currency=_override_or_value(override, "trading_currency", _pick_last_text(group["trading_currency"])),
            first_seen_date=_pick_first_date(group["first_seen_date"]),
            last_seen_date=_pick_last_date(group["last_seen_date"]),
            is_active=_override_or_bool(override, "is_active", True),
        )
        assets.append(asset)

    return assets


def sync_market_assets_from_normalized_degiro(
    *,
    repository: DuckDBMarketDataRepository | None = None,
    settings: Settings | None = None,
    normalized_degiro_dir: str | Path | None = None,
) -> int:
    """Upsert market assets sourced from normalized DEGIRO parquet datasets."""
    resolved_settings = get_settings() if settings is None else settings
    resolved_repository = repository or DuckDBMarketDataRepository(settings=resolved_settings)
    assets = load_market_assets_from_normalized_degiro(
        settings=resolved_settings,
        normalized_degiro_dir=normalized_degiro_dir,
    )
    return resolved_repository.upsert_assets(assets)


def load_asset_overrides_frame(
    *,
    settings: Settings | None = None,
    overrides_path: str | Path | None = None,
) -> pd.DataFrame:
    """Load local manual mappings for market assets."""
    resolved_settings = get_settings() if settings is None else settings
    path = (
        resolved_settings.market_data_dir / "asset_overrides.csv"
        if overrides_path is None
        else Path(overrides_path).expanduser().resolve()
    )
    if not path.exists():
        return pd.DataFrame(columns=OVERRIDE_COLUMNS)

    frame = pd.read_csv(path)
    if frame.empty:
        return pd.DataFrame(columns=OVERRIDE_COLUMNS)

    available_columns = [column for column in OVERRIDE_COLUMNS if column in frame.columns]
    if not available_columns:
        return pd.DataFrame(columns=OVERRIDE_COLUMNS)

    current = frame.loc[:, available_columns].copy()
    for column in ("asset_id", "asset_name", "asset_type", "ticker", "broker_symbol", "exchange_mic", "trading_currency", "asset_similar", "notes"):
        if column in current.columns:
            current[column] = current[column].map(_normalize_optional_text)
    if "is_active" in current.columns:
        current["is_active"] = current["is_active"].map(_normalize_optional_bool)

    for column in OVERRIDE_COLUMNS:
        if column not in current.columns:
            current[column] = None

    return current.loc[:, OVERRIDE_COLUMNS]


def write_asset_overrides_template(
    asset_ids: list[str],
    *,
    repository: DuckDBMarketDataRepository | None = None,
    settings: Settings | None = None,
    output_path: str | Path | None = None,
) -> Path:
    """Write or update a local overrides CSV template for selected assets."""
    resolved_settings = get_settings() if settings is None else settings
    resolved_repository = repository or DuckDBMarketDataRepository(settings=resolved_settings)
    current_overrides = load_asset_overrides_frame(settings=resolved_settings, overrides_path=output_path)

    override_path = (
        resolved_settings.market_data_dir / "asset_overrides.csv"
        if output_path is None
        else Path(output_path).expanduser().resolve()
    )
    override_path.parent.mkdir(parents=True, exist_ok=True)

    assets = resolved_repository.list_assets(asset_ids=asset_ids, active_only=False)
    rows: list[dict[str, object]] = []
    existing_by_asset_id = {
        row["asset_id"]: row
        for row in current_overrides.to_dict(orient="records")
        if row.get("asset_id")
    }

    for asset in assets:
        existing = existing_by_asset_id.get(asset.asset_id, {})
        rows.append(
            {
                "asset_id": asset.asset_id,
                "asset_name": existing.get("asset_name") or asset.asset_name,
                "asset_type": existing.get("asset_type") or asset.asset_type,
                "ticker": existing.get("ticker"),
                "broker_symbol": existing.get("broker_symbol") or asset.broker_symbol,
                "exchange_mic": existing.get("exchange_mic") or asset.exchange_mic,
                "trading_currency": existing.get("trading_currency") or asset.trading_currency,
                "asset_similar": existing.get("asset_similar") or asset.asset_similar,
                "is_active": asset.is_active if existing.get("is_active") is None else existing.get("is_active"),
                "notes": existing.get("notes"),
            }
        )

    merged = pd.DataFrame(rows, columns=OVERRIDE_COLUMNS)
    if not current_overrides.empty:
        untouched = current_overrides.loc[~current_overrides["asset_id"].isin(merged["asset_id"])].copy()
        merged = pd.concat([current_overrides.loc[current_overrides["asset_id"].isin(merged["asset_id"])].iloc[0:0], merged, untouched], ignore_index=True)

    merged = merged.drop_duplicates(subset=["asset_id"], keep="first").sort_values("asset_id")
    merged.to_csv(override_path, index=False)
    return override_path


def _load_transaction_asset_hints(asset_dir: Path) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for parquet_path in sorted(asset_dir.glob("*.parquet")) if asset_dir.exists() else []:
        frame = pd.read_parquet(parquet_path)
        if frame.empty:
            continue
        available_columns = [column for column in ASSET_HINT_COLUMNS if column in frame.columns]
        if not available_columns:
            continue

        current = frame.loc[:, available_columns].copy()
        current["exchange_mic"] = _series_or_none(current, "execution_venue").fillna(_series_or_none(current, "reference_exchange"))
        current["broker_symbol"] = None
        current["trading_currency"] = current["trading_currency"].astype("string").str.upper()
        current["first_seen_date"] = _to_date_series(current["first_seen_date"])
        current["last_seen_date"] = _to_date_series(current["last_seen_date"])
        rows.append(
            current.loc[
                :,
                [
                    "asset_id",
                    "asset_name",
                    "asset_type",
                    "isin",
                    "broker_symbol",
                    "exchange_mic",
                    "trading_currency",
                    "first_seen_date",
                    "last_seen_date",
                ],
            ]
        )

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _load_snapshot_assets(snapshot_dir: Path) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for parquet_path in sorted(snapshot_dir.glob("*.parquet")) if snapshot_dir.exists() else []:
        frame = pd.read_parquet(parquet_path)
        if frame.empty:
            continue
        available_columns = [column for column in SNAPSHOT_COLUMNS if column in frame.columns]
        if not available_columns:
            continue

        current = frame.loc[:, available_columns].copy()
        current["exchange_mic"] = None
        current["trading_currency"] = _series_or_none(current, "position_currency").astype("string").str.upper()
        snapshot_dates = _to_date_series(current["snapshot_date"])
        current["first_seen_date"] = snapshot_dates
        current["last_seen_date"] = snapshot_dates
        rows.append(
            current.loc[
                :,
                [
                    "asset_id",
                    "asset_name",
                    "asset_type",
                    "isin",
                    "broker_symbol",
                    "exchange_mic",
                    "trading_currency",
                    "first_seen_date",
                    "last_seen_date",
                ],
            ]
        )

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _load_asset_overrides(
    *,
    settings: Settings,
    overrides_path: str | Path | None,
) -> dict[str, dict[str, object]]:
    frame = load_asset_overrides_frame(settings=settings, overrides_path=overrides_path)
    if frame.empty:
        return {}
    return {
        str(row["asset_id"]): row
        for row in frame.to_dict(orient="records")
        if row.get("asset_id")
    }


def _series_or_none(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([None] * len(frame), index=frame.index, dtype="object")


def _to_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _normalize_optional_text(value: object | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _normalize_optional_bool(value: object | None) -> bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    return None


def _override_or_value(override: dict[str, object], key: str, fallback: str | None) -> str | None:
    value = override.get(key)
    return fallback if value is None else str(value)


def _override_or_bool(override: dict[str, object], key: str, fallback: bool) -> bool:
    value = override.get(key)
    return fallback if value is None else bool(value)


def _pick_last_text(series: pd.Series) -> str | None:
    for value in reversed(series.tolist()):
        if pd.isna(value):
            continue
        text = str(value).strip()
        if text:
            return text.upper() if len(text) == 3 and text.isalpha() else text
    return None


def _pick_first_date(series: pd.Series):
    dates = [value for value in series.tolist() if not pd.isna(value)]
    if not dates:
        return None
    return min(dates)


def _pick_last_date(series: pd.Series):
    dates = [value for value in series.tolist() if not pd.isna(value)]
    if not dates:
        return None
    return max(dates)
