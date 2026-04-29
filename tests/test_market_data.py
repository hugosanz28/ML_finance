from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil
from uuid import uuid4

import duckdb
import pandas as pd
import pytest

from src.config import load_settings
from src.config import default_repo_root
from src.market_data import (
    DailyPriceRecord,
    DuckDBMarketDataRepository,
    MarketAsset,
    PriceDataNotFoundError,
    PriceFetchResult,
    PriceProvider,
    PriceRefreshService,
    YFinancePriceProvider,
    build_price_provider,
    load_asset_overrides_frame,
    load_market_assets_from_normalized_degiro,
    write_asset_overrides_template,
)


class StubPriceProvider(PriceProvider):
    def __init__(self, responses: dict[str, PriceFetchResult | Exception]) -> None:
        self._responses = responses

    @property
    def name(self) -> str:
        return "stub"

    def fetch_daily_prices(
        self,
        asset: MarketAsset,
        *,
        start_date: date,
        end_date: date,
    ) -> PriceFetchResult:
        response = self._responses[asset.asset_id]
        if isinstance(response, Exception):
            raise response
        return response


class RecordingPriceProvider(PriceProvider):
    def __init__(self) -> None:
        self.seen_tickers: list[str | None] = []

    @property
    def name(self) -> str:
        return "recording"

    def fetch_daily_prices(
        self,
        asset: MarketAsset,
        *,
        start_date: date,
        end_date: date,
    ) -> PriceFetchResult:
        self.seen_tickers.append(asset.ticker)
        return PriceFetchResult(
            provider_name=self.name,
            resolved_symbol=asset.ticker or asset.asset_id,
            price_records=(
                DailyPriceRecord(
                    price_date=start_date,
                    price_currency=asset.trading_currency,
                    close_price=10.0,
                ),
            ),
        )


@pytest.fixture
def workspace_tmp_path() -> Path:
    base_dir = default_repo_root() / ".test_tmp"
    base_dir.mkdir(exist_ok=True)

    temp_dir = base_dir / uuid4().hex
    temp_dir.mkdir()

    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_build_price_provider_returns_yfinance_provider() -> None:
    provider = build_price_provider("yfinance")

    assert isinstance(provider, YFinancePriceProvider)
    assert provider.name == "yfinance"


def test_build_price_provider_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unsupported price provider"):
        build_price_provider("unknown-provider")


def test_yfinance_provider_tries_multiple_symbols_and_parses_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = YFinancePriceProvider()
    requested_symbols: list[str] = []

    def fake_download_history(symbol: str, *, start_date: date, end_date: date) -> pd.DataFrame:
        requested_symbols.append(symbol)
        if symbol == "MISSING":
            return pd.DataFrame()

        frame = pd.DataFrame(
            {
                "Open": [500.0, 505.0],
                "High": [501.0, 507.0],
                "Low": [499.5, 504.5],
                "Close": [500.5, 506.5],
                "Adj Close": [500.2, 506.0],
                "Volume": [1000, 1200],
            },
            index=pd.to_datetime(["2025-01-02", "2025-01-03"]),
        )
        frame.attrs["currency"] = "usd"
        return frame

    monkeypatch.setattr(provider, "_download_history", fake_download_history)

    asset = MarketAsset(
        asset_id="asset_spy",
        asset_name="SPY",
        asset_type="etf",
        trading_currency="USD",
        ticker="missing",
        broker_symbol="spy",
    )

    result = provider.fetch_daily_prices(
        asset,
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 3),
    )

    assert requested_symbols == ["MISSING", "SPY"]
    assert result.resolved_symbol == "SPY"
    assert len(result.price_records) == 2
    assert result.price_records[0].price_date == date(2025, 1, 2)
    assert result.price_records[0].price_currency == "USD"
    assert result.price_records[0].close_price == pytest.approx(500.5)
    assert result.price_records[1].volume == 1200


def test_yfinance_provider_skips_invalid_candidate_and_uses_next_one(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = YFinancePriceProvider()
    requested_symbols: list[str] = []

    def fake_download_history(symbol: str, *, start_date: date, end_date: date) -> pd.DataFrame:
        requested_symbols.append(symbol)
        if symbol == "BAD":
            raise ValueError("Invalid ISIN number: BAD")

        frame = pd.DataFrame(
            {"Close": [125.0]},
            index=pd.to_datetime(["2025-01-02"]),
        )
        frame.attrs["currency"] = "USD"
        return frame

    monkeypatch.setattr(provider, "_download_history", fake_download_history)

    asset = MarketAsset(
        asset_id="asset_test",
        asset_name="Test Asset",
        asset_type="stock",
        trading_currency="USD",
        ticker="bad",
        broker_symbol="GOOD",
    )

    result = provider.fetch_daily_prices(
        asset,
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 2),
    )

    assert requested_symbols == ["BAD", "GOOD"]
    assert result.resolved_symbol == "GOOD"
    assert result.price_records[0].close_price == pytest.approx(125.0)


def test_load_market_assets_from_normalized_degiro_merges_assets_and_snapshots(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    assets_dir = settings.normalized_data_dir / "degiro" / "assets"
    snapshots_dir = settings.normalized_data_dir / "degiro" / "portfolio_snapshots"
    assets_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "asset_id": "degiro:isin:US02079K3059",
                "asset_name": "ALPHABET INC CLASS A",
                "asset_type": "stock",
                "isin": "US02079K3059",
                "reference_exchange": "NSY",
                "execution_venue": "XNYS",
                "trading_currency": "usd",
                "first_seen_date": "2025-11-20",
                "last_seen_date": "2026-04-10",
            }
        ]
    ).to_parquet(assets_dir / "transactions_assets.parquet", index=False)
    pd.DataFrame(
        [
            {
                "asset_id": "degiro:isin:US02079K3059",
                "asset_name": "ALPHABET INC CLASS A",
                "asset_type": "stock",
                "isin": "US02079K3059",
                "broker_symbol": "GOOGL",
                "snapshot_date": "2026-04-12",
                "position_currency": "USD",
            }
        ]
    ).to_parquet(snapshots_dir / "portfolio.parquet", index=False)

    assets = load_market_assets_from_normalized_degiro(settings=settings)

    assert len(assets) == 1
    assert assets[0].asset_id == "degiro:isin:US02079K3059"
    assert assets[0].broker_symbol == "GOOGL"
    assert assets[0].exchange_mic == "XNYS"
    assert assets[0].trading_currency == "USD"
    assert assets[0].first_seen_date == date(2025, 11, 20)
    assert assets[0].last_seen_date == date(2026, 4, 12)


def test_load_market_assets_from_normalized_degiro_applies_local_overrides(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    assets_dir = settings.normalized_data_dir / "degiro" / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "asset_id": "degiro:isin:ES0144580Y14",
                "asset_name": "IBERDROLA SA",
                "asset_type": "stock",
                "isin": "ES0144580Y14",
                "reference_exchange": "MAD",
                "execution_venue": "MESI",
                "trading_currency": "EUR",
                "first_seen_date": "2025-12-31",
                "last_seen_date": "2026-04-10",
            }
        ]
    ).to_parquet(assets_dir / "transactions_assets.parquet", index=False)
    settings.market_data_dir.mkdir(parents=True, exist_ok=True)
    (settings.market_data_dir / "asset_overrides.csv").write_text(
        "\n".join(
            [
                "asset_id,ticker,asset_similar,is_active,notes",
                "degiro:isin:ES0144580Y14,IBE.MC,,true,Manual yahoo ticker",
            ]
        ),
        encoding="utf-8",
    )

    overrides = load_asset_overrides_frame(settings=settings)
    assets = load_market_assets_from_normalized_degiro(settings=settings)

    assert overrides.iloc[0]["ticker"] == "IBE.MC"
    assert assets[0].ticker == "IBE.MC"
    assert assets[0].asset_id == "degiro:isin:ES0144580Y14"


def test_write_asset_overrides_template_creates_rows_for_selected_assets(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    repository = DuckDBMarketDataRepository(settings=settings)
    repository.upsert_assets(
        [
            MarketAsset(
                asset_id="degiro:isin:US02079K3059",
                asset_name="ALPHABET INC CLASS A",
                asset_type="stock",
                trading_currency="USD",
                exchange_mic="XNAS",
            )
        ]
    )

    output_path = write_asset_overrides_template(
        ["degiro:isin:US02079K3059"],
        repository=repository,
        settings=settings,
    )
    frame = load_asset_overrides_frame(settings=settings)

    assert output_path == settings.market_data_dir / "asset_overrides.csv"
    assert frame.iloc[0]["asset_id"] == "degiro:isin:US02079K3059"
    assert frame.iloc[0]["exchange_mic"] == "XNAS"
    assert pd.isna(frame.iloc[0]["ticker"])


def test_repository_upsert_assets_updates_existing_asset_metadata(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    repository = DuckDBMarketDataRepository(settings=settings)

    repository.upsert_assets(
        [
            MarketAsset(
                asset_id="degiro:isin:ES0144580Y14",
                asset_name="IBERDROLA SA",
                asset_type="stock",
                trading_currency="EUR",
                first_seen_date=date(2025, 12, 31),
                last_seen_date=date(2026, 4, 10),
            )
        ]
    )

    updated_count = repository.upsert_assets(
        [
            MarketAsset(
                asset_id="degiro:isin:ES0144580Y14",
                asset_name="IBERDROLA SA",
                asset_type="stock",
                trading_currency="EUR",
                isin="ES0144580Y14",
                ticker="IBE.MC",
                broker_symbol="IBE",
                exchange_mic="XMAD",
                first_seen_date=date(2025, 12, 31),
                last_seen_date=date(2026, 4, 12),
                is_active=False,
            )
        ]
    )

    asset = repository.get_asset("degiro:isin:ES0144580Y14")

    assert updated_count == 1
    assert asset is not None
    assert asset.isin == "ES0144580Y14"
    assert asset.ticker == "IBE.MC"
    assert asset.broker_symbol == "IBE"
    assert asset.exchange_mic == "XMAD"
    assert asset.last_seen_date == date(2026, 4, 12)
    assert asset.is_active is False


def test_price_refresh_service_uses_override_metadata_even_if_db_row_already_exists(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    repository = DuckDBMarketDataRepository(settings=settings)
    repository.upsert_assets(
        [
            MarketAsset(
                asset_id="degiro:isin:ES0144580Y14",
                asset_name="IBERDROLA SA",
                asset_type="stock",
                trading_currency="EUR",
            )
        ]
    )

    assets_dir = settings.normalized_data_dir / "degiro" / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "asset_id": "degiro:isin:ES0144580Y14",
                "asset_name": "IBERDROLA SA",
                "asset_type": "stock",
                "isin": "ES0144580Y14",
                "reference_exchange": "MAD",
                "execution_venue": "XMAD",
                "trading_currency": "EUR",
                "first_seen_date": "2025-12-31",
                "last_seen_date": "2026-04-10",
            }
        ]
    ).to_parquet(assets_dir / "transactions_assets.parquet", index=False)
    settings.market_data_dir.mkdir(parents=True, exist_ok=True)
    (settings.market_data_dir / "asset_overrides.csv").write_text(
        "\n".join(
            [
                "asset_id,ticker,is_active",
                "degiro:isin:ES0144580Y14,IBE.MC,true",
            ]
        ),
        encoding="utf-8",
    )

    provider = RecordingPriceProvider()
    service = PriceRefreshService(repository=repository, provider=provider, settings=settings)

    summary = service.refresh_prices(
        start_date=date(2026, 4, 10),
        end_date=date(2026, 4, 10),
        bootstrap_degiro_assets=False,
    )

    assert summary.updated_assets == 1
    assert provider.seen_tickers == ["IBE.MC"]


def test_price_refresh_service_persists_prices_into_duckdb(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    repository = DuckDBMarketDataRepository(settings=settings)
    repository.upsert_assets(
        [
            MarketAsset(
                asset_id="asset_spy",
                asset_name="SPDR S&P 500 ETF Trust",
                asset_type="etf",
                trading_currency="USD",
                ticker="SPY",
            )
        ]
    )

    provider = StubPriceProvider(
        {
            "asset_spy": PriceFetchResult(
                provider_name="stub",
                resolved_symbol="SPY",
                price_records=(
                    DailyPriceRecord(
                        price_date=date(2025, 1, 2),
                        price_currency="USD",
                        open_price=500.0,
                        high_price=501.0,
                        low_price=499.0,
                        close_price=500.5,
                        adjusted_close_price=500.2,
                        volume=1000,
                    ),
                    DailyPriceRecord(
                        price_date=date(2025, 1, 3),
                        price_currency="USD",
                        close_price=506.5,
                    ),
                ),
            )
        }
    )
    service = PriceRefreshService(repository=repository, provider=provider, settings=settings)

    summary = service.refresh_prices(start_date=date(2025, 1, 2), end_date=date(2025, 1, 3))

    assert summary.provider_name == "stub"
    assert summary.updated_assets == 1
    assert summary.total_records == 2
    assert summary.outcomes[0].resolved_asset_id == "asset_spy"
    assert summary.outcomes[0].resolved_symbol == "SPY"

    connection = duckdb.connect(str(settings.portfolio_db_path))
    rows = connection.execute(
        """
        SELECT
            asset_id,
            price_date,
            price_provider,
            price_currency,
            close_price,
            volume
        FROM prices_daily
        ORDER BY price_date
        """
    ).fetchall()
    connection.close()

    assert rows == [
        ("asset_spy", date(2025, 1, 2), "stub", "USD", 500.50000000, 1000),
        ("asset_spy", date(2025, 1, 3), "stub", "USD", 506.50000000, None),
    ]


def test_price_refresh_service_bootstraps_assets_from_normalized_degiro(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    assets_dir = settings.normalized_data_dir / "degiro" / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "asset_id": "degiro:isin:US02079K3059",
                "asset_name": "ALPHABET INC CLASS A",
                "asset_type": "stock",
                "isin": "US02079K3059",
                "reference_exchange": "NSY",
                "execution_venue": "XNYS",
                "trading_currency": "USD",
                "first_seen_date": "2025-11-20",
                "last_seen_date": "2026-04-10",
            }
        ]
    ).to_parquet(assets_dir / "transactions_assets.parquet", index=False)

    repository = DuckDBMarketDataRepository(settings=settings)
    provider = StubPriceProvider(
        {
            "degiro:isin:US02079K3059": PriceFetchResult(
                provider_name="stub",
                resolved_symbol="US02079K3059",
                price_records=(
                    DailyPriceRecord(
                        price_date=date(2026, 4, 10),
                        price_currency="USD",
                        close_price=154.25,
                    ),
                ),
            )
        }
    )
    service = PriceRefreshService(repository=repository, provider=provider, settings=settings)

    summary = service.refresh_prices(start_date=date(2026, 4, 10), end_date=date(2026, 4, 10))

    assert summary.updated_assets == 1
    assert summary.outcomes[0].asset_id == "degiro:isin:US02079K3059"
    assert summary.outcomes[0].resolved_symbol == "US02079K3059"

    asset = repository.get_asset("degiro:isin:US02079K3059")
    assert asset is not None
    assert asset.exchange_mic == "XNYS"
    assert asset.trading_currency == "USD"


def test_price_refresh_service_uses_asset_similar_when_primary_asset_has_no_data(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    repository = DuckDBMarketDataRepository(settings=settings)
    repository.upsert_assets(
        [
            MarketAsset(
                asset_id="asset_original",
                asset_name="Original ETF",
                asset_type="etf",
                trading_currency="USD",
                ticker="ORIGINAL",
                asset_similar="asset_proxy",
            ),
            MarketAsset(
                asset_id="asset_proxy",
                asset_name="Proxy ETF",
                asset_type="etf",
                trading_currency="USD",
                ticker="PROXY",
                is_active=False,
            ),
        ]
    )

    provider = StubPriceProvider(
        {
            "asset_original": PriceDataNotFoundError("Original symbol missing"),
            "asset_proxy": PriceFetchResult(
                provider_name="stub",
                resolved_symbol="PROXY",
                price_records=(
                    DailyPriceRecord(
                        price_date=date(2025, 1, 2),
                        price_currency="USD",
                        close_price=100.25,
                    ),
                ),
            ),
        }
    )
    service = PriceRefreshService(repository=repository, provider=provider, settings=settings)

    summary = service.refresh_prices(
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 2),
        asset_ids=["asset_original"],
    )

    assert summary.updated_assets == 1
    assert summary.skipped_assets == 0
    assert summary.total_records == 1
    assert summary.outcomes[0].asset_id == "asset_original"
    assert summary.outcomes[0].resolved_asset_id == "asset_proxy"
    assert summary.outcomes[0].resolved_symbol == "PROXY"
    assert summary.outcomes[0].used_proxy is True
    assert "asset_similar" in str(summary.outcomes[0].message)

    connection = duckdb.connect(str(settings.portfolio_db_path))
    rows = connection.execute(
        """
        SELECT asset_id, price_provider, close_price
        FROM prices_daily
        """
    ).fetchall()
    connection.close()

    assert rows == [("asset_original", "stub", 100.25000000)]


def test_price_refresh_service_skips_assets_without_prices(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={"DATA_DIR": "private/data", "PORTFOLIO_DB_PATH": "private/data/portfolio.duckdb"},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )
    repository = DuckDBMarketDataRepository(settings=settings)
    repository.upsert_assets(
        [
            MarketAsset(
                asset_id="asset_missing",
                asset_name="Missing Asset",
                asset_type="stock",
                trading_currency="USD",
                ticker="MISS",
            )
        ]
    )

    provider = StubPriceProvider({"asset_missing": PriceDataNotFoundError("No history available")})
    service = PriceRefreshService(repository=repository, provider=provider, settings=settings)

    summary = service.refresh_prices(start_date=date(2025, 1, 2), end_date=date(2025, 1, 2))

    assert summary.updated_assets == 0
    assert summary.skipped_assets == 1
    assert summary.total_records == 0
    assert "No history available" in str(summary.outcomes[0].message)
