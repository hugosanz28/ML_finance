from pathlib import Path

import duckdb
import pytest


SCHEMA_PATH = Path("src/data/sql/001_initial_schema.sql")


def apply_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_schema_creates_expected_tables() -> None:
    connection = duckdb.connect(":memory:")
    apply_schema(connection)

    tables = {
        row[0]
        for row in connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    }

    assert tables >= {
        "assets_master",
        "transactions",
        "cash_movements",
        "portfolio_snapshots",
        "prices_daily",
        "fx_rates",
        "reports_history",
    }


def test_schema_exposes_critical_columns() -> None:
    connection = duckdb.connect(":memory:")
    apply_schema(connection)

    expected_columns = {
        "assets_master": {
            "asset_id",
            "asset_type",
            "asset_name",
            "asset_similar",
            "trading_currency",
        },
        "transactions": {
            "transaction_id",
            "asset_id",
            "trade_date",
            "transaction_type",
            "net_cash_amount",
        },
        "cash_movements": {
            "cash_movement_id",
            "movement_date",
            "movement_type",
            "amount",
        },
        "portfolio_snapshots": {
            "snapshot_id",
            "snapshot_date",
            "asset_id",
            "snapshot_source",
        },
        "prices_daily": {
            "asset_id",
            "price_date",
            "price_provider",
            "close_price",
        },
        "fx_rates": {
            "base_currency",
            "quote_currency",
            "rate_date",
            "rate",
        },
        "reports_history": {
            "report_id",
            "report_type",
            "generated_at",
            "report_path",
        },
    }

    for table_name, required_columns in expected_columns.items():
        table_columns = {
            row[1]
            for row in connection.execute(f"SELECT * FROM pragma_table_info('{table_name}')").fetchall()
        }
        assert required_columns <= table_columns


def test_schema_enforces_primary_and_foreign_keys() -> None:
    connection = duckdb.connect(":memory:")
    apply_schema(connection)

    connection.execute(
        """
        INSERT INTO assets_master (
            asset_id,
            asset_type,
            asset_name,
            isin,
            ticker,
            trading_currency
        )
        VALUES (
            'asset_spy',
            'etf',
            'SPDR S&P 500 ETF Trust',
            'US78462F1030',
            'SPY',
            'USD'
        )
        """
    )

    connection.execute(
        """
        INSERT INTO assets_master (
            asset_id,
            asset_type,
            asset_name,
            asset_similar,
            trading_currency
        )
        VALUES (
            'asset_proxy_target',
            'etf',
            'Proxy ETF',
            'asset_spy',
            'USD'
        )
        """
    )

    connection.execute(
        """
        INSERT INTO transactions (
            transaction_id,
            asset_id,
            trade_date,
            transaction_type,
            quantity,
            unit_price,
            gross_amount,
            net_cash_amount,
            transaction_currency
        )
        VALUES (
            'txn_001',
            'asset_spy',
            DATE '2025-01-15',
            'BUY',
            2.00000000,
            500.00000000,
            1000.00000000,
            -1002.50000000,
            'USD'
        )
        """
    )

    connection.execute(
        """
        INSERT INTO prices_daily (
            asset_id,
            price_date,
            price_provider,
            price_currency,
            close_price
        )
        VALUES (
            'asset_spy',
            DATE '2025-01-15',
            'yfinance',
            'USD',
            503.12000000
        )
        """
    )

    connection.execute(
        """
        INSERT INTO fx_rates (
            base_currency,
            quote_currency,
            rate_date,
            rate_provider,
            rate
        )
        VALUES (
            'EUR',
            'USD',
            DATE '2025-01-15',
            'ecb',
            1.0800000000
        )
        """
    )

    connection.execute(
        """
        INSERT INTO cash_movements (
            cash_movement_id,
            asset_id,
            movement_date,
            movement_type,
            amount,
            movement_currency
        )
        VALUES (
            'cash_001',
            'asset_spy',
            DATE '2025-01-20',
            'DIVIDEND',
            5.25000000,
            'USD'
        )
        """
    )

    connection.execute(
        """
        INSERT INTO portfolio_snapshots (
            snapshot_id,
            snapshot_date,
            asset_id,
            snapshot_source,
            quantity,
            market_price,
            market_value,
            position_currency
        )
        VALUES (
            'snap_001',
            DATE '2025-01-31',
            'asset_spy',
            'broker_export',
            2.00000000,
            505.00000000,
            1010.00000000,
            'USD'
        )
        """
    )

    connection.execute(
        """
        INSERT INTO reports_history (
            report_id,
            report_type,
            report_period_start,
            report_period_end,
            report_path
        )
        VALUES (
            'report_2025_01',
            'monthly',
            DATE '2025-01-01',
            DATE '2025-01-31',
            'src/data/local/reports/2025-01-monthly.md'
        )
        """
    )

    assert connection.execute("SELECT COUNT(*) FROM assets_master").fetchone()[0] == 2
    assert connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 1
    assert connection.execute("SELECT COUNT(*) FROM prices_daily").fetchone()[0] == 1

    with pytest.raises(duckdb.ConstraintException):
        connection.execute(
            """
            INSERT INTO assets_master (
                asset_id,
                asset_type,
                asset_name,
                asset_similar,
                trading_currency
            )
            VALUES (
                'asset_invalid_proxy',
                'etf',
                'Invalid Proxy ETF',
                'missing_asset',
                'USD'
            )
            """
        )

    with pytest.raises(duckdb.ConstraintException):
        connection.execute(
            """
            INSERT INTO transactions (
                transaction_id,
                asset_id,
                trade_date,
                transaction_type,
                quantity,
                unit_price,
                gross_amount,
                net_cash_amount,
                transaction_currency
            )
            VALUES (
                'txn_001',
                'asset_spy',
                DATE '2025-01-16',
                'BUY',
                1.00000000,
                100.00000000,
                100.00000000,
                -100.00000000,
                'USD'
            )
            """
        )

    with pytest.raises(duckdb.ConstraintException):
        connection.execute(
            """
            INSERT INTO transactions (
                transaction_id,
                asset_id,
                trade_date,
                transaction_type,
                quantity,
                unit_price,
                gross_amount,
                net_cash_amount,
                transaction_currency
            )
            VALUES (
                'txn_002',
                'missing_asset',
                DATE '2025-01-16',
                'BUY',
                1.00000000,
                100.00000000,
                100.00000000,
                -100.00000000,
                'USD'
            )
            """
        )

    with pytest.raises(duckdb.ConstraintException):
        connection.execute(
            """
            INSERT INTO prices_daily (
                asset_id,
                price_date,
                price_provider,
                price_currency,
                close_price
            )
            VALUES (
                'asset_spy',
                DATE '2025-01-15',
                'yfinance',
                'USD',
                503.12000000
            )
            """
        )
