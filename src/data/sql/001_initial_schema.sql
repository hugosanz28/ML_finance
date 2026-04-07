-- Initial DuckDB schema for ML_finance.
--
-- Scope:
-- - normalized broker events and reference data
-- - market data needed for valuation
-- - report metadata
--
-- The schema stays broker-agnostic but includes lineage fields so a future
-- DEGIRO importer can load normalized records without an immediate migration.

CREATE TABLE IF NOT EXISTS assets_master (
    asset_id VARCHAR PRIMARY KEY,
    asset_type VARCHAR NOT NULL,
    asset_name VARCHAR NOT NULL,
    asset_similar VARCHAR,
    isin VARCHAR,
    ticker VARCHAR,
    broker_symbol VARCHAR,
    exchange_mic VARCHAR,
    trading_currency VARCHAR NOT NULL CHECK (length(trading_currency) = 3),
    issuer_name VARCHAR,
    country_code VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    first_seen_date DATE,
    last_seen_date DATE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (asset_similar) REFERENCES assets_master(asset_id),
    CHECK (isin IS NULL OR length(isin) = 12),
    CHECK (country_code IS NULL OR length(country_code) = 2),
    CHECK (asset_similar IS NULL OR asset_similar <> asset_id)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR PRIMARY KEY,
    broker VARCHAR NOT NULL DEFAULT 'DEGIRO',
    account_id VARCHAR,
    asset_id VARCHAR NOT NULL,
    external_reference VARCHAR,
    trade_date DATE NOT NULL,
    settlement_date DATE,
    transaction_type VARCHAR NOT NULL,
    quantity DECIMAL(20,8) NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(20,8) NOT NULL CHECK (unit_price >= 0),
    gross_amount DECIMAL(20,8) NOT NULL CHECK (gross_amount >= 0),
    fees_amount DECIMAL(20,8) NOT NULL DEFAULT 0 CHECK (fees_amount >= 0),
    taxes_amount DECIMAL(20,8) NOT NULL DEFAULT 0 CHECK (taxes_amount >= 0),
    net_cash_amount DECIMAL(20,8) NOT NULL,
    transaction_currency VARCHAR NOT NULL CHECK (length(transaction_currency) = 3),
    base_currency VARCHAR CHECK (length(base_currency) = 3),
    fx_rate_to_base DECIMAL(20,10) CHECK (fx_rate_to_base > 0),
    net_cash_amount_base DECIMAL(20,8),
    notes VARCHAR,
    source_file VARCHAR,
    source_row BIGINT,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (asset_id) REFERENCES assets_master(asset_id),
    CHECK (settlement_date IS NULL OR settlement_date >= trade_date)
);

CREATE TABLE IF NOT EXISTS cash_movements (
    cash_movement_id VARCHAR PRIMARY KEY,
    broker VARCHAR NOT NULL DEFAULT 'DEGIRO',
    account_id VARCHAR,
    asset_id VARCHAR,
    external_reference VARCHAR,
    movement_date DATE NOT NULL,
    value_date DATE,
    movement_type VARCHAR NOT NULL,
    description VARCHAR,
    amount DECIMAL(20,8) NOT NULL,
    movement_currency VARCHAR NOT NULL CHECK (length(movement_currency) = 3),
    base_currency VARCHAR CHECK (length(base_currency) = 3),
    fx_rate_to_base DECIMAL(20,10) CHECK (fx_rate_to_base > 0),
    amount_base DECIMAL(20,8),
    source_file VARCHAR,
    source_row BIGINT,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (asset_id) REFERENCES assets_master(asset_id),
    CHECK (value_date IS NULL OR value_date >= movement_date)
);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    broker VARCHAR NOT NULL DEFAULT 'SYSTEM',
    account_id VARCHAR,
    snapshot_date DATE NOT NULL,
    asset_id VARCHAR NOT NULL,
    snapshot_source VARCHAR NOT NULL,
    quantity DECIMAL(20,8) NOT NULL CHECK (quantity >= 0),
    average_cost DECIMAL(20,8),
    market_price DECIMAL(20,8),
    market_value DECIMAL(20,8),
    position_currency VARCHAR NOT NULL CHECK (length(position_currency) = 3),
    base_currency VARCHAR CHECK (length(base_currency) = 3),
    fx_rate_to_base DECIMAL(20,10) CHECK (fx_rate_to_base > 0),
    market_value_base DECIMAL(20,8),
    unrealized_pnl_base DECIMAL(20,8),
    source_file VARCHAR,
    source_row BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (asset_id) REFERENCES assets_master(asset_id),
    CHECK (market_value IS NULL OR market_value >= 0)
);

CREATE TABLE IF NOT EXISTS prices_daily (
    asset_id VARCHAR NOT NULL,
    price_date DATE NOT NULL,
    price_provider VARCHAR NOT NULL,
    price_currency VARCHAR NOT NULL CHECK (length(price_currency) = 3),
    open_price DECIMAL(20,8),
    high_price DECIMAL(20,8),
    low_price DECIMAL(20,8),
    close_price DECIMAL(20,8) NOT NULL CHECK (close_price >= 0),
    adjusted_close_price DECIMAL(20,8),
    volume BIGINT CHECK (volume IS NULL OR volume >= 0),
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asset_id, price_date, price_provider),
    FOREIGN KEY (asset_id) REFERENCES assets_master(asset_id),
    CHECK (high_price IS NULL OR low_price IS NULL OR high_price >= low_price)
);

CREATE TABLE IF NOT EXISTS fx_rates (
    base_currency VARCHAR NOT NULL CHECK (length(base_currency) = 3),
    quote_currency VARCHAR NOT NULL CHECK (length(quote_currency) = 3),
    rate_date DATE NOT NULL,
    rate_provider VARCHAR NOT NULL,
    rate DECIMAL(20,10) NOT NULL CHECK (rate > 0),
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (base_currency, quote_currency, rate_date, rate_provider),
    CHECK (base_currency <> quote_currency)
);

CREATE TABLE IF NOT EXISTS reports_history (
    report_id VARCHAR PRIMARY KEY,
    report_type VARCHAR NOT NULL,
    report_period_start DATE,
    report_period_end DATE,
    as_of_date DATE,
    generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    report_format VARCHAR NOT NULL DEFAULT 'md',
    report_path VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'generated',
    base_currency VARCHAR CHECK (length(base_currency) = 3),
    source_snapshot_date DATE,
    parameters_json VARCHAR,
    report_hash VARCHAR,
    notes VARCHAR,
    CHECK (
        report_period_start IS NULL
        OR report_period_end IS NULL
        OR report_period_end >= report_period_start
    )
);
