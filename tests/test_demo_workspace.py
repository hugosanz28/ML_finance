from pathlib import Path

from src.config import default_repo_root, load_settings
from src.degiro_exports.cash_movements import parse_degiro_cash_movements_csv
from src.degiro_exports.portfolio_snapshots import parse_degiro_portfolio_snapshot_csv
from src.degiro_exports.transactions import parse_degiro_transactions_csv


def test_demo_env_is_separated_from_private_local_paths() -> None:
    repo_root = default_repo_root()
    settings = load_settings(
        env={},
        repo_root=repo_root,
        env_file=repo_root / "demo" / "synthetic_config" / ".env.demo",
    )

    assert settings.data_dir == repo_root / "demo" / "local_data"
    assert settings.degiro_exports_dir == repo_root / "demo" / "synthetic_degiro_exports"
    assert settings.portfolio_db_path == repo_root / "demo" / "local_data" / "portfolio.duckdb"
    assert settings.investment_brief_path == repo_root / "demo" / "synthetic_config" / "investment_brief.md"
    assert settings.portfolio_targets_path == repo_root / "demo" / "synthetic_config" / "portfolio_targets.yaml"
    assert settings.data_dir != repo_root / "src" / "data" / "local"
    assert settings.degiro_exports_dir != repo_root / "src" / "degiro_exports" / "local"


def test_demo_synthetic_degiro_exports_parse_with_real_parsers() -> None:
    repo_root = default_repo_root()
    incoming = repo_root / "demo" / "synthetic_degiro_exports" / "incoming"

    transactions = parse_degiro_transactions_csv(
        incoming / "transactions_2026-01-15_2026-04-30.csv",
        base_currency="EUR",
        source_root=repo_root / "demo" / "synthetic_degiro_exports",
    )
    cash_movements = parse_degiro_cash_movements_csv(
        incoming / "account_2026-01-15_2026-04-30.csv",
        base_currency="EUR",
        source_root=repo_root / "demo" / "synthetic_degiro_exports",
    )
    snapshots = parse_degiro_portfolio_snapshot_csv(
        incoming / "portfolio_2026-04-30.csv",
        base_currency="EUR",
        source_root=repo_root / "demo" / "synthetic_degiro_exports",
    )

    assert len(transactions.transactions) == 4
    assert len(cash_movements.cash_movements) == 9
    assert len(snapshots.snapshots) == 4
    assert set(transactions.transactions["transaction_type"]) == {"BUY"}
    assert "DEPOSIT" in set(cash_movements.cash_movements["movement_type"])
    assert snapshots.snapshots["market_value_base"].sum() > 9000
