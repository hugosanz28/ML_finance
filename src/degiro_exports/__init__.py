"""Helpers for working with DEGIRO export files."""

from .cash_movements import (
    ParsedDegiroCashMovements,
    parse_and_persist_degiro_cash_movements,
    parse_degiro_cash_movements_csv,
    persist_degiro_cash_movements_dataset,
)
from .importer import (
    DegiroImportOutcome,
    DegiroImportSummary,
    classify_degiro_export,
    import_degiro_exports,
)
from .portfolio_snapshots import (
    ParsedDegiroPortfolioSnapshots,
    parse_and_persist_degiro_portfolio_snapshots,
    parse_degiro_portfolio_snapshot_csv,
    persist_degiro_portfolio_snapshots_dataset,
)
from .transactions import (
    ParsedDegiroTransactions,
    parse_and_persist_degiro_transactions,
    parse_degiro_transactions_csv,
    persist_degiro_transactions_dataset,
)
from .warehouse import DegiroWarehouseLoadSummary, load_normalized_degiro_to_duckdb

__all__ = [
    "DegiroImportOutcome",
    "DegiroImportSummary",
    "DegiroWarehouseLoadSummary",
    "ParsedDegiroCashMovements",
    "ParsedDegiroPortfolioSnapshots",
    "ParsedDegiroTransactions",
    "classify_degiro_export",
    "import_degiro_exports",
    "load_normalized_degiro_to_duckdb",
    "parse_degiro_cash_movements_csv",
    "parse_degiro_portfolio_snapshot_csv",
    "parse_degiro_transactions_csv",
    "persist_degiro_cash_movements_dataset",
    "persist_degiro_portfolio_snapshots_dataset",
    "persist_degiro_transactions_dataset",
    "parse_and_persist_degiro_cash_movements",
    "parse_and_persist_degiro_portfolio_snapshots",
    "parse_and_persist_degiro_transactions",
]
