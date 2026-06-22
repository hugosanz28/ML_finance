from pathlib import Path
import shutil
from uuid import uuid4

import pytest

from src.config import default_repo_root, ensure_local_directories, load_settings


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


def test_load_settings_uses_repo_relative_defaults(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={},
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )

    assert settings.repo_root == workspace_tmp_path
    assert settings.degiro_exports_dir == workspace_tmp_path / "src" / "degiro_exports" / "local"
    assert settings.example_exports_dir == workspace_tmp_path / "src" / "degiro_exports" / "example"
    assert settings.data_dir == workspace_tmp_path / "src" / "data" / "local"
    assert settings.sample_data_dir == workspace_tmp_path / "src" / "data" / "sample"
    assert settings.market_data_dir == settings.data_dir / "market_data"
    assert settings.reports_dir == settings.data_dir / "reports"
    assert settings.portfolio_db_path == settings.data_dir / "portfolio.duckdb"
    assert settings.raw_data_dir == settings.data_dir / "raw"
    assert settings.normalized_data_dir == settings.data_dir / "normalized"
    assert settings.curated_data_dir == settings.data_dir / "curated"
    assert settings.default_currency == "EUR"
    assert settings.default_timezone == "Europe/Madrid"
    assert settings.price_provider == "yfinance"
    assert settings.monthly_contribution_eur == 500.0
    assert settings.investment_brief_path == workspace_tmp_path / "src" / "data" / "local" / "investment_brief.md"
    assert settings.portfolio_targets_path == workspace_tmp_path / "src" / "data" / "local" / "portfolio_targets.yaml"


def test_load_settings_reads_values_from_env_file(workspace_tmp_path: Path) -> None:
    env_file = workspace_tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "DEGIRO_EXPORTS_DIR=private/degiro",
                "DATA_DIR=private/data",
                "DEFAULT_CURRENCY=USD",
                "DEFAULT_TIMEZONE=America/New_York",
                "MONTHLY_CONTRIBUTION_EUR=750",
                "INVESTMENT_BRIEF_PATH=private/brief.md",
                "PORTFOLIO_TARGETS_PATH=private/targets.yaml",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(env={}, repo_root=workspace_tmp_path, env_file=env_file)

    assert settings.degiro_exports_dir == workspace_tmp_path / "private" / "degiro"
    assert settings.data_dir == workspace_tmp_path / "private" / "data"
    assert settings.market_data_dir == settings.data_dir / "market_data"
    assert settings.reports_dir == settings.data_dir / "reports"
    assert settings.portfolio_db_path == settings.data_dir / "portfolio.duckdb"
    assert settings.default_currency == "USD"
    assert settings.default_timezone == "America/New_York"
    assert settings.monthly_contribution_eur == 750.0
    assert settings.investment_brief_path == workspace_tmp_path / "private" / "brief.md"
    assert settings.portfolio_targets_path == workspace_tmp_path / "private" / "targets.yaml"


def test_explicit_overrides_take_precedence_over_env_file(workspace_tmp_path: Path) -> None:
    env_file = workspace_tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "DATA_DIR=private/data",
                "REPORTS_DIR=private/data/reports_from_env_file",
                "PORTFOLIO_DB_PATH=private/data/portfolio_from_env_file.duckdb",
                "PRICE_PROVIDER=yfinance",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(
        env={
            "REPORTS_DIR": "artifacts/reports",
            "PORTFOLIO_DB_PATH": "state/custom.duckdb",
            "SAMPLE_DATA_DIR": str(workspace_tmp_path / "external-sample"),
            "PRICE_PROVIDER": "stooq",
        },
        repo_root=workspace_tmp_path,
        env_file=env_file,
    )

    assert settings.data_dir == workspace_tmp_path / "private" / "data"
    assert settings.reports_dir == workspace_tmp_path / "artifacts" / "reports"
    assert settings.portfolio_db_path == workspace_tmp_path / "state" / "custom.duckdb"
    assert settings.sample_data_dir == workspace_tmp_path / "external-sample"
    assert settings.price_provider == "stooq"


def test_load_settings_can_use_env_file_from_environment(monkeypatch: pytest.MonkeyPatch, workspace_tmp_path: Path) -> None:
    env_file = workspace_tmp_path / "demo" / ".env.demo"
    env_file.parent.mkdir(parents=True)
    env_file.write_text(
        "\n".join(
            [
                "DATA_DIR=demo/local_data",
                "DEGIRO_EXPORTS_DIR=demo/synthetic_degiro_exports",
                "REPORTS_DIR=demo/local_data/reports",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("ML_FINANCE_ENV_FILE", str(env_file))

    settings = load_settings(env={}, repo_root=workspace_tmp_path)

    assert settings.env_file == env_file
    assert settings.data_dir == workspace_tmp_path / "demo" / "local_data"
    assert settings.degiro_exports_dir == workspace_tmp_path / "demo" / "synthetic_degiro_exports"
    assert settings.reports_dir == workspace_tmp_path / "demo" / "local_data" / "reports"


def test_ensure_local_directories_creates_private_workspace_dirs(workspace_tmp_path: Path) -> None:
    settings = load_settings(
        env={
            "DEGIRO_EXPORTS_DIR": "private/degiro_exports",
            "DATA_DIR": "private/data",
            "PORTFOLIO_DB_PATH": "private/state/portfolio.duckdb",
        },
        repo_root=workspace_tmp_path,
        env_file=workspace_tmp_path / ".env.missing",
    )

    created_directories = ensure_local_directories(settings)

    for directory in created_directories:
        assert directory.is_dir()

    assert settings.portfolio_db_path.parent in created_directories
    assert not settings.portfolio_db_path.exists()


def test_load_settings_raises_for_invalid_numeric_values(workspace_tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="MONTHLY_CONTRIBUTION_EUR"):
        load_settings(
            env={"MONTHLY_CONTRIBUTION_EUR": "not-a-number"},
            repo_root=workspace_tmp_path,
            env_file=workspace_tmp_path / ".env.missing",
        )
