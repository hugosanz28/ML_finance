"""Project configuration helpers for environment values and common paths."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values


DEFAULT_ENV_VALUES = {
    "DEGIRO_EXPORTS_DIR": "src/degiro_exports/local",
    "EXAMPLE_EXPORTS_DIR": "src/degiro_exports/example",
    "DATA_DIR": "src/data/local",
    "SAMPLE_DATA_DIR": "src/data/sample",
    "DEFAULT_CURRENCY": "EUR",
    "DEFAULT_TIMEZONE": "Europe/Madrid",
    "PRICE_PROVIDER": "yfinance",
    "MONTHLY_CONTRIBUTION_EUR": "500",
    "INVESTMENT_BRIEF_PATH": "src/data/local/investment_brief.md",
}


def default_repo_root() -> Path:
    """Return the repository root based on this module location."""
    return Path(__file__).resolve().parent.parent


def _read_env_file(env_file: Path) -> dict[str, str]:
    if not env_file.exists():
        return {}

    values = dotenv_values(env_file)
    return {key: value for key, value in values.items() if value is not None}


def _resolve_path(value: str, repo_root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _get_required_str(values: Mapping[str, str], key: str, default: str | None = None) -> str:
    value = values.get(key, default)
    if value is None:
        raise ValueError(f"Missing required configuration value: {key}")

    text = str(value).strip()
    if not text:
        raise ValueError(f"Configuration value cannot be empty: {key}")
    return text


def _get_float(values: Mapping[str, str], key: str, default: str) -> float:
    raw_value = _get_required_str(values, key, default)
    try:
        return float(raw_value)
    except ValueError as exc:
        raise ValueError(f"Invalid numeric value for {key}: {raw_value}") from exc


def _unique_paths(paths: tuple[Path, ...]) -> tuple[Path, ...]:
    seen: set[Path] = set()
    ordered: list[Path] = []
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        ordered.append(path)
    return tuple(ordered)


@dataclass(frozen=True)
class Settings:
    """Resolved project settings and path helpers."""

    repo_root: Path
    env_file: Path
    degiro_exports_dir: Path
    example_exports_dir: Path
    data_dir: Path
    sample_data_dir: Path
    portfolio_db_path: Path
    market_data_dir: Path
    reports_dir: Path
    raw_data_dir: Path
    normalized_data_dir: Path
    curated_data_dir: Path
    default_currency: str
    default_timezone: str
    price_provider: str
    monthly_contribution_eur: float
    investment_brief_path: Path

    @property
    def initial_schema_path(self) -> Path:
        return self.repo_root / "src" / "data" / "sql" / "001_initial_schema.sql"

    @property
    def local_directories(self) -> tuple[Path, ...]:
        return _unique_paths(
            (
                self.degiro_exports_dir,
                self.data_dir,
                self.raw_data_dir,
                self.normalized_data_dir,
                self.curated_data_dir,
                self.market_data_dir,
                self.reports_dir,
                self.portfolio_db_path.parent,
            )
        )


def load_settings(
    *,
    env: Mapping[str, str] | None = None,
    env_file: str | Path | None = None,
    repo_root: str | Path | None = None,
) -> Settings:
    """Build settings from defaults, .env values, and environment overrides."""
    resolved_repo_root = default_repo_root() if repo_root is None else Path(repo_root).expanduser().resolve()
    resolved_env_file = resolved_repo_root / ".env" if env_file is None else Path(env_file).expanduser()
    if not resolved_env_file.is_absolute():
        resolved_env_file = resolved_repo_root / resolved_env_file
    resolved_env_file = resolved_env_file.resolve()

    values: dict[str, str] = dict(DEFAULT_ENV_VALUES)
    values.update(_read_env_file(resolved_env_file))

    if env is None:
        values.update({key: value for key, value in os.environ.items()})
    else:
        values.update({key: value for key, value in env.items() if value is not None})

    data_dir_value = _get_required_str(values, "DATA_DIR", DEFAULT_ENV_VALUES["DATA_DIR"])

    degiro_exports_dir = _resolve_path(
        _get_required_str(values, "DEGIRO_EXPORTS_DIR", DEFAULT_ENV_VALUES["DEGIRO_EXPORTS_DIR"]),
        resolved_repo_root,
    )
    example_exports_dir = _resolve_path(
        _get_required_str(values, "EXAMPLE_EXPORTS_DIR", DEFAULT_ENV_VALUES["EXAMPLE_EXPORTS_DIR"]),
        resolved_repo_root,
    )
    data_dir = _resolve_path(data_dir_value, resolved_repo_root)
    sample_data_dir = _resolve_path(
        _get_required_str(values, "SAMPLE_DATA_DIR", DEFAULT_ENV_VALUES["SAMPLE_DATA_DIR"]),
        resolved_repo_root,
    )
    portfolio_db_path = _resolve_path(
        _get_required_str(values, "PORTFOLIO_DB_PATH", str(Path(data_dir_value) / "portfolio.duckdb")),
        resolved_repo_root,
    )
    market_data_dir = _resolve_path(
        _get_required_str(values, "MARKET_DATA_DIR", str(Path(data_dir_value) / "market_data")),
        resolved_repo_root,
    )
    reports_dir = _resolve_path(
        _get_required_str(values, "REPORTS_DIR", str(Path(data_dir_value) / "reports")),
        resolved_repo_root,
    )
    investment_brief_path = _resolve_path(
        _get_required_str(values, "INVESTMENT_BRIEF_PATH", DEFAULT_ENV_VALUES["INVESTMENT_BRIEF_PATH"]),
        resolved_repo_root,
    )

    return Settings(
        repo_root=resolved_repo_root,
        env_file=resolved_env_file,
        degiro_exports_dir=degiro_exports_dir,
        example_exports_dir=example_exports_dir,
        data_dir=data_dir,
        sample_data_dir=sample_data_dir,
        portfolio_db_path=portfolio_db_path,
        market_data_dir=market_data_dir,
        reports_dir=reports_dir,
        raw_data_dir=data_dir / "raw",
        normalized_data_dir=data_dir / "normalized",
        curated_data_dir=data_dir / "curated",
        default_currency=_get_required_str(values, "DEFAULT_CURRENCY", DEFAULT_ENV_VALUES["DEFAULT_CURRENCY"]).upper(),
        default_timezone=_get_required_str(values, "DEFAULT_TIMEZONE", DEFAULT_ENV_VALUES["DEFAULT_TIMEZONE"]),
        price_provider=_get_required_str(values, "PRICE_PROVIDER", DEFAULT_ENV_VALUES["PRICE_PROVIDER"]),
        monthly_contribution_eur=_get_float(
            values,
            "MONTHLY_CONTRIBUTION_EUR",
            DEFAULT_ENV_VALUES["MONTHLY_CONTRIBUTION_EUR"],
        ),
        investment_brief_path=investment_brief_path,
    )


def ensure_local_directories(settings: Settings | None = None) -> tuple[Path, ...]:
    """Create local private directories required by the project."""
    resolved_settings = get_settings() if settings is None else settings
    for directory in resolved_settings.local_directories:
        directory.mkdir(parents=True, exist_ok=True)
    return resolved_settings.local_directories


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached project settings using the current environment."""
    return load_settings()


def clear_settings_cache() -> None:
    """Clear cached settings so callers can rebuild them after env changes."""
    get_settings.cache_clear()
