from __future__ import annotations

from streamlit.testing.v1 import AppTest

from src.config import clear_settings_cache, default_repo_root
from src.portfolio.dashboard_agents import SEARCH_PROVIDER_OPTIONS


def test_dashboard_exposes_offline_static_search_option() -> None:
    assert "static" in SEARCH_PROVIDER_OPTIONS


def test_dashboard_renders_with_an_empty_isolated_workspace(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    exports_dir = tmp_path / "exports"
    env_values = {
        "ML_FINANCE_ENV_FILE": str(tmp_path / ".env.missing"),
        "DEGIRO_EXPORTS_DIR": str(exports_dir),
        "EXAMPLE_EXPORTS_DIR": str(tmp_path / "examples"),
        "DATA_DIR": str(data_dir),
        "SAMPLE_DATA_DIR": str(tmp_path / "sample"),
        "PORTFOLIO_DB_PATH": str(data_dir / "portfolio.duckdb"),
        "MARKET_DATA_DIR": str(data_dir / "market_data"),
        "REPORTS_DIR": str(data_dir / "reports"),
        "INVESTMENT_BRIEF_PATH": str(data_dir / "investment_brief.md"),
        "PORTFOLIO_TARGETS_PATH": str(data_dir / "portfolio_targets.yaml"),
    }
    for key, value in env_values.items():
        monkeypatch.setenv(key, value)
    clear_settings_cache()

    app = AppTest.from_file(
        str(default_repo_root() / "src" / "portfolio" / "dashboard.py"),
        default_timeout=30,
    ).run()

    assert not app.exception
    assert [tab.label for tab in app.tabs] == [
        "Vista general",
        "Aportacion",
        "Evolucion",
        "Informes",
        "Actualizar datos",
        "Agentes",
    ]
    assert any(button.label == "Simular aportacion" for button in app.button)
    assert any("No se pudieron calcular metricas" in error.value for error in app.error)
    clear_settings_cache()
