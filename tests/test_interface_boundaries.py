from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import scripts.refresh_fx_rates as refresh_fx_cli
import src.portfolio.dashboard_agents as agents_dashboard
from src.config import default_repo_root
from src.market_data import FxRefreshSummary
from src.portfolio.dashboard_agents import _parse_portfolio_targets_input


USER_FACING_SCRIPTS = (
    "generate_monthly_report.py",
    "import_degiro.py",
    "refresh_fx_rates.py",
    "refresh_market_data.py",
    "run_monitor_tematico.py",
    "run_monthly_agents.py",
)
FORBIDDEN_INTERFACE_PREFIXES = (
    "src.agents",
    "src.degiro_exports",
    "src.market_data",
    "src.reports",
)
FORBIDDEN_TARGET_DOMAIN_SYMBOLS = {
    "PortfolioTargets",
    "load_portfolio_targets",
    "portfolio_targets_from_mapping",
}
PORTFOLIO_TARGET_APPLICATION_CONTRACT = {
    "ReadPortfolioTargetsUseCase",
    "UpdatePortfolioTargetsRequest",
    "UpdatePortfolioTargetsUseCase",
}


@pytest.mark.parametrize("script_name", USER_FACING_SCRIPTS)
def test_user_facing_scripts_are_importable(script_name: str) -> None:
    repo_root = default_repo_root()
    completed = subprocess.run(
        [sys.executable, str(repo_root / "scripts" / script_name), "--help"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "usage:" in completed.stdout.lower()


def test_user_interfaces_do_not_bypass_application_layer() -> None:
    repo_root = default_repo_root()
    interface_paths = [repo_root / "scripts" / name for name in USER_FACING_SCRIPTS]
    interface_paths.extend(sorted((repo_root / "src" / "portfolio").glob("dashboard*.py")))

    violations: list[str] = []
    for path in interface_paths:
        for imported_module, line_number in _imported_modules(path):
            if imported_module.startswith(FORBIDDEN_INTERFACE_PREFIXES):
                relative_path = path.relative_to(repo_root)
                violations.append(f"{relative_path}:{line_number} imports {imported_module}")

    assert violations == []


def test_dashboard_portfolio_targets_use_application_contract() -> None:
    repo_root = default_repo_root()
    dashboard_paths = sorted((repo_root / "src" / "portfolio").glob("dashboard*.py"))
    application_symbols: set[str] = set()
    violations: list[str] = []

    for path in dashboard_paths:
        for module, symbol, line_number in _imported_symbols(path):
            if module.startswith("src.application"):
                application_symbols.add(symbol)
            if module == "src.portfolio.targets" or (
                module == "src.portfolio" and symbol in FORBIDDEN_TARGET_DOMAIN_SYMBOLS
            ):
                relative_path = path.relative_to(repo_root)
                violations.append(f"{relative_path}:{line_number} imports {module}.{symbol}")

    assert violations == []
    assert PORTFOLIO_TARGET_APPLICATION_CONTRACT <= application_symbols


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("{}", {}),
        (
            '{"base_currency":"EUR","target_allocation":{"core":0.8,"satellite":0.2}}',
            {
                "base_currency": "EUR",
                "target_allocation": {"core": 0.8, "satellite": 0.2},
            },
        ),
    ],
)
def test_portfolio_targets_ui_parser_accepts_json_objects(
    raw: str,
    expected: dict[str, object],
) -> None:
    parsed, error = _parse_portfolio_targets_input(raw)

    assert parsed == expected
    assert error is None


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "[]",
        "null",
        '"targets"',
        "42",
        '{"target_allocation":{"core":NaN}}',
        '{"target_allocation":{"core":Infinity}}',
        '{"target_allocation":{"core":-Infinity}}',
    ],
)
def test_portfolio_targets_ui_parser_rejects_non_object_or_non_strict_json(
    raw: str,
) -> None:
    parsed, error = _parse_portfolio_targets_input(raw)

    assert parsed is None
    assert error


def test_portfolio_targets_reload_refreshes_hash_and_editor_together(monkeypatch) -> None:
    session_state: dict[str, object] = {
        "hash": "sha256:old",
        "editor": '{"target_allocation":{"old":1}}',
    }
    monkeypatch.setattr(agents_dashboard.st, "session_state", session_state)

    agents_dashboard._reload_portfolio_targets_editor(
        "hash",
        "editor",
        "sha256:new",
        {"target_allocation": {"core": 0.8, "satellite": 0.2}},
    )

    assert session_state["hash"] == "sha256:new"
    assert json.loads(str(session_state["editor"])) == {
        "target_allocation": {"core": 0.8, "satellite": 0.2}
    }


def test_fx_cli_with_explicit_pairs_does_not_read_normalized_data(monkeypatch) -> None:
    monkeypatch.setattr(
        refresh_fx_cli,
        "parse_args",
        lambda: SimpleNamespace(
            start_date=None,
            end_date=None,
            pairs=["EUR/USD"],
            provider="synthetic",
            only_missing_base=False,
            no_infer_from_normalized=True,
        ),
    )

    class UnexpectedInferenceUseCase:
        def __init__(self) -> None:
            raise AssertionError("FX inference must stay disabled")

    class FakeRefreshFxUseCase:
        def execute(self, request):
            assert request.pairs == (("EUR", "USD"),)
            assert request.infer_from_normalized is False
            return SimpleNamespace(summary=FxRefreshSummary(provider_name="synthetic", outcomes=()))

    monkeypatch.setattr(refresh_fx_cli, "InferFxRequirementsUseCase", UnexpectedInferenceUseCase)
    monkeypatch.setattr(refresh_fx_cli, "RefreshFxUseCase", FakeRefreshFxUseCase)

    assert refresh_fx_cli.main() == 0


def _imported_modules(path: Path) -> list[tuple[str, int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imports.append((node.module, node.lineno))
        elif isinstance(node, ast.Import):
            imports.extend((alias.name, node.lineno) for alias in node.names)
    return imports


def _imported_symbols(path: Path) -> list[tuple[str, str, int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[tuple[str, str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imports.extend((node.module, alias.name, node.lineno) for alias in node.names)
        elif isinstance(node, ast.Import):
            imports.extend((alias.name, "*", node.lineno) for alias in node.names)
    return imports
