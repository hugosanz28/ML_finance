from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import scripts.refresh_fx_rates as refresh_fx_cli
from src.config import default_repo_root
from src.market_data import FxRefreshSummary


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
