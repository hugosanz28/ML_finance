from pathlib import Path
import shutil
from uuid import uuid4

import pytest

from src.config import default_repo_root, load_settings
from src.portfolio.targets import load_portfolio_targets, portfolio_targets_from_mapping


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


def test_load_portfolio_targets_from_simple_yaml(workspace_tmp_path: Path) -> None:
    targets_path = workspace_tmp_path / "private" / "portfolio_targets.yaml"
    targets_path.parent.mkdir(parents=True)
    targets_path.write_text(
        "\n".join(
            [
                "base_currency: EUR",
                "monthly_contribution: 750",
                "risk_profile: medium",
                "target_allocation:",
                "  ETFs: 70",
                "  stocks: 20",
                "  cash: 10",
                "max_single_asset_weight: 15",
                "max_sector_weight: 30",
                "rebalance_mode: contributions_only",
            ]
        ),
        encoding="utf-8",
    )
    settings = load_settings(
        repo_root=workspace_tmp_path,
        env={"PORTFOLIO_TARGETS_PATH": str(targets_path)},
        env_file=workspace_tmp_path / ".env.missing",
    )

    targets = load_portfolio_targets(settings=settings)

    assert targets is not None
    assert targets.base_currency == "EUR"
    assert targets.monthly_contribution == 750
    assert targets.risk_profile == "medium"
    assert targets.target_weights() == {"ETFs": 0.70, "stocks": 0.20, "cash": 0.10}
    assert targets.max_single_asset_weight == 0.15
    assert targets.max_sector_weight == 0.30
    assert targets.to_agent_payload()["rebalance_mode"] == "contributions_only"


def test_load_portfolio_targets_returns_none_when_optional_file_is_missing(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path, env={}, env_file=workspace_tmp_path / ".env.missing")

    assert load_portfolio_targets(settings=settings) is None


def test_portfolio_targets_validation_rejects_missing_allocation() -> None:
    with pytest.raises(ValueError, match="target_allocation"):
        portfolio_targets_from_mapping({"base_currency": "EUR"})
