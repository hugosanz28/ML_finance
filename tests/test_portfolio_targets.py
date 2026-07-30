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
    assert targets.asset_bucket_mapping == {}
    assert targets.to_agent_payload()["rebalance_mode"] == "contributions_only"


def test_load_portfolio_targets_returns_none_when_optional_file_is_missing(workspace_tmp_path: Path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path, env={}, env_file=workspace_tmp_path / ".env.missing")

    assert load_portfolio_targets(settings=settings) is None


def test_portfolio_targets_validation_rejects_missing_allocation() -> None:
    with pytest.raises(ValueError, match="target_allocation"):
        portfolio_targets_from_mapping({"base_currency": "EUR"})


def test_portfolio_targets_accepts_legacy_target_weights_alias() -> None:
    targets = portfolio_targets_from_mapping(
        {
            "base_currency": "eur",
            "monthly_contribution": 500,
            "target_weights": {
                "core": 80,
                "satellite": 20,
            },
        }
    )

    assert targets.base_currency == "EUR"
    assert targets.target_weights() == {"core": 0.80, "satellite": 0.20}
    assert targets.to_storage_mapping() == {
        "base_currency": "EUR",
        "target_allocation": {"core": 0.80, "satellite": 0.20},
        "monthly_contribution": 500.0,
        "risk_profile": None,
        "max_single_asset_weight": None,
        "max_sector_weight": None,
        "rebalance_mode": None,
        "asset_bucket_mapping": {},
    }


def test_portfolio_targets_rejects_conflicting_allocation_aliases() -> None:
    with pytest.raises(ValueError, match="same weights"):
        portfolio_targets_from_mapping(
            {
                "target_allocation": {"core": 0.80, "satellite": 0.20},
                "target_weights": {"core": 0.70, "satellite": 0.30},
            }
        )


def test_portfolio_targets_normalizes_valid_asset_bucket_mapping() -> None:
    targets = portfolio_targets_from_mapping(
        {
            "target_allocation": {"core": 0.80, "cash": 0.20},
            "asset_bucket_mapping": {
                " degiro:isin:IE00TEST0001 ": " core ",
                "degiro:cash:eur": "cash",
            },
        }
    )

    assert targets.asset_bucket_mapping == {
        "degiro:isin:IE00TEST0001": "core",
        "degiro:cash:eur": "cash",
    }
    assert targets.to_storage_mapping()["asset_bucket_mapping"] == targets.asset_bucket_mapping
    assert "asset_bucket_mapping" not in targets.to_agent_payload()


@pytest.mark.parametrize(
    ("asset_bucket_mapping", "error"),
    [
        ([], "must be a mapping"),
        ({1: "core"}, "keys and values must be strings"),
        ({"asset": 1}, "keys and values must be strings"),
        ({" ": "core"}, "empty identifier"),
        ({"asset": " "}, "empty bucket"),
        ({"asset": "satellite"}, "unknown target bucket"),
        (
            {"asset": "core", " asset ": "core"},
            "duplicate identifier 'asset' after normalization",
        ),
    ],
)
def test_portfolio_targets_rejects_invalid_asset_bucket_mapping(
    asset_bucket_mapping: object,
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        portfolio_targets_from_mapping(
            {
                "target_allocation": {"core": 1.0},
                "asset_bucket_mapping": asset_bucket_mapping,
            }
        )


def test_simple_yaml_supports_colons_in_asset_bucket_keys(workspace_tmp_path: Path) -> None:
    targets_path = workspace_tmp_path / "portfolio_targets.yaml"
    targets_path.write_text(
        "\n".join(
            [
                "target_allocation:",
                "  core: 80",
                "  cash: 20",
                "asset_bucket_mapping:",
                "  degiro:isin:IE00TEST0001: core",
                "  degiro:cash:eur: cash",
                "risk_profile: medium:term",
            ]
        ),
        encoding="utf-8",
    )
    settings = load_settings(
        repo_root=workspace_tmp_path,
        env={"PORTFOLIO_TARGETS_PATH": str(targets_path)},
        env_file=workspace_tmp_path / ".env.missing",
    )

    targets = load_portfolio_targets(settings=settings, required=True)

    assert targets is not None
    assert targets.asset_bucket_mapping == {
        "degiro:isin:IE00TEST0001": "core",
        "degiro:cash:eur": "cash",
    }
    assert targets.risk_profile == "medium:term"


def test_portfolio_targets_applies_one_scale_to_the_complete_allocation() -> None:
    percentages = portfolio_targets_from_mapping(
        {"target_allocation": {"equity": 99, "cash": 1}}
    )

    assert percentages.target_weights() == {"equity": 0.99, "cash": 0.01}
    with pytest.raises(ValueError, match="sum to 1.0 or 100.0"):
        portfolio_targets_from_mapping(
            {"target_allocation": {"core": 80, "satellite": 0.20}}
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"target_allocation": {"core": float("nan"), "satellite": 1.0}},
        {"target_allocation": {"core": float("inf"), "satellite": 1.0}},
        {"target_allocation": {"core": float("-inf"), "satellite": 1.0}},
        {
            "target_allocation": {"core": 0.80, "satellite": 0.20},
            "monthly_contribution": float("nan"),
        },
        {
            "target_allocation": {"core": 0.80, "satellite": 0.20},
            "max_single_asset_weight": float("inf"),
        },
    ],
)
def test_portfolio_targets_rejects_non_finite_numbers(payload: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="finite"):
        portfolio_targets_from_mapping(payload)
