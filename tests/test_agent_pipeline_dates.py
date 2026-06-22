from datetime import date
from datetime import datetime, timezone
from pathlib import Path
import shutil
from uuid import uuid4

import pytest

from src.agents.models import AgentFinding, AgentResult, AgentSource
from src.agents.pipeline import (
    _append_agent_asset_reference,
    _serialize_agent_result,
    _validate_agent_input_dates,
    extract_monthly_report_as_of_date,
    prepare_agent_metrics_snapshot,
)
from src.config import default_repo_root, load_settings


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


def test_extract_monthly_report_as_of_date_from_frontmatter() -> None:
    content = """---
as_of_date: 2026-05-26
---

# Informe mensual de cartera (2026-05-26)
"""

    assert extract_monthly_report_as_of_date(content) == date(2026, 5, 26)


def test_extract_monthly_report_as_of_date_from_title() -> None:
    content = "# Informe mensual de cartera (2026-05-06)\n\nContenido."

    assert extract_monthly_report_as_of_date(content) == date(2026, 5, 6)


def test_validate_agent_input_dates_rejects_stale_report_with_current_snapshot() -> None:
    with pytest.raises(ValueError, match="inconsistent dates"):
        _validate_agent_input_dates(
            monthly_report_date=date(2026, 5, 6),
            metrics_snapshot={"as_of_date": "2026-05-26"},
            fallback_as_of_date=date(2026, 5, 6),
        )


def test_validate_agent_input_dates_accepts_matching_report_and_snapshot() -> None:
    _validate_agent_input_dates(
        monthly_report_date=date(2026, 5, 26),
        metrics_snapshot={"as_of_date": "2026-05-26"},
        fallback_as_of_date=date(2026, 5, 26),
    )


def test_prepare_agent_metrics_snapshot_uses_asset_override_names(workspace_tmp_path) -> None:
    settings = load_settings(repo_root=workspace_tmp_path)
    settings.market_data_dir.mkdir(parents=True)
    (settings.market_data_dir / "asset_overrides.csv").write_text(
        "\n".join(
            [
                "asset_id,asset_name,asset_type,ticker,broker_symbol,exchange_mic,trading_currency,asset_similar,is_active,notes",
                "degiro:isin:IE00BF1B7389,SPDR MSCI All Country World EUR Hdg UCITS ETF,etf,SPP1.DE,,XETA,EUR,,True,",
            ]
        ),
        encoding="utf-8",
    )

    snapshot = prepare_agent_metrics_snapshot(
        {
            "as_of_date": "2026-05-26",
            "positions": [
                {
                    "asset_id": "degiro:isin:IE00BF1B7389",
                    "asset_name": "STST SPDR MSCI ALL CNTRY WORLD E...",
                    "asset_type": "stock",
                    "isin": "IE00BF1B7389",
                }
            ],
        },
        settings=settings,
    )

    position = snapshot["positions"][0]
    assert position["asset_name"] == "SPDR MSCI All Country World EUR Hdg UCITS ETF"
    assert position["broker_asset_name"] == "STST SPDR MSCI ALL CNTRY WORLD E..."
    assert position["ticker"] == "SPP1.DE"
    assert position["trading_currency"] == "EUR"
    assert position["asset_type"] == "etf"


def test_append_agent_asset_reference_adds_normalized_identifiers() -> None:
    report = "# Informe mensual de cartera (2026-05-26)\n"
    enriched = _append_agent_asset_reference(
        report,
        metrics_snapshot={
            "positions": [
                {
                    "asset_name": "SPDR MSCI All Country World EUR Hdg UCITS ETF",
                    "broker_asset_name": "STST SPDR MSCI ALL CNTRY WORLD E...",
                    "isin": "IE00BF1B7389",
                    "ticker": "SPP1.DE",
                    "trading_currency": "EUR",
                    "asset_type": "etf",
                }
            ]
        },
    )

    assert "## Referencia de activos para agentes" in enriched
    assert "SPDR MSCI All Country World EUR Hdg UCITS ETF" in enriched
    assert "IE00BF1B7389" in enriched


def test_serialize_agent_result_omits_large_input_source_content() -> None:
    source = AgentSource(
        source_type="report",
        label="Latest monthly report",
        location="report.md",
        retrieved_at=datetime(2026, 5, 26, tzinfo=timezone.utc),
        effective_date=date(2026, 5, 26),
        metadata={"input_key": "latest_monthly_report", "content": "x" * 10_000},
    )
    result = AgentResult(
        status="success",
        summary="ok",
        sources=(source,),
        findings=(AgentFinding(title="Finding", detail="Detail", sources=(source,)),),
    )

    payload = _serialize_agent_result(result)

    assert "content" not in payload["sources"][0]["metadata"]
    assert payload["sources"][0]["metadata"]["omitted_metadata_keys"] == ["content"]
    assert "content" not in payload["findings"][0]["sources"][0]["metadata"]
