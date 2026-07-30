"""LLM providers for `analista_activos`."""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
from typing import Any, Protocol

from dotenv import dotenv_values

from src.agents.provider_audit import (
    record_provider_failure,
    record_provider_raw_response,
)
from src.agents.prompts import load_prompt
from src.agents.analista_activos._types import (
    AssetAnalysis,
    AssetAssessment,
    AssetUnderReview,
    MonitorContextFinding,
)


class AssetLLMProvider(Protocol):
    """Protocol for the LLM brain used by the asset analyst."""

    @property
    def name(self) -> str:
        """Stable provider identifier."""

    def analyze(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        portfolio_metrics_snapshot: str | None,
        assets: tuple[AssetUnderReview, ...],
        monitor_findings: tuple[MonitorContextFinding, ...],
        max_assets: int,
    ) -> AssetAnalysis:
        """Evaluate assets against the account mandate."""


class AssetLLMProviderError(RuntimeError):
    """Raised when the LLM provider cannot complete a request."""


class StaticAssetLLMProvider:
    """Deterministic LLM provider for tests and local fixtures."""

    def __init__(self, analysis: AssetAnalysis | None = None) -> None:
        self._analysis = analysis

    @property
    def name(self) -> str:
        return "static_llm"

    def analyze(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        portfolio_metrics_snapshot: str | None,
        assets: tuple[AssetUnderReview, ...],
        monitor_findings: tuple[MonitorContextFinding, ...],
        max_assets: int,
    ) -> AssetAnalysis:
        if self._analysis is None:
            if not portfolio_metrics_snapshot and not monitor_findings:
                return AssetAnalysis(summary="Sin analisis de activos.")
            assessments = tuple(
                AssetAssessment(
                    asset_name=asset.name,
                    asset_type=asset.asset_type,
                    portfolio_fit=_static_portfolio_fit(asset),
                    explicit_judgement=_static_judgement(asset),
                    horizon_fit="compatible_with_manual_review",
                    risk_level=_static_risk_level(asset),
                    valuation_signal="neutral",
                    rationale=(
                        "Evaluacion sintetica local para demo: revisar peso, rol y concentracion "
                        "antes de decidir la aportacion mensual."
                    ),
                    concentration_view=(
                        f"Peso actual aproximado: {asset.current_weight:.2%}."
                        if asset.current_weight is not None
                        else "Peso actual no disponible."
                    ),
                    portfolio_role_view=f"Rol inferido: {asset.role or 'portfolio'}.",
                    monitor_context_used=tuple(finding.title for finding in monitor_findings[:3]),
                    tags=("demo", "synthetic", asset.asset_type),
                )
                for asset in assets[:max_assets]
            )
            return AssetAnalysis(
                summary="Analisis sintetico local de activos para demo.",
                assessments=assessments,
            )
        return AssetAnalysis(
            summary=self._analysis.summary,
            assessments=self._analysis.assessments[:max_assets],
            warnings=self._analysis.warnings,
        )


def _static_portfolio_fit(asset: AssetUnderReview) -> str:
    role = (asset.role or "").lower()
    if "cash" in role or asset.asset_type == "cash":
        return "defensive"
    if "satellite" in role or asset.asset_type in {"stock", "crypto"}:
        return "satellite"
    return "core"


def _static_judgement(asset: AssetUnderReview) -> str:
    if asset.asset_type in {"stock", "crypto"}:
        return "watch"
    return "maintain"


def _static_risk_level(asset: AssetUnderReview) -> str:
    if asset.asset_type in {"stock", "crypto"}:
        return "medium"
    if asset.asset_type == "cash":
        return "low"
    return "moderate"


class OpenAIAssetLLMProvider:
    """OpenAI-backed asset analysis provider using Structured Outputs."""

    def __init__(self, *, model: str | None = None, api_key: str | None = None) -> None:
        repo_env = _repo_env_values()
        self.model = model or os.environ.get("OPENAI_MODEL") or repo_env.get("OPENAI_MODEL") or "gpt-4.1-mini"
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY") or repo_env.get("OPENAI_API_KEY")
        self._client: Any | None = None

    @property
    def name(self) -> str:
        return "openai"

    def analyze(
        self,
        *,
        investment_brief: str,
        latest_monthly_report: str,
        portfolio_metrics_snapshot: str | None,
        assets: tuple[AssetUnderReview, ...],
        monitor_findings: tuple[MonitorContextFinding, ...],
        max_assets: int,
    ) -> AssetAnalysis:
        payload = {
            "investment_brief": _truncate_text(investment_brief),
            "latest_monthly_report": _truncate_text(latest_monthly_report),
            "portfolio_metrics_snapshot": _truncate_text(portfolio_metrics_snapshot or ""),
            "assets": [_asset_payload(asset) for asset in assets[:max_assets]],
            "monitor_findings": [_monitor_finding_payload(finding) for finding in monitor_findings],
            "max_assets": max_assets,
        }
        data = self._call_structured(
            system_prompt=_ANALYSIS_SYSTEM_PROMPT,
            user_payload=payload,
            schema_name="analista_activos_analysis",
            schema=_analysis_schema(),
        )
        return AssetAnalysis(
            summary=str(data["summary"]),
            assessments=tuple(_assessment_from_payload(item) for item in data.get("assessments", [])[:max_assets]),
            warnings=tuple(str(warning) for warning in data.get("warnings", [])),
        )

    def _client_instance(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise AssetLLMProviderError(
                "The OpenAI Python package is not installed. Run `pip install -r requirements.txt`."
            ) from exc
        self._client = OpenAI(api_key=self.api_key or None)
        return self._client

    def _call_structured(
        self,
        *,
        system_prompt: str,
        user_payload: dict[str, Any],
        schema_name: str,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            response = self._client_instance().responses.create(
                model=self.model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": (
                            "Return JSON that matches the provided schema. "
                            f"Input payload:\n{json.dumps(user_payload, ensure_ascii=False)}"
                        ),
                    },
                ],
                text={
                    "format": {
                        "type": "json_schema",
                        "name": schema_name,
                        "strict": True,
                        "schema": schema,
                    }
                },
            )
        except Exception as exc:
            record_provider_failure(self, operation=schema_name)
            raise AssetLLMProviderError(f"OpenAI request failed: {exc}") from exc

        record_provider_raw_response(self, response, operation=schema_name)
        text = getattr(response, "output_text", None)
        if not text:
            raise AssetLLMProviderError("OpenAI response did not include output_text.")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise AssetLLMProviderError("OpenAI response was not valid JSON.") from exc
        if not isinstance(parsed, dict):
            raise AssetLLMProviderError("OpenAI response JSON root was not an object.")
        return parsed


_ANALYSIS_SYSTEM_PROMPT = load_prompt("analista_activos.analysis")


def _asset_payload(asset: AssetUnderReview) -> dict[str, Any]:
    return {
        "name": asset.name,
        "asset_type": asset.asset_type,
        "role": asset.role,
        "ticker": asset.ticker or "",
        "asset_id": asset.asset_id or "",
        "current_weight": asset.current_weight,
        "source_key": asset.source_key or "",
        "metadata": dict(asset.metadata),
    }


def _monitor_finding_payload(finding: MonitorContextFinding) -> dict[str, Any]:
    return asdict(finding)


def _assessment_from_payload(item: dict[str, Any]) -> AssetAssessment:
    return AssetAssessment(
        asset_name=str(item["asset_name"]),
        asset_type=str(item["asset_type"]),
        portfolio_fit=str(item["portfolio_fit"]),
        explicit_judgement=str(item["explicit_judgement"]),
        horizon_fit=str(item["horizon_fit"]),
        risk_level=str(item["risk_level"]),
        valuation_signal=str(item["valuation_signal"]),
        rationale=str(item["rationale"]),
        business_summary=str(item.get("business_summary") or ""),
        fundamentals_view=str(item.get("fundamentals_view") or ""),
        valuation_view=str(item.get("valuation_view") or ""),
        main_risks=tuple(str(value) for value in item.get("main_risks", [])),
        etf_provider=str(item.get("etf_provider") or ""),
        etf_index=str(item.get("etf_index") or ""),
        top_holdings=tuple(str(value) for value in item.get("top_holdings", [])),
        sector_exposure=str(item.get("sector_exposure") or ""),
        geographic_bias=str(item.get("geographic_bias") or ""),
        concentration_view=str(item.get("concentration_view") or ""),
        portfolio_role_view=str(item.get("portfolio_role_view") or ""),
        volatility_view=str(item.get("volatility_view") or ""),
        liquidity_view=str(item.get("liquidity_view") or ""),
        monitor_context_used=tuple(str(value) for value in item.get("monitor_context_used", [])),
        warnings=tuple(str(value) for value in item.get("warnings", [])),
        tags=tuple(str(value) for value in item.get("tags", [])),
    )


def _analysis_schema() -> dict[str, Any]:
    assessment_properties: dict[str, Any] = {
        "asset_name": {"type": "string"},
        "asset_type": {"type": "string", "enum": ["stock", "etf", "crypto", "metal", "fund", "cash_like", "other"]},
        "portfolio_fit": {"type": "string", "enum": ["core", "satellite", "watch_only", "reduce", "not_fit"]},
        "explicit_judgement": {
            "type": "string",
            "enum": ["maintain", "watch", "incorporate", "do_not_incorporate", "reduce"],
        },
        "horizon_fit": {"type": "string", "enum": ["aligned", "borderline", "misaligned", "unknown"]},
        "risk_level": {"type": "string", "enum": ["low", "medium", "high", "very_high", "unknown"]},
        "valuation_signal": {
            "type": "string",
            "enum": ["reasonable", "demanding", "overextended", "unknown"],
        },
        "rationale": {"type": "string"},
        "business_summary": {"type": "string"},
        "fundamentals_view": {"type": "string"},
        "valuation_view": {"type": "string"},
        "main_risks": {"type": "array", "items": {"type": "string"}},
        "etf_provider": {"type": "string"},
        "etf_index": {"type": "string"},
        "top_holdings": {"type": "array", "items": {"type": "string"}},
        "sector_exposure": {"type": "string"},
        "geographic_bias": {"type": "string"},
        "concentration_view": {"type": "string"},
        "portfolio_role_view": {"type": "string"},
        "volatility_view": {"type": "string"},
        "liquidity_view": {"type": "string"},
        "monitor_context_used": {"type": "array", "items": {"type": "string"}},
        "warnings": {"type": "array", "items": {"type": "string"}},
        "tags": {"type": "array", "items": {"type": "string"}},
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["summary", "assessments", "warnings"],
        "properties": {
            "summary": {"type": "string"},
            "assessments": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": list(assessment_properties),
                    "properties": assessment_properties,
                },
            },
            "warnings": {"type": "array", "items": {"type": "string"}},
        },
    }


def _truncate_text(value: str, *, max_chars: int = 8000) -> str:
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "\n...[truncated]"


def _repo_env_values() -> dict[str, str]:
    env_file = Path(__file__).resolve().parents[3] / ".env"
    if not env_file.exists():
        return {}
    values = dotenv_values(env_file)
    return {key: value for key, value in values.items() if isinstance(value, str) and value}
