"""Build the asset review universe from agent inputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.agents.analista_activos._types import AssetUnderReview, MonitorContextFinding
from src.agents.models import AgentContext, AgentFinding, AgentInputRef, AgentRequest, AgentResult


def build_assets_under_review(request: AgentRequest, context: AgentContext) -> tuple[AssetUnderReview, ...]:
    """Build a deduplicated list of current positions and candidates."""
    assets: list[AssetUnderReview] = []
    monthly_report = context.get_input("latest_monthly_report")
    assets.extend(_assets_from_monthly_report(monthly_report, context))

    if context.has_input("portfolio_metrics_snapshot"):
        assets = _merge_metrics(assets, context.get_input("portfolio_metrics_snapshot"), context)

    if context.has_input("watchlist_candidates"):
        assets.extend(_assets_from_candidates(context.get_input("watchlist_candidates"), context))

    if context.has_input("user_satellite_interest"):
        assets.extend(_assets_from_user_interest(context.get_input("user_satellite_interest"), context))

    assets.extend(_assets_from_request_scope(request))
    return _deduplicate_assets(assets)


def build_monitor_context(context: AgentContext) -> tuple[MonitorContextFinding, ...]:
    """Extract prior thematic findings when the optional monitor result exists."""
    input_ref = _first_existing_input(
        context,
        ("monitor_tematico_result", "monitor_tematico", "theme_monitor_result"),
    )
    if input_ref is None:
        return ()

    raw_findings = input_ref.metadata.get("findings")
    agent_result = input_ref.metadata.get("agent_result") or input_ref.metadata.get("result")
    if raw_findings is None and isinstance(agent_result, AgentResult):
        raw_findings = agent_result.findings
    if raw_findings is None and isinstance(agent_result, Mapping):
        raw_findings = agent_result.get("findings")
    if raw_findings is None:
        loaded = _load_structured_input(input_ref, context)
        if isinstance(loaded, Mapping):
            raw_findings = loaded.get("findings")
        else:
            raw_findings = loaded

    if not isinstance(raw_findings, Iterable) or isinstance(raw_findings, (str, bytes, Mapping)):
        return ()
    return tuple(_monitor_finding_from_raw(item) for item in raw_findings)


def collect_input_texts(context: AgentContext) -> dict[str, str]:
    """Read textual payloads used by the LLM."""
    values = {
        "investment_brief": read_input_text(context.get_input("investment_brief"), context),
        "latest_monthly_report": read_input_text(context.get_input("latest_monthly_report"), context),
    }
    for optional_key in (
        "portfolio_metrics_snapshot",
        "watchlist_candidates",
        "user_satellite_interest",
        "monitor_tematico_result",
        "monitor_tematico",
        "theme_monitor_result",
    ):
        if context.has_input(optional_key):
            values[optional_key] = read_input_text(context.get_input(optional_key), context)
    return values


def read_input_text(input_ref: AgentInputRef, context: AgentContext) -> str:
    inline = input_ref.metadata.get("content") or input_ref.metadata.get("text")
    if inline:
        return str(inline)
    if input_ref.metadata:
        return str(dict(input_ref.metadata))
    path = _resolve_input_path(input_ref.location, context)
    if path is not None and path.is_file():
        return path.read_text(encoding="utf-8")
    return input_ref.description or ""


def _assets_from_monthly_report(input_ref: AgentInputRef, context: AgentContext) -> tuple[AssetUnderReview, ...]:
    metadata_positions = input_ref.metadata.get("positions")
    if isinstance(metadata_positions, Iterable) and not isinstance(metadata_positions, (str, bytes, Mapping)):
        return tuple(_asset_from_position(position, source_key=input_ref.key) for position in metadata_positions)

    content = read_input_text(input_ref, context)
    return tuple(
        AssetUnderReview(
            name=name,
            asset_type=_infer_asset_type(name=name, ticker=None, metadata={}),
            role="portfolio",
            source_key=input_ref.key,
            metadata={"origin": "monthly_report_allocation"},
        )
        for name in _parse_allocation_asset_names(content)
    )


def _merge_metrics(
    assets: list[AssetUnderReview],
    input_ref: AgentInputRef,
    context: AgentContext,
) -> list[AssetUnderReview]:
    metrics_positions = input_ref.metadata.get("positions") or input_ref.metadata.get("assets")
    if metrics_positions is None:
        loaded = _load_structured_input(input_ref, context)
        if isinstance(loaded, Mapping):
            metrics_positions = loaded.get("positions") or loaded.get("assets")
        else:
            metrics_positions = loaded

    if not isinstance(metrics_positions, Iterable) or isinstance(metrics_positions, (str, bytes, Mapping)):
        return assets

    by_key = {_asset_key(asset): asset for asset in assets}
    for metric in metrics_positions:
        if not isinstance(metric, Mapping):
            continue
        metric_asset = _asset_from_position(metric, source_key=input_ref.key)
        key = _asset_key(metric_asset)
        existing = by_key.get(key)
        if existing is None:
            assets.append(metric_asset)
            by_key[key] = metric_asset
            continue
        merged = AssetUnderReview(
            name=existing.name,
            asset_type=existing.asset_type if existing.asset_type != "other" else metric_asset.asset_type,
            role=existing.role,
            ticker=existing.ticker or metric_asset.ticker,
            asset_id=existing.asset_id or metric_asset.asset_id,
            current_weight=existing.current_weight if existing.current_weight is not None else metric_asset.current_weight,
            source_key=existing.source_key,
            metadata={
                **dict(metric_asset.metadata),
                **dict(existing.metadata),
                "metrics_snapshot": dict(metric),
            },
        )
        assets[assets.index(existing)] = merged
        by_key[key] = merged
    return assets


def _assets_from_candidates(input_ref: AgentInputRef, context: AgentContext) -> tuple[AssetUnderReview, ...]:
    candidates = input_ref.metadata.get("candidates")
    if candidates is None:
        candidates = _load_structured_input(input_ref, context)
    if not isinstance(candidates, Iterable) or isinstance(candidates, (str, bytes, Mapping)):
        return ()
    return tuple(_asset_from_candidate(candidate, source_key=input_ref.key) for candidate in candidates)


def _assets_from_user_interest(input_ref: AgentInputRef, context: AgentContext) -> tuple[AssetUnderReview, ...]:
    raw_text = input_ref.metadata.get("text") or input_ref.metadata.get("content") or input_ref.description
    if not raw_text:
        raw_text = read_input_text(input_ref, context)
    text = str(raw_text).strip()
    if not text:
        return ()
    return (
        AssetUnderReview(
            name=text,
            asset_type=_infer_asset_type(name=text, ticker=None, metadata={}),
            role="candidate",
            source_key=input_ref.key,
            metadata={"origin": "user_satellite_interest"},
        ),
    )


def _assets_from_request_scope(request: AgentRequest) -> tuple[AssetUnderReview, ...]:
    raw_assets = request.scope.get("assets") or request.scope.get("candidates") or ()
    if not isinstance(raw_assets, Iterable) or isinstance(raw_assets, (str, bytes, Mapping)):
        return ()
    return tuple(_asset_from_candidate(asset, source_key="request.scope") for asset in raw_assets)


def _asset_from_position(position: Any, *, source_key: str) -> AssetUnderReview:
    if not isinstance(position, Mapping):
        name = str(position).strip()
        return AssetUnderReview(
            name=name or "activo sin nombre",
            asset_type=_infer_asset_type(name=name, ticker=None, metadata={}),
            role="portfolio",
            source_key=source_key,
            metadata={"origin": "monthly_report_position"},
        )

    name = str(position.get("asset_name") or position.get("name") or position.get("ticker") or "").strip()
    ticker = _optional_str(position.get("ticker") or position.get("symbol"))
    role = str(position.get("role") or position.get("intended_role") or "portfolio").lower()
    return AssetUnderReview(
        name=name or ticker or "activo sin nombre",
        asset_type=str(position.get("asset_type") or _infer_asset_type(name=name, ticker=ticker, metadata=position)).lower(),
        role=role,
        ticker=ticker,
        asset_id=_optional_str(position.get("asset_id") or position.get("isin")),
        current_weight=_safe_float(position.get("weight") or position.get("current_weight")),
        source_key=source_key,
        metadata={"origin": "position", **{key: value for key, value in position.items() if isinstance(key, str)}},
    )


def _asset_from_candidate(candidate: Any, *, source_key: str) -> AssetUnderReview:
    if not isinstance(candidate, Mapping):
        name = str(candidate).strip()
        return AssetUnderReview(
            name=name or "candidato sin nombre",
            asset_type=_infer_asset_type(name=name, ticker=None, metadata={}),
            role="candidate",
            source_key=source_key,
            metadata={"origin": "candidate"},
        )

    name = str(candidate.get("name") or candidate.get("asset_name") or candidate.get("ticker") or "").strip()
    ticker = _optional_str(candidate.get("ticker") or candidate.get("symbol"))
    role = str(candidate.get("intended_role") or candidate.get("role") or "candidate").lower()
    return AssetUnderReview(
        name=name or ticker or "candidato sin nombre",
        asset_type=str(candidate.get("asset_type") or _infer_asset_type(name=name, ticker=ticker, metadata=candidate)).lower(),
        role=role,
        ticker=ticker,
        asset_id=_optional_str(candidate.get("asset_id") or candidate.get("isin")),
        current_weight=_safe_float(candidate.get("weight") or candidate.get("current_weight")),
        source_key=source_key,
        metadata={"origin": "candidate", **{key: value for key, value in candidate.items() if isinstance(key, str)}},
    )


def _monitor_finding_from_raw(item: Any) -> MonitorContextFinding:
    if isinstance(item, AgentFinding):
        return MonitorContextFinding(
            title=item.title,
            detail=item.detail,
            category=item.category,
            severity=item.severity,
            asset_id=item.asset_id,
            metadata=item.metadata,
        )
    if isinstance(item, Mapping):
        return MonitorContextFinding(
            title=str(item.get("title") or ""),
            detail=str(item.get("detail") or item.get("summary") or ""),
            category=str(item.get("category") or "general"),
            severity=str(item.get("severity") or "info"),
            asset_id=_optional_str(item.get("asset_id")),
            metadata=item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {},
        )
    return MonitorContextFinding(title=str(item), detail="")


def _load_structured_input(input_ref: AgentInputRef, context: AgentContext) -> Any:
    content = read_input_text(input_ref, context)
    if not content:
        return None
    location = input_ref.location.lower()
    if location.endswith(".json"):
        return json.loads(content)
    if location.endswith(".csv"):
        return tuple(csv.DictReader(content.splitlines()))
    return tuple(line.strip("- ").strip() for line in content.splitlines() if line.strip())


def _resolve_input_path(location: str, context: AgentContext) -> Path | None:
    if not location:
        return None
    path = Path(location).expanduser()
    if not path.is_absolute():
        path = context.settings.repo_root / path
    return path.resolve()


def _parse_allocation_asset_names(content: str) -> tuple[str, ...]:
    names: list[str] = []
    in_allocation = False
    for line in content.splitlines():
        normalized = line.strip()
        if normalized.startswith("## Asignacion actual"):
            in_allocation = True
            continue
        if in_allocation and normalized.startswith("## "):
            break
        if not in_allocation or not normalized.startswith("|") or "---" in normalized:
            continue
        columns = [column.strip() for column in normalized.strip("|").split("|")]
        if not columns or columns[0].lower() == "activo":
            continue
        if columns[0]:
            names.append(columns[0])
    return tuple(names)


def _deduplicate_assets(assets: Iterable[AssetUnderReview]) -> tuple[AssetUnderReview, ...]:
    deduped: dict[str, AssetUnderReview] = {}
    for asset in assets:
        key = _asset_key(asset)
        if not key:
            continue
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = asset
            continue
        deduped[key] = _prefer_position(existing, asset)
    return tuple(deduped.values())


def _prefer_position(existing: AssetUnderReview, incoming: AssetUnderReview) -> AssetUnderReview:
    if existing.role != "candidate":
        return existing
    if incoming.role != "candidate":
        return incoming
    return existing


def _asset_key(asset: AssetUnderReview) -> str:
    return " ".join((asset.asset_id or asset.ticker or asset.name).lower().split())


def _infer_asset_type(*, name: str, ticker: str | None, metadata: Mapping[str, Any]) -> str:
    explicit = metadata.get("asset_type") or metadata.get("type")
    if explicit:
        return str(explicit).lower()
    text = " ".join(str(value).lower() for value in (name, ticker or "", metadata.get("theme") or ""))
    if "bitcoin" in text or "btc" in text or "crypto" in text:
        return "crypto"
    if "gold" in text or "oro" in text or "metal" in text:
        return "metal"
    if "etf" in text or "ucits" in text or "msci" in text or "index" in text:
        return "etf"
    if "cash" in text or "liquidez" in text or "money market" in text:
        return "cash_like"
    return "other"


def _first_existing_input(context: AgentContext, keys: tuple[str, ...]) -> AgentInputRef | None:
    for key in keys:
        if context.has_input(key):
            return context.get_input(key)
    return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
