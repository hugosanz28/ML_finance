"""Build monitorable topics from agent inputs.

The agent should not search the web blindly. This module turns the project
inputs into a small, explicit monitoring universe before any external lookup is
performed.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.agents.models import AgentContext, AgentInputRef, AgentRequest
from src.agents.monitor_tematico._types import ObservedTopic


def build_observed_topics(request: AgentRequest, context: AgentContext) -> tuple[ObservedTopic, ...]:
    """Build the deduplicated monitoring universe for one run.

    Topic precedence is intentionally practical:
    1. current portfolio from the monthly report,
    2. optional recurring watchlist,
    3. optional one-off user interest,
    4. optional request-scoped topics,
    5. baseline macro context relevant to the account mandate.
    """
    topics: list[ObservedTopic] = []
    report_input = context.get_input("latest_monthly_report")
    topics.extend(_topics_from_monthly_report(report_input, context))

    if context.has_input("watchlist_candidates"):
        topics.extend(_topics_from_watchlist(context.get_input("watchlist_candidates"), context))

    if context.has_input("user_satellite_interest"):
        topics.extend(_topics_from_user_interest(context.get_input("user_satellite_interest"), context))

    topics.extend(_topics_from_request_scope(request))
    topics.extend(_portfolio_context_topics())
    return _deduplicate_topics(topics)


def _topics_from_monthly_report(input_ref: AgentInputRef, context: AgentContext) -> tuple[ObservedTopic, ...]:
    """Extract current positions from the monthly report input.

    Tests and future orchestrators can pass structured positions in metadata.
    If that is not available, v1 falls back to parsing the allocation table from
    the Markdown report.
    """
    metadata_positions = input_ref.metadata.get("positions")
    if isinstance(metadata_positions, Iterable) and not isinstance(metadata_positions, (str, bytes, Mapping)):
        return tuple(_topic_from_position(position, source_key=input_ref.key) for position in metadata_positions)

    content = _read_input_text(input_ref, context)
    if not content:
        return ()

    return tuple(
        ObservedTopic(
            name=asset_name,
            role="portfolio",
            query_terms=(asset_name,),
            priority="high" if index < 5 else "medium",
            source_key=input_ref.key,
            metadata={"origin": "monthly_report_allocation"},
        )
        for index, asset_name in enumerate(_parse_allocation_asset_names(content))
    )


def _topics_from_watchlist(input_ref: AgentInputRef, context: AgentContext) -> tuple[ObservedTopic, ...]:
    """Extract recurring candidates when the optional watchlist exists."""
    candidates = input_ref.metadata.get("candidates")
    if candidates is None:
        candidates = _load_structured_input(input_ref, context)
    if not isinstance(candidates, Iterable) or isinstance(candidates, (str, bytes, Mapping)):
        return ()

    return tuple(_topic_from_candidate(candidate, source_key=input_ref.key) for candidate in candidates)


def _topics_from_user_interest(input_ref: AgentInputRef, context: AgentContext) -> tuple[ObservedTopic, ...]:
    """Convert the optional one-off user idea into a high-priority candidate."""
    raw_text = input_ref.metadata.get("text") or input_ref.metadata.get("content") or input_ref.description
    if not raw_text:
        raw_text = _read_input_text(input_ref, context)
    text = str(raw_text).strip()
    if not text:
        return ()

    return (
        ObservedTopic(
            name=text,
            role="candidate",
            query_terms=(text,),
            priority="high",
            source_key=input_ref.key,
            metadata={"origin": "user_satellite_interest"},
        ),
    )


def _topics_from_request_scope(request: AgentRequest) -> tuple[ObservedTopic, ...]:
    raw_topics = request.scope.get("topics", ())
    if not isinstance(raw_topics, Iterable) or isinstance(raw_topics, (str, bytes, Mapping)):
        return ()

    return tuple(_topic_from_candidate(topic, source_key="request.scope") for topic in raw_topics)


def _portfolio_context_topics() -> tuple[ObservedTopic, ...]:
    """Add broad context topics even when no watchlist is configured.

    These defaults keep the monthly review aware of rates, inflation, and global
    equity context, which matter for a 3-4 year housing down-payment objective.
    """
    return (
        ObservedTopic(
            name="tipos de interes e inflacion",
            role="core",
            query_terms=("eurozone interest rates inflation markets",),
            priority="medium",
            source_key="derived",
            metadata={"origin": "default_macro_context"},
        ),
        ObservedTopic(
            name="renta variable global",
            role="core",
            query_terms=("global equities market outlook",),
            priority="medium",
            source_key="derived",
            metadata={"origin": "default_macro_context"},
        ),
    )


def _topic_from_position(position: Any, *, source_key: str) -> ObservedTopic:
    if isinstance(position, Mapping):
        name = str(position.get("asset_name") or position.get("name") or position.get("ticker") or "").strip()
        asset_id = position.get("asset_id")
        weight = _safe_float(position.get("weight"))
        role = str(position.get("role") or position.get("intended_role") or _infer_role(name)).lower()
        priority = "high" if weight is not None and weight >= 0.10 else "medium"
        query_terms = tuple(
            str(value).strip()
            for value in (position.get("ticker"), name)
            if value is not None and str(value).strip()
        )
        metadata = {key: value for key, value in position.items() if isinstance(key, str)}
    else:
        name = str(position).strip()
        asset_id = None
        role = _infer_role(name)
        priority = "medium"
        query_terms = (name,)
        metadata = {}

    return ObservedTopic(
        name=name or "posicion sin nombre",
        role=role,
        query_terms=query_terms or (name,),
        priority=priority,
        source_key=source_key,
        asset_id=None if asset_id is None else str(asset_id),
        metadata={"origin": "monthly_report_position", **metadata},
    )


def _topic_from_candidate(candidate: Any, *, source_key: str) -> ObservedTopic:
    if isinstance(candidate, Mapping):
        name = str(candidate.get("name") or candidate.get("asset_name") or candidate.get("ticker") or "").strip()
        role = str(candidate.get("intended_role") or candidate.get("role") or "candidate").lower()
        priority = str(candidate.get("priority") or "medium").lower()
        query_terms = tuple(
            str(value).strip()
            for value in (candidate.get("ticker"), name, candidate.get("theme"))
            if value is not None and str(value).strip()
        )
        metadata = {key: value for key, value in candidate.items() if isinstance(key, str)}
    else:
        name = str(candidate).strip()
        role = "candidate"
        priority = "medium"
        query_terms = (name,)
        metadata = {}

    return ObservedTopic(
        name=name or "candidato sin nombre",
        role=role,
        query_terms=query_terms or (name,),
        priority=priority if priority in {"high", "medium", "low", "info"} else "medium",
        source_key=source_key,
        metadata={"origin": "candidate", **metadata},
    )


def _read_input_text(input_ref: AgentInputRef, context: AgentContext) -> str:
    inline = input_ref.metadata.get("content") or input_ref.metadata.get("text")
    if inline:
        return str(inline)

    path = _resolve_input_path(input_ref.location, context)
    if path is None or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


def _load_structured_input(input_ref: AgentInputRef, context: AgentContext) -> Any:
    content = _read_input_text(input_ref, context)
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
    """Best-effort parser for the current Markdown monthly report table."""
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


def _deduplicate_topics(topics: Iterable[ObservedTopic]) -> tuple[ObservedTopic, ...]:
    """Deduplicate by normalized topic name, keeping the highest priority item."""
    deduped: dict[str, ObservedTopic] = {}
    for topic in topics:
        normalized_name = " ".join(topic.name.lower().split())
        if not normalized_name:
            continue
        existing = deduped.get(normalized_name)
        if existing is None or _priority_rank(topic.priority) < _priority_rank(existing.priority):
            deduped[normalized_name] = topic
    return tuple(deduped.values())


def _infer_role(name: str) -> str:
    lowered = name.lower()
    if "bitcoin" in lowered or "btc" in lowered:
        return "satellite"
    if "etf" in lowered or "msci" in lowered or "world" in lowered or "global" in lowered:
        return "core"
    return "portfolio"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _priority_rank(priority: str) -> int:
    return {"high": 0, "medium": 1, "low": 2, "info": 3}.get(priority, 1)
