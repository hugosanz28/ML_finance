"""Run `monitor_tematico` manually from the command line."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.agents import AgentInputRef, AgentRequest, build_agent_context
from src.agents.monitor_tematico import (
    CachedSearchProvider,
    DuckDuckGoHtmlSearchProvider,
    MonitorTematicoAgent,
    NullSearchProvider,
    OpenAIThemeLLMProvider,
    StaticThemeLLMProvider,
    build_observed_topics,
)
from src.config import load_settings
from src.reports import get_latest_monthly_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--investment-brief-file",
        type=Path,
        help="Path to a plain-text investment brief file.",
    )
    parser.add_argument(
        "--investment-brief-text",
        help="Inline investment brief text. Use this instead of --investment-brief-file if preferred.",
    )
    parser.add_argument(
        "--monthly-report",
        type=Path,
        help="Path to a monthly Markdown report. Defaults to the latest persisted monthly report.",
    )
    parser.add_argument(
        "--watchlist-file",
        type=Path,
        help="Optional watchlist file in JSON, CSV, or plain text.",
    )
    parser.add_argument(
        "--user-satellite-interest",
        help="Optional one-off satellite idea for this run.",
    )
    parser.add_argument(
        "--llm-provider",
        choices=("openai", "static"),
        default="openai",
        help="LLM provider. Use `static` for plumbing without real API calls.",
    )
    parser.add_argument(
        "--search-provider",
        choices=("duckduckgo", "null"),
        default="duckduckgo",
        help="Search provider. Use `null` to avoid any real web search.",
    )
    parser.add_argument(
        "--disable-cache",
        action="store_true",
        help="Disable local caching for web search results.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        help="Override the search cache directory.",
    )
    parser.add_argument("--max-topics", type=int, default=8)
    parser.add_argument("--max-queries", type=int, default=8)
    parser.add_argument("--max-results-per-query", type=int, default=2)
    parser.add_argument("--max-findings", type=int, default=10)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve inputs and observed topics without calling the LLM or web search.",
    )
    parser.add_argument(
        "--stdout-json",
        action="store_true",
        help="Print the full result payload as JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings()
    investment_brief = _resolve_investment_brief(args)
    monthly_report_path = _resolve_monthly_report_path(args, settings)
    monthly_report_text = monthly_report_path.read_text(encoding="utf-8")

    context = build_agent_context(
        agent_name="monitor_tematico",
        as_of_date=datetime.now().date(),
        generated_at=datetime.now().astimezone(),
        base_currency=settings.default_currency,
        settings=settings,
        input_refs=_build_input_refs(
            investment_brief=investment_brief,
            monthly_report_path=monthly_report_path,
            monthly_report_text=monthly_report_text,
            watchlist_file=args.watchlist_file,
            user_satellite_interest=args.user_satellite_interest,
        ),
    )
    request = AgentRequest(
        parameters={
            "max_topics": args.max_topics,
            "max_queries": args.max_queries,
            "max_results_per_query": args.max_results_per_query,
            "max_findings": args.max_findings,
        }
    )

    if args.dry_run:
        topics = build_observed_topics(request, context)
        payload = {
            "mode": "dry_run",
            "monthly_report": str(monthly_report_path),
            "input_keys": context.available_input_keys,
            "observed_topics": [
                {
                    "name": topic.name,
                    "role": topic.role,
                    "priority": topic.priority,
                    "query_terms": topic.query_terms,
                }
                for topic in topics
            ],
            "llm_provider": args.llm_provider,
            "search_provider": args.search_provider,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    search_provider = _build_search_provider(args, settings)
    llm_provider = _build_llm_provider(args)
    agent = MonitorTematicoAgent(search_provider=search_provider, llm_provider=llm_provider)
    result = agent.execute(request, context)

    print(f"Status: {result.status}")
    print(f"Summary: {result.summary}")
    if result.warnings:
        print("Warnings:")
        for warning in result.warnings:
            print(f"- {warning}")
    if result.findings:
        print("Findings:")
        for finding in result.findings:
            print(f"- [{finding.severity}] {finding.title}")
    if args.stdout_json:
        print("\n---\n")
        print(json.dumps(_serialize_result(result), ensure_ascii=False, indent=2))
    return 0


def _resolve_investment_brief(args: argparse.Namespace) -> str:
    if args.investment_brief_text:
        return args.investment_brief_text
    if args.investment_brief_file is not None:
        return args.investment_brief_file.expanduser().resolve().read_text(encoding="utf-8")
    raise SystemExit("Provide --investment-brief-file or --investment-brief-text.")


def _resolve_monthly_report_path(args: argparse.Namespace, settings) -> Path:
    if args.monthly_report is not None:
        return args.monthly_report.expanduser().resolve()
    latest = get_latest_monthly_report(settings=settings)
    if latest is None:
        raise SystemExit("No monthly report found. Use --monthly-report or generate one first.")
    return Path(latest.report_path).expanduser().resolve()


def _build_input_refs(
    *,
    investment_brief: str,
    monthly_report_path: Path,
    monthly_report_text: str,
    watchlist_file: Path | None,
    user_satellite_interest: str | None,
) -> tuple[AgentInputRef, ...]:
    refs = [
        AgentInputRef(
            key="investment_brief",
            label="Investment brief",
            location="manual://investment-brief",
            source_type="manual",
            metadata={"content": investment_brief},
        ),
        AgentInputRef(
            key="latest_monthly_report",
            label="Latest monthly report",
            location=str(monthly_report_path),
            source_type="report",
            metadata={"content": monthly_report_text},
        ),
    ]
    if watchlist_file is not None:
        watchlist_path = watchlist_file.expanduser().resolve()
        refs.append(
            AgentInputRef(
                key="watchlist_candidates",
                label="Watchlist candidates",
                location=str(watchlist_path),
                source_type="manual",
            )
        )
    if user_satellite_interest:
        refs.append(
            AgentInputRef(
                key="user_satellite_interest",
                label="User satellite interest",
                location="manual://user-satellite-interest",
                source_type="manual",
                metadata={"text": user_satellite_interest},
            )
        )
    return tuple(refs)


def _build_search_provider(args: argparse.Namespace, settings):
    if args.search_provider == "null":
        return NullSearchProvider()

    provider = DuckDuckGoHtmlSearchProvider()
    if args.disable_cache:
        return provider
    cache_dir = args.cache_dir or (settings.data_dir / "agents" / "monitor_tematico" / "search_cache")
    return CachedSearchProvider(provider, cache_dir=cache_dir)


def _build_llm_provider(args: argparse.Namespace):
    if args.llm_provider == "static":
        return StaticThemeLLMProvider()
    return OpenAIThemeLLMProvider()


def _serialize_result(result) -> dict[str, object]:
    return {
        "status": result.status,
        "summary": result.summary,
        "warnings": list(result.warnings),
        "errors": list(result.errors),
        "metadata": dict(result.metadata),
        "findings": [
            {
                "title": finding.title,
                "detail": finding.detail,
                "category": finding.category,
                "severity": finding.severity,
                "asset_id": finding.asset_id,
                "tags": list(finding.tags),
                "metadata": dict(finding.metadata),
                "sources": [
                    {
                        "label": source.label,
                        "location": source.location,
                        "source_type": source.source_type,
                        "effective_date": source.effective_date.isoformat() if source.effective_date else None,
                    }
                    for source in finding.sources
                ],
            }
            for finding in result.findings
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
