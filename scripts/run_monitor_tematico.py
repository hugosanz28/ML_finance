"""Run `monitor_tematico` manually from the command line."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.application.monitor import (
    RunMonitorTematicoRequest,
    RunMonitorTematicoUseCase,
)


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
        default="static",
        help="LLM provider. Defaults to `static`; choose `openai` explicitly for a real API call.",
    )
    parser.add_argument(
        "--search-provider",
        choices=("null", "static", "duckduckgo", "tavily"),
        default="null",
        help="Search provider. Defaults to `null`; choose `duckduckgo` or `tavily` explicitly for web search.",
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
    try:
        use_case_result = RunMonitorTematicoUseCase().execute(
            RunMonitorTematicoRequest(
                investment_brief_text=args.investment_brief_text,
                investment_brief_path=args.investment_brief_file,
                monthly_report_path=args.monthly_report,
                watchlist_path=args.watchlist_file,
                user_satellite_interest=args.user_satellite_interest,
                llm_provider=args.llm_provider,
                search_provider=args.search_provider,
                disable_cache=args.disable_cache,
                cache_dir=args.cache_dir,
                max_topics=args.max_topics,
                max_queries=args.max_queries,
                max_results_per_query=args.max_results_per_query,
                max_findings=args.max_findings,
                dry_run=args.dry_run,
            )
        )
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc))
        return 1

    if args.dry_run:
        print(json.dumps(use_case_result.payload, ensure_ascii=False, indent=2))
        return 0

    # Preserve the agent-facing CLI status while ApplicationResult keeps its
    # normalized status vocabulary for other adapters.
    print(f"Status: {use_case_result.payload['status']}")
    print(f"Summary: {use_case_result.result.message}")
    if use_case_result.result.warnings:
        print("Warnings:")
        for warning in use_case_result.result.warnings:
            print(f"- {warning}")
    findings = use_case_result.payload.get("findings") or []
    if findings:
        print("Findings:")
        for finding in findings:
            print(f"- [{finding['severity']}] {finding['title']}")
    if args.stdout_json:
        print("\n---\n")
        print(json.dumps(use_case_result.payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
