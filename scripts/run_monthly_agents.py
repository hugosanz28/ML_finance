"""Run the monthly three-agent review pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.agents import run_monthly_agent_pipeline
from src.config import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--investment-brief-file", type=Path, help="Path to the investment brief Markdown file.")
    parser.add_argument("--investment-brief-text", help="Inline investment brief text.")
    parser.add_argument("--monthly-report", type=Path, help="Path to a monthly Markdown report.")
    parser.add_argument("--user-satellite-interest", help="Optional one-off satellite idea for this run.")
    parser.add_argument("--llm-provider", choices=("static", "openai"), default="static")
    parser.add_argument("--search-provider", choices=("null", "duckduckgo"), default="null")
    parser.add_argument("--no-persist", action="store_true", help="Do not write pipeline_result.json.")
    parser.add_argument("--output-dir", type=Path, help="Output directory for persisted agent results.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    result = run_monthly_agent_pipeline(
        settings=settings,
        investment_brief_text=args.investment_brief_text,
        investment_brief_path=args.investment_brief_file,
        monthly_report_path=args.monthly_report,
        user_satellite_interest=args.user_satellite_interest,
        llm_provider=args.llm_provider,
        search_provider=args.search_provider,
        persist=not args.no_persist,
        output_dir=args.output_dir,
    )

    print(f"Run: {result.run_id}")
    print(f"As of: {result.as_of_date.isoformat()}")
    if result.output_dir:
        print(f"Output: {result.output_dir}")
    for name, agent_result in (
        ("monitor_tematico", result.monitor_tematico),
        ("analista_activos", result.analista_activos),
        ("asistente_aportacion_mensual", result.asistente_aportacion_mensual),
    ):
        print(f"- {name}: {agent_result.status} | findings={len(agent_result.findings)} | {agent_result.summary}")
        for warning in agent_result.warnings:
            print(f"  warning: {warning}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
