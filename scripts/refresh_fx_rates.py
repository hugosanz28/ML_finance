"""Refresh daily FX rates for currency pairs used by normalized DEGIRO data."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.application.market_data import (
    InferFxRequirementsRequest,
    InferFxRequirementsUseCase,
    RefreshFxRequest,
    RefreshFxUseCase,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, help="Refresh start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", type=date.fromisoformat, help="Refresh end date in YYYY-MM-DD format.")
    parser.add_argument(
        "--pair",
        action="append",
        dest="pairs",
        help="Refresh one pair as BASE/QUOTE, for example EUR/USD. Repeat for multiple pairs.",
    )
    parser.add_argument("--provider", help="Override the configured provider for this run.")
    parser.add_argument(
        "--only-missing-base",
        action="store_true",
        help="Infer pairs only from normalized rows missing converted base amounts.",
    )
    parser.add_argument(
        "--no-infer-from-normalized",
        action="store_true",
        help="Do not infer pairs from normalized DEGIRO data. Requires at least one --pair.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = [_parse_pair(pair) for pair in args.pairs or []]

    infer_from_normalized = not args.no_infer_from_normalized
    if not pairs and not infer_from_normalized:
        print("No pairs selected. Use --pair or allow inference from normalized DEGIRO data.")
        return 1

    requirements = (
        InferFxRequirementsUseCase()
        .execute(InferFxRequirementsRequest(only_missing_base=args.only_missing_base))
        .requirements
        if infer_from_normalized
        else ()
    )
    if infer_from_normalized:
        print("Inferred FX requirements:")
        if requirements:
            for requirement in requirements:
                print(
                    f"- {requirement.pair}: {requirement.start_date.isoformat()} -> "
                    f"{requirement.end_date.isoformat()} | rows={requirement.source_rows} | "
                    f"missing_base={requirement.missing_base_rows}"
                )
        else:
            print("- none")

    summary = RefreshFxUseCase().execute(
        RefreshFxRequest(
            start_date=args.start_date,
            end_date=args.end_date,
            pairs=tuple(pairs),
            provider=args.provider,
            infer_from_normalized=infer_from_normalized,
            only_missing_base=args.only_missing_base,
        )
    ).summary

    print(f"\nProvider: {summary.provider_name}")
    print(f"Pairs updated: {summary.updated_pairs}")
    print(f"Pairs skipped: {summary.skipped_pairs}")
    print(f"Rows written: {summary.total_records}")

    if summary.outcomes:
        print("\nPer-pair outcomes:")
        for outcome in summary.outcomes:
            note = f" | {outcome.message}" if outcome.message else ""
            print(f"- {outcome.pair}: {outcome.status} | rows={outcome.records_written}{note}")

    return 0 if summary.skipped_pairs == 0 else 2


def _parse_pair(raw_pair: str) -> tuple[str, str]:
    parts = [part.strip().upper() for part in raw_pair.split("/")]
    if len(parts) != 2 or any(len(part) != 3 for part in parts):
        raise argparse.ArgumentTypeError(f"Invalid pair format: {raw_pair}. Expected BASE/QUOTE.")
    return parts[0], parts[1]


if __name__ == "__main__":
    raise SystemExit(main())
