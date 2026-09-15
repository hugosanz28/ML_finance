"""Explicit personal/local benchmark download. Stop the API worker before CLI use."""

import argparse
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.application.benchmarks import RefreshBenchmarksRequest, RefreshBenchmarksUseCase
from src.application.operational_workspace import OperationalWorkspace
from src.config import load_settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, type=date.fromisoformat, help="Opening baseline date; include the full history needed")
    parser.add_argument("--end-date", required=True, type=date.fromisoformat, help="Last completed date, exclusive of today")
    parser.add_argument("--provider", required=True, choices=["yfinance_ecb"])
    parser.add_argument("--confirm", action="store_true", help="Allow external downloads and replacement of the local benchmark cache")
    args = parser.parse_args()
    if not args.confirm:
        parser.error("--confirm is required for external downloads")
    workspace = None
    try:
        workspace = OperationalWorkspace(load_settings(), "real")
        workspace.open()
        result = RefreshBenchmarksUseCase(settings=workspace.settings).execute(RefreshBenchmarksRequest(args.start_date, args.end_date, args.provider))
        print(result.message)
        print(result.artifacts["content_sha256"])
    except Exception:
        # Do not leak provider responses or local paths in the CLI error output.
        print("Benchmark refresh failed. Check real configuration, dates and stop other writers. Previous successful cache remains unchanged.", file=sys.stderr)
        return 1
    finally:
        if workspace is not None:
            workspace.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
