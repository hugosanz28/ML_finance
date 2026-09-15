"""Export only the synthetic demo HTTP contracts for frontend integration tests."""

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from src.api.app import create_app
from src.config import default_repo_root, load_settings


def main() -> None:
    root = default_repo_root()
    settings = load_settings(env_file=root / "demo/synthetic_config/.env.demo", env={})
    # Never accept an environment/path override for a versionable UI fixture.
    if settings.data_dir.resolve() != root / "demo/local_data" or settings.price_provider != "synthetic":
        raise ValueError("Only the synthetic demo workspace may be exported")
    if not settings.portfolio_db_path.is_file():
        raise ValueError("Run scripts/bootstrap_demo.py with the synthetic environment first")
    with TestClient(create_app(settings=settings), base_url="http://127.0.0.1") as client:
        def read(route):
            response = client.get(f"/api/v1/{route}")
            response.raise_for_status()
            return response.json()

        analytics = [read(f"analytics/summary?period={period}&benchmark_id={benchmark}")
                     for period in ("since_inception", "last_year", "last_quarter", "last_month")
                     for benchmark in ("msci_world", "sp500", "portfolio_60_40", "estr_cash")]
        payload = {
            "analytics": analytics,
            "portfolio": read(f"portfolio/state?include_history=true&as_of_date={analytics[0]['period']['end_date']}"),
            "definitions": read("analytics/metric-definitions"),
        }
    output = root / ".test_tmp/frontend-api.json"
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    print("Exported 16 synthetic analytics contracts to .test_tmp/frontend-api.json")


if __name__ == "__main__":
    main()
