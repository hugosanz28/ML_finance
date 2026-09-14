"""Run the read-only API on loopback without touching or bootstrapping local data."""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.api.app import create_app
from src.config import load_settings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", help="Server-side environment file, e.g. demo/synthetic_config/.env.demo")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    import uvicorn

    app = create_app(settings=load_settings(env_file=args.env_file))
    # No public bind option or access logs containing user queries.
    uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False, proxy_headers=False)


if __name__ == "__main__":
    main()
