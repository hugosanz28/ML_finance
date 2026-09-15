"""Serve a fresh synthetic workspace for browser tests; never reuse user data."""

from pathlib import Path
import shutil
import socket
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import uvicorn

from scripts.bootstrap_demo import _upsert_synthetic_prices
from src.api.app import create_app
from src.application.degiro import ImportDegiroUseCase
from src.application.operational_workspace import OperationalWorkspace
from src.config import default_repo_root, load_settings
from src.market_data.repository import DuckDBMarketDataRepository


def main():
    repo = default_repo_root()
    temporary_root = repo / ".test_tmp"
    temporary_root.mkdir(exist_ok=True)
    # Fresh bounded test root. A hard-killed Windows test run may leave only this ignored directory.
    with tempfile.TemporaryDirectory(prefix="frontend-e2e-", dir=temporary_root) as folder:
        root = Path(folder)
        for source in ("demo/synthetic_config", "demo/synthetic_degiro_exports", "src/data/sql"):
            shutil.copytree(repo / source, root / source)
        settings = load_settings(repo_root=root, env_file=root / "absent.env", env={
            "DATA_DIR": "demo/local_data", "PRICE_PROVIDER": "synthetic",
        })
        workspace = OperationalWorkspace(settings, "demo")
        workspace.open()
        try:
            ImportDegiroUseCase(settings=workspace.settings).execute()
            # Test fixture seeding only; the UI never calls repositories directly.
            _upsert_synthetic_prices(DuckDBMarketDataRepository(settings=workspace.settings))
        finally:
            workspace.close()
        original = socket.socket.connect

        def offline(sock, address):
            if isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}:
                return original(sock, address)
            raise RuntimeError("External network forbidden in E2E demo")

        socket.socket.connect = offline
        uvicorn.run(create_app(settings=settings, workspace_mode="demo"), host="127.0.0.1", port=8000,
                    access_log=False, proxy_headers=False)


if __name__ == "__main__":
    main()
