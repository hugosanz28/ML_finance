"""Server-selected write boundaries and a single API worker per local workspace."""

from dataclasses import replace
from pathlib import Path
import os
import shutil

from src.config import Settings


class OperationError(Exception):
    def __init__(self, code: str, status_code: int = 409):
        super().__init__(code)
        self.code = code
        self.status_code = status_code


class OperationalWorkspace:
    def __init__(self, settings: Settings, mode: str):
        if mode not in {"demo", "real"}:
            raise OperationError("workspace_mode_required", 422)
        self.mode = mode
        self.source_settings = settings
        self.root = settings.repo_root / ("demo/local_data" if mode == "demo" else "src/data/local")
        if settings.data_dir.resolve() != self.root.resolve():
            raise OperationError("workspace_path_not_allowed", 422)
        self.exports_root = self.root / "degiro_exports" if mode == "demo" else settings.repo_root / "src/degiro_exports/local"
        if mode == "demo":
            if settings.price_provider != "synthetic":
                raise OperationError("demo_requires_synthetic", 422)
            settings = replace(settings, degiro_exports_dir=self.exports_root,
                               investment_brief_path=self.root / "investment_brief.md",
                               portfolio_targets_path=self.root / "portfolio_targets.yaml",
                               benchmark_selection_path=self.root / "benchmark_selection.json")
        self.settings = settings
        self._lock_file = None
        self.validate()

    def validate(self):
        """Reject symlink/junction escapes as well as wrong environment paths."""
        allowed = (self.root.absolute(), self.exports_root.absolute())
        paths = [self.settings.degiro_exports_dir, self.settings.portfolio_db_path,
                 self.settings.market_data_dir, self.settings.reports_dir, self.settings.raw_data_dir,
                 self.settings.normalized_data_dir, self.settings.curated_data_dir,
                 self.settings.investment_brief_path, self.settings.portfolio_targets_path,
                 self.settings.benchmark_selection_path, self.root / "jobs.duckdb", self.root / "api-worker.lock"]
        for path in paths:
            if not any(path.resolve().is_relative_to(root) for root in allowed):
                raise OperationError("workspace_path_not_allowed", 422)
        # Existing links inside outputs must not redirect a later write outside the sandbox.
        for root in allowed:
            for directory, names, files in os.walk(root, followlinks=False):
                for name in [*names, *files]:
                    path = Path(directory) / name
                    if not path.resolve().is_relative_to(root):
                        raise OperationError("workspace_link_not_allowed", 422)

    def open(self):
        self.validate()
        self.root.mkdir(parents=True, exist_ok=True)
        handle = (self.root / "api-worker.lock").open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                handle.seek(0)
                # Windows byte-range locks can cover a byte beyond EOF.
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise OperationError("workspace_already_in_use") from exc
        self._lock_file = handle
        try:
            if self.mode == "demo":
                self._seed_demo()
        except Exception:
            self.close()
            raise

    def _seed_demo(self):
        # Never overwrite demo fixtures or user-edited working copies.
        demo = self.settings.repo_root / "demo"
        for filename, destination in (
            ("investment_brief.md", self.settings.investment_brief_path),
            ("portfolio_targets.yaml", self.settings.portfolio_targets_path),
            ("benchmark_selection.json", self.settings.benchmark_selection_path),
        ):
            source = demo / "synthetic_config" / filename
            if not source.resolve().is_relative_to(demo.absolute()):
                raise OperationError("demo_source_not_allowed", 422)
            if source.is_file() and not destination.exists():
                shutil.copyfile(source, destination)
        incoming = self.exports_root / "incoming"
        incoming.mkdir(parents=True, exist_ok=True)
        for source in (demo / "synthetic_degiro_exports/incoming").glob("*.csv"):
            if not source.resolve().is_relative_to(demo.absolute()):
                raise OperationError("demo_source_not_allowed", 422)
            if not (incoming / source.name).exists():
                shutil.copyfile(source, incoming / source.name)

    def close(self):
        if self._lock_file is not None:
            # Closing the handle releases the OS lock, including after process failure.
            self._lock_file.close()
            self._lock_file = None
