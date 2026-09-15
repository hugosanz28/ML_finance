"""Public workspace metadata; never infer demo from a benchmark provider alone."""

from dataclasses import dataclass

from src.config import Settings


@dataclass(frozen=True)
class WorkspaceStatusRequest:
    operations_mode: str | None = None


class GetWorkspaceStatusUseCase:
    def __init__(self, *, settings: Settings):
        self.settings = settings

    def execute(self, request: WorkspaceStatusRequest) -> dict:
        mode = request.operations_mode
        if mode is None:
            # Only the documented synthetic workspace earns a demo label.
            demo = self.settings.repo_root / "demo"
            mode = "demo" if (self.settings.data_dir.resolve() == (demo / "local_data").resolve()
                              and self.settings.price_provider == "synthetic"
                              and self.settings.degiro_exports_dir.resolve().is_relative_to(demo.resolve())) else "real"
        return {"mode": "operations" if request.operations_mode else "read_only", "workspace_mode": mode}
