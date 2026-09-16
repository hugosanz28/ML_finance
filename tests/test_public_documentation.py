import hashlib
import json
import re
import struct
from urllib.parse import unquote

from src.config import default_repo_root


def _public_markdown_files():
    repo_root = default_repo_root()
    files = list(repo_root.glob("*.md"))
    for relative_root in ("docs", "scripts", "tests", "notebooks", "src"):
        files.extend((repo_root / relative_root).rglob("*.md"))
    files.extend((repo_root / "demo").glob("*.md"))
    files.extend((repo_root / "demo" / "synthetic_config").glob("*.md"))
    files.extend((repo_root / "frontend").glob("*.md"))
    return sorted(set(files))


def test_public_repository_documents_are_present() -> None:
    repo_root = default_repo_root()

    for relative_path in (
        "LICENSE",
        "CONTRIBUTING.md",
        "SECURITY.md",
        "CODE_OF_CONDUCT.md",
        "CHANGELOG.md",
        "src/data/sample/investment_brief.example.md",
    ):
        path = repo_root / relative_path
        assert path.is_file()
        assert path.read_text(encoding="utf-8").strip()


def test_readme_states_runtime_and_manual_review_boundaries() -> None:
    readme = (default_repo_root() / "README.md").read_text(encoding="utf-8")

    assert "Python 3.11" in readme
    assert "No ejecuta" in readme
    assert "revision manual" in readme
    assert "demo/synthetic_config/.env.demo" in readme


def test_readme_local_images_do_not_have_broken_targets() -> None:
    repo_root = default_repo_root()
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    local_image_targets = [
        target
        for target in re.findall(r"!\[[^\]]*\]\(([^)]+)\)", readme)
        if not target.startswith(("http://", "https://"))
    ]

    assert all((repo_root / target).is_file() for target in local_image_targets)


def test_public_markdown_local_links_resolve() -> None:
    external_prefixes = ("http://", "https://", "mailto:", "#")

    for markdown_path in _public_markdown_files():
        content = markdown_path.read_text(encoding="utf-8")
        for match in re.finditer(r"!?\[[^\]]*\]\(([^)]+)\)", content):
            target = match.group(1).strip().strip("<>")
            if target.startswith(external_prefixes):
                continue
            path_text = unquote(target.split("#", 1)[0])
            if not path_text:
                continue
            resolved_path = markdown_path.parent / path_text
            assert resolved_path.exists(), f"Broken Markdown link in {markdown_path}: {target}"


def test_documented_project_scripts_exist() -> None:
    repo_root = default_repo_root()
    script_pattern = re.compile(r"(?:\.?[\\/])?scripts[\\/]([A-Za-z0-9_.-]+\.(?:py|ps1))")

    for markdown_path in _public_markdown_files():
        content = markdown_path.read_text(encoding="utf-8")
        for script_name in script_pattern.findall(content):
            assert (repo_root / "scripts" / script_name).is_file(), (
                f"Missing script referenced by {markdown_path}: scripts/{script_name}"
            )


def test_roadmap_uses_current_time_horizons() -> None:
    roadmap = (default_repo_root() / "docs" / "roadmap.md").read_text(encoding="utf-8")

    assert "## Ahora" in roadmap
    assert "## Siguiente" in roadmap
    assert "## Más adelante" in roadmap
    assert "## Fase 1" not in roadmap


def test_showcase_assets_match_reviewed_manifest_and_synthetic_inputs() -> None:
    """Integrity checks complement, but never replace, visual privacy review."""
    repo = default_repo_root()
    assets = repo / "docs/assets/showcase"
    manifest = json.loads((assets / "manifest.json").read_text(encoding="utf-8"))
    names = {
        "overview.png", "performance.png", "contribution.png",
        "social-preview.png", "walkthrough.webm",
    }
    assert set(manifest["assets_sha256"]) == names
    assert manifest["as_of_date"] == "2026-04-30"
    assert manifest["command"] == "cd frontend && npm run showcase"
    assert manifest["source"] == "fresh synthetic workspace via scripts/run_frontend_e2e_api.py"
    assert hashlib.sha256((assets / "social-preview.svg").read_bytes()).hexdigest() == manifest[
        "cover_source_sha256"
    ]
    # Check input names before reading: the manifest cannot select private paths.
    fixtures = repo / "demo/synthetic_degiro_exports/incoming"
    assert set(manifest["inputs_sha256"]) == {p.name for p in fixtures.glob("*.csv")}
    for name, digest in manifest["inputs_sha256"].items():
        assert hashlib.sha256((fixtures / name).read_bytes()).hexdigest() == digest
    for name in names:
        data = (assets / name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == manifest["assets_sha256"][name]
        assert 1000 < len(data) < 10_000_000
        if name.endswith(".png"):
            assert data[:8] == b"\x89PNG\r\n\x1a\n"
            width, height = struct.unpack(">II", data[16:24])
            if name == "social-preview.png":
                assert (width, height) == (1200, 630)
            else:
                assert width == 1440 and 1000 <= height <= 4000
        else:
            assert data[:4] == b"\x1aE\xdf\xa3"  # WebM/EBML container, not a text placeholder.


def test_showcase_command_and_ci_use_isolated_capture_configuration() -> None:
    repo = default_repo_root()
    package = json.loads((repo / "frontend/package.json").read_text(encoding="utf-8"))
    assert package["scripts"]["showcase"] == "playwright test --config showcase.config.ts"
    config = (repo / "frontend/showcase.config.ts").read_text(encoding="utf-8")
    assert 'import base from "./playwright.config"' in config
    assert 'testMatch: "**/showcase.spec.ts"' in config
    workflow = (repo / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "run: npm run showcase" in workflow
