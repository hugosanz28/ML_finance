import re
from urllib.parse import unquote

from src.config import default_repo_root


def _public_markdown_files():
    repo_root = default_repo_root()
    files = list(repo_root.glob("*.md"))
    for relative_root in ("docs", "scripts", "tests", "notebooks", "src"):
        files.extend((repo_root / relative_root).rglob("*.md"))
    files.extend((repo_root / "demo").glob("*.md"))
    files.extend((repo_root / "demo" / "synthetic_config").glob("*.md"))
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
