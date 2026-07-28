from src.config import default_repo_root


COMMAND_SCRIPTS = (
    "test.ps1",
    "run_dashboard.ps1",
    "run_demo.ps1",
    "refresh_market_data.ps1",
)


def test_development_command_scripts_are_documented() -> None:
    repo_root = default_repo_root()
    readme = (repo_root / "README.md").read_text(encoding="utf-8")
    scripts_readme = (repo_root / "scripts" / "README.md").read_text(encoding="utf-8")
    readme_alternatives = {
        "test.ps1": ".\\.venv\\Scripts\\python.exe -m pytest",
        "refresh_market_data.ps1": "refrescar FX/precios",
    }

    for script_name in COMMAND_SCRIPTS:
        script_path = repo_root / "scripts" / script_name
        assert script_path.exists()
        assert f".\\scripts\\{script_name}" in readme or readme_alternatives[script_name] in readme
        assert script_name in scripts_readme


def test_demo_command_uses_synthetic_env_file() -> None:
    repo_root = default_repo_root()
    content = (repo_root / "scripts" / "run_demo.ps1").read_text(encoding="utf-8")

    assert "demo/synthetic_config/.env.demo" in content
    assert "scripts\\bootstrap_demo.py" in content
    assert "src\\portfolio\\dashboard.py" in content
