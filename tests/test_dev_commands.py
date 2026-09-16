from src.config import default_repo_root


COMMAND_SCRIPTS = (
    "test.ps1",
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


def test_react_demo_commands_use_synthetic_configuration() -> None:
    content = (default_repo_root() / "README.md").read_text(encoding="utf-8")
    assert "demo/synthetic_config/.env.demo" in content
    assert "scripts/bootstrap_demo.py" in content
    assert "scripts/run_api.py" in content
    assert "--operations demo" in content
    assert "npm run dev" in content


def test_retired_ui_is_not_a_runtime_dependency_or_entrypoint() -> None:
    import tomllib

    repo = default_repo_root()
    config = tomllib.loads((repo / "pyproject.toml").read_text(encoding="utf-8"))
    requirements = config["project"]["dependencies"]
    assert not any(item.startswith(("streamlit", "altair")) for item in requirements)
    assert not list((repo / "src/portfolio").glob("dashboard*.py"))
    assert (repo / "src/application/dashboard.py").exists()  # Shared use cases remain.
