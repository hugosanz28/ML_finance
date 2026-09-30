import hashlib
import json
from pathlib import Path
from unittest.mock import patch
import zipfile

import duckdb
import pytest

from src.application.local_backup import BackupLocalRequest, BackupLocalUseCase, RestoreLocalRequest, RestoreLocalUseCase
from src.application.operational_workspace import OperationalWorkspace, OperationError
from src.config import load_settings


@pytest.fixture
def workspace(tmp_path):
    settings = load_settings(repo_root=tmp_path, env_file=tmp_path / ".env", env={"PRICE_PROVIDER": "synthetic"})
    settings.data_dir.mkdir(parents=True)
    settings.degiro_exports_dir.mkdir(parents=True)
    (tmp_path / ".env").write_text("# synthetic configuration\n")
    (settings.degiro_exports_dir / "Account.csv").write_text("synthetic,fixture\n")
    with duckdb.connect(str(settings.data_dir / "personal_finance.duckdb")) as db:
        db.execute("CREATE TABLE sample(amount INTEGER)")
        db.execute("INSERT INTO sample VALUES (123)")
    with patch("src.application.local_backup.socket.socket") as connection:
        connection.return_value.__enter__.return_value.connect_ex.return_value = 1
        yield settings


def backup(settings):
    return Path(BackupLocalUseCase(settings=settings).execute(BackupLocalRequest()).artifacts["archive"])


def test_round_trip_restores_database_exports_and_configuration(workspace):
    source = backup(workspace)
    (workspace.data_dir / "extra.json").write_text("{}")
    (workspace.repo_root / ".env").write_text("# changed\n")
    with duckdb.connect(str(workspace.data_dir / "personal_finance.duckdb")) as db:
        db.execute("DELETE FROM sample")
    result = RestoreLocalUseCase(settings=workspace).execute(RestoreLocalRequest(source, confirm=True))
    assert Path(result.artifacts["safety_copy"]).exists()
    assert not (workspace.data_dir / "extra.json").exists()
    assert (workspace.repo_root / ".env").read_text() == "# synthetic configuration\n"
    with duckdb.connect(str(workspace.data_dir / "personal_finance.duckdb"), read_only=True) as db:
        assert db.execute("SELECT * FROM sample").fetchall() == [(123,)]
    with zipfile.ZipFile(result.artifacts["safety_copy"]) as archive:
        assert "src/data/local/extra.json" in archive.namelist()


def test_backup_excludes_transient_files_and_never_copies_itself(workspace):
    (workspace.data_dir / "server.log").write_text("synthetic")
    first = backup(workspace)
    second = backup(workspace)
    assert first != second
    with zipfile.ZipFile(second) as archive:
        assert all(not name.endswith((".zip", ".log", ".lock")) for name in archive.namelist())


def test_empty_workspace_without_env_can_be_backed_up(workspace):
    (workspace.repo_root / ".env").unlink()
    assert backup(workspace).exists()


def test_restore_requires_explicit_confirmation(workspace):
    with pytest.raises(ValueError, match="confirmación"):
        RestoreLocalUseCase(settings=workspace).execute(RestoreLocalRequest(Path("missing.zip")))


def test_active_worker_blocks_backup(workspace):
    worker = OperationalWorkspace(workspace, "real")
    worker.open()
    try:
        with pytest.raises(OperationError):
            backup(workspace)
    finally:
        worker.close()


def test_listening_api_blocks_backup(workspace):
    with patch("src.application.local_backup.socket.socket") as connection:
        connection.return_value.__enter__.return_value.connect_ex.return_value = 0
        with pytest.raises(ValueError, match="Detén la API"):
            backup(workspace)


@pytest.mark.parametrize("name", ["../.env", "src/data/local/../../code.py", "C:/secret", "src/data/local/NUL",
                                  "src/data/local/foo:bar", "src/data/local/file.", "src/data/local/server.lock"])
def test_rejects_unsafe_archive_paths_without_changing_current_data(workspace, name):
    source = workspace.repo_root / "bad.zip"
    body = b"synthetic"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(name, body)
        archive.writestr("manifest.json", json.dumps({"schema_version": 1, "files": {
            name: {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}}}))
    before = (workspace.repo_root / ".env").read_bytes()
    with pytest.raises(ValueError):
        RestoreLocalUseCase(settings=workspace).execute(RestoreLocalRequest(source, True))
    assert (workspace.repo_root / ".env").read_bytes() == before


def test_corrupt_hash_rejected_before_mutation(workspace):
    source = workspace.repo_root / "bad.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(".env", b"bad")
        archive.writestr("manifest.json", json.dumps({"schema_version": 1, "files": {".env": {"size": 3, "sha256": "wrong"}}}))
    with pytest.raises(ValueError, match="integridad"):
        RestoreLocalUseCase(settings=workspace).execute(RestoreLocalRequest(source, True))
    assert (workspace.repo_root / ".env").read_text() == "# synthetic configuration\n"


def test_restore_rolls_back_on_install_failure(workspace, monkeypatch):
    source = backup(workspace)
    (workspace.repo_root / ".env").write_text("# keep current\n")
    import src.application.local_backup as module
    replace = module.os.replace
    installs = 0

    def fail_once(src, dst):
        nonlocal installs
        if "incoming" in Path(src).parts:
            installs += 1
            if installs == 2:
                raise OSError("synthetic failure after one successful replacement")
        replace(src, dst)

    monkeypatch.setattr(module.os, "replace", fail_once)
    with pytest.raises(OSError):
        RestoreLocalUseCase(settings=workspace).execute(RestoreLocalRequest(source, True))
    assert (workspace.repo_root / ".env").read_text() == "# keep current\n"
    assert (workspace.data_dir / "personal_finance.duckdb").is_file()


def test_restore_rejects_link_destinations(workspace):
    source = backup(workspace)
    outside = workspace.repo_root / "outside"
    outside.write_text("keep")
    destination = workspace.degiro_exports_dir / "Account.csv"
    destination.unlink()
    try:
        destination.symlink_to(outside)
    except OSError:
        pytest.skip("Symlink creation requires Windows privileges")
    with pytest.raises(ValueError):
        RestoreLocalUseCase(settings=workspace).execute(RestoreLocalRequest(source, True))
    assert outside.read_text() == "keep"
