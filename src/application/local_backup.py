"""Offline private workspace backups; never exposed through the HTTP API."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import socket
import tempfile
from uuid import uuid4
import zipfile

from src.application.operational_workspace import OperationalWorkspace
from src.application.types import ApplicationResult
from src.config import Settings


ROOTS = ("src/data/local", "src/degiro_exports/local", ".env")
MAX_BYTES = 10 * 1024**3


@dataclass(frozen=True)
class BackupLocalRequest:
    pass


@dataclass(frozen=True)
class RestoreLocalRequest:
    archive: Path
    confirm: bool = False


def _digest(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _allowed(name):
    path = PurePosixPath(name)
    # Reject Windows aliases, drive paths and traversal before joining any path.
    reserved = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
    if (not name or "\\" in name or ":" in name or path.is_absolute()
            or any(not part or part in {".", ".."} or part.endswith((".", " "))
                   or part.split(".")[0].upper() in reserved for part in name.split("/"))):
        return False
    return name == ".env" or any(name.startswith(root + "/") for root in ROOTS[:2])


def _transient(name):
    path = PurePosixPath(name)
    return (path.name.endswith((".log", ".lock", ".tmp"))
            or any(part in {"__pycache__", ".pytest_cache"} or part.startswith("test_tmp_") for part in path.parts))


def _safe_path(root, name):
    if not _allowed(name):
        raise ValueError("Ruta no permitida en la copia")
    path = root / name
    # Reject even internal links: a restore must not replace an alias to another file.
    for parent in [path, *path.parents]:
        if parent == root:
            break
        if parent.is_symlink() or (hasattr(parent, "is_junction") and parent.is_junction()):
            raise ValueError("No se admiten enlaces en los datos privados")
    if path.resolve() != path.absolute() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("Ruta fuera del repositorio")
    return path


def _files(root):
    result = {}
    for relative in ROOTS:
        base = root / relative
        paths = [base] if relative == ".env" else base.rglob("*")
        for path in paths:
            if not path.exists() and not path.is_symlink():
                continue
            name = path.relative_to(root).as_posix()
            if _transient(name):
                continue
            _safe_path(root, name if path.is_file() or path.is_symlink() else name + "/probe")
            if path.is_file():
                result[name] = path
    return result


class BackupLocalUseCase:
    def __init__(self, *, settings: Settings):
        self.settings = settings
        self.root = settings.repo_root.resolve()

    def _prepare(self):
        if self.settings.env_file != self.root / ".env":
            raise ValueError("La copia sencilla requiere la configuración .env del repositorio")
        workspace = OperationalWorkspace(self.settings, "real")
        # Read-only APIs also need to be stopped before replacing database files.
        with socket.socket() as connection:
            connection.settimeout(.3)
            if connection.connect_ex(("127.0.0.1", 8000)) == 0:
                raise ValueError("Detén la API antes de copiar o restaurar")
        directory = self.root / ".local_backups"
        if directory.resolve() != directory.absolute() or directory.is_symlink():
            raise ValueError("La carpeta de copias no puede ser un enlace")
        directory.mkdir(exist_ok=True)
        workspace.open()
        return workspace, directory

    def _create(self, directory):
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        destination = directory / f"ml-finance-{stamp}-{uuid4().hex[:8]}.zip"
        temporary = destination.with_suffix(".tmp")
        entries = {}
        try:
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for name, path in sorted(_files(self.root).items()):
                    before = path.stat()
                    entries[name] = {"size": before.st_size, "sha256": _digest(path)}
                    archive.write(path, name)
                    after = path.stat()
                    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                        raise ValueError("Los datos cambiaron durante la copia; detén los otros procesos")
                archive.writestr("manifest.json", json.dumps({"schema_version": 1, "files": entries}))
            # Check the bytes stored, not only the source hashes.
            with tempfile.TemporaryDirectory(dir=directory) as stage:
                self._unpack(temporary, Path(stage))
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
        return destination, len(entries)

    def _unpack(self, source, stage):
        with zipfile.ZipFile(source) as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if (len(names) > 100000 or len(set(n.casefold() for n in names)) != len(names)
                    or sum(info.file_size for info in infos) > MAX_BYTES
                    or archive.getinfo("manifest.json").file_size > 8 * 1024**2):
                raise ValueError("Copia demasiado grande o con entradas duplicadas")
            manifest = json.loads(archive.read("manifest.json"))
            entries = manifest["files"]
            if manifest["schema_version"] != 1 or set(names) != {*entries, "manifest.json"}:
                raise ValueError("Manifiesto de copia inválido")
            for name, meta in entries.items():
                if not _allowed(name) or _transient(name):
                    raise ValueError("Contenido no permitido en la copia")
                info = archive.getinfo(name)
                if info.is_dir() or (info.external_attr >> 16) & 0o170000 == 0o120000:
                    raise ValueError("Enlace no permitido en la copia")
                path = _safe_path(stage, name)
                path.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(name) as src, path.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
                if path.stat().st_size != meta["size"] or _digest(path) != meta["sha256"]:
                    raise ValueError("La copia no supera la verificación de integridad")
        return entries

    def execute(self, request: BackupLocalRequest) -> ApplicationResult:
        workspace, directory = self._prepare()
        try:
            destination, count = self._create(directory)
            return ApplicationResult(name="backup_local", status="succeeded", message="Copia privada creada y verificada.",
                                     artifacts={"archive": str(destination), "files": count})
        finally:
            workspace.close()


class RestoreLocalUseCase(BackupLocalUseCase):
    def execute(self, request: RestoreLocalRequest) -> ApplicationResult:
        if not request.confirm:
            raise ValueError("La restauración requiere confirmación explícita")
        workspace, directory = self._prepare()
        try:
            with tempfile.TemporaryDirectory(dir=directory) as temporary:
                stage = Path(temporary) / "incoming"
                entries = self._unpack(request.archive, stage)
                current = _files(self.root)
                for name in entries:
                    path = _safe_path(self.root, name)
                    if path.exists() and not path.is_file():
                        raise ValueError("Conflicto de archivo y directorio")
                # Always create a recovery point before touching current files.
                safety_copy, _ = self._create(directory)
                rollback = Path(temporary) / "rollback"
                moved, installed = [], []
                try:
                    for name, path in current.items():
                        target = _safe_path(rollback, name)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(path, target)
                        moved.append(name)
                    for name in entries:
                        target = _safe_path(self.root, name)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(stage / name, target)
                        installed.append(name)
                except Exception:
                    for name in reversed(installed):
                        _safe_path(self.root, name).unlink()
                    for name in reversed(moved):
                        os.replace(rollback / name, _safe_path(self.root, name))
                    raise
            return ApplicationResult(name="restore_local", status="succeeded", message="Datos restaurados; la copia previa se conserva.",
                                     artifacts={"safety_copy": str(safety_copy), "files": len(entries)})
        finally:
            workspace.close()
