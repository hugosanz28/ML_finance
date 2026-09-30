"""Create or restore a verified private local backup, with the API stopped."""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.application.local_backup import (
    BackupLocalRequest, BackupLocalUseCase, RestoreLocalRequest, RestoreLocalUseCase,
)
from src.config import load_settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("backup", "restore"))
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--confirm", action="store_true")
    args = parser.parse_args()
    if args.action == "restore" and (not args.archive or not args.confirm):
        parser.error("restore requires --archive and --confirm (replaces local data)")
    settings = load_settings(env_file=Path(__file__).resolve().parents[1] / ".env")
    try:
        result = (BackupLocalUseCase(settings=settings).execute(BackupLocalRequest()) if args.action == "backup"
                  else RestoreLocalUseCase(settings=settings).execute(RestoreLocalRequest(args.archive, args.confirm)))
        print(result.message)
        for key, value in result.artifacts.items():
            print(f"{key}: {value}")
    except Exception:
        print("No se ha completado la operación. Detén la API y otros procesos CLI, revisa las rutas locales, "
              "permisos y que el ZIP sea una copia válida de esta aplicación. No se muestran datos privados.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
