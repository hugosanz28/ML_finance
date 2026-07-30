"""Small helpers for controlled local application settings files."""

from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import uuid4


def text_content_hash(content: str) -> str:
    """Return the stable hash used for optimistic concurrency checks."""
    return f"sha256:{hashlib.sha256(content.encode('utf-8')).hexdigest()}"


def write_text_atomically(path: Path, content: str) -> None:
    """Replace one configured text file without exposing a partial write."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary_path.write_text(content, encoding="utf-8")
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


__all__ = ["text_content_hash", "write_text_atomically"]
