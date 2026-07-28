"""Upload helpers for the Streamlit dashboard DEGIRO import flow."""

from __future__ import annotations

from datetime import date
from typing import Any

from src.application.uploads import (
    DegiroUpload,
    SaveDegiroUploadsRequest,
    SaveDegiroUploadsUseCase,
    canonical_degiro_upload_name,
    detect_degiro_upload_kind,
    extract_dates_from_filename,
)
from src.config import Settings


def _save_uploaded_degiro_files(uploaded_files: list[Any], *, settings: Settings, uploaded_at: date) -> list[dict[str, str]]:
    request = SaveDegiroUploadsRequest(
        uploads=tuple(
            DegiroUpload(filename=uploaded_file.name, content=bytes(uploaded_file.getbuffer()))
            for uploaded_file in uploaded_files
        ),
        uploaded_at=uploaded_at,
    )
    result = SaveDegiroUploadsUseCase(settings=settings).execute(request)
    return [
        {
            "archivo_original": outcome.original_filename,
            "tipo_detectado": outcome.detected_kind,
            "guardado_como": outcome.saved_as or "",
            "status": outcome.status,
            "detalle": outcome.detail,
        }
        for outcome in result.outcomes
    ]


# Backwards-compatible names keep existing transform tests independent of Streamlit.
_canonical_degiro_upload_name = canonical_degiro_upload_name
_detect_degiro_upload_kind = detect_degiro_upload_kind
_extract_dates_from_filename = extract_dates_from_filename
