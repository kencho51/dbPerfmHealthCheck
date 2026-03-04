"""
POST /api/validate — dry-run CSV validation (no DB writes).

Accepts a multipart file upload and returns a `ValidationResult` without
touching the database.  Used by the upload page's preview step in the
Next.js frontend.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, UploadFile

from api.services.extractor import _detect_file_category, _extract_environment
from api.services.validator import ValidationResult, validate_csv

router = APIRouter()


@router.post(
    "/validate",
    summary="Dry-run validate a CSV file (no DB writes)",
    response_model=None,
)
async def validate_upload(file: UploadFile) -> dict:
    """
    Validate a Splunk performance CSV without ingesting it.

    Returns the full `ValidationResult` including sample rows so the UI
    can display a preview before the user confirms the upload.
    """
    filename = file.filename or "unknown.csv"
    contents = await file.read()

    with tempfile.NamedTemporaryFile(
        suffix=".csv", delete=False, prefix=f"validate_{Path(filename).stem}_"
    ) as tmp:
        tmp.write(contents)
        tmp_path = Path(tmp.name)

    try:
        result = validate_csv(tmp_path)
        # Patch with original filename context
        result.file_type   = _detect_file_category(filename)
        result.environment = _extract_environment(filename)
    finally:
        tmp_path.unlink(missing_ok=True)

    return result.to_dict()
