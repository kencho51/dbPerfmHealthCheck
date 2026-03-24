"""
POST /api/upload — multipart CSV upload, validate → extract → ingest.

Flow:
  1. Receive UploadFile
  2. Write to a temp file
  3. Run validator — reject if hard errors are found
  4. Extract rows via QueryExtractor service (normalised 7-column form → raw_query)
  5. Extract typed rows (full native columns → raw_query_* type-specific table)
  6. Ingest both (dedup upsert)
  7. Return summary JSON

Idempotent: uploading the same CSV twice only bumps occurrence_count.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, status

from api.services.extractor import extract_from_file, extract_typed_from_file
from api.services.ingestor import ingest_rows
from api.services.typed_ingestor import ingest_typed_rows
from api.services.validator import validate_csv

router = APIRouter()


@router.post("/upload", summary="Upload and ingest a Splunk CSV file")
async def upload_csv(
    file: UploadFile,
) -> dict:
    """
    Validate, extract, and ingest a single Splunk performance CSV.

    Writes to **two** destinations:
    - `raw_query` — normalised 7-column form (curated/labelling workflow)
    - `raw_query_<type>` — full native columns (analytics)

    Returns `{ filename, file_type, environment, row_count,
               inserted, updated, skipped, warnings, errors,
               typed_inserted, typed_errors }`.
    """
    filename = file.filename or "unknown.csv"

    # -- Write to temp file so pandas can read it --------------------------------
    contents = await file.read()
    with tempfile.NamedTemporaryFile(
        suffix=".csv", delete=False, prefix=f"upload_{Path(filename).stem}_"
    ) as tmp:
        tmp.write(contents)
        tmp_path = Path(tmp.name)

    try:
        # -- Validate ------------------------------------------------------------
        validation = validate_csv(tmp_path)

        # Override detected name with the original filename for env/type detection
        from api.services.extractor import _extract_environment, _detect_file_category, _detect_typed_table
        file_type   = _detect_file_category(filename)
        environment = _extract_environment(filename)
        table_type  = _detect_typed_table(filename)
        validation.file_type   = file_type
        validation.environment = environment

        if not validation.is_valid:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail={
                    "message": "CSV validation failed — no data ingested.",
                    "errors":  validation.errors,
                    "warnings": validation.warnings,
                },
            )

        # -- Extract normalised rows (raw_query) ---------------------------------
        rows = extract_from_file(tmp_path)
        # Patch env/type derived from original filename (temp path loses context)
        for row in rows:
            if row.get("environment") in ("unknown", "", None):
                row["environment"] = environment

        # -- Extract typed rows (raw_query_<type>) --------------------------------
        typed_rows = extract_typed_from_file(tmp_path)
        # Patch environment on typed rows too
        for row in typed_rows:
            if row.get("environment") in ("unknown", "", None):
                row["environment"] = environment
            # Ensure _hash_parts reflect corrected environment
            if row.get("_hash_parts"):
                row["_hash_parts"] = [
                    environment if p == "unknown" else p
                    for p in row["_hash_parts"]
                ]

        # -- Ingest normalised ---------------------------------------------------
        ingest_result = await ingest_rows(rows)

        # -- Ingest typed --------------------------------------------------------
        typed_result = await ingest_typed_rows(typed_rows, table_type)

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": f"Ingest failed: {type(exc).__name__}: {exc}"},
        ) from exc
    finally:
        tmp_path.unlink(missing_ok=True)

    return {
        "filename":       filename,
        "file_type":      file_type,
        "table_type":     table_type,
        "environment":    environment,
        "row_count":      validation.row_count,
        # raw_query (normalised)
        "inserted":       ingest_result.inserted,
        "updated":        ingest_result.updated,
        "skipped":        ingest_result.skipped,
        "warnings":       validation.warnings,
        "errors":         ingest_result.errors,
        # raw_query_<type> (full fidelity)
        "typed_inserted": typed_result.inserted,
        "typed_skipped":  typed_result.skipped,
        "typed_errors":   typed_result.errors,
    }
