"""
POST /api/upload — multipart CSV upload, validate → extract → ingest.

Flow:
  1. Receive UploadFile
  2. Write to a temp file
  3. Run validator — reject if hard errors are found
  4. Extract rows via QueryExtractor service (normalised 7-column form → raw_query)
  5. Extract typed rows (full native columns → raw_query_* type-specific table)
  6. Ingest both (dedup upsert)
  7. Link typed rows to raw_query via post-ingest SQL UPDATE (sets raw_query_id FK)
  8. Return summary JSON

Idempotent: uploading the same CSV twice only bumps occurrence_count.
"""
from __future__ import annotations

import asyncio
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, status
from sqlalchemy import text

from api.database import open_session
from api.services.extractor import extract_from_file, extract_typed_from_file
from api.services.ingestor import ingest_rows
from api.services.typed_ingestor import ingest_typed_rows
from api.services.validator import validate_csv
from api.services.ingestor import _derive_month_year

router = APIRouter()

# ---------------------------------------------------------------------------
# Mapping: table_type → SQL that sets raw_query_id where NULL.
#
# Matching strategy: join on (environment, month_year, type) + the query-text
# column that the normal extractor maps to raw_query.query_details.
#
#   slow_sql   → raw_query.query_details  =  raw_query_slow_sql.query_final
#   blocker    → raw_query.query_details  =  raw_query_blocker.all_query
#   deadlock   → raw_query.query_details  =  raw_query_deadlock.sql_text
#   slow_mongo → raw_query.query_details  =  raw_query_slow_mongo.command_json
#
# Performance: a composite index ix_raw_query_link_key on
# (type, host, db_name, environment, month_year) enables the correlated subquery
# to use an index range scan instead of a full-table scan.  At 78 K raw_query
# rows this reduces 870 M comparisons → ~11 K small index probes.
# The index is created idempotently (IF NOT EXISTS) before each link query.
# ---------------------------------------------------------------------------
_CREATE_COMPOSITE_IDX = """
    CREATE INDEX IF NOT EXISTS ix_raw_query_link_key
    ON raw_query (type, host, db_name, environment, month_year)
"""

_LINK_SQL: dict[str, str] = {
    "slow_sql": """
        UPDATE raw_query_slow_sql
        SET raw_query_id = (
            SELECT rq.id FROM raw_query rq
            WHERE rq.type        = 'slow_query'
              AND rq.source      = 'sql'
              AND rq.environment IS raw_query_slow_sql.environment
              AND rq.month_year  IS raw_query_slow_sql.month_year
              AND rq.host        IS raw_query_slow_sql.host
              AND rq.db_name     IS raw_query_slow_sql.db_name
              AND rq.query_details IS raw_query_slow_sql.query_final
            LIMIT 1
        )
        WHERE raw_query_id IS NULL
    """,
    "blocker": """
        UPDATE raw_query_blocker
        SET raw_query_id = (
            SELECT rq.id FROM raw_query rq
            WHERE rq.type        = 'blocker'
              AND rq.source      = 'sql'
              AND rq.environment IS raw_query_blocker.environment
              AND rq.month_year  IS raw_query_blocker.month_year
              AND rq.query_details IS raw_query_blocker.all_query
            LIMIT 1
        )
        WHERE raw_query_id IS NULL
    """,
    "deadlock": """
        UPDATE raw_query_deadlock
        SET raw_query_id = (
            SELECT rq.id FROM raw_query rq
            WHERE rq.type        = 'deadlock'
              AND rq.source      = 'sql'
              AND rq.environment IS raw_query_deadlock.environment
              AND rq.month_year  IS raw_query_deadlock.month_year
              AND rq.host        IS raw_query_deadlock.host
              AND rq.db_name     IS raw_query_deadlock.db_name
              AND rq.query_details IS raw_query_deadlock.sql_text
            LIMIT 1
        )
        WHERE raw_query_id IS NULL
    """,
    "slow_mongo": """
        UPDATE raw_query_slow_mongo
        SET raw_query_id = (
            SELECT rq.id FROM raw_query rq
            WHERE rq.type        = 'slow_query_mongo'
              AND rq.source      = 'mongodb'
              AND rq.environment IS raw_query_slow_mongo.environment
              AND rq.month_year  IS raw_query_slow_mongo.month_year
              AND rq.host        IS raw_query_slow_mongo.host
              AND rq.db_name     IS raw_query_slow_mongo.db_name
              AND rq.query_details IS raw_query_slow_mongo.command_json
            LIMIT 1
        )
        WHERE raw_query_id IS NULL
    """,
}


async def _link_typed_to_raw(table_type: str) -> None:
    """Run the post-ingest SQL UPDATE to set raw_query_id on the typed table.

    Creates a composite index on raw_query(type, host, db_name, environment,
    month_year) before the correlated subquery so SQLite uses an index range
    scan instead of a full-table scan (critical at 100 K+ raw_query rows).
    """
    sql = _LINK_SQL.get(table_type)
    if not sql:
        return
    async with open_session() as session:
        await session.execute(text(_CREATE_COMPOSITE_IDX))
        await session.execute(text(sql))


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
        validation = await asyncio.to_thread(validate_csv, tmp_path)

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

        # -- Extract both row sets concurrently ----------------------------------
        # CPU-bound Polars/json work runs in the thread pool; asyncio.gather
        # dispatches both tasks simultaneously so wall-clock time ≈ max(t1, t2)
        # instead of t1 + t2.  For MongoDB CSVs this saves ~0.2 s per file.
        rows, typed_rows = await asyncio.gather(
            asyncio.to_thread(extract_from_file, tmp_path),
            asyncio.to_thread(extract_typed_from_file, tmp_path),
        )

        # Patch env/type derived from original filename (temp path loses context)
        for row in rows:
            if row.get("environment") in ("unknown", "", None):
                row["environment"] = environment

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

        # -- Link typed rows to raw_query (Phase 1 — set raw_query_id FK) ------
        # Idempotent: WHERE raw_query_id IS NULL means re-uploads are safe.
        await _link_typed_to_raw(table_type)

        # -- Log the upload (persist actual CSV row count for monthly stats) ----
        # rows from extract_from_file() have a "time" field (raw string), not "month_year".
        # Derive month_year the same way the ingestor does.
        month_year_log = _derive_month_year(rows[0].get("time")) if rows else None
        async with open_session() as session:
            await session.execute(text("""
                INSERT INTO upload_log
                    (filename, file_type, environment, month_year,
                     csv_row_count, inserted, updated, uploaded_at)
                VALUES
                    (:filename, :file_type, :environment, :month_year,
                     :csv_row_count, :inserted, :updated, :uploaded_at)
            """), {
                "filename":      filename,
                "file_type":     file_type,
                "environment":   environment,
                "month_year":    month_year_log,
                "csv_row_count": validation.row_count,
                "inserted":      ingest_result.inserted,
                "updated":       ingest_result.updated,
                "uploaded_at":   datetime.now(timezone.utc).isoformat(),
            })
        from api.analytics_db import invalidate_cache
        invalidate_cache("upload_log")

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
        "typed_updated":  typed_result.updated,
        "typed_skipped":  typed_result.skipped,
        "typed_errors":   typed_result.errors,
    }
