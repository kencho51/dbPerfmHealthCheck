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
from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, status
from sqlalchemy import text

from api.database import write_session
from api.services.extractor import extract_from_file, extract_typed_from_file
from api.services.ingestor import _derive_month_year, ingest_rows
from api.services.typed_ingestor import ingest_typed_rows
from api.services.validator import validate_csv

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

# Separate index for blocker link query which has NO host/db_name predicate.
# Using (type, source, environment, month_year) lets SQLite narrow the scan
# without the host/db_name columns that blocker rows don't carry.
_CREATE_SOURCE_IDX = """
    CREATE INDEX IF NOT EXISTS ix_raw_query_link_source
    ON raw_query (type, source, environment, month_year)
"""

# Map each table_type to the most selective index DDL for its link query.
_LINK_AUX_IDX: dict[str, str] = {
    "slow_sql": _CREATE_COMPOSITE_IDX,
    "deadlock": _CREATE_COMPOSITE_IDX,
    "slow_mongo": _CREATE_COMPOSITE_IDX,
    "blocker": _CREATE_SOURCE_IDX,
}

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
              AND rq.host        IS raw_query_slow_mongo.host
              AND rq.db_name     IS raw_query_slow_mongo.db_name
              AND rq.query_details IS raw_query_slow_mongo.command_json
            LIMIT 1
        )
        WHERE raw_query_id IS NULL
    """,
}


async def _link_typed_to_raw(table_type: str) -> None:
    """Run the post-ingest UPDATE to set raw_query_id FK on the typed table.

    Creates the most selective available index before the correlated subquery:
    - slow_sql / deadlock / slow_mongo: ix_raw_query_link_key on
      (type, host, db_name, environment, month_year) — these queries filter on
      both host AND db_name so the full composite prefix is used.
    - blocker: ix_raw_query_link_source on (type, source, environment,
      month_year) — the blocker link SQL has no host/db_name predicate, so
      the composite index is useless; without this fix: 28 K×35 K = 875 M
      comparisons = 10-minute timeout.

    Both indexes use IF NOT EXISTS so there is no overhead on repeat uploads.
    This function is intentionally called as a background task (not awaited
    in the hot path) so the upload HTTP response is not blocked.

    For slow_mongo, a second Python-side pass handles NEW-format raw_query rows
    where query_details = queryShapeHash or ns:op_type.  Both extractors derive
    the same query_key and typed_ingestor stores
      query_hash = MD5(host | db_name | env | query_key)
    so we re-derive that hash from raw_query and match against query_hash.
    """
    import hashlib
    import logging

    sql = _LINK_SQL.get(table_type)
    if not sql:
        return
    idx_ddl = _LINK_AUX_IDX.get(table_type, _CREATE_COMPOSITE_IDX)
    try:
        async with write_session() as session:
            await session.execute(text(idx_ddl))
            await session.execute(text(sql))

            # ── slow_mongo extra pass: hash-based linking for new-format rows ─
            if table_type == "slow_mongo":
                await _link_slow_mongo_by_shape(session)
    except Exception:  # noqa: BLE001
        logging.getLogger(__name__).warning(
            "_link_typed_to_raw(%s) failed — will retry on next upload",
            table_type,
            exc_info=True,
        )


async def _link_slow_mongo_by_shape(session) -> None:
    """Second-pass link for slow_mongo when extractor outputs queryShapeHash
    or ns:op_type as query_details (not the raw command JSON).

    typed_ingestor._make_hash uses: MD5(host|db_name|env|query_key)
    where query_key == raw_query.query_details for new-format rows.
    We reconstruct that hash in Python and match against query_hash.
    """
    import hashlib

    # Load unlinked typed rows (small set after first-pass SQL)
    unlinked_result = await session.execute(
        text("SELECT id, query_hash FROM raw_query_slow_mongo WHERE raw_query_id IS NULL")
    )
    unlinked: dict[str, int] = {row[1]: row[0] for row in unlinked_result}  # hash → typed_id
    if not unlinked:
        return

    # Stream new-format raw_query slow_mongo rows
    rq_result = await session.execute(
        text(
            "SELECT id, host, db_name, environment, query_details "
            "FROM raw_query WHERE type='slow_query_mongo' AND source='mongodb'"
        )
    )
    updates: list[tuple[int, int]] = []  # (raw_query_id, typed_id)
    for rq_id, host, db_name, env, qd in rq_result:
        if not qd:
            continue
        candidate = hashlib.md5(
            "|".join(str(p or "").strip() for p in [host, db_name, env, qd]).encode("utf-8")
        ).hexdigest()
        if candidate in unlinked:
            updates.append((rq_id, unlinked[candidate]))

    for rq_id, typed_id in updates:
        await session.execute(
            text(
                "UPDATE raw_query_slow_mongo SET raw_query_id=:rq WHERE id=:t AND raw_query_id IS NULL"
            ),
            {"rq": rq_id, "t": typed_id},
        )


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
        from api.services.extractor import (
            _detect_file_category,
            _detect_typed_table,
            _extract_environment,
        )

        file_type = _detect_file_category(filename)
        environment = _extract_environment(filename)
        table_type = _detect_typed_table(filename)
        validation.file_type = file_type
        validation.environment = environment

        if not validation.is_valid:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail={
                    "message": "CSV validation failed — no data ingested.",
                    "errors": validation.errors,
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
                    environment if p == "unknown" else p for p in row["_hash_parts"]
                ]

        # -- Ingest normalised ---------------------------------------------------
        ingest_result = await ingest_rows(rows)

        # -- Ingest typed --------------------------------------------------------
        typed_result = await ingest_typed_rows(typed_rows, table_type)

        # -- Log the upload (persist actual CSV row count for monthly stats) ----
        # rows from extract_from_file() have a "time" field (raw string), not "month_year".
        # Derive month_year the same way the ingestor does.
        month_year_log = _derive_month_year(rows[0].get("time")) if rows else None
        async with write_session() as session:
            # Remove any previous entry for this filename so re-uploads don't
            # inflate the monthly CSV-row totals in the analytics table.
            await session.execute(
                text("DELETE FROM upload_log WHERE filename = :fn"),
                {"fn": filename},
            )
            await session.execute(
                text("""
                INSERT INTO upload_log
                    (filename, file_type, environment, month_year,
                     csv_row_count, inserted, updated, uploaded_at)
                VALUES
                    (:filename, :file_type, :environment, :month_year,
                     :csv_row_count, :inserted, :updated, :uploaded_at)
            """),
                {
                    "filename": filename,
                    "file_type": file_type,
                    "environment": environment,
                    "month_year": month_year_log,
                    "csv_row_count": validation.row_count,
                    "inserted": ingest_result.inserted,
                    "updated": ingest_result.updated,
                    "uploaded_at": datetime.now(UTC).isoformat(),
                },
            )
        from api.analytics_db import invalidate_cache

        invalidate_cache("upload_log")

        # -- Link typed rows to raw_query (Phase 1 — set raw_query_id FK) ------
        # Scheduled AFTER upload_log commit so no active write transaction
        # exists when the task fires.  Idempotent (WHERE raw_query_id IS NULL).
        asyncio.create_task(_link_typed_to_raw(table_type))

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
        "filename": filename,
        "file_type": file_type,
        "table_type": table_type,
        "environment": environment,
        "row_count": validation.row_count,
        # raw_query (normalised)
        "inserted": ingest_result.inserted,
        "updated": ingest_result.updated,
        "skipped": ingest_result.skipped,
        "warnings": validation.warnings,
        "errors": ingest_result.errors,
        # raw_query_<type> (full fidelity)
        "typed_inserted": typed_result.inserted,
        "typed_updated": typed_result.updated,
        "typed_skipped": typed_result.skipped,
        "typed_errors": typed_result.errors,
    }
