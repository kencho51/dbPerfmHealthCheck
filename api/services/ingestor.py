"""
Ingestor service ??bulk-ingest extracted rows into raw_query.

Architecture
------------
Step 1 ??DuckDB (CPU, runs in thread pool):
    ??Polars DataFrame registered as in-memory DuckDB view (zero-copy)
    ??MD5 hash computed with md5(concat_ws(...)) ??vectorised
    ??month_year derived via try_strptime() + strftime()
    ??Enum values clamped to known constants
    ??Returns list of normalised Python dicts (one per unique hash)

Step 2 ??Neon HTTPS REST API (async):
    ??INSERT ... ON CONFLICT (query_hash) DO UPDATE
    ??Batched into chunks of BATCH_SIZE to stay within payload limits
    ??No port 5432 required ??safe behind corporate proxies

Deduplication key (query_hash):
    MD5( source | host | db_name | environment | type | time | query_details )

On conflict:
    occurrence_count  +=  1
    last_seen / updated_at  refreshed
    all other fields left unchanged
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone

import polars as pl
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api.models import RawQuery

# Rows sent per Neon HTTPS call.  50 rows ??~5 KB ??well within limits.
BATCH_SIZE = 50

# Datetime format strings tried by DuckDB's try_strptime in order.
_STRPTIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%b %d %Y %I:%M%p",
    "%b %d %Y %I:%M:%S%p",
    "%b %d %Y",
]
_FORMATS_SQL = "[" + ", ".join(f"'{f}'" for f in _STRPTIME_FORMATS) + "]"


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class IngestResult:
    inserted: int = 0
    updated:  int = 0
    skipped:  int = 0
    errors:   list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.skipped


# ---------------------------------------------------------------------------
# Step 1 ??DuckDB normalisation (synchronous, runs in thread pool)
# ---------------------------------------------------------------------------

def _normalize_sync(rows: list[dict]) -> list[dict]:
    """
    Use DuckDB in-memory to normalise enums, compute MD5 hashes, and derive
    month_year.  Returns one dict per unique hash (occurrences summed).
    """
    import duckdb

    if not rows:
        return []

    df = pl.DataFrame(
        {
            "source":          [str(r.get("source")        or "") for r in rows],
            "host":            [str(r.get("host")          or "") for r in rows],
            "db_name":         [str(r.get("db_name")       or "") for r in rows],
            "environment":     [str(r.get("environment")   or "") for r in rows],
            "type":            [str(r.get("type")          or "") for r in rows],
            "time":            [str(r.get("time")          or "") for r in rows],
            "query_details":   [str(r.get("query_details") or "") for r in rows],
            "occurrence_count":[int(r.get("occurrence_count") or 1) for r in rows],
        }
    )

    duck = duckdb.connect()
    try:
        duck.register("staging_raw", df)
        duck.execute(f"""
            CREATE OR REPLACE TEMP TABLE staging AS
            SELECT
                md5(concat_ws('|',
                    lower(trim(source)),
                    lower(trim(host)),
                    lower(trim(db_name)),
                    lower(trim(environment)),
                    lower(trim(type)),
                    trim(time),
                    trim(query_details)
                )) AS query_hash,

                NULLIF(trim(time),          '') AS time,
                NULLIF(trim(query_details), '') AS query_details,
                NULLIF(trim(host),          '') AS host,
                NULLIF(trim(db_name),       '') AS db_name,

                CASE
                    WHEN lower(trim(source)) IN ('sql', 'mongodb') THEN lower(trim(source))
                    ELSE 'sql'
                END AS source,
                CASE
                    WHEN lower(trim(environment)) IN ('prod', 'sat', 'unknown')
                    THEN lower(trim(environment))
                    ELSE 'unknown'
                END AS environment,
                CASE
                    WHEN lower(trim(type)) IN
                         ('slow_query', 'slow_query_mongo', 'blocker', 'deadlock')
                    THEN lower(trim(type))
                    ELSE 'unknown'
                END AS type,

                strftime(
                    try_strptime(
                        trim(regexp_replace(
                            regexp_replace(trim(time), '[+-]\\d{{2}}:?\\d{{2}}$', ''),
                            '\\s+', ' '
                        )),
                        {_FORMATS_SQL}
                    ),
                    '%Y-%m'
                ) AS month_year,

                SUM(CAST(occurrence_count AS INTEGER)) AS occurrence_count

            FROM staging_raw
            GROUP BY ALL
        """)
        staging_rows = duck.execute("""
            SELECT query_hash, time, source, host, db_name, environment, type,
                   query_details, month_year, occurrence_count
            FROM staging
        """).fetchall()
    finally:
        duck.close()

    now = datetime.now(tz=timezone.utc)
    return [
        {
            "query_hash":       r[0],
            "time":             r[1],
            "source":           r[2],
            "host":             r[3],
            "db_name":          r[4],
            "environment":      r[5],
            "type":             r[6],
            "query_details":    r[7],
            "month_year":       r[8],
            "occurrence_count": r[9],
            "first_seen":       now,
            "last_seen":        now,
            "created_at":       now,
            "updated_at":       now,
        }
        for r in staging_rows
    ]


# ---------------------------------------------------------------------------
# Step 2 ??Neon HTTPS batch upsert (async)
# ---------------------------------------------------------------------------

async def _upsert_neon(normalized: list[dict], result: IngestResult) -> None:
    """
    Batch-upsert normalised rows into Neon PostgreSQL via HTTPS.

    Uses INSERT ... ON CONFLICT (query_hash) DO UPDATE to atomically
    increment occurrence_count and refresh timestamps on duplicates.
    """
    from api.database import NeonSession

    now = datetime.now(tz=timezone.utc)
    session = NeonSession()
    try:
        for i in range(0, len(normalized), BATCH_SIZE):
            chunk = normalized[i : i + BATCH_SIZE]
            stmt = (
                pg_insert(RawQuery)
                .values(chunk)
                .on_conflict_do_update(
                    index_elements=["query_hash"],
                    set_={
                        "occurrence_count": RawQuery.occurrence_count + 1,
                        "last_seen":        now,
                        "updated_at":       now,
                    },
                )
            )
            await session.execute(stmt)
        await session.commit()
        result.inserted += len(normalized)
    except Exception as exc:
        await session.rollback()
        result.errors.append(f"Neon upsert error: {type(exc).__name__}: {exc}")
        result.skipped += len(normalized)
    finally:
        await session.close()


# ---------------------------------------------------------------------------
# Public async entry point
# ---------------------------------------------------------------------------

async def ingest_rows(rows: list[dict]) -> IngestResult:
    """
    Ingest a list of raw row dicts into raw_query.

    Steps:
      1. DuckDB normalisation in a thread pool (CPU-bound, non-blocking).
      2. Neon HTTPS batch upsert (async, no port 5432 needed).

    Idempotent ??uploading the same CSV twice only bumps occurrence_count.
    """
    result = IngestResult()
    if not rows:
        return result

    try:
        normalized = await asyncio.to_thread(_normalize_sync, rows)
    except Exception as exc:
        result.errors.append(f"DuckDB normalisation error: {exc}")
        result.skipped = len(rows)
        return result

    if not normalized:
        result.skipped = len(rows)
        return result

    await _upsert_neon(normalized, result)
    return result
