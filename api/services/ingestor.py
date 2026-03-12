"""
Ingestor service — bulk-ingest extracted rows into raw_query.

Architecture (bring-in-duckdb-for-analysis branch)
---------------------------------------------------
Previous implementation:  row-by-row Python loop with SQLAlchemy
  • One INSERT + one conditional UPDATE per row

New implementation:  DuckDB (compute) + sqlite3 (writes)
  • Polars DataFrame registered as an in-memory DuckDB view (zero-copy)
  • MD5 hash computed in DuckDB via md5(concat_ws(...)) — vectorised
  • month_year derived via try_strptime(col, [format_list]) in SQL
  • Staging result collected from DuckDB as Python tuples
  • sqlite3 executemany for bulk INSERT / UPDATE (no DuckDB extension needed)

DuckDB is used ONLY for pure in-memory computation — no LOAD sqlite,
no ATTACH.  This avoids corporate file-permission errors when DuckDB
tries to move extension files into its local cache directory.

Deduplication key (query_hash):
    MD5( source | host | db_name | environment | type | time | query_details )

On conflict (hash already in raw_query):
    occurrence_count  +=  1
    last_seen          =  now
    month_year backfilled if currently NULL
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone

import polars as pl


# ---------------------------------------------------------------------------
# Result type (public interface unchanged)
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
# DuckDB format list for month_year derivation
# Mirrors the formats that the old _derive_month_year() tried in sequence.
# DuckDB try_strptime accepts a LIST of format strings and returns the first
# that parses successfully — replaces 14 sequential Python try/except blocks.
# ---------------------------------------------------------------------------

_STRPTIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%f",   # ISO with microseconds (after TZ strip)
    "%Y-%m-%dT%H:%M:%S",      # ISO without microseconds
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S.%f",   # Splunk deadlock format
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%m/%d/%Y %I:%M:%S %p",   # maxElapsedQueries US AM/PM
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%b %d %Y %I:%M%p",       # "Jan 26 2026 9:00AM"
    "%b %d %Y %I:%M:%S%p",
    "%b %d %Y",
]
_FORMATS_SQL = "[" + ", ".join(f"'{f}'" for f in _STRPTIME_FORMATS) + "]"


# ---------------------------------------------------------------------------
# Internal: synchronous DuckDB bulk ingest (called via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _ingest_sync(rows: list[dict]) -> IngestResult:
    """
    Synchronous ingestion pipeline.

    Steps
    -----
    1. Build a Polars DataFrame from the raw row dicts.
    2. DuckDB (pure in-memory, no SQLite extension): normalize enums,
       compute MD5 hash, derive month_year → collect as Python tuples.
    3. Python sqlite3: read existing hashes from raw_query.
    4. Split batches into INSERT (new) and UPDATE (duplicate) sets.
    5. sqlite3 executemany: bulk INSERT + bulk UPDATE in one transaction.

    This approach avoids LOAD sqlite / ATTACH entirely — DuckDB is used
    only for vectorised string/time computation, not for writing to SQLite.
    """
    if not rows:
        return IngestResult()

    import duckdb
    import sqlite3 as _sqlite3
    from api.database import SQLITE_PATH

    result  = IngestResult()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # ── 1. Build Polars DataFrame ──────────────────────────────────────────
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

    # ── 2. DuckDB: normalize + hash + derive month_year (pure in-memory) ──
    duck = duckdb.connect()
    try:
        duck.register("staging_raw", df)
        duck.execute(f"""
            CREATE OR REPLACE TEMP TABLE staging AS
            SELECT
                -- MD5 dedup key: 7 normalised fields joined by '|'
                md5(concat_ws('|',
                    lower(trim(source)),
                    lower(trim(host)),
                    lower(trim(db_name)),
                    lower(trim(environment)),
                    lower(trim(type)),
                    trim(time),
                    trim(query_details)
                )) AS query_hash,

                NULLIF(trim(time), '')           AS time,
                NULLIF(trim(query_details), '')  AS query_details,
                NULLIF(trim(host), '')           AS host,
                NULLIF(trim(db_name), '')        AS db_name,

                -- Clamp to known enum values
                CASE
                    WHEN lower(trim(source)) IN ('sql', 'mongodb')
                    THEN lower(trim(source))
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

                CAST(occurrence_count AS INTEGER) AS occurrence_count

            FROM staging_raw
        """)
        staging_rows: list[tuple] = duck.execute("""
            SELECT query_hash, time, source, host, db_name, environment, type,
                   query_details, month_year, occurrence_count
            FROM staging
        """).fetchall()
    finally:
        duck.close()

    if not staging_rows:
        return result

    # ── 3–5. sqlite3: diff against existing hashes + write ────────────────
    sqlite_con = _sqlite3.connect(str(SQLITE_PATH))
    try:
        staging_hashes = [r[0] for r in staging_rows]
        ph = ",".join("?" * len(staging_hashes))
        existing = {
            row[0]
            for row in sqlite_con.execute(
                f"SELECT query_hash FROM raw_query WHERE query_hash IN ({ph})",  # noqa: S608
                staging_hashes,
            ).fetchall()
        }

        to_insert = [r for r in staging_rows if r[0] not in existing]
        to_update = [r for r in staging_rows if r[0] in existing]

        if to_insert:
            sqlite_con.executemany(
                """INSERT INTO raw_query (
                    query_hash, time, source, host, db_name, environment, type,
                    query_details, month_year, occurrence_count,
                    first_seen, last_seen, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (r[0], r[1], r[2], r[3], r[4], r[5], r[6],
                     r[7], r[8], r[9],
                     now_str, now_str, now_str, now_str)
                    for r in to_insert
                ],
            )

        if to_update:
            sqlite_con.executemany(
                "UPDATE raw_query SET occurrence_count = occurrence_count + 1,"
                " last_seen = ?, updated_at = ? WHERE query_hash = ?",
                [(now_str, now_str, r[0]) for r in to_update],
            )
            # Backfill month_year where it was previously NULL
            sqlite_con.executemany(
                "UPDATE raw_query SET month_year = ?"
                " WHERE query_hash = ? AND month_year IS NULL",
                [(r[8], r[0]) for r in to_update if r[8]],
            )

        sqlite_con.commit()
        result.inserted = len(to_insert)
        result.updated  = len(to_update)

    except Exception as exc:
        sqlite_con.rollback()
        result.errors.append(f"Ingest error: {type(exc).__name__}: {exc}")
        result.skipped = len(rows)
    finally:
        sqlite_con.close()

    return result


# ---------------------------------------------------------------------------
# Public async entry point
# ---------------------------------------------------------------------------

async def ingest_rows(rows: list[dict]) -> IngestResult:
    """
    Async wrapper — runs the synchronous DuckDB bulk ingest in a thread pool.

    Breaking change from previous version:
        ``session: AsyncSession`` parameter removed.
        DuckDB writes directly to the SQLite file via ATTACH (READ_WRITE mode).
        The upload router no longer needs to inject an async session here.
    """
    return await asyncio.to_thread(_ingest_sync, rows)
