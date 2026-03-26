"""
Ingestor service — bulk-ingest extracted rows into raw_query.

Architecture
------------
Step 1 — Python normalisation (CPU, runs in thread pool via asyncio.to_thread):
    • Hash computed with hashlib.md5 (byte-compatible with the original DuckDB formula)
    • month_year derived via _derive_month_year() — same 14-format strptime loop
    • Enum values clamped to known constants
    • Returns one dict per unique hash (occurrences summed)

Step 2 — SQLite async upsert:
    • INSERT ... ON CONFLICT (query_hash) DO UPDATE
    • Batched into chunks of BATCH_SIZE
    • Uses the shared aiosqlite async session (open_session)

Deduplication key (query_hash):
    MD5( source | host | db_name | environment | type | time | query_details )
    (extra_metadata appended only when non-empty — mirrors DuckDB NULLIF logic)

On conflict:
    occurrence_count  +=  incoming occurrence_count
    last_seen / updated_at  refreshed
    all other fields left unchanged
"""
from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from api.models import RawQuery

# Rows per INSERT … ON CONFLICT batch.  1 000 is the practical sweet spot
# for SQLite: large enough to minimise round-trip overhead, small enough to
# stay well below the 32 768 parameter limit (1 000 × ~10 columns = 10 000).
BATCH_SIZE = 1000

# Datetime format strings tried by _derive_month_year in order.
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


def _derive_month_year(time_str: str | None) -> str | None:
    """Return 'YYYY-MM' for *time_str* by trying every format in _STRPTIME_FORMATS.

    Returns ``None`` for ``None`` input, empty strings, or values that cannot
    be parsed by any known format.  This mirrors the DuckDB ``try_strptime``
    logic used in ``_normalize_sync`` and is exposed as a named helper so it
    can be unit-tested independently.
    """
    if not time_str:
        return None
    # Strip timezone offset (+HH:MM, +HHMM, -HH:MM, -HHMM) so strptime can parse
    # formats like '2026-02-28T23:55:18.000+0800'.  Mirrors the DuckDB
    # regexp_replace('[+-]\d{2}:?\d{2}$', '') used in the normalisation query.
    cleaned = re.sub(r"[+-]\d{2}:?\d{2}$", "", time_str.strip()).strip()
    if not cleaned:
        return None
    for fmt in _STRPTIME_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m")
        except ValueError:
            continue
    return None


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
# Step 1 — Python normalisation (hash-compatible with the original DuckDB formula)
# ---------------------------------------------------------------------------

_VALID_SOURCES      = frozenset(("sql", "mongodb"))
_VALID_ENVIRONMENTS = frozenset(("prod", "sat", "unknown"))
_VALID_TYPES        = frozenset(("slow_query", "slow_query_mongo", "blocker", "deadlock"))


def _normalize_sync(rows: list[dict]) -> list[dict]:
    """
    Normalise, hash, and deduplicate extracted rows.

    The query_hash formula replicates the original DuckDB expression exactly::

        md5(concat_ws('|',
            lower(trim(source)),
            lower(trim(host)),
            lower(trim(db_name)),
            lower(trim(environment)),
            lower(trim(type)),
            trim(time),
            trim(query_details),
            NULLIF(trim(extra_metadata), '')   -- omitted when empty
        ))

    ``concat_ws`` skips NULL values, so an empty ``extra_metadata`` does NOT
    contribute a trailing ``|`` to the hash input.  Replacing DuckDB with a
    plain Python loop eliminates the ~3 s DuckDB startup overhead per file
    while producing byte-for-byte identical hashes.
    """
    if not rows:
        return []

    now   = datetime.now(tz=timezone.utc)
    seen: dict[str, dict] = {}

    for r in rows:
        # --- Trimmed-only values stored in the DB (DuckDB: NULLIF(trim(x), '')) ----
        host_stored = (r.get("host")           or "").strip()
        db_stored   = (r.get("db_name")        or "").strip()
        t_stored    = (r.get("time")           or "").strip()
        qd_stored   = (r.get("query_details")  or "").strip()
        em_stored   = (r.get("extra_metadata") or "").strip()
        occ         = int(r.get("occurrence_count") or 1)

        # --- Lowercase values for hash  + enum-clamped for stored fields ----------
        # DuckDB hash: lower(trim(source|host|db|env|type)) + trim(time|qd|em)
        src = (r.get("source")      or "").lower().strip()
        env = (r.get("environment") or "").lower().strip()
        typ = (r.get("type")        or "").lower().strip()

        src = src if src in _VALID_SOURCES      else "sql"
        env = env if env in _VALID_ENVIRONMENTS else "unknown"
        typ = typ if typ in _VALID_TYPES        else "unknown"

        # --- Compute MD5 (identical to DuckDB concat_ws + NULLIF logic) ---------
        parts = [src, host_stored.lower(), db_stored.lower(), env, typ, t_stored, qd_stored]
        if em_stored:               # NULLIF(trim(extra_metadata), '') → skip when empty
            parts.append(em_stored)
        qhash = hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()

        # --- Deduplicate (DuckDB GROUP BY ALL + SUM) ----------------------------
        if qhash in seen:
            seen[qhash]["occurrence_count"] += occ
        else:
            seen[qhash] = {
                "query_hash":       qhash,
                "time":             t_stored    or None,   # original case, trimmed
                "source":           src         or None,   # lowercased (enum)
                "host":             host_stored or None,   # original case, trimmed
                "db_name":          db_stored   or None,   # original case, trimmed
                "environment":      env         or None,   # lowercased (enum)
                "type":             typ         or None,   # lowercased (enum)
                "query_details":    qd_stored   or None,   # original case, trimmed
                "extra_metadata":   em_stored   or None,   # original case, trimmed
                "month_year":       _derive_month_year(t_stored),
                "occurrence_count": occ,
                "first_seen":       now,
                "last_seen":        now,
                "created_at":       now,
                "updated_at":       now,
            }

    return list(seen.values())


# ---------------------------------------------------------------------------
# Step 2 — SQLite batch upsert (async)
# ---------------------------------------------------------------------------

async def _upsert_sqlite(normalized: list[dict], result: IngestResult) -> None:
    """
    Batch-upsert normalised rows into SQLite via the shared async session.

    Uses a single `INSERT … ON CONFLICT (query_hash) DO UPDATE` per batch
    so re-uploads only cost one bulk statement instead of N individual
    UPDATE calls.  The previous SELECT + per-row UPDATE pattern created up
    to 1 700 separate async round-trips for 1 655 rows, each carrying the
    full aiosqlite thread-handoff overhead.

    inserted / updated counts are approximated: a pre-check SELECT on the
    batch's hash list is issued once per batch (O(B log N) with the index)
    to split new vs existing rows for accurate reporting.
    """
    from sqlalchemy import text
    from api.database import open_session

    try:
        async with open_session() as session:
            for i in range(0, len(normalized), BATCH_SIZE):
                chunk = normalized[i : i + BATCH_SIZE]
                chunk_hashes = [r["query_hash"] for r in chunk]

                # -- Count new vs existing for reporting only -----------------
                # One indexed SELECT per batch; does NOT gate the upsert.
                ph = ", ".join(f":h{j}" for j in range(len(chunk_hashes)))
                params = {f"h{j}": h for j, h in enumerate(chunk_hashes)}
                rows_exist = await session.execute(
                    text(f"SELECT query_hash FROM raw_query WHERE query_hash IN ({ph})"),  # noqa: S608
                    params,
                )
                existing_hashes: set[str] = {row[0] for row in rows_exist}
                result.inserted += sum(1 for h in chunk_hashes if h not in existing_hashes)
                result.updated  += sum(1 for h in chunk_hashes if h in existing_hashes)

                # -- Single bulk upsert (no per-row UPDATE loop) --------------
                stmt = sqlite_insert(RawQuery).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["query_hash"],
                    set_={
                        "occurrence_count": (
                            RawQuery.occurrence_count + stmt.excluded.occurrence_count
                        ),
                        "last_seen":  stmt.excluded.last_seen,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                await session.execute(stmt)

        # Invalidate the analytics DataFrame cache so the next dashboard
        # request reads fresh data from SQLite rather than stale cached rows.
        from api.analytics_db import invalidate_cache
        invalidate_cache("raw_query")
    except Exception as exc:
        result.errors.append(f"SQLite upsert error: {type(exc).__name__}: {exc}")
        result.skipped += len(normalized)


# ---------------------------------------------------------------------------
# Public async entry point
# ---------------------------------------------------------------------------

async def ingest_rows(rows: list[dict]) -> IngestResult:
    """
    Ingest a list of raw row dicts into raw_query.

    Steps:
      1. DuckDB normalisation in a thread pool (CPU-bound, non-blocking).
      2. SQLite async upsert via open_session.

    Idempotent — uploading the same CSV twice only bumps occurrence_count.
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

    await _upsert_sqlite(normalized, result)
    return result
