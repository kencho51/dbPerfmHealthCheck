"""
Ingestor service — deduplicates extracted rows and upserts into raw_query table.

Deduplication key (query_hash):
    MD5( source | host | db_name | environment | type | query_details )

On conflict (same hash already in DB):
    - occurrence_count += 1
    - last_seen updated to now
    - all other fields left unchanged

Returns `IngestResult` with inserted / updated / skipped counts.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert
from api.database import NeonSession

from api.models import RawQuery

# Rows sent per HTTPS call.  50 rows ≈ ~5 KB payload — well within limits.
BATCH_SIZE = 50


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _derive_month_year(time_str: str | None) -> str | None:
    """
    Attempt to extract 'YYYY-MM' from a timestamp string.
    Returns None if the string is empty or unparseable.
    """
    if not time_str:
        return None

    # Normalise whitespace (e.g. "Jan 26 2026  9:00AM" → "Jan 26 2026 9:00AM")
    time_clean = re.sub(r"\s+", " ", time_str.strip())

    # Try common formats — add more as needed
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        # Splunk deadlock CSVs use slash-separated dates (YYYY/MM/DD)
        "%Y/%m/%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        # maxElapsedQueries CSVs use M/D/YYYY h:MM:SS AM/PM
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        # Month-name formats: "Jan 26 2026 9:00AM"
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%b %d %Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(time_clean[:26], fmt)
            return dt.strftime("%Y-%m")
        except ValueError:
            continue

    # Regex fallback: search for YYYY-MM or YYYY/MM anywhere in the string
    m = re.search(r"(\d{4})[/\-](\d{2})", time_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    return None


def compute_hash(row: dict) -> str:
    """
    Compute MD5 deduplication hash for a row dict.
    Keys used: source, host, db_name, environment, type, time, query_details.
    Including `time` ensures the same query pattern observed across different
    time windows is stored as distinct raw rows (idempotent on re-upload).
    """
    parts = "|".join([
        (row.get("source") or "").strip().lower(),
        (row.get("host") or "").strip().lower(),
        (row.get("db_name") or "").strip().lower(),
        (row.get("environment") or "").strip().lower(),
        (row.get("type") or "").strip().lower(),
        (row.get("time") or "").strip(),
        (row.get("query_details") or "").strip(),
    ])
    return hashlib.md5(parts.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class IngestResult:
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.skipped


# ---------------------------------------------------------------------------
# Main ingest function
# ---------------------------------------------------------------------------

async def ingest_rows(rows: list[dict], session: NeonSession) -> IngestResult:
    """
    Upsert raw_query rows in batches of BATCH_SIZE via the Neon HTTPS REST API.

    Each batch sends a single INSERT ... ON CONFLICT (query_hash) DO UPDATE
    statement, reducing 1000 rows from ~1000 HTTP calls to ~20 calls (~50x
    faster than the previous row-by-row approach).

    On conflict (same hash already in DB):
        - occurrence_count += 1
        - last_seen / updated_at refreshed
        - all other fields left unchanged
    """
    result = IngestResult()
    if not rows:
        return result

    now = _now()
    # Use an ordered dict keyed by query_hash to deduplicate within this upload.
    # PostgreSQL's ON CONFLICT DO UPDATE fails if the same key appears more than
    # once in a single INSERT ... VALUES (...) batch ("cannot affect row a second
    # time").  When two rows produce the same hash (identical content re-uploaded)
    # we merge them by summing occurrence_count rather than duplicating the entry.
    seen: dict[str, dict] = {}

    for row in rows:
        try:
            q_hash      = compute_hash(row)
            source      = (row.get("source") or "unknown").lower()
            environment = (row.get("environment") or "unknown").lower()
            q_type      = (row.get("type") or "unknown").lower()

            if source not in ("sql", "mongodb"):                                         source = "sql"
            if environment not in ("prod", "sat", "unknown"):                            environment = "unknown"
            if q_type not in ("slow_query", "slow_query_mongo", "blocker", "deadlock", "unknown"): q_type = "unknown"

            if q_hash in seen:
                # Duplicate within this upload — just accumulate the count.
                seen[q_hash]["occurrence_count"] += int(row.get("occurrence_count") or 1)
            else:
                seen[q_hash] = {
                    "query_hash":       q_hash,
                    "time":             row.get("time") or None,
                    "source":           source,
                    "host":             row.get("host") or None,
                    "db_name":          row.get("db_name") or None,
                    "environment":      environment,
                    "type":             q_type,
                    "query_details":    row.get("query_details") or None,
                    "month_year":       _derive_month_year(row.get("time")),
                    "occurrence_count": int(row.get("occurrence_count") or 1),
                    "first_seen":       now,
                    "last_seen":        now,
                    "created_at":       now,
                    "updated_at":       now,
                }
        except Exception as exc:
            result.errors.append(f"Row error ({row.get('host', '?')}): {exc}")
            result.skipped += 1

    all_values = list(seen.values())

    # --- Batch upsert: one HTTPS call per BATCH_SIZE rows ---
    for i in range(0, len(all_values), BATCH_SIZE):
        chunk = all_values[i : i + BATCH_SIZE]
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
        result.inserted += len(chunk)   # INSERT + UPDATE both counted as processed

    # Rows that were deduplicated within this upload (not distinct DB operations)
    result.skipped += len(rows) - len(all_values)

    return result
