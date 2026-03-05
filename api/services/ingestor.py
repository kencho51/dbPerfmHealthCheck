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
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy import update as sa_update
from sqlmodel.ext.asyncio.session import AsyncSession

from api.models import RawQuery

# SQLModel emits a DeprecationWarning when .execute() is used on a session,
# suggesting .exec() instead.  For INSERT and UPDATE statements (non-SELECT),
# .execute() is the correct SQLAlchemy method; .exec() is SQLModel's SELECT-only
# wrapper.  Suppress the false-positive warning for this module only.
warnings.filterwarnings(
    "ignore",
    message=".*You probably want to use.*session.exec.*",
    category=DeprecationWarning,
)


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
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(time_str.strip()[:26], fmt)
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

async def ingest_rows(rows: list[dict], session: AsyncSession) -> IngestResult:
    """
    Insert or update raw_query rows in bulk.

    Uses SQLite's INSERT OR IGNORE + a follow-up UPDATE on conflict via
    SQLAlchemy's dialect-level upsert so we don't lose the occurrence counter.
    """
    result = IngestResult()

    if not rows:
        return result

    now = _now()

    for row in rows:
        try:
            q_hash = compute_hash(row)

            # Normalise enum-like fields to their string values
            source      = (row.get("source") or "unknown").lower()
            environment = (row.get("environment") or "unknown").lower()
            q_type      = (row.get("type") or "unknown").lower()

            # Clamp to known enum values
            if source not in ("sql", "mongodb"):
                source = "sql"
            if environment not in ("prod", "sat", "unknown"):
                environment = "unknown"
            if q_type not in ("slow_query", "slow_query_mongo", "blocker", "deadlock", "unknown"):
                q_type = "unknown"

            values = {
                "query_hash":      q_hash,
                "time":            row.get("time") or None,
                "source":          source,
                "host":            row.get("host") or None,
                "db_name":         row.get("db_name") or None,
                "environment":     environment,
                "type":            q_type,
                "query_details":   row.get("query_details") or None,
                "month_year":      _derive_month_year(row.get("time")),
                "occurrence_count": int(row.get("occurrence_count") or 1),
                "first_seen":      now,
                "last_seen":       now,
                "created_at":      now,
                "updated_at":      now,
            }

            stmt = sqlite_insert(RawQuery).values(**values)

            # On conflict: bump the counter and refresh last_seen / updated_at
            stmt = sqlite_insert(RawQuery).values(**values).on_conflict_do_nothing(
                index_elements=["query_hash"]
            )

            # Use execute() (not exec()) — exec() is SQLModel's select-only helper
            exec_result = await session.execute(stmt)

            if exec_result.rowcount == 1:
                # Fresh insert — row did not exist
                result.inserted += 1
            else:
                # Row already existed (conflict ignored) — bump counters manually
                # Also backfill month_year in case it was NULL previously (e.g. date
                # format not yet supported when the row was first inserted).
                derived_month = _derive_month_year(row.get("time"))
                upd = (
                    sa_update(RawQuery)
                    .where(RawQuery.query_hash == q_hash)
                    .values(
                        occurrence_count=RawQuery.occurrence_count + 1,
                        last_seen=now,
                        updated_at=now,
                        **({"month_year": derived_month} if derived_month else {}),
                    )
                )
                await session.execute(upd)
                result.updated += 1

        except Exception as exc:
            result.errors.append(f"Row error ({row.get('host', '?')}): {exc}")
            result.skipped += 1

    return result
