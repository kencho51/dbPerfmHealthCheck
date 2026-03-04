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

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlmodel.ext.asyncio.session import AsyncSession

from api.models import RawQuery


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
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(time_str[:26], fmt)  # cap microseconds
            return dt.strftime("%Y-%m")
        except ValueError:
            continue

    # Regex fallback: search for YYYY-MM anywhere in the string
    m = re.search(r"(\d{4})-(\d{2})", time_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    return None


def compute_hash(row: dict) -> str:
    """
    Compute MD5 deduplication hash for a row dict.
    Keys used: source, host, db_name, environment, type, query_details.
    """
    parts = "|".join([
        (row.get("source") or "").strip().lower(),
        (row.get("host") or "").strip().lower(),
        (row.get("db_name") or "").strip().lower(),
        (row.get("environment") or "").strip().lower(),
        (row.get("type") or "").strip().lower(),
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
            if q_type not in ("slow_query", "blocker", "deadlock", "unknown"):
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
                "occurrence_count": 1,
                "first_seen":      now,
                "last_seen":       now,
                "created_at":      now,
                "updated_at":      now,
            }

            stmt = sqlite_insert(RawQuery).values(**values)

            # On conflict: bump the counter and refresh last_seen / updated_at
            stmt = stmt.on_conflict_do_update(
                index_elements=["query_hash"],
                set_={
                    "occurrence_count": RawQuery.occurrence_count + 1,
                    "last_seen":        now,
                    "updated_at":       now,
                },
            )

            exec_result = await session.exec(stmt)  # type: ignore[arg-type]

            # rowcount == 1 → insert;  rowcount == 2 → update (SQLite upsert)
            if exec_result.rowcount == 1:
                result.inserted += 1
            else:
                result.updated += 1

        except Exception as exc:
            result.errors.append(f"Row error ({row.get('host', '?')}): {exc}")
            result.skipped += 1

    await session.commit()
    return result
