"""
typed_ingestor.py — Ingest rows into the type-specific raw_query_* tables.

Each typed row dict (produced by extractor.extract_typed_from_file) contains:
  - `table_type`   — one of slow_sql | blocker | deadlock | slow_mongo |
                      datafile_sql | datafile_mongo
  - `_hash_parts`  — list of strings to MD5-hash for deduplication
  - all other keys  — native CSV columns

Deduplication strategy (mirrors ingestor.py):
  INSERT ... ON CONFLICT (query_hash) DO UPDATE
    occurrence_count += incoming count
    last_seen / updated_at refreshed
    all other columns left unchanged

Architecture:
  Step 1 — MD5 hash computed in-process (no DuckDB needed here — simple concat)
  Step 2 — aiosqlite async upsert via SQLAlchemy Core INSERT … ON CONFLICT
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from api.models import (
    RawQueryBlocker,
    RawQueryDeadlock,
    RawQuerySlowMongo,
    RawQuerySlowSql,
)

BATCH_SIZE = 500

_TABLE_MODEL_MAP = {
    "slow_sql": RawQuerySlowSql,
    "blocker": RawQueryBlocker,
    "deadlock": RawQueryDeadlock,
    "slow_mongo": RawQuerySlowMongo,
}

# Columns that belong to the bookkeeping / hash infrastructure and should NOT
# be passed through to the SQLModel INSERT.
_INTERNAL_KEYS = {"table_type", "_hash_parts"}


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _make_hash(parts: list[str]) -> str:
    raw = "|".join(str(p or "").strip() for p in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()  # nosec B324 – non-security dedup key, must stay MD5 to match existing stored hashes


def _derive_month_year_from_parts(parts: list) -> str | None:
    """
    Try to derive a YYYY-MM string from any date-like value in `parts`.
    Falls back to None if nothing parses.
    """
    import re

    patterns = [
        ("%Y-%m-%dT%H:%M:%S.%f", r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+"),
        ("%Y-%m-%dT%H:%M:%S", r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),
        ("%Y-%m-%d %H:%M:%S", r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"),
        ("%Y-%m-%d", r"\d{4}-\d{2}-\d{2}"),
        ("%Y/%m/%d %H:%M:%S.%f", r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+"),
        ("%Y/%m/%d %H:%M:%S", r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}"),
        ("%Y/%m/%d", r"\d{4}/\d{2}/\d{2}"),
        ("%m/%d/%Y %I:%M:%S %p", r"\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [AP]M"),
        ("%m/%d/%Y", r"\d{1,2}/\d{1,2}/\d{4}"),
    ]
    for val in parts:
        s = str(val or "").strip()
        if not s:
            continue
        for fmt, pat in patterns:
            m = re.search(pat, s)
            if m:
                try:
                    return datetime.strptime(m.group(), fmt).strftime("%Y-%m")
                except ValueError:
                    continue
    return None


@dataclass
class TypedIngestResult:
    table_type: str
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.skipped


def _normalise_rows(rows: list[dict]) -> list[dict]:
    """
    Add query_hash, month_year, and booking-keeping datetime defaults to each
    typed row dict.  Strip internal keys (_hash_parts, table_type).
    Returns a new list of cleaned dicts suitable for SQLAlchemy INSERT.
    """
    now = _now()
    normalised = []
    for row in rows:
        hash_parts = row.get("_hash_parts") or []
        query_hash = _make_hash(hash_parts)
        month_year = _derive_month_year_from_parts(hash_parts)

        clean = {k: v for k, v in row.items() if k not in _INTERNAL_KEYS}
        clean["query_hash"] = query_hash
        # Prefer an explicitly-set month_year from the extractor (e.g. derived from
        # event_time); fall back to the value derived from _hash_parts.  Without
        # this, types that don't include a date in _hash_parts always get None.
        clean["month_year"] = clean.get("month_year") or month_year
        clean["occurrence_count"] = 1
        clean["first_seen"] = now
        clean["last_seen"] = now
        clean["created_at"] = now
        clean["updated_at"] = now
        normalised.append(clean)

    # Deduplicate within the batch by query_hash (keep first occurrence,
    # accumulate occurrence_count).
    seen: dict[str, dict] = {}
    for row in normalised:
        h = row["query_hash"]
        if h in seen:
            seen[h]["occurrence_count"] += 1
        else:
            seen[h] = row
    return list(seen.values())


async def ingest_typed_rows(
    rows: list[dict],
    table_type: str,
) -> TypedIngestResult:
    """
    Upsert a list of typed rows into the appropriate raw_query_* table.

    Args:
        rows:       Output of extract_typed_from_file() for one file.
        table_type: One of the keys in _TABLE_MODEL_MAP.

    Returns:
        TypedIngestResult with inserted / updated / skipped / errors counts.
    """
    result = TypedIngestResult(table_type=table_type)

    if not rows:
        return result

    model = _TABLE_MODEL_MAP.get(table_type)
    if model is None:
        result.errors.append(f"Unknown table_type: {table_type!r}")
        return result

    normalised = _normalise_rows(rows)

    from sqlalchemy import text

    from api.database import write_session  # local import to avoid circular refs

    async with write_session() as session:
        for i in range(0, len(normalised), BATCH_SIZE):
            batch = normalised[i : i + BATCH_SIZE]

            # Get the actual columns defined on the model so we can drop any
            # extra keys that don't belong (e.g. "raw_xml" on a blocker row).
            valid_cols = {c.name for c in model.__table__.columns}  # type: ignore[attr-defined]

            clean_batch = [{k: v for k, v in r.items() if k in valid_cols} for r in batch]

            # -- Split into new vs existing rows (accurate insert/update counts) --
            chunk_hashes = [r["query_hash"] for r in clean_batch]
            ph = ", ".join(f":h{j}" for j in range(len(chunk_hashes)))
            params = {f"h{j}": h for j, h in enumerate(chunk_hashes)}
            existing = await session.exec(
                text(f"SELECT query_hash FROM {model.__tablename__} WHERE query_hash IN ({ph})"),  # noqa: S608
                params=params,
            )
            existing_hashes: set[str] = {row[0] for row in existing}

            new_rows = [r for r in clean_batch if r["query_hash"] not in existing_hashes]
            existing_rows = [r for r in clean_batch if r["query_hash"] in existing_hashes]

            stmt = sqlite_insert(model).values(clean_batch)  # type: ignore[arg-type]

            # ON CONFLICT: bump count + refresh timestamps; leave all other
            # columns (content) untouched so re-uploading is idempotent.
            stmt = stmt.on_conflict_do_update(
                index_elements=["query_hash"],
                set_={
                    "occurrence_count": (
                        model.occurrence_count  # type: ignore[attr-defined]
                        + stmt.excluded.occurrence_count
                    ),
                    "last_seen": stmt.excluded.last_seen,
                    "updated_at": stmt.excluded.updated_at,
                },
            )

            try:
                await session.execute(stmt)
                result.inserted += len(new_rows)
                result.updated += len(existing_rows)
            except Exception as exc:  # noqa: BLE001
                result.errors.append(f"Batch {i // BATCH_SIZE}: {exc}")

        # write_session() context manager commits automatically on clean exit;
        # the explicit commit below was redundant and is removed.

    # Flush the WAL back into the main DB file so subsequent readers don't have
    # to scan a large WAL.  PASSIVE mode is non-blocking.
    from api.database import engine as _engine  # noqa: PLC0415

    async with _engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA wal_checkpoint(PASSIVE)")

    return result


async def ingest_typed_file(file_path: Path) -> TypedIngestResult:
    """
    Convenience wrapper: extract + ingest a single typed CSV file.
    Returns TypedIngestResult(table_type="unknown") for unrecognised files.
    """
    from api.services.extractor import _detect_typed_table, extract_typed_from_file

    table_type = _detect_typed_table(file_path.name)
    if table_type == "unknown":
        return TypedIngestResult(table_type="unknown", skipped=0)

    rows = extract_typed_from_file(file_path)
    return await ingest_typed_rows(rows, table_type)
