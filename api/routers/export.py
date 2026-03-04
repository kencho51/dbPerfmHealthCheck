"""
GET /api/export  — stream the full RawQuery dataset as a UTF-8 CSV.

Joins Pattern (name, pattern_tag, severity) for ML-readiness.
Applies the same filter parameters as GET /api/queries so callers can
export a filtered slice (e.g. only prod + slow_query).

Response uses StreamingResponse with chunked transfer so large datasets
(100k+ rows) never fully materialise in memory.

CSV columns (in order):
    id, query_hash, time, source, host, db_name, environment, type,
    month_year, occurrence_count, first_seen, last_seen,
    query_details,
    pattern_id, pattern_name, pattern_tag, pattern_severity
"""
from __future__ import annotations

import csv
import enum
import io
import warnings
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Optional

# session.execute() on a multi-column join select returns SQLAlchemy Row objects
# (not SQLModel models) — this is the correct path.  Suppress the SQLModel
# false-positive DeprecationWarning that recommends exec() instead.
warnings.filterwarnings(
    "ignore",
    message=".*You probably want to use.*session.exec.*",
    category=DeprecationWarning,
)

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.models import EnvironmentType, Pattern, QueryType, RawQuery, SourceType

router = APIRouter()

_CHUNK = 500   # rows per DB fetch; keeps memory constant

_CSV_FIELDS = [
    "id", "query_hash", "time", "source", "host", "db_name",
    "environment", "type", "month_year", "occurrence_count",
    "first_seen", "last_seen", "query_details",
    "pattern_id", "pattern_name", "pattern_tag", "pattern_severity",
]


def _apply_filters(
    stmt,
    *,
    environment: Optional[EnvironmentType],
    type:        Optional[QueryType],
    source:      Optional[SourceType],
    host:        Optional[str],
    db_name:     Optional[str],
    month_year:  Optional[list[str]],
    pattern_id:  Optional[int],
    has_pattern: Optional[bool],
    search:      Optional[str],
):
    if environment is not None:
        stmt = stmt.where(RawQuery.environment == environment)
    if type is not None:
        stmt = stmt.where(RawQuery.type == type)
    if source is not None:
        stmt = stmt.where(RawQuery.source == source)
    if host:
        stmt = stmt.where(col(RawQuery.host).contains(host))
    if db_name:
        stmt = stmt.where(col(RawQuery.db_name).contains(db_name))
    if month_year:
        stmt = stmt.where(col(RawQuery.month_year).in_(month_year))
    if pattern_id is not None:
        stmt = stmt.where(RawQuery.pattern_id == pattern_id)
    if has_pattern is True:
        stmt = stmt.where(col(RawQuery.pattern_id).isnot(None))
    if has_pattern is False:
        stmt = stmt.where(col(RawQuery.pattern_id).is_(None))
    if search:
        stmt = stmt.where(col(RawQuery.query_details).contains(search))
    return stmt


def _fmt(v: object) -> str:
    """Convert a value to a CSV-safe string."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, enum.Enum):
        return str(v.value)
    return str(v)


async def _csv_generator(
    session: AsyncSession,
    filter_kwargs: dict,
) -> "AsyncGenerator[str, None]":
    """
    Async generator that yields CSV text in chunks.
    The header row is emitted first, then data is fetched in _CHUNK-sized
    batches using offset pagination to keep memory usage constant.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")

    # ---- Header ----
    writer.writerow(_CSV_FIELDS)
    yield buf.getvalue()

    # ---- Build base SELECT with left-join to Pattern ----
    offset = 0
    while True:
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\r\n")

        stmt = (
            select(
                RawQuery.id,
                RawQuery.query_hash,
                RawQuery.time,
                RawQuery.source,
                RawQuery.host,
                RawQuery.db_name,
                RawQuery.environment,
                RawQuery.type,
                RawQuery.month_year,
                RawQuery.occurrence_count,
                RawQuery.first_seen,
                RawQuery.last_seen,
                RawQuery.query_details,
                RawQuery.pattern_id,
                Pattern.name.label("pattern_name"),        # type: ignore[union-attr]
                Pattern.pattern_tag.label("pattern_tag"),  # type: ignore[union-attr]
                Pattern.severity.label("pattern_severity"),  # type: ignore[union-attr]
            )
            .join(Pattern, RawQuery.pattern_id == Pattern.id, isouter=True)
        )

        stmt = _apply_filters(stmt, **filter_kwargs)
        stmt = stmt.order_by(RawQuery.id).offset(offset).limit(_CHUNK)

        result = await session.execute(stmt)
        rows = result.fetchall()

        if not rows:
            break

        for row in rows:
            writer.writerow([_fmt(v) for v in row])

        yield buf.getvalue()
        offset += _CHUNK

        if len(rows) < _CHUNK:
            break


@router.get(
    "/export",
    summary="Export RawQuery + Pattern join as UTF-8 CSV (ML-ready)",
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "Streaming CSV with pattern metadata joined.",
        }
    },
)
async def export_csv(
    environment: Optional[EnvironmentType] = None,
    type:        Optional[QueryType]        = None,
    source:      Optional[SourceType]       = None,
    host:        Optional[str]              = Query(default=None),
    db_name:     Optional[str]              = Query(default=None),
    month_year:  Optional[list[str]]        = Query(default=None),
    pattern_id:  Optional[int]              = None,
    has_pattern: Optional[bool]             = None,
    search:      Optional[str]              = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> StreamingResponse:
    """
    Stream all matching RawQuery rows joined with Pattern metadata as CSV.

    Suitable for ML training pipelines — labelled rows include
    `pattern_name`, `pattern_tag`, and `pattern_severity` when a pattern
    has been assigned.

    Query parameters mirror `GET /api/queries` for consistent filtering.
    """
    filter_kwargs = dict(
        environment=environment, type=type, source=source, host=host,
        db_name=db_name, month_year=month_year, pattern_id=pattern_id,
        has_pattern=has_pattern, search=search,
    )

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"db_perf_export_{ts}.csv"

    return StreamingResponse(
        _csv_generator(session, filter_kwargs),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )
