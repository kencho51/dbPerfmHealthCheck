"""
GET /api/export  -- stream the full RawQuery dataset as a UTF-8 CSV.

Left-joins curated_query + pattern_label for ML-readiness.
Applies the same filter parameters as GET /api/queries so callers can
export a filtered slice (e.g. only prod + slow_query).

Response uses StreamingResponse with chunked transfer so large datasets
(100k+ rows) never fully materialise in memory.

CSV columns (in order):
    id, query_hash, time, source, host, db_name, environment, type,
    month_year, occurrence_count, first_seen, last_seen,
    query_details,
    curated_id, label_id, label_name, label_severity, notes
"""
from __future__ import annotations

import csv
import enum
import io
import warnings
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.models import CuratedQuery, EnvironmentType, PatternLabel, QueryType, RawQuery, SourceType

warnings.filterwarnings(
    "ignore",
    message=".*You probably want to use.*session.exec.*",
    category=DeprecationWarning,
)

router = APIRouter()

_CHUNK = 500   # rows per DB fetch; keeps memory constant

_CSV_FIELDS = [
    "id", "query_hash", "time", "source", "host", "db_name",
    "environment", "type", "month_year", "occurrence_count",
    "first_seen", "last_seen", "query_details",
    "curated_id", "label_id", "label_name", "label_severity", "notes",
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
    is_curated:  Optional[bool],
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
    if is_curated is True:
        subq = select(CuratedQuery.raw_query_id)
        stmt = stmt.where(col(RawQuery.id).in_(subq))
    if is_curated is False:
        subq = select(CuratedQuery.raw_query_id)
        stmt = stmt.where(col(RawQuery.id).not_in(subq))
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
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")

    # ---- Header ----
    writer.writerow(_CSV_FIELDS)
    yield buf.getvalue()

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
                CuratedQuery.id.label("curated_id"),           # type: ignore[union-attr]
                CuratedQuery.label_id.label("label_id"),       # type: ignore[union-attr]
                PatternLabel.name.label("label_name"),         # type: ignore[union-attr]
                PatternLabel.severity.label("label_severity"), # type: ignore[union-attr]
                CuratedQuery.notes.label("notes"),             # type: ignore[union-attr]
            )
            .join(CuratedQuery, RawQuery.id == CuratedQuery.raw_query_id, isouter=True)
            .join(PatternLabel, CuratedQuery.label_id == PatternLabel.id, isouter=True)
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
    summary="Export RawQuery + Curation join as UTF-8 CSV (ML-ready)",
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "Streaming CSV with curation metadata joined.",
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
    is_curated:  Optional[bool]             = None,
    search:      Optional[str]              = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> StreamingResponse:
    """
    Stream all matching RawQuery rows joined with curation + label metadata as CSV.

    Suitable for ML training pipelines -- curated rows include
    `label_name` and `label_severity` when a label has been assigned.

    Query parameters mirror `GET /api/queries` for consistent filtering.
    """
    filter_kwargs = dict(
        environment=environment, type=type, source=source, host=host,
        db_name=db_name, month_year=month_year, is_curated=is_curated, search=search,
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
