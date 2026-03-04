"""
Query endpoints:
    GET  /api/queries         — paginated, filterable list of raw query rows
    PATCH /api/queries/{id}   — assign a pattern_id to a raw query row
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.models import (
    EnvironmentType,
    QueryType,
    RawQuery,
    RawQueryRead,
    SourceType,
)

router = APIRouter()

_PAGE_MAX = 200


# ---------------------------------------------------------------------------
# GET /api/queries
# ---------------------------------------------------------------------------

@router.get("", response_model=list[RawQueryRead], summary="List raw query rows")
async def list_queries(
    # Filters
    environment: Optional[EnvironmentType] = None,
    type:        Optional[QueryType]        = None,
    source:      Optional[SourceType]       = None,
    host:        Optional[str]              = Query(default=None),
    db_name:     Optional[str]              = Query(default=None),
    month_year:  Optional[list[str]]        = Query(default=None),
    pattern_id:  Optional[int]              = None,
    has_pattern: Optional[bool]             = None,
    search:      Optional[str]              = Query(default=None, description="Substring search in query_details"),
    # Pagination
    offset: int = Query(default=0, ge=0),
    limit:  int = Query(default=50, ge=1, le=_PAGE_MAX),
    session: AsyncSession = Depends(get_session),
) -> list[RawQuery]:
    stmt = select(RawQuery)

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
        stmt = stmt.where(RawQuery.pattern_id.isnot(None))  # type: ignore[union-attr]
    if has_pattern is False:
        stmt = stmt.where(RawQuery.pattern_id.is_(None))  # type: ignore[union-attr]
    if search:
        stmt = stmt.where(col(RawQuery.query_details).contains(search))

    stmt = stmt.order_by(col(RawQuery.last_seen).desc()).offset(offset).limit(limit)

    rows = await session.exec(stmt)
    return list(rows.all())


# ---------------------------------------------------------------------------
# GET /api/queries/{id}
# ---------------------------------------------------------------------------

@router.get("/{query_id}", response_model=RawQueryRead, summary="Get a single raw query")
async def get_query(
    query_id: int,
    session: AsyncSession = Depends(get_session),
) -> RawQuery:
    row = await session.get(RawQuery, query_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Query not found")
    return row


# ---------------------------------------------------------------------------
# PATCH /api/queries/{id}  — assign / unassign a pattern
# ---------------------------------------------------------------------------

class _PatchQuery(dict):
    pass


from pydantic import BaseModel


class QueryPatch(BaseModel):
    pattern_id: Optional[int] = None


@router.patch("/{query_id}", response_model=RawQueryRead, summary="Assign a pattern to a query")
async def patch_query(
    query_id: int,
    body: QueryPatch,
    session: AsyncSession = Depends(get_session),
) -> RawQuery:
    row = await session.get(RawQuery, query_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Query not found")

    row.pattern_id = body.pattern_id
    row.updated_at = datetime.now(tz=timezone.utc)
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row
