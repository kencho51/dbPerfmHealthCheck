"""
Query endpoints:
    GET  /api/queries         — paginated, filterable list of raw query rows
    GET  /api/queries/count   — total matching row count (for pagination)
    GET  /api/queries/{id}    — single row by primary key
    PATCH /api/queries/{id}   — assign / unassign a pattern_id
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel
from sqlmodel import col, func, select
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
# Shared filter builder (reused by list + count endpoints)
# ---------------------------------------------------------------------------

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
    # Sorting
    sort_by:  str = Query(default="id",   pattern="^(id|last_seen|occurrence_count)$"),
    sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    # Pagination
    offset: int = Query(default=0, ge=0),
    limit:  int = Query(default=50, ge=1, le=_PAGE_MAX),
    # Response
    response: Response = None,
    session: AsyncSession = Depends(get_session),
) -> list[RawQuery]:
    """
    Returns paginated rows.  The response header `X-Total-Count` carries the
    total number of matching rows (ignoring offset/limit) so the frontend can
    render a correct pagination control.
    """
    filter_kwargs = dict(
        environment=environment, type=type, source=source, host=host,
        db_name=db_name, month_year=month_year, pattern_id=pattern_id,
        has_pattern=has_pattern, search=search,
    )

    # Total count (for X-Total-Count header)
    count_stmt = _apply_filters(select(func.count(RawQuery.id)), **filter_kwargs)
    total       = (await session.exec(count_stmt)).one()
    if response is not None:
        response.headers["X-Total-Count"] = str(total)
        response.headers["Access-Control-Expose-Headers"] = "X-Total-Count"

    # Paginated data
    stmt = _apply_filters(select(RawQuery), **filter_kwargs)
    _SORT_COLS = {"id": RawQuery.id, "last_seen": RawQuery.last_seen, "occurrence_count": RawQuery.occurrence_count}
    _col_expr = col(_SORT_COLS.get(sort_by, RawQuery.id))
    stmt = stmt.order_by(_col_expr.desc() if sort_dir == "desc" else _col_expr.asc()).offset(offset).limit(limit)
    rows = await session.exec(stmt)
    return list(rows.all())


# ---------------------------------------------------------------------------
# GET /api/queries/count  — dedicated count endpoint (avoids full data fetch)
# ---------------------------------------------------------------------------

@router.get("/count", summary="Count matching raw query rows")
async def count_queries(
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
) -> dict:
    stmt = _apply_filters(
        select(func.count(RawQuery.id)),
        environment=environment, type=type, source=source, host=host,
        db_name=db_name, month_year=month_year, pattern_id=pattern_id,
        has_pattern=has_pattern, search=search,
    )
    total = (await session.exec(stmt)).one()
    return {"count": total}


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
