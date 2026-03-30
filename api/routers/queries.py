"""
Query endpoints:
    GET  /api/queries             -- paginated, filterable list of raw query rows
    GET  /api/queries/count       -- total matching row count (for pagination)
    GET  /api/queries/distinct    -- distinct host and db_name values
    GET  /api/queries/{id}        -- single row by primary key
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlmodel import col, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.host_system import apply_system_filter
from api.models import (
    CuratedQuery,
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
    environment: EnvironmentType | None,
    type: QueryType | None,
    source: SourceType | None,
    host: str | None,
    db_name: str | None,
    month_year: list[str] | None,
    is_curated: bool | None,
    search: str | None,
    system: str | None = None,
):
    if environment is not None:
        stmt = stmt.where(RawQuery.environment == environment)
    if type is not None:
        stmt = stmt.where(RawQuery.type == type)
    if source is not None:
        stmt = stmt.where(RawQuery.source == source)
    if host:
        stmt = stmt.where(RawQuery.host == host)
    if db_name:
        stmt = stmt.where(RawQuery.db_name == db_name)
    if month_year:
        stmt = stmt.where(col(RawQuery.month_year).in_(month_year))
    if is_curated is True:
        curated_ids = select(CuratedQuery.raw_query_id)
        stmt = stmt.where(col(RawQuery.id).in_(curated_ids))
    if is_curated is False:
        curated_ids = select(CuratedQuery.raw_query_id)
        stmt = stmt.where(col(RawQuery.id).not_in(curated_ids))
    if search:
        stmt = stmt.where(col(RawQuery.query_details).contains(search))
    stmt = apply_system_filter(stmt, system)
    return stmt


# ---------------------------------------------------------------------------
# GET /api/queries
# ---------------------------------------------------------------------------


@router.get("", response_model=list[RawQueryRead], summary="List raw query rows")
async def list_queries(
    # Filters
    environment: EnvironmentType | None = None,
    type: QueryType | None = None,
    source: SourceType | None = None,
    host: str | None = Query(default=None),
    db_name: str | None = Query(default=None),
    month_year: list[str] | None = Query(default=None),
    is_curated: bool | None = None,
    search: str | None = Query(default=None, description="Substring search in query_details"),
    # Sorting
    sort_by: str = Query(default="id", pattern="^(id|last_seen|occurrence_count)$"),
    sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    # Pagination
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=_PAGE_MAX),
    # Response
    response: Response = None,
    session: AsyncSession = Depends(get_session),
) -> list[RawQueryRead]:
    filter_kwargs = dict(
        environment=environment,
        type=type,
        source=source,
        host=host,
        db_name=db_name,
        month_year=month_year,
        is_curated=is_curated,
        search=search,
    )

    # Total count (for X-Total-Count header)
    count_stmt = _apply_filters(select(func.count(RawQuery.id)), **filter_kwargs)
    total = (await session.exec(count_stmt)).one()
    if response is not None:
        response.headers["X-Total-Count"] = str(total)
        response.headers["Access-Control-Expose-Headers"] = "X-Total-Count"

    # Paginated data
    _SORT_COLS = {
        "id": RawQuery.id,
        "last_seen": RawQuery.last_seen,
        "occurrence_count": RawQuery.occurrence_count,
    }
    _col_expr = col(_SORT_COLS.get(sort_by, RawQuery.id))
    stmt = _apply_filters(select(RawQuery), **filter_kwargs)
    stmt = (
        stmt.order_by(_col_expr.desc() if sort_dir == "desc" else _col_expr.asc())
        .offset(offset)
        .limit(limit)
    )
    rows = list((await session.exec(stmt)).all())

    if not rows:
        return []

    # Batch-fetch curated entries to inject curated_id
    raw_ids = [r.id for r in rows]
    curated_rows = (
        await session.exec(select(CuratedQuery).where(col(CuratedQuery.raw_query_id).in_(raw_ids)))
    ).all()
    curated_map = {cq.raw_query_id: cq.id for cq in curated_rows}

    result = []
    for rq in rows:
        read = RawQueryRead.model_validate(rq)
        read.curated_id = curated_map.get(rq.id)
        result.append(read)
    return result


# ---------------------------------------------------------------------------
# GET /api/queries/distinct  -- dropdown option lists for Host & Database
# ---------------------------------------------------------------------------


@router.get("/distinct", summary="Distinct host and db_name values")
async def distinct_values(
    session: AsyncSession = Depends(get_session),
) -> dict:
    hosts = (
        await session.exec(
            select(RawQuery.host)
            .distinct()
            .where(col(RawQuery.host).isnot(None))
            .order_by(RawQuery.host)
        )
    ).all()
    db_names = (
        await session.exec(
            select(RawQuery.db_name)
            .distinct()
            .where(col(RawQuery.db_name).isnot(None))
            .order_by(RawQuery.db_name)
        )
    ).all()
    return {"hosts": list(hosts), "db_names": list(db_names)}


# ---------------------------------------------------------------------------
# GET /api/queries/count  -- dedicated count endpoint
# ---------------------------------------------------------------------------


@router.get("/count", summary="Count matching raw query rows")
async def count_queries(
    environment: EnvironmentType | None = None,
    type: QueryType | None = None,
    source: SourceType | None = None,
    host: str | None = Query(default=None),
    db_name: str | None = Query(default=None),
    month_year: list[str] | None = Query(default=None),
    is_curated: bool | None = None,
    search: str | None = Query(default=None),
    system: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> dict:
    stmt = _apply_filters(
        select(func.count(RawQuery.id)),
        environment=environment,
        type=type,
        source=source,
        host=host,
        db_name=db_name,
        month_year=month_year,
        is_curated=is_curated,
        search=search,
        system=system,
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
) -> RawQueryRead:
    rq = await session.get(RawQuery, query_id)
    if not rq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Query not found")

    read = RawQueryRead.model_validate(rq)
    # Inject curated_id
    cq = (
        await session.exec(select(CuratedQuery).where(CuratedQuery.raw_query_id == query_id))
    ).first()
    read.curated_id = cq.id if cq else None
    return read
