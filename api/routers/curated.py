"""
Curated query endpoints  manage curation assignments.

    GET    /api/curated           paginated list with filters
    GET    /api/curated/count     total count for pagination
    POST   /api/curated           assign a raw_query to curation
    PATCH  /api/curated/{id}      update label / notes
    DELETE /api/curated/{id}      unassign (deletes curated row; raw_query untouched)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlmodel import col, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.models import (
    CuratedQuery,
    CuratedQueryCreate,
    CuratedQueryRead,
    CuratedQueryUpdate,
    EnvironmentType,
    PatternLabel,
    PatternLabelRead,
    QueryType,
    RawQuery,
    SourceType,
)

router = APIRouter()

_PAGE_MAX = 200


# ---------------------------------------------------------------------------
# Internal helper: build a CuratedQueryRead from ORM rows
# ---------------------------------------------------------------------------

def _to_read(cq: CuratedQuery, rq: RawQuery, label: Optional[PatternLabel]) -> CuratedQueryRead:
    return CuratedQueryRead(
        # curated fields
        id=cq.id,
        raw_query_id=cq.raw_query_id,
        label_id=cq.label_id,
        label=PatternLabelRead.model_validate(label) if label else None,
        notes=cq.notes,
        created_at=cq.created_at,
        updated_at=cq.updated_at,
        # raw_query fields
        query_hash=rq.query_hash,
        time=rq.time,
        source=rq.source,
        host=rq.host,
        db_name=rq.db_name,
        environment=rq.environment,
        type=rq.type,
        query_details=rq.query_details,
        month_year=rq.month_year,
        occurrence_count=rq.occurrence_count,
        first_seen=rq.first_seen,
        last_seen=rq.last_seen,
    )


# ---------------------------------------------------------------------------
# Shared filter builder
# ---------------------------------------------------------------------------

def _apply_filters(
    stmt,
    *,
    environment: Optional[EnvironmentType],
    type: Optional[QueryType],
    source: Optional[SourceType],
    host: Optional[str],
    db_name: Optional[str],
    month_year: Optional[list[str]],
    label_id: Optional[int],
    search: Optional[str],
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
    if label_id is not None:
        stmt = stmt.where(CuratedQuery.label_id == label_id)
    if search:
        stmt = stmt.where(col(RawQuery.query_details).contains(search))
    return stmt


# ---------------------------------------------------------------------------
# GET /api/curated
# ---------------------------------------------------------------------------

@router.get("", response_model=list[CuratedQueryRead], summary="List curated queries")
async def list_curated(
    environment: Optional[EnvironmentType] = None,
    type: Optional[QueryType] = None,
    source: Optional[SourceType] = None,
    host: Optional[str] = Query(default=None),
    db_name: Optional[str] = Query(default=None),
    month_year: Optional[list[str]] = Query(default=None),
    label_id: Optional[int] = None,
    search: Optional[str] = Query(default=None),
    sort_by: str = Query(default="id", pattern="^(id|last_seen|occurrence_count)$"),
    sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=_PAGE_MAX),
    response: Response = None,
    session: AsyncSession = Depends(get_session),
) -> list[CuratedQueryRead]:
    filter_kwargs = dict(
        environment=environment, type=type, source=source, host=host,
        db_name=db_name, month_year=month_year, label_id=label_id, search=search,
    )

    base_stmt = select(CuratedQuery, RawQuery, PatternLabel).join(
        RawQuery, CuratedQuery.raw_query_id == RawQuery.id
    ).outerjoin(PatternLabel, CuratedQuery.label_id == PatternLabel.id)

    # Count
    count_stmt = _apply_filters(
        select(func.count(CuratedQuery.id)).join(
            RawQuery, CuratedQuery.raw_query_id == RawQuery.id
        ).outerjoin(PatternLabel, CuratedQuery.label_id == PatternLabel.id),
        **filter_kwargs,
    )
    total = (await session.exec(count_stmt)).one()
    if response is not None:
        response.headers["X-Total-Count"] = str(total)
        response.headers["Access-Control-Expose-Headers"] = "X-Total-Count"

    # Sort
    _SORT = {
        "id": CuratedQuery.id,
        "last_seen": RawQuery.last_seen,
        "occurrence_count": RawQuery.occurrence_count,
    }
    sort_col = col(_SORT.get(sort_by, CuratedQuery.id))
    stmt = _apply_filters(base_stmt, **filter_kwargs)
    stmt = stmt.order_by(sort_col.desc() if sort_dir == "desc" else sort_col.asc()).offset(offset).limit(limit)

    rows = (await session.exec(stmt)).all()
    return [_to_read(cq, rq, lbl) for cq, rq, lbl in rows]


# ---------------------------------------------------------------------------
# GET /api/curated/count
# ---------------------------------------------------------------------------

@router.get("/count", summary="Count curated queries")
async def count_curated(
    environment: Optional[EnvironmentType] = None,
    type: Optional[QueryType] = None,
    source: Optional[SourceType] = None,
    host: Optional[str] = Query(default=None),
    db_name: Optional[str] = Query(default=None),
    month_year: Optional[list[str]] = Query(default=None),
    label_id: Optional[int] = None,
    search: Optional[str] = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> dict:
    stmt = _apply_filters(
        select(func.count(CuratedQuery.id)).join(
            RawQuery, CuratedQuery.raw_query_id == RawQuery.id
        ).outerjoin(PatternLabel, CuratedQuery.label_id == PatternLabel.id),
        environment=environment, type=type, source=source, host=host,
        db_name=db_name, month_year=month_year, label_id=label_id, search=search,
    )
    total = (await session.exec(stmt)).one()
    return {"count": total}


# ---------------------------------------------------------------------------
# POST /api/curated   assign a raw_query
# ---------------------------------------------------------------------------

@router.post("", response_model=CuratedQueryRead, status_code=status.HTTP_201_CREATED, summary="Assign a raw query to curation")
async def create_curated(
    body: CuratedQueryCreate,
    session: AsyncSession = Depends(get_session),
) -> CuratedQueryRead:
    # Validate raw_query exists
    rq = await session.get(RawQuery, body.raw_query_id)
    if not rq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RawQuery not found")

    # Check not already curated
    existing = (await session.exec(
        select(CuratedQuery).where(CuratedQuery.raw_query_id == body.raw_query_id)
    )).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Raw query is already curated")

    # Validate label if provided
    label: Optional[PatternLabel] = None
    if body.label_id is not None:
        label = await session.get(PatternLabel, body.label_id)
        if not label:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Label not found")

    cq = CuratedQuery(**body.model_dump())
    session.add(cq)
    await session.commit()
    await session.refresh(cq)
    return _to_read(cq, rq, label)


# ---------------------------------------------------------------------------
# PATCH /api/curated/{id}   update label / notes
# ---------------------------------------------------------------------------

@router.patch("/{curated_id}", response_model=CuratedQueryRead, summary="Update curated query label or notes")
async def update_curated(
    curated_id: int,
    body: CuratedQueryUpdate,
    session: AsyncSession = Depends(get_session),
) -> CuratedQueryRead:
    cq = await session.get(CuratedQuery, curated_id)
    if not cq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Curated entry not found")

    data = body.model_dump(exclude_unset=True)
    label: Optional[PatternLabel] = None

    if "label_id" in data:
        if data["label_id"] is not None:
            label = await session.get(PatternLabel, data["label_id"])
            if not label:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Label not found")
        cq.label_id = data["label_id"]
    else:
        # Keep existing label
        if cq.label_id is not None:
            label = await session.get(PatternLabel, cq.label_id)

    if "notes" in data:
        cq.notes = data["notes"]

    cq.updated_at = datetime.now(tz=timezone.utc)
    session.add(cq)
    await session.commit()
    await session.refresh(cq)

    rq = await session.get(RawQuery, cq.raw_query_id)
    return _to_read(cq, rq, label)


# ---------------------------------------------------------------------------
# GET /api/curated/{id}  — fetch a single curated entry by its id
# ---------------------------------------------------------------------------

@router.get("/{curated_id}", response_model=CuratedQueryRead, summary="Get a single curated entry")
async def get_curated(
    curated_id: int,
    session: AsyncSession = Depends(get_session),
) -> CuratedQueryRead:
    cq = await session.get(CuratedQuery, curated_id)
    if not cq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Curated entry not found")
    rq = await session.get(RawQuery, cq.raw_query_id)
    label = await session.get(PatternLabel, cq.label_id) if cq.label_id else None
    return _to_read(cq, rq, label)


# ---------------------------------------------------------------------------
# DELETE /api/curated/{id}   unassign (raw_query row preserved)
# ---------------------------------------------------------------------------

@router.delete("/{curated_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Unassign a curated query")
async def delete_curated(
    curated_id: int,
    session: AsyncSession = Depends(get_session),
) -> None:
    cq = await session.get(CuratedQuery, curated_id)
    if not cq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Curated entry not found")
    await session.delete(cq)
    await session.commit()
