"""
Pattern endpoints:
    GET  /api/patterns              — list patterns (filterable)
    POST /api/patterns              — create a new pattern
    GET  /api/patterns/{id}         — fetch a single pattern
    PATCH /api/patterns/{id}        — update name / description / severity / notes
    GET  /api/patterns/{id}/queries — list raw queries linked to this pattern
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
    Pattern,
    PatternCreate,
    PatternRead,
    PatternUpdate,
    RawQuery,
    RawQueryRead,
    SeverityType,
    SourceType,
)

router = APIRouter()

_PAGE_MAX = 200


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# GET /api/patterns
# ---------------------------------------------------------------------------

@router.get("", response_model=list[PatternRead], summary="List curated patterns")
async def list_patterns(
    severity:    Optional[SeverityType]    = None,
    pattern_tag: Optional[str]             = Query(default=None),
    environment: Optional[EnvironmentType] = None,
    source:      Optional[SourceType]      = None,
    search:      Optional[str]             = Query(default=None, description="Substring search in name / description"),
    offset: int = Query(default=0, ge=0),
    limit:  int = Query(default=50, ge=1, le=_PAGE_MAX),
    session: AsyncSession = Depends(get_session),
) -> list[Pattern]:
    stmt = select(Pattern)

    if severity is not None:
        stmt = stmt.where(Pattern.severity == severity)
    if pattern_tag:
        stmt = stmt.where(col(Pattern.pattern_tag).contains(pattern_tag))
    if environment is not None:
        stmt = stmt.where(Pattern.environment == environment)
    if source is not None:
        stmt = stmt.where(Pattern.source == source)
    if search:
        stmt = stmt.where(
            col(Pattern.name).contains(search) | col(Pattern.description).contains(search)
        )

    stmt = stmt.order_by(col(Pattern.updated_at).desc()).offset(offset).limit(limit)
    rows = await session.exec(stmt)
    return list(rows.all())


# ---------------------------------------------------------------------------
# GET /api/patterns/{id}
# ---------------------------------------------------------------------------

@router.get("/{pattern_id}", response_model=PatternRead, summary="Get a single pattern")
async def get_pattern(
    pattern_id: int,
    session: AsyncSession = Depends(get_session),
) -> Pattern:
    p = await session.get(Pattern, pattern_id)
    if not p:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pattern not found")
    return p


# ---------------------------------------------------------------------------
# POST /api/patterns
# ---------------------------------------------------------------------------

@router.post("", response_model=PatternRead, status_code=status.HTTP_201_CREATED,
             summary="Create a new pattern")
async def create_pattern(
    body: PatternCreate,
    session: AsyncSession = Depends(get_session),
) -> Pattern:
    now = _now()

    # If an example_query_hash is provided, inherit context from that raw row
    source      = None
    environment = None
    q_type      = None
    first_seen  = None
    last_seen   = None

    if body.example_query_hash:
        from sqlmodel import select as sel
        raw_stmt = sel(RawQuery).where(RawQuery.query_hash == body.example_query_hash)
        raw_row  = (await session.exec(raw_stmt)).first()
        if raw_row:
            source      = raw_row.source
            environment = raw_row.environment
            q_type      = raw_row.type
            first_seen  = raw_row.first_seen
            last_seen   = raw_row.last_seen

    pattern = Pattern(
        name=body.name,
        description=body.description,
        pattern_tag=body.pattern_tag,
        severity=body.severity,
        example_query_hash=body.example_query_hash,
        notes=body.notes,
        source=source,
        environment=environment,
        type=q_type,
        first_seen=first_seen,
        last_seen=last_seen,
        total_occurrences=0,
        created_at=now,
        updated_at=now,
    )
    session.add(pattern)
    await session.commit()
    await session.refresh(pattern)
    return pattern


# ---------------------------------------------------------------------------
# PATCH /api/patterns/{id}
# ---------------------------------------------------------------------------

@router.patch("/{pattern_id}", response_model=PatternRead, summary="Update a pattern")
async def update_pattern(
    pattern_id: int,
    body: PatternUpdate,
    session: AsyncSession = Depends(get_session),
) -> Pattern:
    p = await session.get(Pattern, pattern_id)
    if not p:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pattern not found")

    update_data = body.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(p, key, value)
    p.updated_at = _now()

    session.add(p)
    await session.commit()
    await session.refresh(p)
    return p


# ---------------------------------------------------------------------------
# GET /api/patterns/{id}/queries
# ---------------------------------------------------------------------------

@router.get(
    "/{pattern_id}/queries",
    response_model=list[RawQueryRead],
    summary="List raw queries linked to a pattern",
)
async def list_pattern_queries(
    pattern_id: int,
    offset: int = Query(default=0, ge=0),
    limit:  int = Query(default=50, ge=1, le=_PAGE_MAX),
    session: AsyncSession = Depends(get_session),
) -> list[RawQuery]:
    # Verify pattern exists
    p = await session.get(Pattern, pattern_id)
    if not p:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pattern not found")

    stmt = (
        select(RawQuery)
        .where(RawQuery.pattern_id == pattern_id)
        .order_by(col(RawQuery.last_seen).desc())
        .offset(offset)
        .limit(limit)
    )
    rows = await session.exec(stmt)
    return list(rows.all())
