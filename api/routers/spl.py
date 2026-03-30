"""
SPL Library endpoints — store, edit, and list Splunk Processing Language queries.

    GET    /api/spl               list all SPL entries (optional ?query_type= filter)
    GET    /api/spl/types         distinct query_type values (for combobox)
    POST   /api/spl               create
    PUT    /api/spl/{id}          full update
    DELETE /api/spl/{id}          delete
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.models import (
    SplQuery,
    SplQueryCreate,
    SplQueryRead,
    SplQueryUpdate,
)

router = APIRouter()

# Default query types that are always present in the dropdown even before
# the user creates any entries (seeded data is optional; these are the
# well-known types that correspond to the existing CSV export SPLs).
_DEFAULT_TYPES = ["slow_query", "slow_query_mongo", "blocker", "deadlock"]


# ---------------------------------------------------------------------------
# GET /api/spl
# ---------------------------------------------------------------------------


@router.get("", response_model=list[SplQueryRead], summary="List SPL queries")
async def list_spl(
    query_type: str | None = Query(default=None, description="Filter by query type"),
    session: AsyncSession = Depends(get_session),
) -> list[SplQuery]:
    stmt = select(SplQuery).order_by(SplQuery.query_type, SplQuery.name)
    if query_type:
        stmt = stmt.where(SplQuery.query_type == query_type)
    rows = await session.exec(stmt)
    return list(rows.all())


# ---------------------------------------------------------------------------
# GET /api/spl/types  — distinct types (for combobox)
# ---------------------------------------------------------------------------


@router.get("/types", response_model=list[str], summary="Distinct SPL query types")
async def list_spl_types(
    session: AsyncSession = Depends(get_session),
) -> list[str]:
    stmt = select(col(SplQuery.query_type)).distinct().order_by(col(SplQuery.query_type))
    rows = await session.exec(stmt)
    db_types = list(rows.all())
    # Merge DB types with defaults, preserving default order, then append extras
    merged: list[str] = []
    seen: set[str] = set()
    for t in _DEFAULT_TYPES + db_types:
        if t not in seen:
            merged.append(t)
            seen.add(t)
    return merged


# ---------------------------------------------------------------------------
# POST /api/spl
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=SplQueryRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create an SPL query",
)
async def create_spl(
    body: SplQueryCreate,
    session: AsyncSession = Depends(get_session),
) -> SplQuery:
    entry = SplQuery(**body.model_dump())
    session.add(entry)
    await session.commit()
    await session.refresh(entry)
    return entry


# ---------------------------------------------------------------------------
# PUT /api/spl/{id}
# ---------------------------------------------------------------------------


@router.put("/{spl_id}", response_model=SplQueryRead, summary="Update an SPL query")
async def update_spl(
    spl_id: int,
    body: SplQueryUpdate,
    session: AsyncSession = Depends(get_session),
) -> SplQuery:
    entry = await session.get(SplQuery, spl_id)
    if not entry:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="SPL query not found")

    data = body.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(entry, k, v)
    entry.updated_at = datetime.now(tz=UTC)

    session.add(entry)
    await session.commit()
    await session.refresh(entry)
    return entry


# ---------------------------------------------------------------------------
# DELETE /api/spl/{id}
# ---------------------------------------------------------------------------


@router.delete("/{spl_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete an SPL query")
async def delete_spl(
    spl_id: int,
    session: AsyncSession = Depends(get_session),
) -> None:
    entry = await session.get(SplQuery, spl_id)
    if not entry:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="SPL query not found")
    await session.delete(entry)
    await session.commit()
