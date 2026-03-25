"""
Label endpoints  full CRUD for PatternLabel.

    GET    /api/labels         list all labels (sorted by name)
    POST   /api/labels         create a new label
    PATCH  /api/labels/{id}    update name / severity / description
    DELETE /api/labels/{id}    delete (rejected if any curated_query rows reference it)
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.models import (
    CuratedQuery,
    PatternLabel,
    PatternLabelCreate,
    PatternLabelRead,
    PatternLabelUpdate,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# GET /api/labels
# ---------------------------------------------------------------------------

@router.get("", response_model=list[PatternLabelRead], summary="List all labels")
async def list_labels(
    session: AsyncSession = Depends(get_session),
) -> list[PatternLabel]:
    rows = await session.exec(select(PatternLabel).order_by(PatternLabel.name))
    return list(rows.all())


# ---------------------------------------------------------------------------
# POST /api/labels
# ---------------------------------------------------------------------------

@router.post("", response_model=PatternLabelRead, status_code=status.HTTP_201_CREATED, summary="Create a new label")
async def create_label(
    body: PatternLabelCreate,
    session: AsyncSession = Depends(get_session),
) -> PatternLabel:
    label = PatternLabel(**body.model_dump())
    session.add(label)
    await session.commit()
    await session.refresh(label)
    return label


# ---------------------------------------------------------------------------
# PATCH /api/labels/{id}
# ---------------------------------------------------------------------------

@router.patch("/{label_id}", response_model=PatternLabelRead, summary="Update a label")
async def update_label(
    label_id: int,
    body: PatternLabelUpdate,
    session: AsyncSession = Depends(get_session),
) -> PatternLabel:
    label = await session.get(PatternLabel, label_id)
    if not label:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Label not found")

    data = body.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(label, k, v)
    label.updated_at = datetime.now(tz=timezone.utc)

    session.add(label)
    await session.commit()
    await session.refresh(label)
    return label


# ---------------------------------------------------------------------------
# DELETE /api/labels/{id}
# ---------------------------------------------------------------------------

@router.delete("/{label_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a label")
async def delete_label(
    label_id: int,
    session: AsyncSession = Depends(get_session),
) -> None:
    label = await session.get(PatternLabel, label_id)
    if not label:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Label not found")

    # Reject if any curated_query rows still reference this label
    ref_count_stmt = select(func.count(CuratedQuery.id)).where(CuratedQuery.label_id == label_id)
    ref_count = (await session.exec(ref_count_stmt)).one()
    if ref_count > 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot delete: {ref_count} curated query row(s) still reference this label. Re-assign or unassign them first.",
        )

    await session.delete(label)
    await session.commit()
