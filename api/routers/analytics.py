"""
Analytics endpoints — all queries run against the full RawQuery dataset.

GET /api/analytics/summary          — counts by environment × type
GET /api/analytics/by-host          — top N hosts by occurrence_count sum
GET /api/analytics/by-month         — rows per month_year (trend line)
GET /api/analytics/by-db            — top databases by occurrence count
GET /api/analytics/pattern-coverage — % of raw rows with a linked pattern_id
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, text
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.models import EnvironmentType, QueryType, RawQuery, SourceType

router = APIRouter()


# ---------------------------------------------------------------------------
# GET /api/analytics/summary
# ---------------------------------------------------------------------------

@router.get("/summary", summary="Row counts grouped by environment and type")
async def analytics_summary(
    source:      Optional[SourceType]      = None,
    environment: Optional[EnvironmentType] = None,
    session: AsyncSession = Depends(get_session),
) -> list[dict]:
    stmt = (
        select(
            RawQuery.environment,
            RawQuery.type,
            RawQuery.source,
            func.count(RawQuery.id).label("row_count"),
            func.sum(RawQuery.occurrence_count).label("total_occurrences"),
        )
        .group_by(RawQuery.environment, RawQuery.type, RawQuery.source)
        .order_by(col(RawQuery.environment), col(RawQuery.type))
    )
    if source is not None:
        stmt = stmt.where(RawQuery.source == source)
    if environment is not None:
        stmt = stmt.where(RawQuery.environment == environment)

    rows = await session.exec(stmt)
    return [
        {
            "environment":       r.environment,
            "type":              r.type,
            "source":            r.source,
            "row_count":         r.row_count,
            "total_occurrences": r.total_occurrences,
        }
        for r in rows.all()
    ]


# ---------------------------------------------------------------------------
# GET /api/analytics/by-host
# ---------------------------------------------------------------------------

@router.get("/by-host", summary="Top hosts by total occurrence count")
async def analytics_by_host(
    top_n: int = Query(default=20, ge=1, le=100),
    environment: Optional[EnvironmentType] = None,
    source:      Optional[SourceType]      = None,
    session: AsyncSession = Depends(get_session),
) -> list[dict]:
    stmt = (
        select(
            RawQuery.host,
            RawQuery.environment,
            func.count(RawQuery.id).label("row_count"),
            func.sum(RawQuery.occurrence_count).label("total_occurrences"),
        )
        .where(RawQuery.host.isnot(None))  # type: ignore[union-attr]
        .group_by(RawQuery.host, RawQuery.environment)
        .order_by(func.sum(RawQuery.occurrence_count).desc())
        .limit(top_n)
    )
    if environment is not None:
        stmt = stmt.where(RawQuery.environment == environment)
    if source is not None:
        stmt = stmt.where(RawQuery.source == source)

    rows = await session.exec(stmt)
    return [
        {
            "host":              r.host,
            "environment":       r.environment,
            "row_count":         r.row_count,
            "total_occurrences": r.total_occurrences,
        }
        for r in rows.all()
    ]


# ---------------------------------------------------------------------------
# GET /api/analytics/by-month
# ---------------------------------------------------------------------------

@router.get("/by-month", summary="Row count per month (trend line)")
async def analytics_by_month(
    environment: Optional[EnvironmentType] = None,
    source:      Optional[SourceType]      = None,
    session: AsyncSession = Depends(get_session),
) -> list[dict]:
    stmt = (
        select(
            RawQuery.month_year,
            func.count(RawQuery.id).label("row_count"),
            func.sum(RawQuery.occurrence_count).label("total_occurrences"),
        )
        .where(RawQuery.month_year.isnot(None))  # type: ignore[union-attr]
        .group_by(RawQuery.month_year)
        .order_by(col(RawQuery.month_year))
    )
    if environment is not None:
        stmt = stmt.where(RawQuery.environment == environment)
    if source is not None:
        stmt = stmt.where(RawQuery.source == source)

    rows = await session.exec(stmt)
    return [
        {
            "month_year":        r.month_year,
            "row_count":         r.row_count,
            "total_occurrences": r.total_occurrences,
        }
        for r in rows.all()
    ]


# ---------------------------------------------------------------------------
# GET /api/analytics/by-db
# ---------------------------------------------------------------------------

@router.get("/by-db", summary="Top databases by occurrence count")
async def analytics_by_db(
    top_n: int = Query(default=20, ge=1, le=100),
    environment: Optional[EnvironmentType] = None,
    source:      Optional[SourceType]      = None,
    session: AsyncSession = Depends(get_session),
) -> list[dict]:
    stmt = (
        select(
            RawQuery.db_name,
            RawQuery.source,
            func.count(RawQuery.id).label("row_count"),
            func.sum(RawQuery.occurrence_count).label("total_occurrences"),
        )
        .where(RawQuery.db_name.isnot(None))  # type: ignore[union-attr]
        .where(RawQuery.db_name != "")
        .group_by(RawQuery.db_name, RawQuery.source)
        .order_by(func.sum(RawQuery.occurrence_count).desc())
        .limit(top_n)
    )
    if environment is not None:
        stmt = stmt.where(RawQuery.environment == environment)
    if source is not None:
        stmt = stmt.where(RawQuery.source == source)

    rows = await session.exec(stmt)
    return [
        {
            "db_name":           r.db_name,
            "source":            r.source,
            "row_count":         r.row_count,
            "total_occurrences": r.total_occurrences,
        }
        for r in rows.all()
    ]


# ---------------------------------------------------------------------------
# GET /api/analytics/pattern-coverage
# ---------------------------------------------------------------------------

@router.get("/pattern-coverage", summary="Pattern tagging coverage statistics")
async def analytics_pattern_coverage(
    session: AsyncSession = Depends(get_session),
) -> dict:
    total_stmt   = select(func.count(RawQuery.id))
    tagged_stmt  = select(func.count(RawQuery.id)).where(RawQuery.pattern_id.isnot(None))  # type: ignore[union-attr]

    total_result  = await session.exec(total_stmt)
    tagged_result = await session.exec(tagged_stmt)

    total  = total_result.one()
    tagged = tagged_result.one()
    coverage = round(tagged / total, 4) if total else 0.0

    return {
        "total_rows":    total,
        "tagged_rows":   tagged,
        "untagged_rows": total - tagged,
        "coverage_pct":  round(coverage * 100, 2),
    }
