"""
Query endpoints:
    GET  /api/queries             -- paginated, filterable list of raw query rows
    GET  /api/queries/count       -- total matching row count (for pagination)
    GET  /api/queries/distinct    -- distinct host and db_name values
    GET  /api/queries/{id}        -- single row by primary key
"""

from __future__ import annotations

import hashlib

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
    RawQueryBlocker,
    RawQueryBlockerRead,
    RawQueryDeadlock,
    RawQueryDeadlockRead,
    RawQueryRead,
    RawQuerySlowMongo,
    RawQuerySlowMongoRead,
    RawQuerySlowSql,
    RawQuerySlowSqlRead,
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
# GET /api/queries/{id}/typed-detail
# ---------------------------------------------------------------------------

_TYPED_MODEL_MAP = {
    "slow_query":       (RawQuerySlowSql,   RawQuerySlowSqlRead),
    "blocker":          (RawQueryBlocker,   RawQueryBlockerRead),
    "deadlock":         (RawQueryDeadlock,  RawQueryDeadlockRead),
    "slow_query_mongo": (RawQuerySlowMongo, RawQuerySlowMongoRead),
}


@router.get("/{query_id}/typed-detail", summary="Get type-specific rich detail for a raw_query row")
async def get_typed_detail(
    query_id: int,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Returns { "type": str, "data": <typed-row dict> | null }.

    Looks up %raw_query to determine the type, then queries the corresponding
    typed table (raw_query_slow_sql / raw_query_blocker / raw_query_deadlock /
    raw_query_slow_mongo) via the raw_query_id FK.
    Returns data=null when no typed row has been linked yet.
    """
    rq = await session.get(RawQuery, query_id)
    if not rq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Query not found")

    # rq.type is stored as a plain str in SQLite — normalise before lookup
    rq_type = rq.type if isinstance(rq.type, str) else rq.type.value

    type_pair = _TYPED_MODEL_MAP.get(rq_type)
    if type_pair is None:
        return {"type": rq_type, "data": None}

    model_cls, read_cls = type_pair

    # Map each type to the typed-table column that mirrors raw_query.query_details
    _QUERY_TEXT_COL: dict[str, str] = {
        "slow_query":       "query_final",
        "blocker":          "all_query",
        "deadlock":         "sql_text",
        "slow_query_mongo": "command_json",
    }
    text_col = _QUERY_TEXT_COL.get(rq_type)

    # 1) Fast path — FK is already set
    typed_row = (
        await session.exec(
            select(model_cls).where(model_cls.raw_query_id == query_id)
        )
    ).first()

    # 2) Text fallback — works for OLD-format raw_query rows where
    #    query_details was stored as the full command JSON (before the
    #    extractor was optimised to store queryShapeHash / ns:op_type).
    if not typed_row and text_col and rq.query_details:
        text_col_attr = getattr(model_cls, text_col)
        typed_row = (
            await session.exec(
                select(model_cls).where(text_col_attr == rq.query_details)
            )
        ).first()
        # Backfill the FK so future lookups hit the fast path
        if typed_row and typed_row.raw_query_id is None:
            typed_row.raw_query_id = query_id
            session.add(typed_row)
            await session.commit()
            await session.refresh(typed_row)

    # 3) Hash-reconstruction fallback — works for NEW-format slow_mongo rows
    #    where query_details = queryShapeHash or "ns:op_type".
    #    Both extractors derive the same query_key, and typed_ingestor computes:
    #      query_hash = MD5(host | db_name | env | query_key)
    #    Since query_key == query_details for new-format raw_query rows, we can
    #    reconstruct the typed query_hash and look it up directly.
    if not typed_row and rq_type == "slow_query_mongo" and rq.query_details:
        candidate_hash = hashlib.md5(
            "|".join(
                str(p or "").strip()
                for p in [rq.host, rq.db_name, rq.environment, rq.query_details]
            ).encode("utf-8")
        ).hexdigest()
        typed_row = (
            await session.exec(
                select(model_cls).where(model_cls.query_hash == candidate_hash)
            )
        ).first()
        if typed_row and typed_row.raw_query_id is None:
            typed_row.raw_query_id = query_id
            session.add(typed_row)
            await session.commit()
            await session.refresh(typed_row)

    # 4) Collection-fuzzy fallback — for OLD-format slow_mongo rows where
    #    query_details is the full command JSON (e.g. {"find":"reportDocument",...}).
    #    Each execution has a unique $oid / filter so text & hash both miss.
    #    The typed table has a representative row for the same collection+host
    #    with real metrics (duration_ms, plan_summary etc.) — use that.
    if not typed_row and rq_type == "slow_query_mongo" and rq.query_details:
        import json as _json
        collection: str | None = None
        try:
            cmd = _json.loads(rq.query_details)
            # First key is the command op (find/aggregate/update…), value is the collection
            first_val = next(iter(cmd.values()), None)
            if isinstance(first_val, str):
                collection = first_val
        except Exception:
            pass
        if collection:
            stmt = (
                select(model_cls)
                .where(model_cls.collection == collection)
                .where(model_cls.environment == rq.environment)
            )
            if rq.host:
                stmt = stmt.where(model_cls.host == rq.host)
            if rq.db_name:
                stmt = stmt.where(model_cls.db_name == rq.db_name)
            # Prefer the row with highest occurrence_count (most representative)
            stmt = stmt.order_by(model_cls.occurrence_count.desc())  # type: ignore[attr-defined]
            typed_row = (await session.exec(stmt)).first()

    if not typed_row:
        return {"type": rq_type, "data": None}

    return {
        "type": rq_type,
        "data": read_cls.model_validate(typed_row).model_dump(mode="json"),
    }


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
