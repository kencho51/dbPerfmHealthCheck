"""
Analytics endpoints — all queries run against the full RawQuery dataset.

GET /api/analytics/summary          — counts by environment × type
GET /api/analytics/by-host          — top N hosts by occurrence_count sum
GET /api/analytics/by-month         — rows per month_year (trend line)
GET /api/analytics/by-db            — top databases by occurrence count
GET /api/analytics/curation-coverage — % of raw rows with a curated_query entry
GET /api/analytics/by-hour          — heatmap: row count by hour-of-day × weekday
GET /api/analytics/top-fingerprints — top N normalised query fingerprints by occurrence count
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, text
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.host_system import apply_system_filter
from api.models import CuratedQuery, EnvironmentType, QueryType, RawQuery, SourceType

router = APIRouter()


# ---------------------------------------------------------------------------
# GET /api/analytics/summary
# ---------------------------------------------------------------------------

@router.get("/summary", summary="Row counts grouped by environment and type")
async def analytics_summary(
    source:      Optional[SourceType]      = None,
    environment: Optional[EnvironmentType] = None,
    host:        Optional[str]             = None,
    db_name:     Optional[str]             = None,
    month_year:  Optional[str]             = None,
    system:      Optional[str]             = None,
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
    if host is not None:
        stmt = stmt.where(RawQuery.host == host)
    if db_name is not None:
        stmt = stmt.where(RawQuery.db_name == db_name)
    if month_year is not None:
        stmt = stmt.where(RawQuery.month_year == month_year)
    stmt = apply_system_filter(stmt, system)

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
    host:        Optional[str]             = None,
    db_name:     Optional[str]             = None,
    month_year:  Optional[str]             = None,
    system:      Optional[str]             = None,
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
    if host is not None:
        stmt = stmt.where(RawQuery.host == host)
    if db_name is not None:
        stmt = stmt.where(RawQuery.db_name == db_name)
    if month_year is not None:
        stmt = stmt.where(RawQuery.month_year == month_year)
    stmt = apply_system_filter(stmt, system)

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
    type:        Optional[QueryType]       = None,
    host:        Optional[str]             = None,
    db_name:     Optional[str]             = None,
    month_year:  Optional[str]             = None,
    system:      Optional[str]             = None,
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
    if type is not None:
        stmt = stmt.where(RawQuery.type == type)
    if host is not None:
        stmt = stmt.where(RawQuery.host == host)
    if db_name is not None:
        stmt = stmt.where(RawQuery.db_name == db_name)
    if month_year is not None:
        stmt = stmt.where(RawQuery.month_year == month_year)
    stmt = apply_system_filter(stmt, system)

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
    host:        Optional[str]             = None,
    db_name:     Optional[str]             = None,
    month_year:  Optional[str]             = None,
    system:      Optional[str]             = None,
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
    if host is not None:
        stmt = stmt.where(RawQuery.host == host)
    if db_name is not None:
        stmt = stmt.where(RawQuery.db_name == db_name)
    if month_year is not None:
        stmt = stmt.where(RawQuery.month_year == month_year)
    stmt = apply_system_filter(stmt, system)

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
# GET /api/analytics/curation-coverage
# ---------------------------------------------------------------------------

@router.get("/curation-coverage", summary="Curation coverage statistics")
async def analytics_curation_coverage(
    host:        Optional[str]             = None,
    db_name:     Optional[str]             = None,
    environment: Optional[EnvironmentType] = None,
    source:      Optional[SourceType]      = None,
    month_year:  Optional[str]             = None,
    system:      Optional[str]             = None,
    session: AsyncSession = Depends(get_session),
) -> dict:
    total_stmt = select(func.count(RawQuery.id))
    if host is not None:
        total_stmt = total_stmt.where(RawQuery.host == host)
    if db_name is not None:
        total_stmt = total_stmt.where(RawQuery.db_name == db_name)
    if environment is not None:
        total_stmt = total_stmt.where(RawQuery.environment == environment)
    if source is not None:
        total_stmt = total_stmt.where(RawQuery.source == source)
    if month_year is not None:
        total_stmt = total_stmt.where(RawQuery.month_year == month_year)
    total_stmt = apply_system_filter(total_stmt, system)

    curated_stmt = (
        select(func.count(CuratedQuery.id))
        .join(RawQuery, CuratedQuery.raw_query_id == RawQuery.id)  # type: ignore[arg-type]
    )
    if host is not None:
        curated_stmt = curated_stmt.where(RawQuery.host == host)
    if db_name is not None:
        curated_stmt = curated_stmt.where(RawQuery.db_name == db_name)
    if environment is not None:
        curated_stmt = curated_stmt.where(RawQuery.environment == environment)
    if source is not None:
        curated_stmt = curated_stmt.where(RawQuery.source == source)
    if month_year is not None:
        curated_stmt = curated_stmt.where(RawQuery.month_year == month_year)
    curated_stmt = apply_system_filter(curated_stmt, system)

    total_result   = await session.exec(total_stmt)
    curated_result = await session.exec(curated_stmt)

    total   = total_result.one()
    curated = curated_result.one()
    # Compute pct with 4 decimal places to preserve precision for small ratios
    # e.g. 1 curated out of 50 000 = 0.002% rather than 0.0%
    coverage_pct = round(curated / total * 100, 4) if total else 0.0

    return {
        "total_rows":    total,
        "curated_rows":  curated,
        "uncurated_rows": total - curated,
        "coverage_pct":  coverage_pct,
    }


# ---------------------------------------------------------------------------
# GET /api/analytics/by-hour
# ---------------------------------------------------------------------------

@router.get("/by-hour", summary="Query count heatmap: hour-of-day × day-of-week")
async def analytics_by_hour(
    environment: Optional[EnvironmentType] = None,
    source:      Optional[SourceType]      = None,
    host:        Optional[str]             = None,
    db_name:     Optional[str]             = None,
    month_year:  Optional[str]             = None,
    type:        Optional[QueryType]       = None,
    system:      Optional[str]             = None,
    session: AsyncSession = Depends(get_session),
) -> list[dict]:
    """
    Returns sparse list of cells keyed by {hour, weekday} with:
      count       — sum of occurrence_count for that cell
      by_type     — {slow_query: N, blocker: N, ...} breakdown
      top_hosts   — [{host, count}, ...] top 5 hosts in that cell
      top_dbs     — [{db_name, count}, ...] top 5 databases in that cell

    hour:    0–23
    weekday: 0=Monday … 6=Sunday  (ISO weekday − 1)

    Parsing is done in-process with Polars because raw_query.time stores
    heterogeneous Splunk formats (ISO+tz, US AM/PM, ISO without tz).
    Missing cells (zero events) are omitted — the frontend fills them with 0.
    """
    import polars as pl

    stmt = select(
        RawQuery.time,
        RawQuery.occurrence_count,
        RawQuery.type,
        RawQuery.host,
        RawQuery.db_name,
    ).where(
        RawQuery.time.isnot(None)  # type: ignore[union-attr]
    )
    if environment is not None:
        stmt = stmt.where(RawQuery.environment == environment)
    if source is not None:
        stmt = stmt.where(RawQuery.source == source)
    if host is not None:
        stmt = stmt.where(RawQuery.host == host)
    if db_name is not None:
        stmt = stmt.where(RawQuery.db_name == db_name)
    if month_year is not None:
        stmt = stmt.where(RawQuery.month_year == month_year)
    if type is not None:
        stmt = stmt.where(RawQuery.type == type)
    stmt = apply_system_filter(stmt, system)

    rows = (await session.exec(stmt)).all()
    if not rows:
        return []

    df = pl.DataFrame({
        "t":       [r.time for r in rows],
        "occ":     [r.occurrence_count for r in rows],
        "qtype":   [r.type or "unknown" for r in rows],
        "host":    [r.host or "" for r in rows],
        "db_name": [r.db_name or "" for r in rows],
    })

    # Strip trailing timezone offsets (+0800 / +08:00 / -0500) so we can use
    # non-TZ format strings while still keeping local hour-of-day information.
    df = df.with_columns(
        pl.col("t")
        .str.replace(r"[+-]\d{2}:?\d{2}$", "")
        .str.strip_chars()
        .alias("t_clean")
    )

    FORMATS = [
        "%Y-%m-%dT%H:%M:%S%.f",   # 2025-11-30T17:35:54.000  (after TZ strip)
        "%Y-%m-%dT%H:%M:%S",      # 2025-11-30T17:35:54
        "%m/%d/%Y %I:%M:%S %p",   # 1/26/2026 8:58:53 AM
        "%m/%d/%Y %H:%M:%S",      # 1/26/2026 17:35:54
        "%Y-%m-%d %H:%M:%S%.f",   # 2025-11-30 17:35:54.000
        "%Y-%m-%d %H:%M:%S",      # 2025-11-30 17:35:54
        "%Y/%m/%d %H:%M:%S",      # 2025/11/30 17:35:54
    ]

    df = df.with_columns(
        pl.coalesce(
            *[pl.col("t_clean").str.strptime(pl.Datetime, fmt, strict=False) for fmt in FORMATS]
        ).alias("dt")
    )

    # Keep only parseable rows and derive hour + weekday
    base = (
        df.filter(pl.col("dt").is_not_null())
        .with_columns(
            pl.col("dt").dt.hour().alias("hour"),
            (pl.col("dt").dt.weekday() - 1).alias("weekday"),
        )
    )

    # ---- Total count per cell ------------------------------------------------
    totals = (
        base.group_by(["hour", "weekday"])
        .agg(pl.col("occ").sum().alias("count"))
    )

    # ---- by_type breakdown per cell -----------------------------------------
    by_type_df = (
        base.group_by(["hour", "weekday", "qtype"])
        .agg(pl.col("occ").sum().alias("tc"))
    )
    type_lookup: dict[tuple, dict] = {}
    for row in by_type_df.iter_rows(named=True):
        key = (row["hour"], row["weekday"])
        type_lookup.setdefault(key, {})[row["qtype"]] = row["tc"]

    # ---- top_hosts per cell (top 5) -----------------------------------------
    by_host_df = (
        base.filter(pl.col("host") != "")
        .group_by(["hour", "weekday", "host"])
        .agg(pl.col("occ").sum().alias("hc"))
    )
    host_lookup: dict[tuple, list] = {}
    for row in by_host_df.iter_rows(named=True):
        key = (row["hour"], row["weekday"])
        host_lookup.setdefault(key, []).append({"host": row["host"], "count": row["hc"]})
    for key in host_lookup:
        host_lookup[key].sort(key=lambda x: -x["count"])
        host_lookup[key] = host_lookup[key][:5]

    # ---- top_dbs per cell (top 5) -------------------------------------------
    by_db_df = (
        base.filter(pl.col("db_name") != "")
        .group_by(["hour", "weekday", "db_name"])
        .agg(pl.col("occ").sum().alias("dc"))
    )
    db_lookup: dict[tuple, list] = {}
    for row in by_db_df.iter_rows(named=True):
        key = (row["hour"], row["weekday"])
        db_lookup.setdefault(key, []).append({"db_name": row["db_name"], "count": row["dc"]})
    for key in db_lookup:
        db_lookup[key].sort(key=lambda x: -x["count"])
        db_lookup[key] = db_lookup[key][:5]

    # ---- Assemble -----------------------------------------------------------
    result = []
    for row in totals.sort(["weekday", "hour"]).iter_rows(named=True):
        key = (row["hour"], row["weekday"])
        result.append({
            "hour":      row["hour"],
            "weekday":   row["weekday"],
            "count":     row["count"],
            "by_type":   type_lookup.get(key, {}),
            "top_hosts": host_lookup.get(key, []),
            "top_dbs":   db_lookup.get(key, []),
        })
    return result


# ---------------------------------------------------------------------------
# GET /api/analytics/top-fingerprints
# ---------------------------------------------------------------------------

@router.get("/top-fingerprints", summary="Top normalised query fingerprints by occurrence count")
async def analytics_top_fingerprints(
    top_n:       int                    = Query(default=20, ge=1, le=200),
    environment: Optional[EnvironmentType] = None,
    source:      Optional[SourceType]      = None,
    host:        Optional[str]             = None,
    db_name:     Optional[str]             = None,
    month_year:  Optional[str]             = None,
    type:        Optional[QueryType]       = None,
    system:      Optional[str]             = None,
    session: AsyncSession = Depends(get_session),
) -> list[dict]:
    """
    Normalises each ``query_details`` value to a stable fingerprint by:
      1. Lowercasing
      2. Collapsing single-quoted string literals → ``'?'``
      3. Collapsing standalone numeric literals → ``?``
      4. Collapsing whitespace runs to a single space
      5. Truncating to 300 characters

    Returns the top ``top_n`` fingerprints by total ``occurrence_count`` sum,
    with a per-type breakdown and the most-common host / database for each.
    """
    import polars as pl

    stmt = select(
        RawQuery.query_details,
        RawQuery.type,
        RawQuery.host,
        RawQuery.db_name,
        RawQuery.occurrence_count,
    ).where(
        RawQuery.query_details.isnot(None)  # type: ignore[union-attr]
    )
    if environment is not None:
        stmt = stmt.where(RawQuery.environment == environment)
    if source is not None:
        stmt = stmt.where(RawQuery.source == source)
    if host is not None:
        stmt = stmt.where(RawQuery.host == host)
    if db_name is not None:
        stmt = stmt.where(RawQuery.db_name == db_name)
    if month_year is not None:
        stmt = stmt.where(RawQuery.month_year == month_year)
    if type is not None:
        stmt = stmt.where(RawQuery.type == type)
    stmt = apply_system_filter(stmt, system)

    rows = (await session.exec(stmt)).all()
    if not rows:
        return []

    df = pl.DataFrame({
        "q":     [r.query_details or "" for r in rows],
        "qtype": [r.type or "unknown"   for r in rows],
        "host":  [r.host or ""          for r in rows],
        "db":    [r.db_name or ""       for r in rows],
        "occ":   [r.occurrence_count    for r in rows],
    })

    # Vectorised fingerprinting via Polars regex
    df = df.with_columns(
        pl.col("q")
        .str.to_lowercase()
        .str.replace_all(r"'[^']*'", "'?'")
        .str.replace_all(r"\b\d+\.?\d*\b", "?")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.slice(0, 300)
        .alias("fp")
    )

    # Total occurrences per fingerprint → take top N
    totals = (
        df.group_by("fp")
        .agg(
            pl.col("occ").sum().alias("count"),
            pl.len().alias("row_count"),
        )
        .sort("count", descending=True)
        .head(top_n)
    )

    top_fps = set(totals["fp"].to_list())
    top_df  = df.filter(pl.col("fp").is_in(top_fps))

    # per-type breakdown
    by_type_df = (
        top_df.group_by(["fp", "qtype"])
        .agg(pl.col("occ").sum().alias("tc"))
    )
    type_lookup: dict[str, dict] = {}
    for row in by_type_df.iter_rows(named=True):
        type_lookup.setdefault(row["fp"], {})[row["qtype"]] = row["tc"]

    # most-common host / db per fingerprint (Python-level: simpler than nested Polars sort+group)
    host_agg: dict[str, dict[str, int]] = {}
    db_agg:   dict[str, dict[str, int]] = {}
    for row in top_df.iter_rows(named=True):
        fp, h, d, occ = row["fp"], row["host"], row["db"], row["occ"]
        if h:
            host_agg.setdefault(fp, {})
            host_agg[fp][h] = host_agg[fp].get(h, 0) + occ
        if d:
            db_agg.setdefault(fp, {})
            db_agg[fp][d] = db_agg[fp].get(d, 0) + occ

    host_lookup = {
        fp: max(hosts.keys(), key=lambda k: hosts[k])
        for fp, hosts in host_agg.items()
    }
    db_lookup = {
        fp: max(dbs.keys(), key=lambda k: dbs[k])
        for fp, dbs in db_agg.items()
    }

    result = []
    for row in totals.iter_rows(named=True):
        fp = row["fp"]
        result.append({
            "fingerprint":  fp,
            "count":        row["count"],
            "row_count":    row["row_count"],
            "by_type":      type_lookup.get(fp, {}),
            "example_host": host_lookup.get(fp, ""),
            "example_db":   db_lookup.get(fp, ""),
        })
    return result
