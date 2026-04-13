"""
Analytics endpoints — DuckDB OLAP over Neon PostgreSQL data.

Data flow: Neon PostgreSQL (HTTPS REST API) → Polars DataFrame → in-memory DuckDB.
DuckDB is used instead of direct SQL for analytics because:
  • Columnar-vectorized execution — GROUP BY aggregations over 10k–100k rows
    complete in milliseconds (vs row-by-row OLTP engines).
  • REGEXP_REPLACE in SQL — top-fingerprints fingerprinting happens inside DuckDB.
  • try_strptime([format_list]) — by-hour parses 7 datetime formats in one expression.
  • QUANTILE_CONT — P50/P95 host stats (Phase 8C).

Each endpoint follows the same pattern:
    1. _<name>_sync(**filters) → runs synchronous DuckDB SQL, returns list[dict]
    2. async def endpoint() → calls asyncio.to_thread(_<name>_sync, ...)

No SQLModel session is injected into any route in this module.

GET /api/analytics/summary          — counts by environment × type
GET /api/analytics/by-host          — top N hosts by occurrence_count sum
GET /api/analytics/by-month         — rows per month_year (trend line)
GET /api/analytics/by-db            — top databases by occurrence count
GET /api/analytics/curation-coverage — % of raw rows with a curated_query entry
GET /api/analytics/by-hour          — heatmap: row count by hour-of-day × weekday
GET /api/analytics/top-fingerprints — top N normalised query fingerprints
GET /api/analytics/host-stats       — P50/P95/P99 occurrence distribution per host (8C)
GET /api/analytics/co-occurrence    — hosts with both blocker + deadlock events (8D)
                                      by-month now includes row_delta / occ_delta via LAG (8E)
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Query

from api.analytics_db import get_duck
from api.models import EnvironmentType, QueryType, SourceType

router = APIRouter()


# ---------------------------------------------------------------------------
# Shared filter builder
# ---------------------------------------------------------------------------


def _build_filters(
    *,
    source: str | None = None,
    environment: str | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    type_: str | None = None,
    system: str | None = None,
    extra: list[str] | None = None,
) -> tuple[str, list[Any]]:
    """
    Return (WHERE clause string, positional params list) for DuckDB queries.

    Always produces ``WHERE 1=1 AND ...`` so the caller can safely append
    further ``AND`` expressions without worrying about the ``WHERE`` keyword.
    ``system`` resolves to ``upper(host) IN (...)`` via SYSTEM_HOSTS mapping.
    Extra raw SQL conditions (no params) can be added via ``extra``.
    """
    conditions: list[str] = ["1=1"]
    params: list[Any] = []

    for col_name, val in [
        ("source", source),
        ("environment", environment),
        ("host", host),
        ("db_name", db_name),
        ("month_year", month_year),
        ("type", type_),
    ]:
        if val is not None:
            conditions.append(f"{col_name} = ?")
            params.append(val)

    if system is not None:
        from api.host_system import SYSTEM_HOSTS

        hosts = SYSTEM_HOSTS.get(system.upper(), [])
        if hosts:
            ph = ", ".join("?" * len(hosts))
            conditions.append(f"upper(host) IN ({ph})")
            params.extend(h.upper() for h in hosts)
        else:
            conditions.append("1 = 0")  # unknown system → no rows

    for cond in extra or []:
        conditions.append(cond)

    return "WHERE " + " AND ".join(conditions), params


# DuckDB strptime format list — shared by by-hour and any future time-parsing queries.
# try_strptime(text, [...]) returns NULL on mismatch; no exception raised.
_STRPTIME_SQL = """[
    '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S',
    '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S',
    '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %H:%M:%S'
]"""


# ---------------------------------------------------------------------------
# GET /api/analytics/summary
# ---------------------------------------------------------------------------


def _summary_sync(
    source: str | None,
    environment: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
) -> list[dict]:
    where, params = _build_filters(
        source=source,
        environment=environment,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
    )
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            SELECT environment, type, source,
                   COUNT(*) AS row_count,
                   SUM(occurrence_count) AS total_occurrences
            FROM raw_query
            {where}
            GROUP BY environment, type, source
            ORDER BY environment, type
        """,
            params,
        ).fetchall()
        return [
            {
                "environment": r[0],
                "type": r[1],
                "source": r[2],
                "row_count": r[3],
                "total_occurrences": r[4],
            }
            for r in rows
        ]
    finally:
        con.close()


@router.get("/summary", summary="Row counts grouped by environment and type")
async def analytics_summary(
    source: SourceType | None = None,
    environment: EnvironmentType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    system: str | None = None,
) -> list[dict]:
    return await asyncio.to_thread(
        _summary_sync,
        source and source.value,
        environment and environment.value,
        host,
        db_name,
        month_year,
        system,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/by-host
# ---------------------------------------------------------------------------


def _by_host_sync(
    top_n: int,
    source: str | None,
    environment: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
) -> list[dict]:
    where, params = _build_filters(
        source=source,
        environment=environment,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
        extra=["host IS NOT NULL"],
    )
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            SELECT host, environment,
                   COUNT(*) AS row_count,
                   SUM(occurrence_count) AS total_occurrences
            FROM raw_query
            {where}
            GROUP BY host, environment
            ORDER BY SUM(occurrence_count) DESC
            LIMIT ?
        """,
            params + [top_n],
        ).fetchall()
        return [
            {"host": r[0], "environment": r[1], "row_count": r[2], "total_occurrences": r[3]}
            for r in rows
        ]
    finally:
        con.close()


@router.get("/by-host", summary="Top hosts by total occurrence count")
async def analytics_by_host(
    top_n: int = Query(default=20, ge=1, le=100),
    environment: EnvironmentType | None = None,
    source: SourceType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    system: str | None = None,
) -> list[dict]:
    return await asyncio.to_thread(
        _by_host_sync,
        top_n,
        source and source.value,
        environment and environment.value,
        host,
        db_name,
        month_year,
        system,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/by-month
# ---------------------------------------------------------------------------


def _by_month_sync(
    source: str | None,
    environment: str | None,
    type_: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
) -> list[dict]:
    where, params = _build_filters(
        source=source,
        environment=environment,
        type_=type_,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
        extra=["month_year IS NOT NULL"],
    )
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            SELECT month_year,
                   row_count,
                   total_occurrences,
                   row_count - LAG(row_count, 1) OVER (ORDER BY month_year)
                       AS row_delta,
                   total_occurrences - LAG(total_occurrences, 1) OVER (ORDER BY month_year)
                       AS occ_delta,
                   prod_count,
                   sat_count
            FROM (
                SELECT month_year,
                       COUNT(*) AS row_count,
                       SUM(occurrence_count) AS total_occurrences,
                       COUNT(*) FILTER (WHERE environment = 'prod') AS prod_count,
                       COUNT(*) FILTER (WHERE environment = 'sat') AS sat_count
                FROM raw_query
                {where}
                GROUP BY month_year
            ) t
            ORDER BY month_year
        """,
            params,
        ).fetchall()
        return [
            {
                "month_year": r[0],
                "row_count": r[1],
                "total_occurrences": r[2],
                "row_delta": r[3],  # None for the first month
                "occ_delta": r[4],
                "prod_count": r[5],
                "sat_count": r[6],
            }
            for r in rows
        ]
    finally:
        con.close()


@router.get("/by-month", summary="Row count per month (trend line)")
async def analytics_by_month(
    environment: EnvironmentType | None = None,
    source: SourceType | None = None,
    type: QueryType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    system: str | None = None,
) -> list[dict]:
    return await asyncio.to_thread(
        _by_month_sync,
        source and source.value,
        environment and environment.value,
        type and type.value,
        host,
        db_name,
        month_year,
        system,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/by-month-type
# ---------------------------------------------------------------------------


def _by_month_type_sync(environment: str | None) -> list[dict]:
    """Pivot: one row per month_year.

    per-type columns (blocker/deadlock/slow_query/slow_query_mongo) show the
    number of CSV rows uploaded for that type and month, taken from upload_log
    (latest upload per filename, to avoid double-counting re-uploads).
    They always sum to total_file_rows.

    total_patterns is the COUNT(*) of distinct normalised SQL-text rows stored
    in raw_query for that month.  It is typically larger than total_file_rows
    because each deadlock/blocker CSV event expands to several SQL text
    entries in the normalised table.
    """
    con = get_duck("raw_query", "upload_log")

    # Build filter for raw_query
    rq_where_clauses = ["month_year IS NOT NULL"]
    rq_params = []
    if environment:
        rq_where_clauses.append("environment = ?")
        rq_params.append(environment.lower())

    rq_where_str = " AND ".join(rq_where_clauses)

    # Build filter for upload_log
    ul_where_clauses = ["month_year IS NOT NULL"]
    ul_params = []
    if environment:
        ul_where_clauses.append("environment = ?")
        ul_params.append(environment.lower())

    ul_where_str = " AND ".join(ul_where_clauses)

    try:
        rows = con.execute(
            f"""
            SELECT
                coalesce(r.month_year, u.month_year) AS month_year,
                coalesce(u.blocker, 0)          AS blocker,
                coalesce(u.deadlock, 0)         AS deadlock,
                coalesce(u.slow_query, 0)       AS slow_query,
                coalesce(u.slow_query_mongo, 0) AS slow_query_mongo,
                u.total_file_rows,
                coalesce(r.total_patterns, 0)   AS total_patterns,
                coalesce(u.blocker_prod, 0)          AS blocker_prod,
                coalesce(u.deadlock_prod, 0)         AS deadlock_prod,
                coalesce(u.slow_query_prod, 0)       AS slow_query_prod,
                coalesce(u.slow_query_mongo_prod, 0) AS slow_query_mongo_prod,
                coalesce(u.blocker_sat, 0)          AS blocker_sat,
                coalesce(u.deadlock_sat, 0)         AS deadlock_sat,
                coalesce(u.slow_query_sat, 0)       AS slow_query_sat,
                coalesce(u.slow_query_mongo_sat, 0) AS slow_query_mongo_sat
            FROM (
                SELECT month_year, COUNT(*) AS total_patterns
                FROM raw_query
                WHERE {rq_where_str}
                GROUP BY month_year
            ) r
            FULL OUTER JOIN (
                -- Per-type CSV row counts, keeping only the latest upload for
                -- each filename so re-uploads don't inflate the numbers.
                SELECT
                    month_year,
                    SUM(csv_row_count) FILTER (WHERE file_type = 'blocker')          AS blocker,
                    SUM(csv_row_count) FILTER (WHERE file_type = 'deadlock')         AS deadlock,
                    SUM(csv_row_count) FILTER (WHERE file_type = 'slow_query_sql')   AS slow_query,
                    SUM(csv_row_count) FILTER (WHERE file_type = 'slow_query_mongo')
                        AS slow_query_mongo,
                    SUM(csv_row_count) AS total_file_rows,
                    SUM(csv_row_count) FILTER (
                        WHERE file_type = 'blocker' AND environment = 'prod'
                    ) AS blocker_prod,
                    SUM(csv_row_count) FILTER (
                        WHERE file_type = 'deadlock' AND environment = 'prod'
                    ) AS deadlock_prod,
                    SUM(csv_row_count) FILTER (
                        WHERE file_type = 'slow_query_sql' AND environment = 'prod'
                    ) AS slow_query_prod,
                    SUM(csv_row_count) FILTER (
                        WHERE file_type = 'slow_query_mongo' AND environment = 'prod'
                    ) AS slow_query_mongo_prod,
                    SUM(csv_row_count) FILTER (
                        WHERE file_type = 'blocker' AND environment = 'sat'
                    ) AS blocker_sat,
                    SUM(csv_row_count) FILTER (
                        WHERE file_type = 'deadlock' AND environment = 'sat'
                    ) AS deadlock_sat,
                    SUM(csv_row_count) FILTER (
                        WHERE file_type = 'slow_query_sql' AND environment = 'sat'
                    ) AS slow_query_sat,
                    SUM(csv_row_count) FILTER (
                        WHERE file_type = 'slow_query_mongo' AND environment = 'sat'
                    ) AS slow_query_mongo_sat
                FROM (
                    SELECT month_year, file_type, environment, csv_row_count,
                           ROW_NUMBER() OVER (
                               PARTITION BY filename
                               ORDER BY CAST(uploaded_at AS TIMESTAMP) DESC
                           ) AS rn
                    FROM upload_log
                    WHERE {ul_where_str}
                )
                WHERE rn = 1
                GROUP BY month_year
            ) u ON r.month_year = u.month_year
            ORDER BY coalesce(r.month_year, u.month_year) DESC
        """,
            rq_params + ul_params,
        ).fetchall()
        return [
            {
                "month_year": r[0],
                "blocker": r[1],
                "deadlock": r[2],
                "slow_query": r[3],
                "slow_query_mongo": r[4],
                "total_file_rows": r[5],  # None for pre-log months
                "total_patterns": r[6],  # unique SQL entries in raw_query
                "blocker_prod": r[7],
                "deadlock_prod": r[8],
                "slow_query_prod": r[9],
                "slow_query_mongo_prod": r[10],
                "blocker_sat": r[11],
                "deadlock_sat": r[12],
                "slow_query_sat": r[13],
                "slow_query_mongo_sat": r[14],
            }
            for r in rows
        ]
    finally:
        con.close()


@router.get("/by-month-type", summary="Row counts per month broken down by query type")
async def analytics_by_month_type(
    environment: str | None = Query(None, description="Optional environment filter (prod/sat)"),
) -> list[dict]:
    return await asyncio.to_thread(_by_month_type_sync, environment)


# ---------------------------------------------------------------------------
# GET /api/analytics/by-db
# ---------------------------------------------------------------------------


def _by_db_sync(
    top_n: int,
    source: str | None,
    environment: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
) -> list[dict]:
    where, params = _build_filters(
        source=source,
        environment=environment,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
        extra=["db_name IS NOT NULL", "db_name != ''"],
    )
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            SELECT db_name, source,
                   COUNT(*) AS row_count,
                   SUM(occurrence_count) AS total_occurrences
            FROM raw_query
            {where}
            GROUP BY db_name, source
            ORDER BY SUM(occurrence_count) DESC
            LIMIT ?
        """,
            params + [top_n],
        ).fetchall()
        return [
            {"db_name": r[0], "source": r[1], "row_count": r[2], "total_occurrences": r[3]}
            for r in rows
        ]
    finally:
        con.close()


@router.get("/by-db", summary="Top databases by occurrence count")
async def analytics_by_db(
    top_n: int = Query(default=20, ge=1, le=100),
    environment: EnvironmentType | None = None,
    source: SourceType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    system: str | None = None,
) -> list[dict]:
    return await asyncio.to_thread(
        _by_db_sync,
        top_n,
        source and source.value,
        environment and environment.value,
        host,
        db_name,
        month_year,
        system,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/curation-coverage
# ---------------------------------------------------------------------------


def _coverage_sync(
    source: str | None,
    environment: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
) -> dict:
    # DuckDB LEFT JOIN across both SQLite tables — single query, no round-trips
    where, params = _build_filters(
        source=source,
        environment=environment,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
    )
    con = get_duck("raw_query", "curated_query")
    try:
        row = con.execute(
            f"""
            SELECT
                COUNT(*)         AS total,
                COUNT(cq.id)     AS curated
            FROM raw_query rq
            LEFT JOIN curated_query cq ON cq.raw_query_id = rq.id
            {where}
        """,
            params,
        ).fetchone()
        total, curated = row[0], row[1]
        coverage_pct = round(curated / total * 100, 4) if total else 0.0
        return {
            "total_rows": total,
            "curated_rows": curated,
            "uncurated_rows": total - curated,
            "coverage_pct": coverage_pct,
        }
    finally:
        con.close()


@router.get("/curation-coverage", summary="Curation coverage statistics")
async def analytics_curation_coverage(
    host: str | None = None,
    db_name: str | None = None,
    environment: EnvironmentType | None = None,
    source: SourceType | None = None,
    month_year: str | None = None,
    system: str | None = None,
) -> dict:
    return await asyncio.to_thread(
        _coverage_sync,
        source and source.value,
        environment and environment.value,
        host,
        db_name,
        month_year,
        system,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/by-hour
# ---------------------------------------------------------------------------


def _by_hour_sync(
    source: str | None,
    environment: str | None,
    type_: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
    week_start: str | None = None,
    week_end: str | None = None,
) -> list[dict]:
    """
    DuckDB replaces the old SQLModel-fetch + Polars multi-step pipeline.

    week_start / week_end are ISO date strings (e.g. '2026-01-05', '2026-01-11')
    representing the exact Monday–Sunday (or partial-week) boundaries of the
    chosen calendar week, as computed by the frontend.  Both must be supplied
    together; omitting both returns all weeks.
    """
    where, params = _build_filters(
        source=source,
        environment=environment,
        type_=type_,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
        extra=["time IS NOT NULL"],
    )
    # Calendar week filter: compare the parsed timestamp's date against the
    # supplied ISO date strings.  Appended after dt is already resolved.
    week_filter = ""
    if week_start and week_end:
        week_filter = (
            f"AND CAST(dt AS DATE) BETWEEN CAST('{week_start}' AS DATE)"
            f" AND CAST('{week_end}' AS DATE)"
        )
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            SELECT
                datepart('hour',   dt) AS hour,
                datepart('isodow', dt) - 1 AS weekday,
                SUM(occurrence_count) AS occ,
                type   AS qtype,
                COALESCE(host,    '') AS host,
                COALESCE(db_name, '') AS db_name
            FROM (
                SELECT
                    try_strptime(
                        trim(regexp_replace(
                            regexp_replace(COALESCE(time, ''), '[+-]\\d{{2}}:?\\d{{2}}$', ''),
                            '\\s+', ' '
                        )),
                        {_STRPTIME_SQL}
                    ) AS dt,
                    occurrence_count, type, host, db_name
                FROM raw_query
                {where}
            ) t
            WHERE dt IS NOT NULL
            {week_filter}
            GROUP BY hour, weekday, qtype, host, db_name
            ORDER BY weekday, hour
        """,
            params,
        ).fetchall()
    finally:
        con.close()

    # Assemble nested structure in Python (minimal work — DuckDB already aggregated)
    cells: dict[tuple[int, int], dict] = {}
    for hour, weekday, occ, qtype, host_v, db_v in rows:
        key = (hour, weekday)
        cell = cells.setdefault(
            key,
            {
                "hour": hour,
                "weekday": weekday,
                "count": 0,
                "by_type": {},
                "_hosts": {},
                "_dbs": {},
            },
        )
        cell["count"] += occ
        cell["by_type"][qtype] = cell["by_type"].get(qtype, 0) + occ
        if host_v:
            cell["_hosts"][host_v] = cell["_hosts"].get(host_v, 0) + occ
        if db_v:
            cell["_dbs"][db_v] = cell["_dbs"].get(db_v, 0) + occ

    result = []
    for cell in sorted(cells.values(), key=lambda c: (c["weekday"], c["hour"])):
        result.append(
            {
                "hour": cell["hour"],
                "weekday": cell["weekday"],
                "count": cell["count"],
                "by_type": cell["by_type"],
                "top_hosts": sorted(
                    [{"host": h, "count": c} for h, c in cell["_hosts"].items()],
                    key=lambda x: -x["count"],
                )[:5],
                "top_dbs": sorted(
                    [{"db_name": d, "count": c} for d, c in cell["_dbs"].items()],
                    key=lambda x: -x["count"],
                )[:5],
            }
        )
    return result


@router.get("/by-hour", summary="Query count heatmap: hour-of-day × day-of-week")
async def analytics_by_hour(
    environment: EnvironmentType | None = None,
    source: SourceType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    type: QueryType | None = None,
    system: str | None = None,
    week_start: str | None = None,
    week_end: str | None = None,
) -> list[dict]:
    """
    week_start / week_end: ISO date strings for the chosen calendar week
    (e.g. week_start=2026-01-05&week_end=2026-01-11).  Both must be
    supplied together; omitting both returns all weeks in the month.
    """
    return await asyncio.to_thread(
        _by_hour_sync,
        source and source.value,
        environment and environment.value,
        type and type.value,
        host,
        db_name,
        month_year,
        system,
        week_start,
        week_end,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/by-hour-queries  — drill-down: raw rows for one cell
# ---------------------------------------------------------------------------


def _by_hour_queries_sync(
    source: str | None,
    environment: str | None,
    type_: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
    week_start: str | None,
    week_end: str | None,
    hour: int,
    weekday: int,
    limit: int,
    offset: int,
) -> dict:
    """
    Return the paginated list of raw_query rows whose timestamp falls in
    the given hour-of-day × weekday bucket.

    Returns { "rows": [...], "total": int } where total is the full count
    (before pagination) computed via COUNT(*) OVER() in one DuckDB pass.
    """
    where, params = _build_filters(
        source=source,
        environment=environment,
        type_=type_,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
        extra=["time IS NOT NULL"],
    )
    week_filter = ""
    if week_start and week_end:
        week_filter = (
            f"AND CAST(dt AS DATE) BETWEEN CAST('{week_start}' AS DATE)"
            f" AND CAST('{week_end}' AS DATE)"
        )
    # hour and weekday are injected as integer literals — no user-controlled
    # string interpolation because they are validated as int by FastAPI first.
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            SELECT
                id, type, host, db_name, environment, source,
                time, month_year, occurrence_count, query_details,
                COUNT(*) OVER() AS total_count
            FROM (
                SELECT
                    id, type, host, db_name, environment, source,
                    time, month_year, occurrence_count, query_details,
                    try_strptime(
                        trim(regexp_replace(
                            regexp_replace(COALESCE(time, ''), '[+-]\\d{{2}}:?\\d{{2}}$', ''),
                            '\\s+', ' '
                        )),
                        {_STRPTIME_SQL}
                    ) AS dt
                FROM raw_query
                {where}
            ) t
            WHERE dt IS NOT NULL
              AND datepart('hour',   dt) = {hour}
              AND datepart('isodow', dt) - 1 = {weekday}
              {week_filter}
            ORDER BY occurrence_count DESC, id DESC
            LIMIT {limit} OFFSET {offset}
        """,
            params,
        ).fetchall()
    finally:
        con.close()

    result_rows = []
    total = 0
    for r in rows:
        result_rows.append(
            {
                "id": r[0],
                "type": r[1],
                "host": r[2],
                "db_name": r[3],
                "environment": r[4],
                "source": r[5],
                "time": r[6],
                "month_year": r[7],
                "occurrence_count": r[8],
                "query_details": r[9],
            }
        )
        total = r[10]  # COUNT(*) OVER() — same for every row
    return {"rows": result_rows, "total": total}


@router.get(
    "/by-hour-queries",
    summary="Drill-down: raw query rows for a specific hour × weekday cell",
)
async def analytics_by_hour_queries(
    hour: int = Query(..., ge=0, le=23),
    weekday: int = Query(..., ge=0, le=6),
    environment: EnvironmentType | None = None,
    source: SourceType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    type: QueryType | None = None,
    system: str | None = None,
    week_start: str | None = None,
    week_end: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return await asyncio.to_thread(
        _by_hour_queries_sync,
        source and source.value,
        environment and environment.value,
        type and type.value,
        host,
        db_name,
        month_year,
        system,
        week_start,
        week_end,
        hour,
        weekday,
        limit,
        offset,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/top-fingerprints
# ---------------------------------------------------------------------------


def _top_fingerprints_sync(
    source: str | None,
    environment: str | None,
    type_: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
    top_n: int,
) -> list[dict]:
    """
    DuckDB replaces the old SQLModel-fetch-all + Polars regex pipeline.

    Fingerprinting rules (identical to old logic):
      1. Lowercase the query_details string.
      2. Replace string literals  ('...')  with '?'.
      3. Replace numeric literals (integers / decimals)  with '?'.
      4. Collapse whitespace.
      5. Truncate to 300 chars.
    """
    where, params = _build_filters(
        source=source,
        environment=environment,
        type_=type_,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
        extra=["query_details IS NOT NULL", "query_details != ''"],
    )
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            WITH stripped AS (
                -- Remove sp_executesql param-declaration header so the 300-char
                -- fingerprint budget captures actual SQL and not type noise.
                -- Handles @p0/@p1 (raw) AND @p? (pre-normalised by _clean() at ingest).
                -- Character class [\d?]: '?' is literal inside [...], not a quantifier.
                SELECT
                    query_details,
                    regexp_replace(
                        lower(COALESCE(query_details, '')),
                        '^\\(@p[\\d?](?:[^)(]|\\([^)]*\\))*\\)\\s*', ''
                    ) AS body,
                    COALESCE(type,        'unknown') AS qtype,
                    COALESCE(host,        '')        AS host,
                    COALESCE(db_name,     '')        AS db_name,
                    COALESCE(month_year,  '')        AS month_year,
                    COALESCE(environment, '')        AS environment,
                    COALESCE(source,      '')        AS src,
                    occurrence_count                 AS occ
                FROM raw_query
                {where}
            ),
            -- For sp_executesql queries whose param block was so large the DMV
            -- truncated the text before reaching the SQL body, look for a sibling
            -- execution of the SAME query from the same db/host that was called with
            -- fewer params (shorter IN-list) and DID fit in the DMV capture buffer.
            -- We normalise that body to a fingerprint and use it as the representative
            -- fingerprint for ALL executions of this query, truncated or not.
            param_sql_bridge AS (
                SELECT
                    db_name,
                    host,
                    first(
                        substring(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(body, '''[^'']*''', '?'),
                                    '\\b\\d+\\.?\\d*\\b', '?'),
                                '\\s+', ' '),
                            1, 300)
                    ) AS recovered_fp,
                    first(query_details) AS recovered_sample
                FROM stripped
                WHERE query_details LIKE '(@P?%'    -- was a param-header query
                  AND body NOT LIKE '(@p%'          -- but strip succeeded → SQL visible
                GROUP BY db_name, host
            ),
            fingerprinted AS (
                SELECT
                    CASE
                        -- The strip regex requires a closing ')' on the param block.
                        -- When query_details was truncated mid-params by the DMV the
                        -- closing ')' is absent; regexp_replace returns body unchanged
                        -- so body still starts with '(@p'.
                        -- Try to recover SQL from a sibling row via param_sql_bridge;
                        -- fall back to a descriptive label if no sibling exists.
                        WHEN length(trim(s.body)) < 5
                          OR s.body LIKE '(@p%'
                        THEN COALESCE(
                                b.recovered_fp,
                                '(sp_executesql — sql body not captured by dmv)')
                        ELSE substring(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(s.body, '''[^'']*''', '?'),
                                    '\\b\\d+\\.?\\d*\\b', '?'),
                                '\\s+', ' '),
                            1, 300)
                    END AS fp,
                    s.qtype,
                    s.host,
                    s.db_name,
                    s.month_year,
                    s.environment,
                    s.src,
                    -- For truncated rows prefer the sample from the complete sibling
                    -- so the expanded row shows readable SQL, not a wall of params.
                    COALESCE(
                        CASE WHEN s.body LIKE '(@p%' THEN b.recovered_sample END,
                        s.query_details
                    ) AS sample,
                    s.occ
                FROM stripped s
                LEFT JOIN param_sql_bridge b
                    ON s.db_name = b.db_name AND s.host = b.host
            )
            SELECT
                fp,
                qtype,
                host,
                db_name,
                month_year,
                environment,
                src,
                SUM(occ)      AS total_occ,
                COUNT(*)      AS row_count,
                first(sample) AS sample
            FROM fingerprinted
            GROUP BY fp, qtype, host, db_name, month_year, environment, src
            ORDER BY total_occ DESC
        """,
            params,
        ).fetchall()
    finally:
        con.close()

    # Group by fingerprint across all dimension combos, pick top_n
    groups: dict[str, dict] = {}
    for fp, qtype, host_v, db_v, my, env, src, occ, rcount, sample in rows:
        g = groups.setdefault(
            fp,
            {
                "fingerprint": fp,
                "sample_query": sample,
                "count": 0,
                "row_count": 0,
                "by_type": {},
                "_by_month": {},
                "_hosts": {},
                "_dbs": {},
                "environments": set(),
                "_sources": set(),
            },
        )
        g["count"] += occ
        g["row_count"] += rcount
        g["by_type"][qtype] = g["by_type"].get(qtype, 0) + occ
        if my:
            g["_by_month"][my] = g["_by_month"].get(my, 0) + occ
        if host_v:
            g["_hosts"][host_v] = g["_hosts"].get(host_v, 0) + occ
        if db_v:
            g["_dbs"][db_v] = g["_dbs"].get(db_v, 0) + occ
        if env:
            g["environments"].add(env)
        if src:
            g["_sources"].add(src)

    top = sorted(groups.values(), key=lambda x: -x["count"])[:top_n]
    for g in top:
        # Flatten to the shape FingerprintRow expects
        hosts_sorted = sorted(g.pop("_hosts").items(), key=lambda x: -x[1])
        dbs_sorted = sorted(g.pop("_dbs").items(), key=lambda x: -x[1])
        sources = sorted(g.pop("_sources"))
        g["example_host"] = hosts_sorted[0][0] if hosts_sorted else ""
        g["example_db"] = dbs_sorted[0][0] if dbs_sorted else ""
        g["months"] = sorted(g.pop("_by_month").keys())
        g["environments"] = sorted(g["environments"])
        g["example_source"] = sources[0] if sources else ""
    return top


@router.get("/top-fingerprints", summary="Top normalised query fingerprints by occurrence count")
async def analytics_top_fingerprints(
    top_n: int = Query(default=20, ge=1, le=200),
    environment: EnvironmentType | None = None,
    source: SourceType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    type: QueryType | None = None,
    system: str | None = None,
) -> list[dict]:
    return await asyncio.to_thread(
        _top_fingerprints_sync,
        source and source.value,
        environment and environment.value,
        type and type.value,
        host,
        db_name,
        month_year,
        system,
        top_n,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/host-stats  (Phase 8C)
# P50 / P95 / P99 occurrence distribution per host via DuckDB QUANTILE_CONT
# ---------------------------------------------------------------------------


def _host_stats_sync(
    top_n: int,
    source: str | None,
    environment: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
) -> list[dict]:
    """
    QUANTILE_CONT(col, q) computes an exact continuous percentile over the
    group — equivalent to numpy.percentile with interpolation='linear'.
    We cast occurrence_count to DOUBLE so the result is always a float.
    Hosts with fewer than 3 rows are excluded (percentile is meaningless).
    """
    where, params = _build_filters(
        source=source,
        environment=environment,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
        extra=["host IS NOT NULL", "host != ''"],
    )
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            SELECT
                host,
                ROUND(QUANTILE_CONT(occurrence_count::DOUBLE, 0.50), 1) AS p50,
                ROUND(QUANTILE_CONT(occurrence_count::DOUBLE, 0.95), 1) AS p95,
                ROUND(QUANTILE_CONT(occurrence_count::DOUBLE, 0.99), 1) AS p99,
                MAX(occurrence_count)            AS max_occ,
                SUM(occurrence_count)            AS total_occurrences,
                COUNT(*)                         AS row_count
            FROM raw_query
            {where}
            GROUP BY host
            HAVING COUNT(*) >= 3
            ORDER BY p95 DESC
            LIMIT ?
        """,
            params + [top_n],
        ).fetchall()
        return [
            {
                "host": r[0],
                "p50": r[1],
                "p95": r[2],
                "p99": r[3],
                "max_occ": r[4],
                "total_occurrences": r[5],
                "row_count": r[6],
            }
            for r in rows
        ]
    finally:
        con.close()


@router.get("/host-stats", summary="P50/P95/P99 occurrence distribution per host")
async def analytics_host_stats(
    top_n: int = Query(default=30, ge=1, le=200),
    environment: EnvironmentType | None = None,
    source: SourceType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    system: str | None = None,
) -> list[dict]:
    return await asyncio.to_thread(
        _host_stats_sync,
        top_n,
        source and source.value,
        environment and environment.value,
        host,
        db_name,
        month_year,
        system,
    )


# ---------------------------------------------------------------------------
# GET /api/analytics/co-occurrence  (Phase 8D)
# Hosts where both blocker AND deadlock events appear in the same month
# ---------------------------------------------------------------------------


def _co_occurrence_sync(
    environment: str | None,
    host: str | None,
    db_name: str | None,
    month_year: str | None,
    system: str | None,
    limit: int = 200,
) -> list[dict]:
    """
    FULL OUTER JOIN between blocker-aggregated and deadlock-aggregated CTEs on
    (host, month_year). Rows where only one type is present are still returned
    (count = 0 for the missing type) so the frontend can colour them differently.
    Only hosts with at least one blocker OR one deadlock are returned.
    """
    # Build shared dimension filters — type is forced inside each CTE
    base_where, params = _build_filters(
        environment=environment,
        host=host,
        db_name=db_name,
        month_year=month_year,
        system=system,
        extra=["host IS NOT NULL", "host != ''", "month_year IS NOT NULL"],
    )
    # params list is reused for both CTEs — DuckDB positional params are per-statement
    con = get_duck("raw_query")
    try:
        rows = con.execute(
            f"""
            WITH blockers AS (
                SELECT host, month_year, SUM(occurrence_count) AS blocker_count
                FROM raw_query
                {base_where} AND type = 'blocker'
                GROUP BY host, month_year
            ),
            deadlocks AS (
                SELECT host, month_year, SUM(occurrence_count) AS deadlock_count
                FROM raw_query
                {base_where} AND type = 'deadlock'
                GROUP BY host, month_year
            )
            SELECT
                COALESCE(b.host,       d.host)       AS host,
                COALESCE(b.month_year, d.month_year) AS month_year,
                COALESCE(b.blocker_count,  0)        AS blocker_count,
                COALESCE(d.deadlock_count, 0)        AS deadlock_count,
                COALESCE(b.blocker_count, 0)
                    + COALESCE(d.deadlock_count, 0)  AS combined_score
            FROM blockers b
            FULL OUTER JOIN deadlocks d
                ON b.host = d.host AND b.month_year = d.month_year
            ORDER BY combined_score DESC, host, month_year
            LIMIT ?
        """,
            params + params + [limit],
        ).fetchall()  # params duplicated for both CTEs
        return [
            {
                "host": r[0],
                "month_year": r[1],
                "blocker_count": r[2],
                "deadlock_count": r[3],
                "combined_score": r[4],
            }
            for r in rows
        ]
    finally:
        con.close()


@router.get("/co-occurrence", summary="Hosts with blocker and/or deadlock events per month")
async def analytics_co_occurrence(
    environment: EnvironmentType | None = None,
    host: str | None = None,
    db_name: str | None = None,
    month_year: str | None = None,
    system: str | None = None,
    limit: int = Query(200, ge=1, le=1000, description="Maximum rows returned (default 200)"),
) -> list[dict]:
    return await asyncio.to_thread(
        _co_occurrence_sync,
        environment and environment.value,
        host,
        db_name,
        month_year,
        system,
        limit,
    )
