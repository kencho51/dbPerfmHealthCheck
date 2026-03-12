"""
DuckDB connection factory — per-request, in-memory, no SQLite extension.

Architecture
-----------
SQLite (master.db)  ←  OLTP writes via SQLModel/aiosqlite (CRUD routers)
                      ↕  Python sqlite3 reads (data loaded into DuckDB)
DuckDB (in-memory)  ←  OLAP reads via analytics / export routers

NO DuckDB SQLite extension is loaded.  The old ``LOAD sqlite`` +
``ATTACH (TYPE SQLITE, ACCESS_MODE …)`` approach required DuckDB to move
extension files into a local cache directory — that fails in corporate
environments with restricted file-system permissions.

Instead, each get_duck() call:
  1. Opens the SQLite file via Python's built-in ``sqlite3`` module.
  2. Loads the requested tables into Polars DataFrames.
  3. Registers them in a fresh in-memory DuckDB connection as virtual tables.

SQL queries then use plain table names (``raw_query``, ``curated_query``)
rather than the old ``db.raw_query`` ATTACH-prefixed names.

Thread-safety: each request opens its own connection (no shared state).

Usage in a router:
    import asyncio
    from api.analytics_db import get_duck, build_where

    def _query_sync(source=None, host=None) -> list[dict]:
        where, params = build_where([
            ("source", source),
            ("host",   host),
        ])
        con = get_duck("raw_query")
        try:
            return con.execute(
                f"SELECT host, COUNT(*) FROM raw_query {where} GROUP BY host",
                params,
            ).fetchall()
        finally:
            con.close()

    @router.get("/endpoint")
    async def endpoint(source: str | None = None, host: str | None = None):
        return await asyncio.to_thread(_query_sync, source, host)
"""
from __future__ import annotations

import sqlite3 as _sqlite3
from typing import Any

import duckdb
import polars as pl

from api.database import SQLITE_PATH

# Tables known to exist in master.db
_KNOWN_TABLES = {"raw_query", "curated_query", "pattern_label"}


def _load_table(table: str) -> pl.DataFrame:
    """
    Read a SQLite table into a Polars DataFrame via Python's sqlite3 module.

    DuckDB infers column types from the Polars Arrow schema, so no manual
    type mapping is needed.
    """
    if table not in _KNOWN_TABLES:
        raise ValueError(f"Unknown table: {table!r}. Known: {sorted(_KNOWN_TABLES)}")
    con = _sqlite3.connect(str(SQLITE_PATH))
    try:
        cursor = con.execute(f"SELECT * FROM {table}")  # noqa: S608
        cols   = [d[0] for d in cursor.description]
        rows   = cursor.fetchall()
    finally:
        con.close()

    if not rows:
        return pl.DataFrame({c: [] for c in cols})
    return pl.DataFrame(rows, schema=cols, orient="row")


def get_duck(*tables: str) -> duckdb.DuckDBPyConnection:
    """
    Open a new in-memory DuckDB connection and register the requested SQLite
    tables as virtual relations.

    Parameters
    ----------
    *tables : str
        Table names to load from SQLite.  Defaults to ``("raw_query",)``.

    Usage
    -----
        con = get_duck("raw_query")
        try:
            rows = con.execute("SELECT host, COUNT(*) FROM raw_query GROUP BY host").fetchall()
        finally:
            con.close()
    """
    if not tables:
        tables = ("raw_query",)
    con = duckdb.connect()          # in-memory; no .duckdb file on disk
    for table in tables:
        df = _load_table(table)
        con.register(table, df)
    return con


def build_where(
    clauses: list[tuple[str, Any]],
    *,
    prefix: str = "WHERE",
) -> tuple[str, list[Any]]:
    """
    Build a SQL WHERE (or AND) fragment from (column, value) pairs.

    Pairs where value is ``None`` are skipped.  Values are returned as a
    positional params list for DuckDB's ``?`` binding.

    Parameters
    ----------
    clauses : list of (col_expr, value)
        e.g. [("source", "sql"), ("environment", None), ("host", "WINDB01")]
    prefix : str
        ``"WHERE"`` (default) for the first condition block, or ``"AND"``
        when appending to an existing clause.

    Returns
    -------
    (sql_fragment, params_list)
        sql_fragment is ``""`` if all values are None.
    """
    active = [(col, val) for col, val in clauses if val is not None]
    if not active:
        return "", []
    fragment = f"{prefix} " + " AND ".join(f"{col} = ?" for col, _ in active)
    params   = [val for _, val in active]
    return fragment, params


def system_clause(system: str | None) -> tuple[str, list[str]]:
    """
    Return (AND fragment, params) for the infrastructure system filter.

    Mirrors ``apply_system_filter`` from ``api.host_system`` but returns
    raw SQL for DuckDB rather than a SQLAlchemy clause.
    """
    if system is None:
        return "", []

    from api.host_system import SYSTEM_HOSTS  # local import to avoid cycle

    hosts = SYSTEM_HOSTS.get(system.upper(), [])
    if not hosts:
        return "AND 1 = 0", []   # unknown system → no rows

    placeholders = ", ".join("?" * len(hosts))
    return f"AND upper(host) IN ({placeholders})", [h.upper() for h in hosts]
