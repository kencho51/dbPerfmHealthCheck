"""
DuckDB connection factory -- per-request, in-memory, backed by SQLite.

Architecture
-----------
SQLite (file)        <--  OLTP writes via SQLModel async engine (CRUD routers)
                           |  read synchronously for DuckDB registration
DuckDB (in-memory)   <--  OLAP reads via analytics / export routers

Data flow
---------
1. _load_table(table) fetches all rows from SQLite via a synchronous
   SQLAlchemy engine derived from the same SQLITE_PATH used by the app.
2. Rows and column names are used to build a Polars DataFrame.
3. The DataFrame is registered in a fresh in-memory DuckDB connection as a
   virtual relation.  DuckDB infers column types from the Polars Arrow schema.

SQL queries then use plain table names (raw_query, curated_query, pattern_label)
rather than any ATTACH-prefixed names.

Thread-safety: each request opens its own DuckDB connection (no shared state).
"""
from __future__ import annotations

from typing import Any

import duckdb
import polars as pl
import sqlalchemy as sa

# Tables known to exist in SQLite
_KNOWN_TABLES = {"raw_query", "curated_query", "pattern_label"}


def _load_table(table: str) -> pl.DataFrame:
    """Fetch a SQLite table via SQLAlchemy as a Polars DataFrame."""
    if table not in _KNOWN_TABLES:
        raise ValueError(f"Unknown table: {table!r}. Known: {sorted(_KNOWN_TABLES)}")

    from api.database import SQLITE_URL

    # Convert async URL (sqlite+aiosqlite://...) to a sync URL for DuckDB's
    # synchronous context.
    sync_url = str(SQLITE_URL).replace("sqlite+aiosqlite", "sqlite")
    engine = sa.create_engine(sync_url)
    with engine.connect() as conn:
        result = conn.execute(sa.text(f"SELECT * FROM {table}"))  # noqa: S608
        columns = list(result.keys())
        rows = result.fetchall()

    if not rows:
        return pl.DataFrame({col: pl.Series([], dtype=pl.Utf8) for col in columns})

    return pl.DataFrame(
        [dict(zip(columns, row)) for row in rows],
        schema=columns,
    )


def get_duck(*tables: str) -> duckdb.DuckDBPyConnection:
    """Open in-memory DuckDB and register the requested Neon tables."""
    if not tables:
        tables = ("raw_query",)
    con = duckdb.connect()
    for table in tables:
        df = _load_table(table)
        con.register(table, df)
    return con


def build_where(
    clauses: list[tuple[str, Any]],
    *,
    prefix: str = "WHERE",
) -> tuple[str, list[Any]]:
    """Build a SQL WHERE fragment from (column, value) pairs; None values skipped."""
    active = [(col, val) for col, val in clauses if val is not None]
    if not active:
        return "", []
    fragment = " AND ".join(f"{col} = ?" for col, _ in active)
    params   = [val for _, val in active]
    return f"{prefix} {fragment}", params