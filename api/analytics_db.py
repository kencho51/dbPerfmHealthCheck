"""
DuckDB connection factory -- per-request, in-memory, backed by Neon PostgreSQL.

Architecture
-----------
Neon PostgreSQL (HTTPS)  <--  OLTP writes via NeonHTTPSession (CRUD routers)
                               |  _sync_http_sql_with_fields (data fetched into DuckDB)
DuckDB (in-memory)        <--  OLAP reads via analytics / export routers

Data flow
---------
1. _load_table(table) fetches all rows from Neon via the HTTPS REST API.
2. Rows and column names are used to build a Polars DataFrame.
3. The DataFrame is registered in a fresh in-memory DuckDB connection as a
   virtual relation.  DuckDB infers column types from the Polars Arrow schema.

SQL queries then use plain table names (raw_query, curated_query)
rather than any ATTACH-prefixed names.

Thread-safety: each request opens its own DuckDB connection (no shared state).
"""
from __future__ import annotations

from typing import Any

import duckdb
import polars as pl

# Tables known to exist in Neon PostgreSQL
_KNOWN_TABLES = {"raw_query", "curated_query", "pattern_label"}


def _load_table(table: str) -> pl.DataFrame:
    """Fetch a Neon PostgreSQL table via HTTPS REST API as a Polars DataFrame."""
    if table not in _KNOWN_TABLES:
        raise ValueError(f"Unknown table: {table!r}. Known: {sorted(_KNOWN_TABLES)}")

    from api.neon_http import _sync_http_sql_with_fields

    columns, rows, _ = _sync_http_sql_with_fields(f"SELECT * FROM {table}")

    if not rows:
        return pl.DataFrame({col: [] for col in columns})

    return pl.DataFrame(rows, schema=columns, orient="row")


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