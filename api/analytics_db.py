"""
DuckDB connection factory -- per-request, in-memory, backed by SQLite.

Architecture
-----------
SQLite (file)        <--  OLTP writes via SQLModel async engine (CRUD routers)
                           |  read once into a TTL cache (60 s)
DataFrame cache      <--  in-memory Polars DataFrames, one per table
DuckDB (in-memory)   <--  OLAP reads; each request gets its own connection
                           backed by cached DataFrames (no SQLite hit)

Thread-safety
-------------
- Per-table locks ensure only ONE thread ever loads a given table from SQLite.
  All other threads that arrive while a load is in progress wait, then read the
  freshly-filled cache — eliminating concurrent SQLite access entirely.
- DuckDB: each request opens its own in-memory connection (no shared state).
- The shared sync engine uses a single connection via QueuePool; WAL pragmas
  allow non-blocking reads even if an async write is in progress.
"""
from __future__ import annotations

import threading
import time
from typing import Any

import duckdb
import polars as pl
import sqlalchemy as sa

# Tables known to exist in SQLite
_KNOWN_TABLES = {"raw_query", "curated_query", "pattern_label"}

# ---------------------------------------------------------------------------
# Shared sync engine — created once, reused for all SQLite reads.
# ---------------------------------------------------------------------------
_engine_lock: threading.Lock = threading.Lock()
_sync_engine: sa.engine.Engine | None = None


def _get_sync_engine() -> sa.engine.Engine:
    global _sync_engine
    if _sync_engine is not None:
        return _sync_engine
    with _engine_lock:
        if _sync_engine is not None:
            return _sync_engine
        from api.database import SQLITE_URL
        sync_url = str(SQLITE_URL).replace("sqlite+aiosqlite", "sqlite")
        engine = sa.create_engine(
            sync_url,
            connect_args={"check_same_thread": False, "timeout": 30},
        )
        with engine.connect() as conn:
            conn.execute(sa.text("PRAGMA journal_mode=WAL"))
            conn.execute(sa.text("PRAGMA synchronous=NORMAL"))
        _sync_engine = engine
    return _sync_engine


# ---------------------------------------------------------------------------
# DataFrame cache — avoids hitting SQLite on every concurrent request.
#
# Structure: { table_name: (loaded_at_monotonic, DataFrame) }
# Per-table lock: only one thread loads a given table at a time; others wait
# and then read the freshly-cached result instead of hitting SQLite again.
# ---------------------------------------------------------------------------
_CACHE_TTL: float = 60.0          # seconds before a stale table is re-fetched
_df_cache: dict[str, tuple[float, pl.DataFrame]] = {}
_table_locks: dict[str, threading.Lock] = {t: threading.Lock() for t in _KNOWN_TABLES}

# Serialises duckdb.connect() + con.register() — DuckDB 1.x is not thread-safe
# for concurrent connection creation on Windows.
_duckdb_lock: threading.Lock = threading.Lock()


def _load_table(table: str) -> pl.DataFrame:
    """
    Return a Polars DataFrame for *table*, reading from SQLite at most once
    per _CACHE_TTL seconds.  Concurrent threads that need the same table will
    block on _table_locks[table] until the first thread finishes loading, then
    immediately return the cached result — preventing parallel SQLite reads.
    """
    if table not in _KNOWN_TABLES:
        raise ValueError(f"Unknown table: {table!r}. Known: {sorted(_KNOWN_TABLES)}")

    # Fast path — check cache without the lock first (avoids contention after warm-up)
    cached = _df_cache.get(table)
    if cached is not None and (time.monotonic() - cached[0]) < _CACHE_TTL:
        return cached[1]

    # Slow path — acquire per-table lock so only one thread hits SQLite
    with _table_locks[table]:
        # Re-check inside the lock (another thread may have just loaded it)
        cached = _df_cache.get(table)
        if cached is not None and (time.monotonic() - cached[0]) < _CACHE_TTL:
            return cached[1]

        with _get_sync_engine().connect() as conn:
            result = conn.execute(sa.text(f"SELECT * FROM {table}"))  # noqa: S608
            columns = list(result.keys())
            rows = result.fetchall()

        df = (
            pl.DataFrame({col: pl.Series([], dtype=pl.Utf8) for col in columns})
            if not rows
            else pl.DataFrame([dict(zip(columns, row)) for row in rows], schema=columns)
        )
        _df_cache[table] = (time.monotonic(), df)
        return df


def invalidate_cache(table: str | None = None) -> None:
    """
    Evict one or all tables from the DataFrame cache.
    Call this after any write (upload, curate, etc.) so the next analytics
    request sees fresh data.
    """
    if table is None:
        _df_cache.clear()
    else:
        _df_cache.pop(table, None)


def get_duck(*tables: str) -> duckdb.DuckDBPyConnection:
    """
    Open in-memory DuckDB and register the requested tables from cache.

    The global _duckdb_lock serialises duckdb.connect() + con.register() calls.
    DuckDB 1.x has known thread-safety issues on Windows when multiple threads
    call duckdb.connect() simultaneously.  The lock eliminates the race; each
    individual DuckDB query completes in < 100 ms so serialisation is invisible.
    Once the connection is created and tables registered, the caller holds its
    own private in-memory DuckDB handle and can execute queries without the lock.
    """
    if not tables:
        tables = ("raw_query",)
    with _duckdb_lock:
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