"""
Database engine and async session factory.

Reads  api/.env  (via python-dotenv) so the backend can be configured
without touching OS environment variables.

Supported backends (set DB_BACKEND in api/.env):
    sqlite  — local file or path set by SQLITE_PATH   (default)
    neon    — not available on this branch; see migrate-to-neon-psql-db

Usage in a FastAPI route:
    async with get_session() as session:
        result = await session.exec(select(RawQuery))
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Global write serialisation lock
#
# SQLite WAL mode allows concurrent readers but only ONE writer at a time.
# Without this lock, rapid concurrent uploads race for the write slot and
# hit "database is locked" once the busy_timeout expires.
#
# asyncio.Lock() is event-loop-safe: Python 3.10+ defers loop binding until
# first acquisition, so creating it at module level is safe with uvicorn.
# ---------------------------------------------------------------------------
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

_write_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# Explicit sqlite3 datetime adapters (replaces the deprecated implicit ones)
#
# Python 3.12 deprecated the built-in sqlite3 datetime adapter/converter that
# silently handled datetime <-> TEXT conversion.  aiosqlite builds on the
# stdlib sqlite3 module, so without explicit adapters every connection emits
# a DeprecationWarning.  Registering them here — before any engine/connection
# is created — silences the warning across the whole application.
# ---------------------------------------------------------------------------


def _adapt_datetime(val: datetime) -> str:
    """Serialise a datetime to an ISO-8601 string for storage in SQLite TEXT."""
    if val.tzinfo is None:
        val = val.replace(tzinfo=UTC)
    return val.isoformat()


def _convert_datetime(val: bytes) -> datetime:
    """Deserialise an ISO-8601 SQLite TEXT back to a timezone-aware datetime."""
    dt = datetime.fromisoformat(val.decode())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


sqlite3.register_adapter(datetime, _adapt_datetime)
sqlite3.register_converter("datetime", _convert_datetime)
sqlite3.register_converter(
    "DATETIME", _convert_datetime
)  # SQLite column names are case-insensitive

# ---------------------------------------------------------------------------
# Load api/.env before reading any env vars
# (does nothing if the file is absent, so tests / CI without .env still work)
# ---------------------------------------------------------------------------

_ENV_FILE = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_FILE, override=False)  # don't override vars already in OS env

# ---------------------------------------------------------------------------
# Backend selector
# ---------------------------------------------------------------------------

DB_BACKEND: str = os.getenv("DB_BACKEND", "sqlite").lower().strip()

if DB_BACKEND == "neon":
    raise OSError(
        "DB_BACKEND=neon is not supported on this branch "
        "(integration-query-analysis-app uses SQLite only).\n"
        "Switch to the 'migrate-to-neon-psql-db' branch for Neon support, "
        "or set DB_BACKEND=sqlite in api/.env."
    )

if DB_BACKEND != "sqlite":
    raise OSError(f"Unknown DB_BACKEND={DB_BACKEND!r}. Valid values: 'sqlite'.")

# ---------------------------------------------------------------------------
# Resolve SQLite path
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB = _PROJECT_ROOT / "db" / "master.db"

SQLITE_PATH: Path = Path(os.getenv("SQLITE_PATH", str(_DEFAULT_DB)))
SQLITE_URL: str = os.getenv("SQLITE_URL", f"sqlite+aiosqlite:///{SQLITE_PATH}")

# Keep SQLITE_PATH in sync when a full URL is provided
if SQLITE_URL.startswith("sqlite+aiosqlite:///"):
    _raw = SQLITE_URL.removeprefix("sqlite+aiosqlite:///")
    if _raw and not _raw.startswith(":"):  # skip special URIs like :memory:
        SQLITE_PATH = Path(_raw)

# ---------------------------------------------------------------------------
# Engine (created once at module import; reused across requests)
# ---------------------------------------------------------------------------

engine: AsyncEngine = create_async_engine(
    SQLITE_URL,
    echo=False,  # set True to log SQL for debugging
    connect_args={
        "check_same_thread": False,
        # 120 s gives the aiosqlite thread time to wait for a SQLite write lock
        # held by the background _link_typed_to_raw thread (see upload.py).
        # The background thread uses its own sqlite3 connection with timeout=600,
        # so it will always yield the lock well within 120 s.
        "timeout": 120,
        # NOTE: do NOT set detect_types here.  detect_types causes the sqlite3
        # converter to fire and return a datetime *before* SQLAlchemy's own
        # DateTime result_processor runs.  SQLAlchemy then calls
        # datetime.fromisoformat(datetime_obj) → TypeError: argument must be
        # str.  SQLAlchemy handles TEXT → datetime conversion itself; we only
        # need register_adapter (above) for the write side (datetime → TEXT).
    },
)


# ---------------------------------------------------------------------------
# Session: FastAPI Depends (async generator) + direct use (context manager)
# ---------------------------------------------------------------------------


async def get_session() -> AsyncGenerator[AsyncSession]:
    """Async generator — for use with FastAPI `Depends(get_session)`."""
    async with AsyncSession(engine, expire_on_commit=False) as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def open_session() -> AsyncGenerator[AsyncSession]:
    """
    Async context manager — for direct programmatic use (scripts, tests,
    services that are called outside a FastAPI request lifecycle).

    Usage:
        async with open_session() as session:
            result = await session.exec(select(RawQuery))
    """
    async with AsyncSession(engine, expire_on_commit=False) as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def write_session() -> AsyncGenerator[AsyncSession]:
    """open_session() wrapped with the global write lock.

    All INSERT / UPDATE / DELETE operations should use this context manager
    instead of open_session().  The lock ensures at most one coroutine holds
    a SQLite write transaction at a time, preventing OperationalError:
    database is locked under concurrent uploads.

    Usage:
        async with write_session() as session:
            await session.execute(stmt)
    """
    async with _write_lock:
        async with open_session() as session:
            yield session


# ---------------------------------------------------------------------------
# DB initialisation helpers (called from app lifespan)
# ---------------------------------------------------------------------------


async def apply_pragmas() -> None:
    """Set SQLite performance/safety PRAGMAs and pre-create link indexes."""
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
        await conn.exec_driver_sql("PRAGMA foreign_keys=ON")
        await conn.exec_driver_sql("PRAGMA cache_size=-64000")  # 64 MB page cache
        await conn.exec_driver_sql("PRAGMA temp_store=MEMORY")
        # Pre-create the two link indexes used by _link_typed_to_raw so they
        # always exist before any upload runs, avoiding a costly full-table-scan
        # index-build inside an active write transaction.
        await conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_raw_query_link_key "
            "ON raw_query (type, host, db_name, environment, month_year)"
        )
        await conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_raw_query_link_source "
            "ON raw_query (type, source, environment, month_year)"
        )


async def create_db_and_tables() -> None:
    """Create all SQLModel tables if they don't yet exist.

    In production this is handled by Alembic migrations; this function is
    kept as a fallback for fresh dev setups without running 'alembic upgrade'.
    """
    # Import models so SQLModel metadata is populated before create_all
    import api.models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
