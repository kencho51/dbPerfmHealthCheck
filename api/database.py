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

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

# ---------------------------------------------------------------------------
# Load api/.env before reading any env vars
# (does nothing if the file is absent, so tests / CI without .env still work)
# ---------------------------------------------------------------------------

_ENV_FILE = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_FILE, override=False)   # don't override vars already in OS env

# ---------------------------------------------------------------------------
# Backend selector
# ---------------------------------------------------------------------------

DB_BACKEND: str = os.getenv("DB_BACKEND", "sqlite").lower().strip()

if DB_BACKEND == "neon":
    raise EnvironmentError(
        "DB_BACKEND=neon is not supported on this branch "
        "(integration-query-analysis-app uses SQLite only).\n"
        "Switch to the 'migrate-to-neon-psql-db' branch for Neon support, "
        "or set DB_BACKEND=sqlite in api/.env."
    )

if DB_BACKEND != "sqlite":
    raise EnvironmentError(
        f"Unknown DB_BACKEND={DB_BACKEND!r}. Valid values: 'sqlite'."
    )

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
    if _raw and not _raw.startswith(":"):   # skip special URIs like :memory:
        SQLITE_PATH = Path(_raw)

# ---------------------------------------------------------------------------
# Engine (created once at module import; reused across requests)
# ---------------------------------------------------------------------------

engine: AsyncEngine = create_async_engine(
    SQLITE_URL,
    echo=False,           # set True to log SQL for debugging
    connect_args={
        "check_same_thread": False,
        "timeout": 30,
    },
)


# ---------------------------------------------------------------------------
# Session: FastAPI Depends (async generator) + direct use (context manager)
# ---------------------------------------------------------------------------

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Async generator — for use with FastAPI `Depends(get_session)`."""
    async with AsyncSession(engine, expire_on_commit=False) as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def open_session() -> AsyncGenerator[AsyncSession, None]:
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


# ---------------------------------------------------------------------------
# DB initialisation helpers (called from app lifespan)
# ---------------------------------------------------------------------------

async def apply_pragmas() -> None:
    """Set SQLite performance/safety PRAGMAs once per connection."""
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
        await conn.exec_driver_sql("PRAGMA foreign_keys=ON")
        await conn.exec_driver_sql("PRAGMA cache_size=-64000")   # 64 MB page cache
        await conn.exec_driver_sql("PRAGMA temp_store=MEMORY")


async def create_db_and_tables() -> None:
    """Create all SQLModel tables if they don't yet exist.

    In production this is handled by Alembic migrations; this function is
    kept as a fallback for fresh dev setups without running 'alembic upgrade'.
    """
    # Import models so SQLModel metadata is populated before create_all
    import api.models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
