"""
SQLite engine and async session factory.

The database file lives at  db/master.db  (relative to the project root).
Override at runtime with the SQLITE_PATH env var.

Usage in a FastAPI route:
    async with get_session() as session:
        result = await session.exec(select(RawQuery))
"""
from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

# ---------------------------------------------------------------------------
# Resolve DB path
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB = _PROJECT_ROOT / "db" / "master.db"

SQLITE_PATH: Path = Path(os.getenv("SQLITE_PATH", str(_DEFAULT_DB)))
SQLITE_URL: str = f"sqlite+aiosqlite:///{SQLITE_PATH}"

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
# Session dependency (use as async context manager or FastAPI Depends)
# ---------------------------------------------------------------------------

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield a SQLModel async session; commit on exit, rollback on error."""
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
