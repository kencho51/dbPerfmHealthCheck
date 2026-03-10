"""
PostgreSQL (Neon) engine and async session factory.

Connection string is read from  api/.env  (DATABASE_URL).
The driver chain is:
  - Runtime   : asyncpg  (postgresql+asyncpg://)
  - Alembic   : psycopg2 (handled in api/migrations/env.py)

The public interface is intentionally identical to the previous SQLite version
so that routers and services require no changes:
    get_session()          — FastAPI Depends generator
    open_session()         — async context manager for scripts / tests
    create_db_and_tables() — idempotent schema bootstrap (Alembic owns this in prod)

Start the dev server from the project root:
    uv run uvicorn api.main:app --port 8000 --reload
"""
from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

# ---------------------------------------------------------------------------
# Load environment variables from api/.env (no-op if already loaded / not found)
# ---------------------------------------------------------------------------
_ENV_FILE = Path(__file__).parent / ".env"
load_dotenv(_ENV_FILE)

# ---------------------------------------------------------------------------
# Resolve PostgreSQL connection URL
# ---------------------------------------------------------------------------
_raw_url: str = os.environ.get("DATABASE_URL", "")
if not _raw_url:
    raise RuntimeError(
        "DATABASE_URL is not set. "
        "Create api/.env with DATABASE_URL=postgresql://... "
        "or export the variable in your shell before starting the server."
    )

# asyncpg requires the  postgresql+asyncpg://  scheme.
# asyncpg does NOT accept sslmode= or channel_binding= as URL query params —
# strip them and pass ssl=True via connect_args instead.
def _build_async_url(raw: str) -> tuple[str, dict]:
    """Return (async_url, connect_args) for asyncpg, handling sslmode/channel_binding."""
    pg_url = raw.replace("postgresql://", "postgresql+asyncpg://", 1)
    parsed = urlparse(pg_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    ssl_needed = params.pop("sslmode", ["disable"])[0].lower() in ("require", "verify-ca", "verify-full")
    params.pop("channel_binding", None)   # asyncpg does not support this param
    clean_query = urlencode({k: v[0] for k, v in params.items()})
    clean_url = urlunparse(parsed._replace(query=clean_query))
    connect_args = {"ssl": ssl_needed} if ssl_needed else {}
    return clean_url, connect_args

ASYNC_DATABASE_URL, _connect_args = _build_async_url(_raw_url)

# ---------------------------------------------------------------------------
# Engine — created once at import time; reused across requests
# ---------------------------------------------------------------------------
engine: AsyncEngine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=False,        # set True to log SQL for debugging
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # discard stale connections from the pool
    connect_args=_connect_args,
)


# ---------------------------------------------------------------------------
# Session: FastAPI Depends (async generator)
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


# ---------------------------------------------------------------------------
# Session: direct programmatic use (scripts, tests, services)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def open_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager — for scripts / tests outside the FastAPI lifecycle.

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
async def create_db_and_tables() -> None:
    """
    Create all SQLModel tables if they don't yet exist.

    In production, Alembic manages schema; this is kept as a dev convenience
    so a fresh checkout can start without running 'alembic upgrade head'.
    """
    import api.models  # noqa: F401 — registers RawQuery + Pattern tables

    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
