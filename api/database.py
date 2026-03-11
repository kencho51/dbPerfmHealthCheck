"""
Database session factory — supports two backends selected by DB_BACKEND env var:

  DB_BACKEND=neon   (default) — all SQL via Neon HTTPS REST API (port 443).
                                 Port 5432 is blocked by the corporate proxy.
  DB_BACKEND=sqlite            — local SQLite file (SQLITE_URL or ./master.db).
                                 Useful for offline development / unit tests.

Public interface:
    get_session()       — FastAPI Depends generator
    open_session()      — async context manager for scripts / tests
    DB_BACKEND          — "neon" | "sqlite"  (for /health display)
    ASYNC_DATABASE_URL  — sanitised connection URL  (for /health display)
"""
from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

from dotenv import load_dotenv
from sqlalchemy import (
    delete as sa_delete,
    inspect as sa_inspect,
    update as sa_update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ---------------------------------------------------------------------------
# Load environment variables from api/.env
# ---------------------------------------------------------------------------
_ENV_FILE = Path(__file__).parent / ".env"
load_dotenv(_ENV_FILE)

DB_BACKEND: str = os.environ.get("DB_BACKEND", "neon").lower().strip()

if DB_BACKEND not in ("neon", "sqlite"):
    raise RuntimeError(f"DB_BACKEND must be 'neon' or 'sqlite', got: {DB_BACKEND!r}")


def _build_pg_url(raw: str) -> str:
    """Strip channel_binding= (unsupported by psycopg2) and normalise scheme."""
    url = raw.replace("postgresql://", "postgresql+psycopg2://", 1)
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("channel_binding", None)
    clean_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=clean_query))


# ---------------------------------------------------------------------------
# Backend-specific setup
# ---------------------------------------------------------------------------
if DB_BACKEND == "neon":
    _raw_url: str = os.environ.get("DATABASE_URL", "")
    if not _raw_url:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Create api/.env with DATABASE_URL=postgresql://..."
        )
    ASYNC_DATABASE_URL: str = _build_pg_url(_raw_url)
else:  # sqlite
    _sqlite_raw = os.environ.get("SQLITE_URL", "sqlite+aiosqlite:///./master.db")
    ASYNC_DATABASE_URL = _sqlite_raw

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlmodel.ext.asyncio.session import AsyncSession as _SQLModelAsyncSession
    from sqlalchemy.orm import sessionmaker as _sessionmaker

    _sqlite_engine = create_async_engine(ASYNC_DATABASE_URL, echo=False)
    _SQLiteSessionFactory = _sessionmaker(
        _sqlite_engine,
        class_=_SQLModelAsyncSession,
        expire_on_commit=False,
    )


# ---------------------------------------------------------------------------
# NeonSession — async session backed entirely by the Neon HTTPS REST API
# ---------------------------------------------------------------------------

class NeonSession:
    """
    Async DB session that routes every SQL statement through Neon's management
    REST API (HTTPS port 443).  No port 5432 required.

    Full interface used by the routers:
        await session.exec(select(...))     → _NeonResult (.all, .one, .first …)
        await session.execute(dml_stmt)     → _NeonExecResult (.rowcount)
        await session.get(Model, pk)        → model instance or None
        session.add(obj)                    → queue INSERT/UPDATE for commit
        await session.delete(obj)           → queue DELETE for commit
        await session.refresh(obj)          → no-op (obj is complete after commit)
        await session.commit()              → flush queued DML via HTTPS
        await session.rollback()            → discard queued DML
        await session.close()               → no-op
    """

    def __init__(self) -> None:
        from api.neon_http import NeonHTTPSession
        self._http              = NeonHTTPSession()
        self._pending_add: list    = []
        self._pending_delete: list = []

    # --- reads ---------------------------------------------------------------

    async def exec(self, stmt):
        return await self._http.exec(stmt)

    async def execute(self, stmt):
        return await self._http.execute(stmt)

    async def get(self, model, pk):
        return await self._http.get(model, pk)

    # --- write queuing -------------------------------------------------------

    def add(self, obj) -> None:
        """Queue an object for INSERT (pk=None) or UPDATE (pk already set)."""
        self._pending_add.append(obj)

    async def delete(self, obj) -> None:
        """Queue an object for DELETE on next commit()."""
        self._pending_delete.append(obj)

    async def refresh(self, obj) -> None:
        """
        No-op in the HTTPS session.

        After commit(), newly-inserted objects already have their PK set
        (via RETURNING) and all fields from construction are intact.
        Updated objects carry all fields from the preceding .get() call.
        """

    # --- transaction control -------------------------------------------------

    async def commit(self) -> None:
        """Flush queued DELETEs then ADDs to Neon via HTTPS REST API."""

        for obj in self._pending_delete:
            mapper     = sa_inspect(type(obj))
            table      = mapper.local_table
            pk_cols    = list(mapper.primary_key)
            conditions = [table.c[c.key] == getattr(obj, c.key) for c in pk_cols]
            await self._http.execute(sa_delete(table).where(*conditions))
        self._pending_delete.clear()

        for obj in self._pending_add:
            mapper   = sa_inspect(type(obj))
            table    = mapper.local_table
            pk_col   = next(iter(mapper.primary_key))
            pk_names = {c.key for c in mapper.primary_key}
            pk_val   = getattr(obj, pk_col.key, None)

            if pk_val is None:
                # INSERT ... RETURNING pk — populates obj.id after insert
                data = {
                    col.key: getattr(obj, col.key, None)
                    for col in mapper.columns
                    if col.key not in pk_names
                }
                stmt   = pg_insert(table).values(**data).returning(table.c[pk_col.key])
                result = await self._http.exec(stmt)
                row    = result.first()
                if row:
                    setattr(obj, pk_col.key, row[0])
            else:
                # UPDATE all non-PK columns
                data = {
                    col.key: getattr(obj, col.key, None)
                    for col in mapper.columns
                    if col.key not in pk_names
                }
                pk_conds = [
                    table.c[c.key] == getattr(obj, c.key)
                    for c in mapper.primary_key
                ]
                await self._http.execute(sa_update(table).where(*pk_conds).values(**data))

        self._pending_add.clear()

    async def rollback(self) -> None:
        self._pending_add.clear()
        self._pending_delete.clear()

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> "NeonSession":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()


# Backward-compat alias (ingestor.py type-hints Psycopg2Session)
Psycopg2Session = NeonSession


# ---------------------------------------------------------------------------
# FastAPI Depends generator  — dispatches on DB_BACKEND
# ---------------------------------------------------------------------------

async def get_session() -> AsyncGenerator:  # type: ignore[type-arg]
    """
    Yield the active DB session.

    DB_BACKEND=neon   → NeonSession  (Neon HTTPS REST API)
    DB_BACKEND=sqlite → SQLModel AsyncSession (local SQLite via aiosqlite)
    """
    if DB_BACKEND == "neon":
        session = NeonSession()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
    else:
        async with _SQLiteSessionFactory() as session:  # type: ignore[operator]
            yield session


# ---------------------------------------------------------------------------
# Async context manager (scripts / tests / seed utilities)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def open_session() -> AsyncGenerator:  # type: ignore[type-arg]
    """
    Async context manager — for scripts / tests outside the FastAPI lifecycle.

    Usage:
        async with open_session() as session:
            result = await session.exec(select(RawQuery))
    """
    if DB_BACKEND == "neon":
        session = NeonSession()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
    else:
        async with _SQLiteSessionFactory() as session:  # type: ignore[operator]
            yield session


