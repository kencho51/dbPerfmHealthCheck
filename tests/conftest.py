"""
Root conftest — project root on sys.path, in-memory SQLite engine, and shared
HTTP client / auth fixtures for all API tests.

Design decisions
----------------
* **Named shared in-memory SQLite + NullPool**: pure in-memory, no files on disk,
  clean state per test run. `NullPool` means each engine.begin() opens a fresh
  OS connection — no asyncio state stored in the pool, so reuse across the
  session-fixture event loop and per-test event loops is safe.
* **Holder connection**: a synchronous `sqlite3` connection opened at module load
  keeps the named in-memory database (`file:testmemdb_pytest?...`) alive between
  the session-fixture `asyncio.run()` call (one event loop) and the per-test
  coroutines (another event loop). Without it, NullPool would let the last
  connection close and the in-memory DB would be destroyed.
* **Lifespan NOT triggered**: httpx ASGITransport does not send lifespan events,
  so `apply_pragmas` / `create_db_and_tables` run only inside `_db_tables`.
* Session fixtures that need async work use `asyncio.run()` (sync fixture) so
  pytest-asyncio's per-test event loop is not required for setup.
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel

# ---------------------------------------------------------------------------
# Project root on sys.path
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).parent.parent))

# ---------------------------------------------------------------------------
# Named shared in-memory SQLite
# ---------------------------------------------------------------------------

_MEM_DB_NAME = "testmemdb_pytest"
_TEST_DB_URL = f"sqlite+aiosqlite:///file:{_MEM_DB_NAME}?mode=memory&cache=shared&uri=true"

# One synchronous holder connection keeps the named in-memory database alive
# for the entire pytest process — no asyncio binding, no file created.
_holder_conn: sqlite3.Connection = sqlite3.connect(
    f"file:{_MEM_DB_NAME}?mode=memory&cache=shared",
    uri=True,
    check_same_thread=False,
)

import api.database as _db_mod  # noqa: E402

_test_engine = create_async_engine(
    _TEST_DB_URL,
    echo=False,
    poolclass=NullPool,
    connect_args={"check_same_thread": False},
)

# Patch module-level globals BEFORE any router/service imports
_db_mod.engine = _test_engine
_db_mod.SQLITE_URL = _TEST_DB_URL
_db_mod.SQLITE_PATH = Path(f":{_MEM_DB_NAME}:")  # informational only
_db_mod.DB_BACKEND = "sqlite"  # ensure tests always report sqlite

# ---------------------------------------------------------------------------
# App (created once; lifespan NOT triggered by ASGITransport)
# ---------------------------------------------------------------------------

from api.main import create_app as _create_app  # noqa: E402

_app = _create_app()

# ---------------------------------------------------------------------------
# Session fixture: create tables once for the whole test run
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _db_tables():
    """Drop/create all SQLModel tables in the shared in-memory database."""
    import api.models  # noqa: F401

    async def _up() -> None:
        async with _test_engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.drop_all)
            await conn.run_sync(SQLModel.metadata.create_all)

    asyncio.run(_up())
    yield

    async def _down() -> None:
        await _test_engine.dispose()

    asyncio.run(_down())
    _holder_conn.close()  # release the in-memory DB


# ---------------------------------------------------------------------------
# Session fixture: seed one admin user (bcrypt is slow — compute once)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _admin_user(_db_tables):
    """Insert testadmin into the in-memory DB (no-op if already exists)."""
    from sqlmodel import select

    from api.database import open_session
    from api.models import User, UserRole
    from api.services.auth_service import hash_password

    async def _create() -> None:
        async with open_session() as session:
            existing = (
                await session.exec(select(User).where(User.username == "testadmin"))
            ).first()
            if existing is None:
                session.add(
                    User(
                        username="testadmin",
                        email="admin@test.local",
                        hashed_password=hash_password("AdminPass123!"),
                        role=UserRole.admin,
                        is_active=True,
                    )
                )

    asyncio.run(_create())


# ---------------------------------------------------------------------------
# Session fixture: JWT token for testadmin (computed once)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def admin_token(_admin_user) -> str:
    """Return a valid JWT access token for testadmin."""

    async def _login() -> str:
        async with AsyncClient(
            transport=ASGITransport(app=_app),
            base_url="http://test",
        ) as ac:
            r = await ac.post(
                "/api/auth/login",
                json={"username": "testadmin", "password": "AdminPass123!"},
            )
            assert r.status_code == 200, f"Admin login failed: {r.status_code} {r.text}"
            return r.json()["access_token"]

    return asyncio.run(_login())


# ---------------------------------------------------------------------------
# Per-test fixtures: HTTP client and auth headers
# ---------------------------------------------------------------------------


@pytest.fixture()
async def client() -> AsyncClient:
    """Async HTTP client bound to the FastAPI app via ASGI (no real socket)."""
    async with AsyncClient(
        transport=ASGITransport(app=_app),
        base_url="http://test",
    ) as ac:
        yield ac


@pytest.fixture()
def auth_headers(admin_token: str) -> dict:
    """HTTP headers with Bearer token for authenticated requests."""
    return {"Authorization": f"Bearer {admin_token}"}
