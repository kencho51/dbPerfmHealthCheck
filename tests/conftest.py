"""
Root conftest — project root on sys.path, temp-file SQLite engine, and shared
HTTP client / auth fixtures for all API tests.

Design decisions
----------------
* **File-based temp SQLite + NullPool**: avoids cross-event-loop connection-pool
  issues that occur with in-memory SQLite when session-scoped sync fixtures
  (asyncio.run) and function-scoped async tests use different event loops.
* **NullPool**: each engine.begin() opens a fresh OS connection; no asyncio
  state is stored in the pool, so reuse across event loops is safe.
* **Lifespan NOT triggered**: httpx ASGITransport does not send lifespan events,
  so apply_pragmas/create_db_and_tables run only in _db_tables (idempotent).
* Session-scoped expensive work (user creation, token fetch) uses asyncio.run()
  in sync fixtures to avoid needing a session-scoped event loop.
"""
from __future__ import annotations

import asyncio
import sys
import tempfile
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
# Temp-file SQLite engine (NullPool — no event-loop binding in pool)
# ---------------------------------------------------------------------------

_TMPDIR = Path(tempfile.mkdtemp(prefix="dbperfm_test_"))
_TEST_DB = _TMPDIR / "test.db"
_TEST_DB_URL = f"sqlite+aiosqlite:///{_TEST_DB}"

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
_db_mod.SQLITE_PATH = _TEST_DB

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
    """Drop/create all SQLModel tables on the temp SQLite file."""
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
    import shutil
    shutil.rmtree(_TMPDIR, ignore_errors=True)


# ---------------------------------------------------------------------------
# Session fixture: seed one admin user (bcrypt is slow — compute once)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _admin_user(_db_tables):
    """Insert testadmin into the test DB (no-op if already exists)."""
    from api.database import open_session  # uses patched engine
    from api.models import User, UserRole
    from api.services.auth_service import hash_password
    from sqlmodel import select

    async def _create() -> None:
        async with open_session() as session:
            existing = (
                await session.exec(select(User).where(User.username == "testadmin"))
            ).first()
            if existing is None:
                user = User(
                    username="testadmin",
                    email="admin@test.local",
                    hashed_password=hash_password("AdminPass123!"),
                    role=UserRole.admin,
                    is_active=True,
                )
                session.add(user)

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
