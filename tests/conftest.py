"""
Shared pytest fixtures for the dbPerfmHealthCheck test suite.

IMPORTANT — environment variables MUST be set before any api.* imports
because api/database.py reads DB_BACKEND / SQLITE_URL at module level.

Run all tests from the project root:
    uv run pytest tests/ -v

Run a single file:
    uv run pytest tests/test_api_auth.py -v
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# ── Env vars — set FIRST, before any api.* imports ────────────────────────
_ROOT = Path(__file__).parent.parent

# Shared in-memory SQLite: all connections in this process see the same DB,
# no file is written to disk, nothing to lock or delete after the session.
_SQLITE_MEM_URL = "sqlite+aiosqlite:///file:testmemdb?mode=memory&cache=shared&uri=true"

os.environ["DB_BACKEND"] = "sqlite"
os.environ["SQLITE_URL"] = _SQLITE_MEM_URL
os.environ["DATABASE_URL"] = _SQLITE_MEM_URL
os.environ.setdefault("JWT_SECRET", "pytest-only-secret-32-chars-not-for-prod!!")
os.environ.setdefault("NEON_SQL_PASS", "not-used-in-tests")

sys.path.insert(0, str(_ROOT))

# ── Standard imports (after env setup) ────────────────────────────────────
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel

import api.models  # noqa: F401 — registers all SQLModel table definitions


# ── Session-scoped DB setup (sync wrapper around async) ───────────────────

@pytest.fixture(scope="session", autouse=True)
def _db_tables():
    """Create all SQLModel tables in the shared in-memory SQLite database.

    Uses 'cache=shared' URI so every connection in this process sees the same
    data. No file is written; nothing to clean up on teardown.
    """
    engine = create_async_engine(_SQLITE_MEM_URL, echo=False)

    async def _up():
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.drop_all)
            await conn.run_sync(SQLModel.metadata.create_all)

    async def _down():
        await engine.dispose()

    asyncio.run(_up())
    yield
    asyncio.run(_down())


# ── App fixture (session-scoped — one instance for all tests) ─────────────

@pytest.fixture(scope="session")
def app():
    from api.main import create_app
    return create_app()


# ── Async HTTP client (function-scoped — fresh per test) ──────────────────

@pytest.fixture
async def client(app):
    """Async HTTPX client wired to the FastAPI app over ASGI (no network)."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        timeout=10.0,
    ) as ac:
        yield ac


# ── Auth helpers ───────────────────────────────────────────────────────────

_ADMIN_CREDS = {
    "username": "testadmin",
    "email": "admin@test.local",
    "password": "AdminPass123!",
}


@pytest.fixture
async def admin_token(client: AsyncClient) -> str:
    """Register the first admin (open registration) and return a JWT.
    Subsequent calls re-use existing credentials (403 on duplicate ignored)."""
    await client.post("/api/auth/register", json=_ADMIN_CREDS)
    r = await client.post("/api/auth/login", json={
        "username": _ADMIN_CREDS["username"],
        "password": _ADMIN_CREDS["password"],
    })
    assert r.status_code == 200, f"Login failed: {r.text}"
    return r.json()["access_token"]


@pytest.fixture
async def auth_headers(admin_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {admin_token}"}


# ── Sample data ────────────────────────────────────────────────────────────

@pytest.fixture
def sample_raw_row() -> dict:
    """Minimal dict matching the RawQuery ingest schema."""
    return {
        "source": "sql",
        "host": "WINFODB06HV11",
        "db_name": "fb_db_v2",
        "environment": "prod",
        "type": "slow_query",
        "time": "2026-01-15 10:30:00",
        "query_details": "SELECT * FROM wagering.bet WHERE betId = @P?",
        "occurrence_count": 1,
    }
