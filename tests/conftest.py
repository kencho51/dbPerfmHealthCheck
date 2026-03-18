"""
Root conftest — ensure the project root is on sys.path and wire up an
in-memory SQLite engine for the test suite.

The patching MUST happen before any `api.*` module imports its own `engine`
reference, so we do it at module load time (not inside a fixture).
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel

# Make the project root importable as a package root
sys.path.insert(0, str(Path(__file__).parent.parent))

# ---------------------------------------------------------------------------
# Patch the module-level `engine` in api.database BEFORE any test imports
# the app or routers (they all call `get_session()` which closes over `engine`).
# ---------------------------------------------------------------------------

_TEST_DB_URL = (
    "sqlite+aiosqlite:///file:testmemdb?mode=memory&cache=shared&uri=true"
)

import api.database as _db_mod  # noqa: E402  (must be after sys.path insert)

_test_engine = create_async_engine(
    _TEST_DB_URL,
    echo=False,
    connect_args={"check_same_thread": False},
)

# Swap out the real engine so every part of the app that reads `api.database.engine`
# (including `apply_pragmas`, `create_db_and_tables`, and `get_session`) uses the
# in-memory engine instead.
_db_mod.engine = _test_engine
_db_mod.SQLITE_URL = _TEST_DB_URL


# ---------------------------------------------------------------------------
# Session-scoped fixture: create (and later dispose) all tables once per run
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _db_tables():
    """Create all SQLModel tables on the shared in-memory SQLite engine."""
    import api.models  # noqa: F401 — ensure all table classes are registered

    async def _up() -> None:
        async with _test_engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.drop_all)
            await conn.run_sync(SQLModel.metadata.create_all)

    asyncio.run(_up())
    yield
    asyncio.run(_test_engine.dispose())
