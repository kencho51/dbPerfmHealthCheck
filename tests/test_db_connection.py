"""
Unit tests for the SQLite-backed analytics_db helpers.

All tests use an isolated in-memory SQLite database — no SQLITE_PATH env var
needed.  The module-level engine in api.database is already patched to the
shared in-memory engine by conftest.py.

Run:
    uv run pytest tests/test_db_connection.py -v
"""

from __future__ import annotations

import polars as pl
import pytest
import sqlalchemy as sa
from sqlmodel import Session

# ---------------------------------------------------------------------------
# Helpers — seed data into the shared in-memory DB for these tests
# ---------------------------------------------------------------------------


def _sync_url() -> str:
    """Return a synchronous SQLite URL derived from the patched database module."""
    from api.database import SQLITE_URL

    return str(SQLITE_URL).replace("sqlite+aiosqlite", "sqlite")


@pytest.fixture()
def seeded_raw_query():
    """Insert two rows into raw_query via ORM (so Python defaults are applied)."""
    from api.models import EnvironmentType, QueryType, RawQuery, SourceType

    engine = sa.create_engine(_sync_url())
    rows = [
        RawQuery(
            query_hash="aaa",
            query_details="SELECT 1",
            type=QueryType.slow_query,
            host="DB01",
            source=SourceType.sql,
            environment=EnvironmentType.prod,
        ),
        RawQuery(
            query_hash="bbb",
            query_details="SELECT 2",
            type=QueryType.blocker,
            host="DB02",
            source=SourceType.sql,
            environment=EnvironmentType.prod,
        ),
    ]
    with Session(engine) as session:
        session.add_all(rows)
        session.commit()

    # Evict the DataFrame cache so _load_table re-reads from SQLite and sees
    # the freshly inserted rows (the TTL cache may hold a stale snapshot from a
    # previously-run test in the same session).
    from api.analytics_db import invalidate_cache
    invalidate_cache("raw_query")

    yield

    with engine.begin() as conn:
        conn.execute(sa.text("DELETE FROM raw_query WHERE query_hash IN ('aaa','bbb')"))
    invalidate_cache("raw_query")


# ---------------------------------------------------------------------------
# _load_table
# ---------------------------------------------------------------------------


class TestLoadTable:
    def test_unknown_table_raises(self):
        from api.analytics_db import _load_table

        with pytest.raises(ValueError, match="Unknown table"):
            _load_table("nonexistent_table")

    def test_returns_dataframe(self, seeded_raw_query):
        from api.analytics_db import _load_table

        df = _load_table("raw_query")
        assert isinstance(df, pl.DataFrame)
        assert "query_hash" in df.columns

    def test_has_seeded_rows(self, seeded_raw_query):
        from api.analytics_db import _load_table

        df = _load_table("raw_query")
        hashes = df["query_hash"].to_list()
        assert "aaa" in hashes
        assert "bbb" in hashes

    def test_empty_table_returns_empty_dataframe(self):
        """curated_query is empty in a fresh test DB — should not error."""
        from api.analytics_db import _load_table

        df = _load_table("curated_query")
        assert isinstance(df, pl.DataFrame)


# ---------------------------------------------------------------------------
# get_duck
# ---------------------------------------------------------------------------


class TestGetDuck:
    def test_returns_duckdb_connection(self, seeded_raw_query):
        from api.analytics_db import _DuckNoClose, get_duck

        con = get_duck("raw_query")
        # get_duck() returns a _DuckNoClose proxy (Phase 2 singleton refactor).
        # Verify it is that proxy and that it exposes the DuckDB execute interface.
        assert isinstance(con, _DuckNoClose)
        assert hasattr(con, "execute")
        con.close()

    def test_table_queryable(self, seeded_raw_query):
        from api.analytics_db import get_duck

        con = get_duck("raw_query")
        result = con.execute("SELECT COUNT(*) AS n FROM raw_query").fetchone()
        assert result[0] >= 2
        con.close()

    def test_defaults_to_raw_query(self, seeded_raw_query):
        from api.analytics_db import get_duck

        con = get_duck()  # no table arg → defaults to raw_query
        result = con.execute("SELECT 1").fetchone()
        assert result[0] == 1
        con.close()


# ---------------------------------------------------------------------------
# build_where
# ---------------------------------------------------------------------------


class TestBuildWhere:
    def _bw(self, clauses):
        from api.analytics_db import build_where

        return build_where(clauses)

    def test_empty_clauses(self):
        sql, params = self._bw([])
        assert sql == ""
        assert params == []

    def test_single_clause(self):
        sql, params = self._bw([("host = ?", "DB01")])
        assert "WHERE" in sql
        assert "host = ?" in sql
        assert params == ["DB01"]

    def test_multiple_clauses_joined_with_and(self):
        sql, params = self._bw([("host = ?", "DB01"), ("query_type = ?", "slow_query")])
        assert "AND" in sql
        assert params == ["DB01", "slow_query"]
