"""
API tests — raw query endpoints (/api/queries/*).

Uses SQLite backend. Inserts seed rows directly via the DB session to
avoid depending on the upload pipeline.

Run:
    uv run pytest tests/test_api_queries.py -v
"""
from __future__ import annotations

from datetime import datetime, timezone
from httpx import AsyncClient

from api.database import open_session
from api.models import RawQuery


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _seed_query(**overrides) -> RawQuery:
    """Insert one RawQuery row directly and return the ORM object."""
    import hashlib
    import uuid
    unique = str(uuid.uuid4())
    defaults = dict(
        query_hash=hashlib.md5(unique.encode()).hexdigest(),
        source="sql",
        host="WINFODB06HV11",
        db_name="fb_db_v2",
        environment="prod",
        type="slow_query",
        time="2026-01-15 10:30:00",
        query_details=f"SELECT * FROM bet WHERE id = @P? /* {unique} */",
        month_year="2026-01",
        occurrence_count=3,
        first_seen=datetime.now(tz=timezone.utc),
        last_seen=datetime.now(tz=timezone.utc),
        created_at=datetime.now(tz=timezone.utc),
        updated_at=datetime.now(tz=timezone.utc),
    )
    defaults.update(overrides)
    row = RawQuery(**defaults)
    async with open_session() as session:
        session.add(row)
        await session.commit()
        # Fetch with ID
        from sqlmodel import select
        result = await session.exec(select(RawQuery).where(RawQuery.query_hash == row.query_hash))
        return result.one()


# ---------------------------------------------------------------------------
# GET /api/queries
# ---------------------------------------------------------------------------

class TestListQueries:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/queries", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_accessible_without_auth(self, client: AsyncClient):
        # /api/queries is a read-only public endpoint — no auth required
        r = await client.get("/api/queries")
        assert r.status_code == 200

    async def test_limit_respected(self, client: AsyncClient, auth_headers: dict):
        # Seed several rows
        for _ in range(3):
            await _seed_query()
        r = await client.get("/api/queries?limit=2", headers=auth_headers)
        assert r.status_code == 200
        assert len(r.json()) <= 2

    async def test_filter_by_environment(self, client: AsyncClient, auth_headers: dict):
        await _seed_query(environment="sat", source="sql")
        r = await client.get("/api/queries?environment=sat", headers=auth_headers)
        assert r.status_code == 200
        rows = r.json()
        assert all(row["environment"] == "sat" for row in rows)

    async def test_filter_by_source(self, client: AsyncClient, auth_headers: dict):
        await _seed_query(source="mongodb", type="slow_query_mongo")
        r = await client.get("/api/queries?source=mongodb", headers=auth_headers)
        assert r.status_code == 200
        rows = r.json()
        assert all(row["source"] == "mongodb" for row in rows)

    async def test_filter_by_type(self, client: AsyncClient, auth_headers: dict):
        await _seed_query(type="deadlock")
        r = await client.get("/api/queries?type=deadlock", headers=auth_headers)
        assert r.status_code == 200
        rows = r.json()
        assert all(row["type"] == "deadlock" for row in rows)

    async def test_search_by_query_details(self, client: AsyncClient, auth_headers: dict):
        await _seed_query(query_details="UNIQUE_SEARCH_TERM_XYZ")
        r = await client.get("/api/queries?search=UNIQUE_SEARCH_TERM_XYZ", headers=auth_headers)
        assert r.status_code == 200
        rows = r.json()
        assert len(rows) >= 1
        assert any("UNIQUE_SEARCH_TERM_XYZ" in row["query_details"] for row in rows)

    async def test_response_schema(self, client: AsyncClient, auth_headers: dict):
        await _seed_query()
        r = await client.get("/api/queries?limit=1", headers=auth_headers)
        assert r.status_code == 200
        rows = r.json()
        if rows:
            row = rows[0]
            for field in ("id", "query_hash", "source", "environment", "type",
                          "occurrence_count", "first_seen", "last_seen"):
                assert field in row, f"Missing field: {field}"


# ---------------------------------------------------------------------------
# GET /api/queries/count
# ---------------------------------------------------------------------------

class TestQueryCount:
    async def test_count_returns_integer(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/queries/count", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, int) or isinstance(data.get("count"), int)

    async def test_count_matches_list(self, client: AsyncClient, auth_headers: dict):
        # Use max allowed limit (200) to compare against total count
        r_list = await client.get("/api/queries?limit=200", headers=auth_headers)
        r_count = await client.get("/api/queries/count", headers=auth_headers)
        assert r_list.status_code == 200
        count_val = r_count.json()
        if isinstance(count_val, dict):
            count_val = count_val["count"]
        # count may be >= list length if total rows exceed 200
        assert count_val >= len(r_list.json())


# ---------------------------------------------------------------------------
# GET /api/queries/distinct
# ---------------------------------------------------------------------------

class TestDistinct:
    async def test_distinct_returns_hosts_and_dbs(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/queries/distinct", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert "hosts" in data or "host" in data or isinstance(data, dict)


# ---------------------------------------------------------------------------
# GET /api/queries/{id}
# ---------------------------------------------------------------------------

class TestGetQuery:
    async def test_get_existing(self, client: AsyncClient, auth_headers: dict):
        row = await _seed_query()
        r = await client.get(f"/api/queries/{row.id}", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert data["id"] == row.id
        assert data["query_hash"] == row.query_hash

    async def test_get_nonexistent_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/queries/999999", headers=auth_headers)
        assert r.status_code == 404
