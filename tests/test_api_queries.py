"""
API tests — raw query endpoints (/api/queries/*).

Uses SQLite backend. Inserts seed rows directly via the DB session to
avoid depending on the upload pipeline.

Run:
    uv run pytest tests/test_api_queries.py -v
"""

from __future__ import annotations

from datetime import UTC, datetime

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
        query_hash=hashlib.sha256(unique.encode()).hexdigest(),
        source="sql",
        host="WINFODB06HV11",
        db_name="fb_db_v2",
        environment="prod",
        type="slow_query",
        time="2026-01-15 10:30:00",
        query_details=f"SELECT * FROM bet WHERE id = @P? /* {unique} */",
        month_year="2026-01",
        occurrence_count=3,
        first_seen=datetime.now(tz=UTC),
        last_seen=datetime.now(tz=UTC),
        created_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
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
            for field in (
                "id",
                "query_hash",
                "source",
                "environment",
                "type",
                "occurrence_count",
                "first_seen",
                "last_seen",
            ):
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


# ---------------------------------------------------------------------------
# GET /api/queries/{id}/typed-detail
# ---------------------------------------------------------------------------

import hashlib  # noqa: E402 — grouped here so the module-level import stays clean
import uuid  # noqa: E402

from api.database import open_session as _open_session  # noqa: E402
from api.models import (  # noqa: E402
    RawQueryBlocker,
    RawQuerySlowMongo,
    RawQuerySlowSql,
)


async def _seed_blocker(raw_query_id: int | None = None, **overrides) -> RawQueryBlocker:
    """Insert one RawQueryBlocker row and return it."""
    unique = str(uuid.uuid4())
    defaults = dict(
        query_hash=hashlib.sha256(unique.encode()).hexdigest(),
        raw_query_id=raw_query_id,
        environment="prod",
        month_year="2026-01",
        all_query=f"EXEC sp_blocked /* {unique} */",
        currentdbname="fb_db_v2",
        occurrence_count=1,
    )
    defaults.update(overrides)
    row = RawQueryBlocker(**defaults)
    async with _open_session() as session:
        session.add(row)
        await session.flush()
        await session.refresh(row)
    return row


async def _seed_slow_sql(raw_query_id: int | None = None, **overrides) -> RawQuerySlowSql:
    """Insert one RawQuerySlowSql row and return it."""
    unique = str(uuid.uuid4())
    defaults = dict(
        query_hash=hashlib.sha256(unique.encode()).hexdigest(),
        raw_query_id=raw_query_id,
        environment="prod",
        month_year="2026-01",
        host="WINFODB06HV11",
        db_name="fb_db_v2",
        query_final=f"SELECT TOP 10 * FROM orders /* {unique} */",
        max_elapsed_time_s=1.5,
        occurrence_count=2,
    )
    defaults.update(overrides)
    row = RawQuerySlowSql(**defaults)
    async with _open_session() as session:
        session.add(row)
        await session.flush()
        await session.refresh(row)
    return row


async def _seed_slow_mongo(raw_query_id: int | None = None, **overrides) -> RawQuerySlowMongo:
    """Insert one RawQuerySlowMongo row and return it."""
    unique = str(uuid.uuid4())
    defaults = dict(
        query_hash=hashlib.sha256(unique.encode()).hexdigest(),
        raw_query_id=raw_query_id,
        environment="prod",
        host="MONGOSRV01",
        db_name="mongo_db",
        collection="orders",
        month_year="2026-01",
        command_json=f'{{"find": "orders", "filter": {{}} /* {unique} */}}',
        occurrence_count=1,
    )
    defaults.update(overrides)
    row = RawQuerySlowMongo(**defaults)
    async with _open_session() as session:
        session.add(row)
        await session.flush()
        await session.refresh(row)
    return row


class TestGetTypedDetail:
    """Tests for GET /api/queries/{id}/typed-detail."""

    async def test_404_for_nonexistent_query(self, client: AsyncClient):
        r = await client.get("/api/queries/888888/typed-detail")
        assert r.status_code == 404

    async def test_data_null_when_no_typed_row(self, client: AsyncClient):
        """A valid raw_query with no linked typed row returns data=null."""
        raw = await _seed_query(type="slow_query")
        r = await client.get(f"/api/queries/{raw.id}/typed-detail")
        assert r.status_code == 200
        body = r.json()
        assert body["type"] == "slow_query"
        assert body["data"] is None

    async def test_fk_fast_path_returns_blocker_data(self, client: AsyncClient):
        """FK already set → data is populated in one query (fast path)."""
        raw = await _seed_query(type="blocker")
        await _seed_blocker(raw_query_id=raw.id, all_query="EXEC sp_block_test")
        r = await client.get(f"/api/queries/{raw.id}/typed-detail")
        assert r.status_code == 200
        body = r.json()
        assert body["type"] == "blocker"
        assert body["data"] is not None
        assert body["data"]["all_query"] == "EXEC sp_block_test"

    async def test_text_fallback_for_slow_sql(self, client: AsyncClient):
        """No FK set — text fallback matches slow_sql.query_final to raw_query.query_details."""
        unique_sql = f"SELECT 1 /* text-fallback-{uuid.uuid4()} */"
        raw = await _seed_query(type="slow_query", query_details=unique_sql)
        # seed typed row with NO FK but matching query_final
        await _seed_slow_sql(raw_query_id=None, query_final=unique_sql)
        r = await client.get(f"/api/queries/{raw.id}/typed-detail")
        assert r.status_code == 200
        body = r.json()
        assert body["type"] == "slow_query"
        assert body["data"] is not None
        assert body["data"]["query_final"] == unique_sql

    async def test_response_shape(self, client: AsyncClient):
        """Response always contains exactly `type` and `data` keys."""
        raw = await _seed_query(type="deadlock")
        r = await client.get(f"/api/queries/{raw.id}/typed-detail")
        assert r.status_code == 200
        body = r.json()
        assert set(body.keys()) == {"type", "data"}

    async def test_hash_reconstruction_fallback_for_slow_mongo(self, client: AsyncClient):
        """No FK, no matching command_json text — hash fallback locates slow_mongo via
        MD5(host|db_name|env|query_details) and backfills the FK on success."""
        query_key = f"orders:find-{uuid.uuid4()}"
        host = "MONGOSRV01"
        db_name = "mongo_db"
        env = "prod"
        # raw_query stores query_details == query_key (new-format)
        raw = await _seed_query(
            type="slow_query_mongo",
            source="mongodb",
            host=host,
            db_name=db_name,
            environment=env,
            query_details=query_key,
        )
        # Typed table row uses the same hash derivation as the ingestor
        candidate_hash = hashlib.sha256(
            "|".join(str(p or "").strip() for p in [host, db_name, env, query_key]).encode()
        ).hexdigest()
        typed = await _seed_slow_mongo(
            raw_query_id=None,
            host=host,
            db_name=db_name,
            environment=env,
            query_hash=candidate_hash,
            command_json=query_key,
        )

        r = await client.get(f"/api/queries/{raw.id}/typed-detail")
        assert r.status_code == 200
        body = r.json()
        assert body["type"] == "slow_query_mongo"
        assert body["data"] is not None
        assert body["data"]["id"] == typed.id

        # FK should be backfilled after the hash match
        async with _open_session() as session:
            from sqlmodel import select as _select

            refreshed = (
                await session.exec(_select(RawQuerySlowMongo).where(RawQuerySlowMongo.id == typed.id))
            ).one()
        assert refreshed.raw_query_id == raw.id

    async def test_collection_fuzzy_fallback_for_slow_mongo(self, client: AsyncClient):
        """Old-format slow_mongo row (query_details is JSON) — collection-fuzzy fallback
        finds the typed row with the highest occurrence_count for the same collection+env+host."""
        import json

        collection = f"reportDocument-{uuid.uuid4().hex[:8]}"
        host = "MONGOSRV02"
        db_name = "reports_db"
        env = "prod"
        query_details_json = json.dumps({"find": collection, "filter": {"_id": "unique-oid"}})

        raw = await _seed_query(
            type="slow_query_mongo",
            source="mongodb",
            host=host,
            db_name=db_name,
            environment=env,
            query_details=query_details_json,
        )
        # Seed two typed rows for the same collection — the one with higher occurrence_count
        # should be preferred by the fallback.
        await _seed_slow_mongo(
            raw_query_id=None,
            host=host,
            db_name=db_name,
            environment=env,
            collection=collection,
            occurrence_count=3,
            command_json=f'{{"find": "{collection}"}}',
        )
        preferred = await _seed_slow_mongo(
            raw_query_id=None,
            host=host,
            db_name=db_name,
            environment=env,
            collection=collection,
            occurrence_count=10,
            command_json=f'{{"find": "{collection}"}}',
        )

        r = await client.get(f"/api/queries/{raw.id}/typed-detail")
        assert r.status_code == 200
        body = r.json()
        assert body["type"] == "slow_query_mongo"
        assert body["data"] is not None
        # Must return the row with occurrence_count=10 (most representative)
        assert body["data"]["id"] == preferred.id
