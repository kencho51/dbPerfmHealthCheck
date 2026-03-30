"""
API tests — curated query endpoints (/api/curated/*).

Tests full CRUD lifecycle: create, list, count, update, delete.
Seeds RawQuery rows directly via the DB session so the curated
endpoints have valid FKs to reference without relying on the upload pipeline.

Run:
    uv run pytest tests/test_api_curated.py -v
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

from httpx import AsyncClient

from api.database import open_session
from api.models import RawQuery

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_raw_query(**overrides) -> RawQuery:
    """Insert one RawQuery row and return it with its DB-assigned id."""
    unique = str(uuid.uuid4())
    defaults = dict(
        query_hash=hashlib.sha256(unique.encode()).hexdigest(),
        source="sql",
        host="WINFODB06HV11",
        db_name="fb_db_v2",
        environment="prod",
        type="slow_query",
        time="2026-01-15 10:30:00",
        query_details=f"SELECT * FROM bet WHERE id = 1 /* {unique} */",
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
        from sqlmodel import select

        result = await session.exec(select(RawQuery).where(RawQuery.query_hash == row.query_hash))
        return result.one()


# ---------------------------------------------------------------------------
# POST /api/curated  (create)
# ---------------------------------------------------------------------------


class TestCreateCurated:
    async def test_create_success(self, client: AsyncClient, auth_headers: dict):
        rq = await _seed_raw_query()
        r = await client.post(
            "/api/curated",
            json={
                "raw_query_id": rq.id,
                "notes": "Confirmed full table scan",
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        data = r.json()
        assert data["raw_query_id"] == rq.id
        assert data["notes"] == "Confirmed full table scan"
        assert "id" in data

    async def test_create_with_label(self, client: AsyncClient, auth_headers: dict):
        r_label = await client.post(
            "/api/labels",
            json={
                "name": f"CuratedTestLabel_{uuid.uuid4().hex[:8]}",
            },
        )
        assert r_label.status_code == 201
        label_id = r_label.json()["id"]

        rq = await _seed_raw_query()
        r = await client.post(
            "/api/curated",
            json={
                "raw_query_id": rq.id,
                "label_id": label_id,
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        assert r.json()["label_id"] == label_id

    async def test_create_duplicate_rejected_409(self, client: AsyncClient, auth_headers: dict):
        rq = await _seed_raw_query()
        await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        r2 = await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        assert r2.status_code == 409

    async def test_create_nonexistent_raw_query_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.post("/api/curated", json={"raw_query_id": 99999999}, headers=auth_headers)
        assert r.status_code == 404

    async def test_response_includes_raw_query_fields(
        self, client: AsyncClient, auth_headers: dict
    ):
        rq = await _seed_raw_query(host="TESTHOST01", db_name="test_db")
        r = await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        assert r.status_code == 201
        data = r.json()
        assert data["host"] == "TESTHOST01"
        assert data["db_name"] == "test_db"
        assert data["query_hash"] == rq.query_hash


# ---------------------------------------------------------------------------
# GET /api/curated  (list)
# ---------------------------------------------------------------------------


class TestListCurated:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/curated", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_created_entry_appears_in_list(self, client: AsyncClient, auth_headers: dict):
        unique_detail = f"UNIQUE_CURATED_SEARCH_{uuid.uuid4().hex[:8]}"
        rq = await _seed_raw_query(query_details=unique_detail)
        await client.post(
            "/api/curated", json={"raw_query_id": rq.id, "notes": "findme"}, headers=auth_headers
        )
        r = await client.get(f"/api/curated?search={unique_detail}", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert any(row["raw_query_id"] == rq.id for row in data)

    async def test_x_total_count_header_present(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/curated", headers=auth_headers)
        assert r.status_code == 200
        assert "x-total-count" in r.headers

    async def test_schema(self, client: AsyncClient, auth_headers: dict):
        rq = await _seed_raw_query()
        await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        r = await client.get("/api/curated?limit=1", headers=auth_headers)
        rows = r.json()
        if rows:
            row = rows[0]
            for field in (
                "id",
                "raw_query_id",
                "query_hash",
                "source",
                "environment",
                "type",
                "occurrence_count",
                "created_at",
            ):
                assert field in row, f"Missing field: {field}"

    async def test_filter_by_environment(self, client: AsyncClient, auth_headers: dict):
        rq = await _seed_raw_query(environment="sat")
        await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        r = await client.get("/api/curated?environment=sat", headers=auth_headers)
        assert r.status_code == 200
        rows = r.json()
        assert all(row["environment"] == "sat" for row in rows)

    async def test_pagination_limit(self, client: AsyncClient, auth_headers: dict):
        for _ in range(3):
            rq = await _seed_raw_query()
            await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        r = await client.get("/api/curated?limit=2", headers=auth_headers)
        assert r.status_code == 200
        assert len(r.json()) <= 2


# ---------------------------------------------------------------------------
# GET /api/curated/count
# ---------------------------------------------------------------------------


class TestCuratedCount:
    async def test_count_returns_integer(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/curated/count", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        count = data.get("count", data) if isinstance(data, dict) else data
        assert isinstance(count, int)

    async def test_count_increases_after_create(self, client: AsyncClient, auth_headers: dict):
        r_before = await client.get("/api/curated/count", headers=auth_headers)
        count_before = r_before.json()
        count_before = (
            count_before.get("count", count_before)
            if isinstance(count_before, dict)
            else count_before
        )

        rq = await _seed_raw_query()
        await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)

        r_after = await client.get("/api/curated/count", headers=auth_headers)
        count_after = r_after.json()
        count_after = (
            count_after.get("count", count_after) if isinstance(count_after, dict) else count_after
        )

        assert count_after >= count_before + 1


# ---------------------------------------------------------------------------
# PATCH /api/curated/{id}  (update)
# ---------------------------------------------------------------------------


class TestUpdateCurated:
    async def _create_entry(self, client, auth_headers) -> dict:
        rq = await _seed_raw_query()
        r = await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        assert r.status_code == 201
        return r.json()

    async def test_update_notes(self, client: AsyncClient, auth_headers: dict):
        entry = await self._create_entry(client, auth_headers)
        r = await client.patch(
            f"/api/curated/{entry['id']}",
            json={
                "notes": "Updated notes text",
            },
            headers=auth_headers,
        )
        assert r.status_code == 200
        assert r.json()["notes"] == "Updated notes text"

    async def test_update_label_id(self, client: AsyncClient, auth_headers: dict):
        r_label = await client.post(
            "/api/labels",
            json={
                "name": f"PatchCuratedLabel_{uuid.uuid4().hex[:8]}",
            },
        )
        label_id = r_label.json()["id"]

        entry = await self._create_entry(client, auth_headers)
        r = await client.patch(
            f"/api/curated/{entry['id']}",
            json={
                "label_id": label_id,
            },
            headers=auth_headers,
        )
        assert r.status_code == 200
        assert r.json()["label_id"] == label_id

    async def test_update_nonexistent_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.patch("/api/curated/999999", json={"notes": "nope"}, headers=auth_headers)
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /api/curated/{id}
# ---------------------------------------------------------------------------


class TestDeleteCurated:
    async def test_delete_success(self, client: AsyncClient, auth_headers: dict):
        rq = await _seed_raw_query()
        r_create = await client.post(
            "/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers
        )
        assert r_create.status_code == 201
        entry_id = r_create.json()["id"]

        r_del = await client.delete(f"/api/curated/{entry_id}", headers=auth_headers)
        assert r_del.status_code == 204

    async def test_delete_returns_no_content(self, client: AsyncClient, auth_headers: dict):
        rq = await _seed_raw_query()
        r_create = await client.post(
            "/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers
        )
        entry_id = r_create.json()["id"]
        r_del = await client.delete(f"/api/curated/{entry_id}", headers=auth_headers)
        assert r_del.status_code == 204
        assert r_del.content == b""

    async def test_deleted_entry_allows_re_curate(self, client: AsyncClient, auth_headers: dict):
        """After deleting, the same raw_query_id can be curated again."""
        rq = await _seed_raw_query()
        r1 = await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        entry_id = r1.json()["id"]
        await client.delete(f"/api/curated/{entry_id}", headers=auth_headers)

        r2 = await client.post("/api/curated", json={"raw_query_id": rq.id}, headers=auth_headers)
        assert r2.status_code == 201

    async def test_delete_nonexistent_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.delete("/api/curated/999999", headers=auth_headers)
        assert r.status_code == 404
