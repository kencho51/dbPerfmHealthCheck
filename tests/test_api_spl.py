"""
API tests — SPL library endpoints (/api/spl/*).

Tests full CRUD lifecycle: create, list, list types, update (PUT), delete.

Run:
    uv run pytest tests/test_api_spl.py -v
"""

from __future__ import annotations

import uuid

from httpx import AsyncClient

# ---------------------------------------------------------------------------
# POST /api/spl  (create)
# ---------------------------------------------------------------------------


class TestCreateSpl:
    async def test_create_success(self, client: AsyncClient, auth_headers: dict):
        r = await client.post(
            "/api/spl",
            json={
                "name": f"TestSpl_{uuid.uuid4().hex[:8]}",
                "query_type": "slow_query",
                "spl": "index=perfmon_db source=db_perf | stats count by host",
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        data = r.json()
        assert "id" in data
        assert data["query_type"] == "slow_query"

    async def test_create_with_description(self, client: AsyncClient, auth_headers: dict):
        r = await client.post(
            "/api/spl",
            json={
                "name": f"DescSpl_{uuid.uuid4().hex[:8]}",
                "query_type": "deadlock",
                "spl": "index=db_deadlock",
                "description": "Finds deadlock events from Splunk",
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        assert r.json()["description"] == "Finds deadlock events from Splunk"

    async def test_create_missing_spl_422(self, client: AsyncClient, auth_headers: dict):
        r = await client.post(
            "/api/spl",
            json={
                "name": "MissingSpl",
                "query_type": "blocker",
            },
            headers=auth_headers,
        )
        assert r.status_code == 422

    async def test_create_missing_query_type_422(self, client: AsyncClient, auth_headers: dict):
        r = await client.post(
            "/api/spl",
            json={
                "name": "NoQueryType",
                "spl": "index=db",
            },
            headers=auth_headers,
        )
        assert r.status_code == 422

    async def test_schema(self, client: AsyncClient, auth_headers: dict):
        r = await client.post(
            "/api/spl",
            json={
                "name": f"SchemaSpl_{uuid.uuid4().hex[:8]}",
                "query_type": "blocker",
                "spl": "index=blocker",
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        data = r.json()
        for field in ("id", "name", "query_type", "spl", "created_at", "updated_at"):
            assert field in data, f"Missing field: {field}"


# ---------------------------------------------------------------------------
# GET /api/spl  (list)
# ---------------------------------------------------------------------------


class TestListSpl:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/spl", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_created_entry_appears(self, client: AsyncClient, auth_headers: dict):
        name = f"ListableSpl_{uuid.uuid4().hex[:8]}"
        await client.post(
            "/api/spl",
            json={
                "name": name,
                "query_type": "deadlock",
                "spl": "index=db_deadlock",
            },
            headers=auth_headers,
        )
        r = await client.get("/api/spl", headers=auth_headers)
        names = [item["name"] for item in r.json()]
        assert name in names

    async def test_filter_by_query_type(self, client: AsyncClient, auth_headers: dict):
        type_name = f"filtertype_{uuid.uuid4().hex[:8]}"
        await client.post(
            "/api/spl",
            json={
                "name": f"TypedSpl_{uuid.uuid4().hex[:8]}",
                "query_type": type_name,
                "spl": "index=db",
            },
            headers=auth_headers,
        )
        r = await client.get(f"/api/spl?query_type={type_name}", headers=auth_headers)
        assert r.status_code == 200
        rows = r.json()
        assert len(rows) >= 1
        assert all(row["query_type"] == type_name for row in rows)

    async def test_sorted_by_query_type_then_name(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/spl", headers=auth_headers)
        rows = r.json()
        keys = [(row["query_type"], row["name"]) for row in rows]
        assert keys == sorted(keys)

    async def test_schema(self, client: AsyncClient, auth_headers: dict):
        await client.post(
            "/api/spl",
            json={
                "name": f"SchemaSpl_{uuid.uuid4().hex[:8]}",
                "query_type": "slow_query",
                "spl": "index=perf",
            },
            headers=auth_headers,
        )
        r = await client.get("/api/spl", headers=auth_headers)
        for item in r.json():
            for field in ("id", "name", "query_type", "spl"):
                assert field in item, f"Missing field '{field}' in SPL list item"


# ---------------------------------------------------------------------------
# GET /api/spl/types  (distinct types)
# ---------------------------------------------------------------------------


class TestSplTypes:
    async def test_returns_list_of_strings(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/spl/types", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert all(isinstance(t, str) for t in data)

    async def test_default_types_included(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/spl/types", headers=auth_headers)
        types = r.json()
        for expected in ("slow_query", "slow_query_mongo", "blocker", "deadlock"):
            assert expected in types, f"Default type '{expected}' missing from {types}"

    async def test_custom_type_appears_after_create(self, client: AsyncClient, auth_headers: dict):
        custom_type = f"mytype_{uuid.uuid4().hex[:8]}"
        await client.post(
            "/api/spl",
            json={
                "name": f"CustomTypeSpl_{uuid.uuid4().hex[:8]}",
                "query_type": custom_type,
                "spl": "index=custom",
            },
            headers=auth_headers,
        )
        r = await client.get("/api/spl/types", headers=auth_headers)
        assert custom_type in r.json()

    async def test_no_duplicate_types(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/spl/types", headers=auth_headers)
        types = r.json()
        assert len(types) == len(set(types)), "Duplicate types in /api/spl/types"


# ---------------------------------------------------------------------------
# PUT /api/spl/{id}  (full update)
# ---------------------------------------------------------------------------


class TestUpdateSpl:
    async def _create(self, client, auth_headers) -> dict:
        r = await client.post(
            "/api/spl",
            json={
                "name": f"UpdateMe_{uuid.uuid4().hex[:8]}",
                "query_type": "slow_query",
                "spl": "original spl text",
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        return r.json()

    async def test_update_spl(self, client: AsyncClient, auth_headers: dict):
        entry = await self._create(client, auth_headers)
        r = await client.put(
            f"/api/spl/{entry['id']}",
            json={
                "name": entry["name"],
                "query_type": entry["query_type"],
                "spl": "updated spl text here",
            },
            headers=auth_headers,
        )
        assert r.status_code == 200
        assert r.json()["spl"] == "updated spl text here"

    async def test_update_name(self, client: AsyncClient, auth_headers: dict):
        entry = await self._create(client, auth_headers)
        new_name = f"Renamed_{uuid.uuid4().hex[:8]}"
        r = await client.put(
            f"/api/spl/{entry['id']}",
            json={
                "name": new_name,
                "query_type": entry["query_type"],
                "spl": entry["spl"],
            },
            headers=auth_headers,
        )
        assert r.status_code == 200
        assert r.json()["name"] == new_name

    async def test_update_nonexistent_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.put(
            "/api/spl/999999",
            json={
                "name": "x",
                "query_type": "blocker",
                "spl": "x",
            },
            headers=auth_headers,
        )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /api/spl/{id}
# ---------------------------------------------------------------------------


class TestDeleteSpl:
    async def test_delete_success(self, client: AsyncClient, auth_headers: dict):
        r_create = await client.post(
            "/api/spl",
            json={
                "name": f"DeleteMe_{uuid.uuid4().hex[:8]}",
                "query_type": "deadlock",
                "spl": "index=deadlock",
            },
            headers=auth_headers,
        )
        assert r_create.status_code == 201
        spl_id = r_create.json()["id"]

        r_del = await client.delete(f"/api/spl/{spl_id}", headers=auth_headers)
        assert r_del.status_code == 204

    async def test_deleted_entry_not_in_list(self, client: AsyncClient, auth_headers: dict):
        name = f"GoneAfterDelete_{uuid.uuid4().hex[:8]}"
        r_create = await client.post(
            "/api/spl",
            json={
                "name": name,
                "query_type": "blocker",
                "spl": "idx=blocker",
            },
            headers=auth_headers,
        )
        spl_id = r_create.json()["id"]
        await client.delete(f"/api/spl/{spl_id}", headers=auth_headers)

        r = await client.get("/api/spl", headers=auth_headers)
        names = [item["name"] for item in r.json()]
        assert name not in names

    async def test_delete_nonexistent_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.delete("/api/spl/999999", headers=auth_headers)
        assert r.status_code == 404

    async def test_delete_returns_no_content(self, client: AsyncClient, auth_headers: dict):
        r_create = await client.post(
            "/api/spl",
            json={
                "name": f"NoContent_{uuid.uuid4().hex[:8]}",
                "query_type": "slow_query",
                "spl": "index=db",
            },
            headers=auth_headers,
        )
        spl_id = r_create.json()["id"]
        r_del = await client.delete(f"/api/spl/{spl_id}", headers=auth_headers)
        assert r_del.status_code == 204
        assert r_del.content == b""
