"""
API tests — label CRUD endpoints (/api/labels).

Run:
    uv run pytest tests/test_api_labels.py -v
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient


# ---------------------------------------------------------------------------
# POST /api/labels  (create)
# ---------------------------------------------------------------------------

class TestCreateLabel:
    async def test_create_success(self, client: AsyncClient, auth_headers: dict):
        r = await client.post("/api/labels", json={
            "name": "Long Running Query",
            "severity": "warning",
            "description": "Query exceeds 60s execution time",
            "source": "sql",
        }, headers=auth_headers)
        assert r.status_code == 201
        data = r.json()
        assert data["name"] == "Long Running Query"
        assert data["severity"] == "warning"
        assert "id" in data

    async def test_create_minimal(self, client: AsyncClient, auth_headers: dict):
        """Only name is required; others default."""
        r = await client.post("/api/labels", json={"name": "MinimalLabel"}, headers=auth_headers)
        assert r.status_code == 201
        data = r.json()
        assert data["name"] == "MinimalLabel"
        assert data["severity"] == "warning"   # default
        assert data["source"] == "both"        # default

    async def test_create_critical_severity(self, client: AsyncClient, auth_headers: dict):
        r = await client.post("/api/labels", json={
            "name": "Deadlock Pattern",
            "severity": "critical",
            "source": "both",
        }, headers=auth_headers)
        assert r.status_code == 201
        assert r.json()["severity"] == "critical"

    async def test_create_invalid_severity_422(self, client: AsyncClient, auth_headers: dict):
        r = await client.post("/api/labels", json={
            "name": "Bad Label",
            "severity": "extreme",  # invalid
        }, headers=auth_headers)
        assert r.status_code == 422

    async def test_create_works_without_auth(self, client: AsyncClient):
        # /api/labels is a public API — no auth token required
        r = await client.post("/api/labels", json={"name": "NoAuthLabel"})
        assert r.status_code == 201


# ---------------------------------------------------------------------------
# GET /api/labels  (list)
# ---------------------------------------------------------------------------

class TestListLabels:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/labels", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_created_label_appears_in_list(self, client: AsyncClient, auth_headers: dict):
        label_name = "UniqueListTestLabel_XYZ"
        await client.post("/api/labels", json={"name": label_name}, headers=auth_headers)
        r = await client.get("/api/labels", headers=auth_headers)
        names = [l["name"] for l in r.json()]
        assert label_name in names

    async def test_list_sorted_by_name(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/labels", headers=auth_headers)
        names = [l["name"] for l in r.json()]
        assert names == sorted(names)

    async def test_label_schema(self, client: AsyncClient, auth_headers: dict):
        await client.post("/api/labels", json={"name": "SchemaTestLabel"}, headers=auth_headers)
        r = await client.get("/api/labels", headers=auth_headers)
        for item in r.json():
            for field in ("id", "name", "severity", "source", "created_at", "updated_at"):
                assert field in item


# ---------------------------------------------------------------------------
# PATCH /api/labels/{id}  (update)
# ---------------------------------------------------------------------------

class TestUpdateLabel:
    async def _create(self, client, auth_headers) -> dict:
        import uuid
        r = await client.post("/api/labels", json={"name": f"UpdateTest_{uuid.uuid4().hex[:8]}"}, headers=auth_headers)
        assert r.status_code == 201
        return r.json()

    async def test_update_name(self, client: AsyncClient, auth_headers: dict):
        label = await self._create(client, auth_headers)
        r = await client.patch(f"/api/labels/{label['id']}", json={"name": "Updated Name"}, headers=auth_headers)
        assert r.status_code == 200
        assert r.json()["name"] == "Updated Name"

    async def test_update_severity(self, client: AsyncClient, auth_headers: dict):
        label = await self._create(client, auth_headers)
        r = await client.patch(f"/api/labels/{label['id']}", json={"severity": "critical"}, headers=auth_headers)
        assert r.status_code == 200
        assert r.json()["severity"] == "critical"

    async def test_update_nonexistent_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.patch("/api/labels/999999", json={"name": "X"}, headers=auth_headers)
        assert r.status_code == 404

    async def test_update_works_without_auth(self, client: AsyncClient, auth_headers: dict):
        # /api/labels is a public API — update works without a token too
        label = await self._create(client, auth_headers)
        r = await client.patch(f"/api/labels/{label['id']}", json={"name": "NoAuthUpdate"})
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# DELETE /api/labels/{id}
# ---------------------------------------------------------------------------

class TestDeleteLabel:
    async def _create(self, client, auth_headers) -> dict:
        import uuid
        r = await client.post("/api/labels", json={"name": f"DeleteTest_{uuid.uuid4().hex[:8]}"}, headers=auth_headers)
        assert r.status_code == 201
        return r.json()

    async def test_delete_exists(self, client: AsyncClient, auth_headers: dict):
        label = await self._create(client, auth_headers)
        r = await client.delete(f"/api/labels/{label['id']}", headers=auth_headers)
        assert r.status_code in (200, 204)
        # Verify gone
        r2 = await client.get("/api/labels", headers=auth_headers)
        ids = [l["id"] for l in r2.json()]
        assert label["id"] not in ids

    async def test_delete_nonexistent_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.delete("/api/labels/999999", headers=auth_headers)
        assert r.status_code == 404

    async def test_delete_works_without_auth(self, client: AsyncClient, auth_headers: dict):
        # /api/labels is a public API — delete works without a token too
        label = await self._create(client, auth_headers)
        r = await client.delete(f"/api/labels/{label['id']}")
        assert r.status_code in (200, 204)
