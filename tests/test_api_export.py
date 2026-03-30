"""
API tests — export endpoint (GET /api/export).

Verifies streaming CSV response: content-type, header columns, data rows,
and filter parameters. Seeds RawQuery rows directly to guarantee known data.

Run:
    uv run pytest tests/test_api_export.py -v
"""

from __future__ import annotations

import csv
import hashlib
import io
import uuid
from datetime import UTC, datetime

from httpx import AsyncClient

from api.database import open_session
from api.models import RawQuery

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_export_query(**overrides) -> RawQuery:
    unique = str(uuid.uuid4())
    defaults = dict(
        query_hash=hashlib.md5(unique.encode()).hexdigest(),
        source="sql",
        host="EXPORTHOST01",
        db_name="export_db",
        environment="prod",
        type="slow_query",
        time="2026-02-01 10:00:00",
        query_details=f"SELECT 1 FROM export_table /* {unique} */",
        month_year="2026-02",
        occurrence_count=1,
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
# GET /api/export
# ---------------------------------------------------------------------------


class TestExport:
    async def test_returns_200(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/export", headers=auth_headers)
        assert r.status_code == 200

    async def test_content_type_is_csv(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/export", headers=auth_headers)
        assert r.status_code == 200
        assert "text/csv" in r.headers.get("content-type", "")

    async def test_csv_has_required_header_columns(self, client: AsyncClient, auth_headers: dict):
        await _seed_export_query()
        r = await client.get("/api/export", headers=auth_headers)
        assert r.status_code == 200
        reader = csv.reader(io.StringIO(r.text))
        headers = next(reader)
        expected_cols = (
            "id",
            "query_hash",
            "source",
            "host",
            "db_name",
            "environment",
            "type",
            "month_year",
            "occurrence_count",
            "query_details",
            "curated_id",
            "label_id",
        )
        for col in expected_cols:
            assert col in headers, f"Expected column '{col}' missing from CSV header"

    async def test_csv_contains_seeded_row(self, client: AsyncClient, auth_headers: dict):
        unique_detail = f"EXPORT_MARKER_{uuid.uuid4().hex}"
        await _seed_export_query(query_details=unique_detail)
        r = await client.get("/api/export", headers=auth_headers)
        assert r.status_code == 200
        assert unique_detail in r.text

    async def test_filter_by_environment(self, client: AsyncClient, auth_headers: dict):
        unique = f"SAT_ONLY_{uuid.uuid4().hex[:8]}"
        await _seed_export_query(environment="sat", query_details=unique)
        r = await client.get("/api/export?environment=sat", headers=auth_headers)
        assert r.status_code == 200
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        assert len(rows) >= 1
        assert all(row["environment"] == "sat" for row in rows)

    async def test_filter_by_source(self, client: AsyncClient, auth_headers: dict):
        unique = f"MONGO_EXPORT_{uuid.uuid4().hex[:8]}"
        await _seed_export_query(source="mongodb", type="slow_query_mongo", query_details=unique)
        r = await client.get("/api/export?source=mongodb", headers=auth_headers)
        assert r.status_code == 200
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        assert all(row["source"] == "mongodb" for row in rows)

    async def test_filter_by_type(self, client: AsyncClient, auth_headers: dict):
        unique = f"BLOCKER_EXPORT_{uuid.uuid4().hex[:8]}"
        await _seed_export_query(type="blocker", query_details=unique)
        r = await client.get("/api/export?type=blocker", headers=auth_headers)
        assert r.status_code == 200
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        assert all(row["type"] == "blocker" for row in rows)

    async def test_search_filter(self, client: AsyncClient, auth_headers: dict):
        unique_detail = f"SEARCH_EXPORT_NEEDLE_{uuid.uuid4().hex[:8]}"
        await _seed_export_query(query_details=unique_detail)
        r = await client.get(f"/api/export?search={unique_detail}", headers=auth_headers)
        assert r.status_code == 200
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        assert len(rows) >= 1
        assert all(unique_detail in row["query_details"] for row in rows)

    async def test_empty_result_still_has_header(self, client: AsyncClient, auth_headers: dict):
        """Filtering to a non-existent host still returns valid CSV with header."""
        r = await client.get("/api/export?host=NONEXISTENT_HOST_ZZZZZ", headers=auth_headers)
        assert r.status_code == 200
        lines = [line for line in r.text.splitlines() if line.strip()]
        assert len(lines) >= 1  # at least the header row
        reader = csv.reader(io.StringIO(lines[0]))
        headers = next(reader)
        assert "id" in headers
