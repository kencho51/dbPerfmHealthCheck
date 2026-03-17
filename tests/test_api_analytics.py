"""
API tests — analytics endpoints (/api/analytics/*).

Analytics routes go through DuckDB which calls _load_table() → Neon HTTPS.
In tests we mock _load_table to return controlled Polars DataFrames so no
Neon credentials are needed.

Run:
    uv run pytest tests/test_api_analytics.py -v
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import polars as pl
import pytest
from httpx import AsyncClient


# ---------------------------------------------------------------------------
# Fixture: mock Neon data for analytics
# ---------------------------------------------------------------------------

def _make_raw_df(n: int = 5) -> pl.DataFrame:
    """Return a minimal raw_query DataFrame for DuckDB to query."""
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return pl.DataFrame({
        "id":               list(range(1, n + 1)),
        "query_hash":       [f"hash_{i:04d}" for i in range(n)],
        "time":             [f"2026-01-{i + 1:02d} 10:00:00" for i in range(n)],
        "source":           ["sql", "mongodb", "sql", "sql", "mongodb"][:n],
        "host":             ["WINFODB06HV11", "PTRMMDBHV01", "WINFODB06HV12",
                             "WINDB11ST01N", "PTRMMDBHV02"][:n],
        "db_name":          ["fb_db_v2", "ptrm_cpc_db", "fb_db_v2",
                             "oi_analytics_db", "ptrm_cpc_db"][:n],
        "environment":      ["prod", "prod", "sat", "prod", "sat"][:n],
        "type":             ["slow_query", "slow_query_mongo", "blocker",
                             "deadlock", "slow_query"][:n],
        "query_details":    [f"SELECT {i}" for i in range(n)],
        "month_year":       ["2026-01"] * n,
        "occurrence_count": [3, 7, 2, 1, 5][:n],
        "first_seen":       [now] * n,
        "last_seen":        [now] * n,
        "created_at":       [now] * n,
        "updated_at":       [now] * n,
    })


def _make_curated_df() -> pl.DataFrame:
    return pl.DataFrame({
        "id": [1],
        "raw_query_id": [1],
        "label_id": [None],
        "notes": [None],
        "created_at": ["2026-01-01 00:00:00"],
        "updated_at": ["2026-01-01 00:00:00"],
    })


@pytest.fixture(autouse=True)
def mock_load_table():
    """Patch _load_table so analytics tests never hit Neon."""
    def _fake_load(table: str) -> pl.DataFrame:
        if table == "raw_query":
            return _make_raw_df()
        if table == "curated_query":
            return _make_curated_df()
        return pl.DataFrame()

    with patch("api.analytics_db._load_table", side_effect=_fake_load):
        yield


# ---------------------------------------------------------------------------
# GET /api/analytics/summary
# ---------------------------------------------------------------------------

class TestSummary:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/summary", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_schema(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/summary", headers=auth_headers)
        for row in r.json():
            assert "environment" in row
            assert "type" in row
            assert "row_count" in row

    async def test_filter_by_environment(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/summary?environment=prod", headers=auth_headers)
        assert r.status_code == 200
        for row in r.json():
            assert row["environment"] == "prod"


# ---------------------------------------------------------------------------
# GET /api/analytics/by-host
# ---------------------------------------------------------------------------

class TestByHost:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/by-host", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_schema(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/by-host", headers=auth_headers)
        for row in r.json():
            assert "host" in row

    async def test_top_n_limit(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/by-host?top_n=3", headers=auth_headers)
        assert r.status_code == 200
        assert len(r.json()) <= 3


# ---------------------------------------------------------------------------
# GET /api/analytics/by-month
# ---------------------------------------------------------------------------

class TestByMonth:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/by-month", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_schema(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/by-month", headers=auth_headers)
        for row in r.json():
            assert "month_year" in row


# ---------------------------------------------------------------------------
# GET /api/analytics/by-db
# ---------------------------------------------------------------------------

class TestByDb:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/by-db", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)


# ---------------------------------------------------------------------------
# GET /api/analytics/curation-coverage
# ---------------------------------------------------------------------------

class TestCurationCoverage:
    async def test_returns_coverage(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/curation-coverage", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        # Should contain some coverage metric
        assert isinstance(data, (dict, list, int, float))


# ---------------------------------------------------------------------------
# GET /api/analytics/top-fingerprints
# ---------------------------------------------------------------------------

class TestTopFingerprints:
    async def test_returns_list(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/analytics/top-fingerprints", headers=auth_headers)
        assert r.status_code == 200
        assert isinstance(r.json(), list)
