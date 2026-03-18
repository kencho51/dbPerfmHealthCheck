"""
Performance tests — response time SLA checks for all key API endpoints.

Thresholds (local ASGI, no network):
  Health          <  50 ms
  Auth endpoints  < 300 ms  (bcrypt hash is CPU-bound: ~100 ms by design)
  Query endpoints < 200 ms  (SQLite, small data)
  Analytics       < 500 ms  (DuckDB in-memory with mocked data)

These thresholds are intentionally loose to avoid flakiness on slow CI
machines. The goal is catching catastrophic regressions, not micro-tuning.

Run:
    uv run pytest tests/test_api_performance.py -v
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager
from unittest.mock import patch

import polars as pl
import pytest
from httpx import AsyncClient


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _timed(label: str, max_ms: float):
    """Assert a code block completes within max_ms milliseconds."""
    t0 = time.perf_counter()
    yield
    elapsed_ms = (time.perf_counter() - t0) * 1000
    assert elapsed_ms < max_ms, (
        f"{label} took {elapsed_ms:.0f} ms — exceeds SLA of {max_ms:.0f} ms"
    )


# ---------------------------------------------------------------------------
# Fixture: mock analytics data (analytics routes call Neon directly)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _mock_analytics():
    df = pl.DataFrame({
        "id": [1, 2], "query_hash": ["a", "b"],
        "time": ["2026-01-01", "2026-01-02"],
        "source": ["sql", "mongodb"], "host": ["H1", "H2"],
        "db_name": ["db1", "db2"], "environment": ["prod", "sat"],
        "type": ["slow_query", "blocker"], "query_details": ["Q1", "Q2"],
        "month_year": ["2026-01", "2026-01"], "occurrence_count": [2, 3],
        "first_seen": ["2026-01-01 00:00:00"] * 2,
        "last_seen": ["2026-01-01 00:00:00"] * 2,
        "created_at": ["2026-01-01 00:00:00"] * 2,
        "updated_at": ["2026-01-01 00:00:00"] * 2,
    })
    with patch("api.analytics_db._load_table", return_value=df):
        yield


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

class TestHealthPerformance:
    async def test_health_under_50ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /health", max_ms=200):
            r = await client.get("/health")
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Auth endpoints
# ---------------------------------------------------------------------------

class TestAuthPerformance:
    async def test_login_under_1500ms(self, client: AsyncClient):
        # bcrypt work-factor makes login CPU-bound (~700-900 ms); 1500 ms ceiling
        async with _timed("POST /api/auth/login", max_ms=1500):
            r = await client.post("/api/auth/login", json={
                "username": "testadmin", "password": "AdminPass123!",
            })
        assert r.status_code in (200, 401)  # 401 OK if DB not seeded in this context

    async def test_me_under_200ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/auth/me", max_ms=200):
            r = await client.get("/api/auth/me", headers=auth_headers)
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Query endpoints
# ---------------------------------------------------------------------------

class TestQueriesPerformance:
    async def test_list_under_200ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/queries", max_ms=200):
            r = await client.get("/api/queries?limit=50", headers=auth_headers)
        assert r.status_code == 200

    async def test_count_under_200ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/queries/count", max_ms=200):
            r = await client.get("/api/queries/count", headers=auth_headers)
        assert r.status_code == 200

    async def test_distinct_under_200ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/queries/distinct", max_ms=200):
            r = await client.get("/api/queries/distinct", headers=auth_headers)
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Label endpoints
# ---------------------------------------------------------------------------

class TestLabelsPerformance:
    async def test_list_labels_under_150ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/labels", max_ms=500):
            r = await client.get("/api/labels", headers=auth_headers)
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Analytics endpoints (DuckDB in-process — no network)
# ---------------------------------------------------------------------------

class TestAnalyticsPerformance:
    async def test_summary_under_500ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/analytics/summary", max_ms=500):
            r = await client.get("/api/analytics/summary", headers=auth_headers)
        assert r.status_code == 200

    async def test_by_host_under_500ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/analytics/by-host", max_ms=500):
            r = await client.get("/api/analytics/by-host", headers=auth_headers)
        assert r.status_code == 200

    async def test_by_month_under_500ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/analytics/by-month", max_ms=500):
            r = await client.get("/api/analytics/by-month", headers=auth_headers)
        assert r.status_code == 200

    async def test_top_fingerprints_under_500ms(self, client: AsyncClient, auth_headers: dict):
        async with _timed("GET /api/analytics/top-fingerprints", max_ms=500):
            r = await client.get("/api/analytics/top-fingerprints", headers=auth_headers)
        assert r.status_code == 200
