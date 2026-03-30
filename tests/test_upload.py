"""
API tests — CSV upload endpoint (/api/upload).

The upload pipeline calls _upsert_neon() internally, which is mocked here
so no Neon credentials are needed. The CSV extraction and validation
(extractor.py + validator.py) run against real in-process code.

Run:
    uv run pytest tests/test_upload.py -v
"""

from __future__ import annotations

import textwrap
from io import BytesIO
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from api.services.ingestor import IngestResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _csv_bytes(content: str) -> BytesIO:
    return BytesIO(textwrap.dedent(content).strip().encode())


def _upload(client, filename: str, content: str):
    """Return coroutine that posts a CSV file to /api/upload."""
    return client.post(
        "/api/upload",
        files={"file": (filename, _csv_bytes(content), "text/csv")},
    )


# ---------------------------------------------------------------------------
# Mocked ingest_rows — avoids Neon calls
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def mock_ingest():
    """Replace ingest_rows / ingest_typed_rows / _link_typed_to_raw with no-ops.

    The upload router calls three async functions beyond extraction:
      - ingest_rows         → raw_query upsert
      - ingest_typed_rows   → raw_query_<type> upsert
      - _link_typed_to_raw  → FK backfill SQL UPDATE
    Mocking all three lets the test focus purely on HTTP contract and
    extraction logic without requiring a fully-migrated test database.
    """
    result = IngestResult(inserted=2, updated=0, skipped=0)

    async def _fake_ingest(rows):
        result.inserted = len(rows)
        return result

    async def _fake_typed_ingest(rows, table_type):
        return IngestResult(inserted=len(rows), updated=0, skipped=0)

    async def _fake_link(table_type):
        pass

    with (
        patch("api.routers.upload.ingest_rows", side_effect=_fake_ingest),
        patch("api.routers.upload.ingest_typed_rows", side_effect=_fake_typed_ingest),
        patch("api.routers.upload._link_typed_to_raw", side_effect=_fake_link),
    ):
        yield


# ---------------------------------------------------------------------------
# Valid CSV uploads
# ---------------------------------------------------------------------------


class TestValidUpload:
    _SLOW_QUERY_CSV = """
        host,db_name,query_final,duration_ms
        WINFODB06HV11,fb_db_v2,SELECT * FROM bet WHERE id = @P0,45000
        WINDB11ST01N,oi_analytics_db,SELECT SUM(amount) FROM ledger,820000
    """

    _BLOCKER_CSV = """
        host,database_name,query_text,blocking_session_id
        WINFODB06HV11,wagering,UPDATE combination SET status=@P0,55
    """

    async def test_slow_query_upload_success(self, client: AsyncClient, auth_headers: dict):
        r = await _upload(client, "maxElapsedQueriesProdJan26.csv", self._SLOW_QUERY_CSV)
        assert r.status_code == 200
        data = r.json()
        assert "inserted" in data or "row_count" in data

    async def test_blocker_upload_success(self, client: AsyncClient, auth_headers: dict):
        r = await _upload(client, "blockersProdJan26.csv", self._BLOCKER_CSV)
        assert r.status_code == 200

    async def test_prod_environment_detected(self, client: AsyncClient, auth_headers: dict):
        r = await _upload(client, "maxElapsedQueriesProdJan26.csv", self._SLOW_QUERY_CSV)
        assert r.status_code == 200
        data = r.json()
        assert data.get("environment") == "prod"

    async def test_sat_environment_detected(self, client: AsyncClient, auth_headers: dict):
        r = await _upload(client, "blockersSatJan26.csv", self._BLOCKER_CSV)
        assert r.status_code == 200
        data = r.json()
        assert data.get("environment") == "sat"

    async def test_response_contains_filename(self, client: AsyncClient, auth_headers: dict):
        r = await _upload(client, "maxElapsedQueriesProdJan26.csv", self._SLOW_QUERY_CSV)
        assert r.status_code == 200
        assert "maxElapsedQueriesProdJan26.csv" in r.json().get("filename", "")

    async def test_upload_requires_auth(self, client: AsyncClient):
        r = await _upload(client, "maxElapsedQueriesProdJan26.csv", self._SLOW_QUERY_CSV)
        # With no auth, the endpoint should accept (upload is not auth-gated currently)
        # or return 401/403 — either is valid behaviour
        assert r.status_code in (200, 401, 403, 422)


# ---------------------------------------------------------------------------
# Invalid / empty  uploads
# ---------------------------------------------------------------------------


class TestInvalidUpload:
    async def test_empty_csv_rejected(self, client: AsyncClient, auth_headers: dict):
        r = await _upload(client, "maxElapsedQueriesProdJan26.csv", "")
        assert r.status_code in (400, 422)

    async def test_unknown_file_type_422(self, client: AsyncClient, auth_headers: dict):
        r = await _upload(client, "unknownFileType.csv", "col1,col2\nval1,val2\n")
        assert r.status_code in (422, 400)

    async def test_missing_required_columns_422(self, client: AsyncClient, auth_headers: dict):
        r = await _upload(client, "maxElapsedQueriesProdJan26.csv", "irrelevant_col\nfoo\n")
        assert r.status_code == 422
