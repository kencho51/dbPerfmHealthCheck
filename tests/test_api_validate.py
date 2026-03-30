"""
API tests — validate endpoint (POST /api/validate).

Tests the dry-run CSV validation endpoint: response shape, environment
detection from filename, and guarantee of no DB writes.

Run:
    uv run pytest tests/test_api_validate.py -v
"""
from __future__ import annotations

import io

from httpx import AsyncClient


# ---------------------------------------------------------------------------
# Minimal CSV payloads
# ---------------------------------------------------------------------------

_SLOW_SQL_CSV = (
    "host,db_name,query_final,time,environment\n"
    "WINFODB06HV11,fb_db_v2,SELECT * FROM bet,2026-01-15 10:00:00,prod\n"
    "WINFODB06HV12,fb_db_v2,SELECT * FROM user,2026-01-15 11:00:00,prod\n"
)

# filename must contain "maxelapsed" and "prod" so the temp file is auto-detected
_SLOW_SQL_FILENAME = "maxElapsedQueriesProd.csv"
_SLOW_SQL_FILENAME_SAT = "maxElapsedQueriesSat.csv"

_BLOCKER_CSV = (
    "database_name,query_text,session_id,blocking_session_id,host\n"
    "fb_db_v2,SELECT * FROM locks,55,12,WINFODB06HV11\n"
)
_BLOCKER_FILENAME = "blockersProd.csv"

_EMPTY_ROWS_CSV = "host,db_name,query_final\n"   # header only, no data rows

_UNKNOWN_FORMAT_CSV = "col_a,col_b,col_c\nval1,val2,val3\n"


def _multipart(content: str, filename: str) -> dict:
    return {"file": (filename, io.BytesIO(content.encode()), "text/csv")}


# ---------------------------------------------------------------------------
# POST /api/validate
# ---------------------------------------------------------------------------

class TestValidate:
    async def test_valid_slow_query_returns_200(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_SLOW_SQL_CSV, _SLOW_SQL_FILENAME),
        )
        assert r.status_code == 200

    async def test_response_schema(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_SLOW_SQL_CSV, _SLOW_SQL_FILENAME),
        )
        assert r.status_code == 200
        data = r.json()
        for field in ("is_valid", "file_type", "environment", "row_count",
                      "warnings", "errors", "null_rates", "sample_rows"):
            assert field in data, f"Missing field: {field}"

    async def test_valid_csv_is_valid(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_SLOW_SQL_CSV, _SLOW_SQL_FILENAME),
        )
        data = r.json()
        assert data["is_valid"] is True
        assert data["row_count"] == 2

    async def test_empty_csv_is_invalid(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_EMPTY_ROWS_CSV, _SLOW_SQL_FILENAME),
        )
        data = r.json()
        assert data["is_valid"] is False
        assert len(data["errors"]) >= 1

    async def test_unknown_format_is_invalid(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_UNKNOWN_FORMAT_CSV, "unknownfile.csv"),
        )
        data = r.json()
        assert data["is_valid"] is False

    async def test_environment_detected_from_filename_prod(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_SLOW_SQL_CSV, _SLOW_SQL_FILENAME),
        )
        assert r.json()["environment"] == "prod"

    async def test_environment_detected_from_filename_sat(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_SLOW_SQL_CSV, _SLOW_SQL_FILENAME_SAT),
        )
        assert r.json()["environment"] == "sat"

    async def test_sample_rows_present(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_SLOW_SQL_CSV, _SLOW_SQL_FILENAME),
        )
        data = r.json()
        assert isinstance(data["sample_rows"], list)

    async def test_no_db_writes(self, client: AsyncClient, auth_headers: dict):
        """Validate must not persist any rows to the database."""
        r_before = await client.get("/api/queries/count", headers=auth_headers)
        count_before = r_before.json()
        count_before = count_before.get("count", count_before) if isinstance(count_before, dict) else count_before

        await client.post(
            "/api/validate",
            files=_multipart(_SLOW_SQL_CSV, _SLOW_SQL_FILENAME),
        )

        r_after = await client.get("/api/queries/count", headers=auth_headers)
        count_after = r_after.json()
        count_after = count_after.get("count", count_after) if isinstance(count_after, dict) else count_after

        assert count_after == count_before

    async def test_blocker_csv_is_valid(self, client: AsyncClient):
        r = await client.post(
            "/api/validate",
            files=_multipart(_BLOCKER_CSV, _BLOCKER_FILENAME),
        )
        assert r.status_code == 200
        data = r.json()
        assert data["is_valid"] is True
