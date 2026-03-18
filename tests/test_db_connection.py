"""
Unit tests for the Neon HTTP SQL client (api/neon_http.py).

All network calls are mocked — these tests run with no Neon credentials
and verify behaviour of the HTTP client, type coercion, and error handling.

Run:
    uv run pytest tests/test_db_connection.py -v
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pytest


# ---------------------------------------------------------------------------
# _coerce() — type coercion from Neon REST JSON strings
# ---------------------------------------------------------------------------

class TestCoerce:
    """_coerce converts raw API string values to Python types."""

    def _coerce(self, value, sa_type):
        from api.neon_http import _coerce
        import sqlalchemy as sa
        col = MagicMock()
        col.type = sa_type
        return _coerce(value, col)

    def test_none_returns_none(self):
        import sqlalchemy as sa
        assert self._coerce(None, sa.Integer()) is None

    def test_integer_coercion(self):
        import sqlalchemy as sa
        assert self._coerce("42", sa.Integer()) == 42

    def test_float_coercion(self):
        import sqlalchemy as sa
        assert self._coerce("3.14", sa.Float()) == pytest.approx(3.14)

    def test_boolean_true(self):
        import sqlalchemy as sa
        assert self._coerce("true", sa.Boolean()) is True

    def test_boolean_false(self):
        import sqlalchemy as sa
        assert self._coerce("false", sa.Boolean()) is False

    def test_datetime_iso_full(self):
        import sqlalchemy as sa
        result = self._coerce("2026-01-15T10:30:00", sa.DateTime())
        assert isinstance(result, datetime)
        assert result.year == 2026

    def test_datetime_neon_short_offset(self):
        """Neon returns +00 (not +00:00) — must be normalised."""
        import sqlalchemy as sa
        result = self._coerce("2026-03-16 03:50:09+00", sa.DateTime())
        assert isinstance(result, datetime)

    def test_datetime_neon_negative_offset(self):
        import sqlalchemy as sa
        result = self._coerce("2026-01-15 08:00:00-05", sa.DateTime())
        assert isinstance(result, datetime)

    def test_string_passthrough(self):
        import sqlalchemy as sa
        assert self._coerce("hello", sa.String()) == "hello"


# ---------------------------------------------------------------------------
# _sync_http_sql — HTTP request / response handling
# ---------------------------------------------------------------------------

def _make_response(rows: list[dict], fields: list[str]) -> MagicMock:
    """Build a fake urllib HTTP response returning Neon JSON format."""
    data = {
        "rows": rows,
        "fields": [{"name": f} for f in fields],
        "rowCount": len(rows),
    }
    body = json.dumps(data).encode()
    mock_resp = MagicMock()
    mock_resp.read.return_value = body
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


class TestSyncHttpSql:
    def test_returns_rows_and_rowcount(self):
        mock_resp = _make_response(
            rows=[{"id": "1", "name": "test"}],
            fields=["id", "name"],
        )
        with patch("urllib.request.urlopen", return_value=mock_resp):
            from api.neon_http import _sync_http_sql
            rows, rowcount = _sync_http_sql("SELECT 1", [])
        assert len(rows) == 1
        assert rows[0] == ["1", "test"]
        assert rowcount == 1

    def test_empty_result(self):
        mock_resp = _make_response(rows=[], fields=["id"])
        with patch("urllib.request.urlopen", return_value=mock_resp):
            from api.neon_http import _sync_http_sql
            rows, rowcount = _sync_http_sql("SELECT 1 WHERE FALSE", [])
        assert rows == []
        assert rowcount == 0

    def test_http_error_raises_runtime_error(self):
        err = HTTPError(
            url="https://test/sql",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=BytesIO(b'{"message":"bad query"}'),
        )
        with patch("urllib.request.urlopen", side_effect=err):
            from api.neon_http import _sync_http_sql
            with pytest.raises(RuntimeError, match="Neon HTTP 400"):
                _sync_http_sql("BAD SQL", [])

    def test_with_fields_returns_column_names(self):
        mock_resp = _make_response(
            rows=[{"host": "DB01", "count": "5"}],
            fields=["host", "count"],
        )
        with patch("urllib.request.urlopen", return_value=mock_resp):
            from api.neon_http import _sync_http_sql_with_fields
            columns, rows, rowcount = _sync_http_sql_with_fields("SELECT host, COUNT(*) FROM raw_query")
        assert columns == ["host", "count"]
        assert rows[0] == ["DB01", "5"]


# ---------------------------------------------------------------------------
# _NeonResult — result wrapper interface
# ---------------------------------------------------------------------------

class TestNeonResult:
    def _make(self, rows):
        from api.neon_http import _NeonResult
        return _NeonResult(rows)

    def test_all(self):
        r = self._make([[1, "a"], [2, "b"]])
        assert r.all() == [(1, "a"), (2, "b")]

    def test_first_returns_first_row(self):
        r = self._make([[1, "a"], [2, "b"]])
        assert r.first() == (1, "a")

    def test_first_empty(self):
        r = self._make([])
        assert r.first() is None

    def test_one_exact(self):
        r = self._make([[42, "x"]])
        assert r.one() == (42, "x")

    def test_one_raises_on_multiple(self):
        r = self._make([[1], [2]])
        with pytest.raises(ValueError):
            r.one()

    def test_scalars(self):
        r = self._make([[10, "ignored"], [20, "also ignored"]])
        scalars = r.scalars()
        assert scalars.all() == [(10,), (20,)]


# ---------------------------------------------------------------------------
# NeonSession — session interface basics
# ---------------------------------------------------------------------------

class TestNeonSessionInterface:
    """Verify NeonSession has the expected async interface."""

    def test_has_required_methods(self):
        from api.neon_http import NeonHTTPSession
        session = NeonHTTPSession()
        for method in ("exec", "execute", "get"):
            assert callable(getattr(session, method, None)), f"Missing method: {method}"

    def test_rollback_clears_pending(self):
        from api.database import NeonSession
        s = NeonSession()
        s.add(MagicMock())
        import asyncio
        asyncio.run(s.rollback())
        assert s._pending_add == []
