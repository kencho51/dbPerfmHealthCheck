"""
Unit/integration tests for api/services/ingestor.py

Covers the three outcomes visible on the Upload page:
  • inserted — brand-new rows added to raw_query
  • updated  — existing rows get occurrence_count accumulated, timestamps refreshed
  • skipped  — entire chunk skipped when the DB layer raises an exception

Additional cases:
  • mixed upload  — first upload inserts, second adds new + updates existing
  • empty input   — nothing happens, all counts are zero
  • intra-CSV dedup — duplicate rows inside one CSV are collapsed before insert

All tests run against the shared in-memory SQLite DB wired up by conftest.py.
The raw_query table is cleared before each test class so tests are isolated.

Run:
    uv run pytest tests/test_ingestor.py -v
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text

from api.database import open_session
from api.services.ingestor import _normalize_sync, ingest_rows

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_row(
    *,
    host: str = "WINFODB01",
    db_name: str = "wagering",
    source: str = "sql",
    environment: str = "prod",
    type_: str = "slow_query",
    time: str = "2025-01-15 10:00:00",
    query_details: str = "SELECT * FROM bet",
    occurrence_count: int = 1,
) -> dict[str, Any]:
    """Return a minimal pre-extraction row dict (as extractor.py would produce)."""
    return {
        "host": host,
        "db_name": db_name,
        "source": source,
        "environment": environment,
        "type": type_,
        "time": time,
        "query_details": query_details,
        "occurrence_count": occurrence_count,
    }


async def _get_raw_query_row(qhash: str) -> dict | None:
    """Fetch one raw_query row by query_hash, return as dict or None."""
    async with open_session() as session:
        result = await session.exec(
            text("SELECT * FROM raw_query WHERE query_hash = :qh"),
            params={"qh": qhash},
        )
        row = result.mappings().first()
        return dict(row) if row else None


async def _clear_raw_query() -> None:
    """Delete all rows from raw_query between test classes."""
    async with open_session() as session:
        await session.exec(text("DELETE FROM raw_query"))


# ---------------------------------------------------------------------------
# Per-class isolation: wipe raw_query before each class runs
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True, scope="class")
def _isolate_db(_db_tables):
    """Clear raw_query once before each test class."""
    asyncio.run(_clear_raw_query())


# ---------------------------------------------------------------------------
# 1. inserted — new rows
# ---------------------------------------------------------------------------


class TestInserted:
    async def test_new_rows_are_inserted(self):
        rows = [_raw_row(host="HOST_A"), _raw_row(host="HOST_B")]
        result = await ingest_rows(rows)

        assert result.inserted == 2
        assert result.updated == 0
        assert result.skipped == 0
        assert result.errors == []

    async def test_inserted_rows_exist_in_db(self):
        row = _raw_row(host="HOST_DB_CHECK", query_details="SELECT 1")
        result = await ingest_rows([row])
        assert result.inserted == 1

        # Derive the hash the same way _normalize_sync does and check DB
        normalized = _normalize_sync([row])
        assert len(normalized) == 1
        stored = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored is not None
        assert stored["host"] == "HOST_DB_CHECK"

    async def test_inserted_row_has_correct_occurrence_count(self):
        row = _raw_row(host="HOST_OCC", occurrence_count=5)
        await ingest_rows([row])

        normalized = _normalize_sync([row])
        stored = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored is not None
        assert stored["occurrence_count"] == 5

    async def test_inserted_row_has_first_seen_set(self):
        before = datetime.now(tz=UTC)
        row = _raw_row(host="HOST_FS")
        await ingest_rows([row])

        normalized = _normalize_sync([row])
        stored = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored is not None
        # first_seen should be >= the timestamp captured before the call
        first_seen = stored["first_seen"]
        if isinstance(first_seen, str):
            first_seen = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
        if first_seen.tzinfo is None:
            first_seen = first_seen.replace(tzinfo=UTC)
        assert first_seen >= before


# ---------------------------------------------------------------------------
# 2. updated — existing rows
# ---------------------------------------------------------------------------


class TestUpdated:
    async def test_second_upload_of_same_rows_is_updated_not_inserted(self):
        row = _raw_row(host="HOST_UPD")
        await ingest_rows([row])  # first — inserts

        result2 = await ingest_rows([row])  # second — updates
        assert result2.inserted == 0
        assert result2.updated == 1
        assert result2.skipped == 0

    async def test_occurrence_count_accumulates_on_update(self):
        row = _raw_row(host="HOST_ACC", occurrence_count=3)
        await ingest_rows([row])

        result2 = await ingest_rows([_raw_row(host="HOST_ACC", occurrence_count=7)])
        assert result2.updated == 1

        normalized = _normalize_sync([row])
        stored = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored is not None
        assert stored["occurrence_count"] == 10  # 3 + 7

    async def test_last_seen_refreshed_on_update(self):
        row = _raw_row(host="HOST_LS")
        await ingest_rows([row])

        normalized = _normalize_sync([row])
        stored_before = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored_before is not None
        last_seen_before = stored_before["last_seen"]

        # Small sleep to ensure timestamps differ
        import time as _time

        _time.sleep(0.05)

        await ingest_rows([row])
        stored_after = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored_after is not None

        # last_seen must have changed
        assert stored_after["last_seen"] != last_seen_before

    async def test_other_fields_unchanged_on_update(self):
        """query_details, source, environment must not be mutated by an update."""
        row = _raw_row(host="HOST_IMMUT", query_details="SELECT immutable", source="sql")
        await ingest_rows([row])

        await ingest_rows([row])  # second upload

        normalized = _normalize_sync([row])
        stored = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored is not None
        assert stored["query_details"] == "SELECT immutable"
        assert stored["source"] == "sql"


# ---------------------------------------------------------------------------
# 3. skipped — DB exception path
# ---------------------------------------------------------------------------


class TestSkipped:
    async def test_skipped_count_set_on_db_error(self):
        rows = [_raw_row(host="HOST_SKIP_A"), _raw_row(host="HOST_SKIP_B")]

        # open_session is imported lazily inside _upsert_sqlite; patch it at
        # api.database (the source module) so the lazy import picks up the mock.
        # It must be an async context manager (matching @asynccontextmanager).
        @asynccontextmanager
        async def _boom(*_a, **_kw):
            raise RuntimeError("Simulated DB failure")
            yield  # type: ignore[misc] — unreachable but required by asynccontextmanager

        import api.database as _db_mod

        original = _db_mod.open_session
        _db_mod.open_session = _boom  # type: ignore[assignment]
        try:
            result = await ingest_rows(rows)
        finally:
            _db_mod.open_session = original  # type: ignore[assignment]

        assert result.skipped == 2
        assert result.inserted == 0
        assert result.updated == 0
        assert len(result.errors) == 1
        assert "SQLite upsert error" in result.errors[0]

    async def test_error_message_contains_exception_type(self):
        rows = [_raw_row(host="HOST_ERR_TYPE")]

        @asynccontextmanager
        async def _bad(*_a, **_kw):
            raise ValueError("bad value")
            yield  # type: ignore[misc]

        import api.database as _db_mod

        original = _db_mod.open_session
        _db_mod.open_session = _bad  # type: ignore[assignment]
        try:
            result = await ingest_rows(rows)
        finally:
            _db_mod.open_session = original  # type: ignore[assignment]

        assert any("ValueError" in e for e in result.errors)


# ---------------------------------------------------------------------------
# 4. mixed upload — some new, some existing
# ---------------------------------------------------------------------------


class TestMixed:
    async def test_mixed_insert_and_update(self):
        existing_rows = [_raw_row(host=f"HOST_MIX_{i}") for i in range(3)]
        await ingest_rows(existing_rows)  # insert 3

        new_rows = [_raw_row(host=f"HOST_MIX_NEW_{i}") for i in range(2)]
        result = await ingest_rows(existing_rows + new_rows)

        assert result.inserted == 2  # only the new ones
        assert result.updated == 3  # the pre-existing ones
        assert result.skipped == 0

    async def test_total_equals_sum_of_all_counts(self):
        rows = [_raw_row(host=f"HOST_TOT_{i}") for i in range(4)]
        result = await ingest_rows(rows)
        assert result.total == result.inserted + result.updated + result.skipped


# ---------------------------------------------------------------------------
# 5. edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    async def test_empty_input_returns_all_zeros(self):
        result = await ingest_rows([])
        assert result.inserted == 0
        assert result.updated == 0
        assert result.skipped == 0
        assert result.errors == []

    async def test_intra_csv_duplicates_collapsed_before_insert(self):
        """Two identical rows in one upload must produce a single DB row."""
        row = _raw_row(host="HOST_INTRA_DUP", query_details="SELECT dup", occurrence_count=4)
        result = await ingest_rows([row, row])  # same row twice

        # DuckDB normalisation groups duplicates → one hash
        assert result.inserted == 1
        assert result.updated == 0

        normalized = _normalize_sync([row])
        stored = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored is not None
        assert stored["occurrence_count"] == 8  # 4 + 4 summed by DuckDB

    async def test_unknown_enum_values_clamped(self):
        """Rows with invalid source/type/environment must still be inserted."""
        row = _raw_row(
            host="HOST_CLAMP", source="oracle", type_="unknown_type", environment="staging"
        )
        result = await ingest_rows([row])
        assert result.inserted == 1

        normalized = _normalize_sync([row])
        stored = await _get_raw_query_row(normalized[0]["query_hash"])
        assert stored is not None
        assert stored["source"] == "sql"  # clamped
        assert stored["type"] == "unknown"  # clamped
        assert stored["environment"] == "unknown"  # clamped


# ---------------------------------------------------------------------------
# 6. hash formula stability (NULLIF fix)
#    Regression: old formula was COALESCE(NULLIF(trim(extra_metadata),''),'')
#    which meant concat_ws received '' instead of NULL, so a row uploaded
#    without extra_metadata and re-uploaded with extra_metadata='' would get
#    the same hash — correct. BUT, a row uploaded originally (before the fix)
#    had a trailing '|' appended by concat_ws whereas a re-uploaded row after
#    the fix did not → double-count on every re-upload.
#    The fix changes to NULLIF(trim(extra_metadata),'') with no COALESCE, so
#    concat_ws skips the NULL entirely and the hash is identical whether the
#    field is absent or ''.
# ---------------------------------------------------------------------------


class TestHashFormula:
    """Hash must be stable: absent extra_metadata == empty-string extra_metadata."""

    @pytest.fixture(autouse=True, scope="class")
    def _isolate_db(self):  # type: ignore[override]
        """No DB access needed — _normalize_sync is a pure function.
        Shadow the module-level autouse fixture to avoid async DB clearing."""
        pass

    def test_absent_and_empty_extra_metadata_produce_same_hash(self):
        row_no_meta = _raw_row(host="HASH_STABLE_01")
        row_empty_meta = {**_raw_row(host="HASH_STABLE_01"), "extra_metadata": ""}

        n1 = _normalize_sync([row_no_meta])
        n2 = _normalize_sync([row_empty_meta])

        assert n1[0]["query_hash"] == n2[0]["query_hash"], (
            "absent and empty extra_metadata must produce identical hashes"
        )

    def test_whitespace_only_extra_metadata_treated_as_empty(self):
        row_no_meta = _raw_row(host="HASH_STABLE_02")
        row_spaces_meta = {**_raw_row(host="HASH_STABLE_02"), "extra_metadata": "   "}

        n1 = _normalize_sync([row_no_meta])
        n2 = _normalize_sync([row_spaces_meta])

        assert n1[0]["query_hash"] == n2[0]["query_hash"], (
            "whitespace-only extra_metadata must be treated as empty"
        )

    def test_nonempty_extra_metadata_gives_different_hash(self):
        row_no_meta = _raw_row(host="HASH_DIFF_01")
        row_with_meta = {**_raw_row(host="HASH_DIFF_01"), "extra_metadata": "pid=123"}

        n1 = _normalize_sync([row_no_meta])
        n2 = _normalize_sync([row_with_meta])

        assert n1[0]["query_hash"] != n2[0]["query_hash"], (
            "non-empty extra_metadata must change the hash"
        )

    def test_different_extra_metadata_gives_different_hash(self):
        row_a = {**_raw_row(host="HASH_DIFF_02"), "extra_metadata": "pid=1"}
        row_b = {**_raw_row(host="HASH_DIFF_02"), "extra_metadata": "pid=2"}

        n1 = _normalize_sync([row_a])
        n2 = _normalize_sync([row_b])

        assert n1[0]["query_hash"] != n2[0]["query_hash"]

    def test_hash_stable_across_calls(self):
        """Hash must be deterministic — same input always produces same output."""
        row = _raw_row(host="HASH_STABLE_03", query_details="SELECT stable")

        n1 = _normalize_sync([row])
        n2 = _normalize_sync([row])

        assert n1[0]["query_hash"] == n2[0]["query_hash"]
