"""
Unit tests for the typed extractor functions in api/services/extractor.py.

Covers the refactored logic introduced in this session:
  TestExtractTypedBlockerPerSession — per-session format (current Splunk export)
  TestExtractTypedBlockerAggregated — legacy aggregated format
  TestExtractTypedSlowSql           — month_year derived from execution time
  TestExtractTypedDeadlockRaw       — month_year on each process row
  TestExtractTypedDeadlockLegacy    — month_year on legacy rows
  TestExtractTypedSlowMongo         — month_year derived from event_time
  TestHashUniqueness                — all types produce distinct hashes per event

Uses tmp_path fixtures to create real CSV files so pl.read_csv works normally.
No DB calls — extractors are pure transformation functions.

Run:
    uv run pytest tests/test_typed_extractor.py -v
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from api.services.extractor import (
    extract_typed_blocker,
    extract_typed_deadlock,
    extract_typed_slow_mongo,
    extract_typed_slow_sql,
)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict]) -> Path:
    """Write a list-of-dicts to a CSV file. Returns the path."""
    with path.open("w", newline="", encoding="utf-8") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)
    return path


# ---------------------------------------------------------------------------
# TestExtractTypedBlockerPerSession
# Validates the NEW per-session format detection added in this session.
# Previously extract_typed_blocker only understood the aggregated format, so
# all per-session files produced empty / identical hashes.
# ---------------------------------------------------------------------------

class TestExtractTypedBlockerPerSession:
    """Per-session CSV: _time, host, database_name, session_id, wait_type,
    command, head_blocker, query_text, blocked_sessions_count, total_blocked_wait_time_ms"""

    _ROWS = [
        {
            "_time": "2026-02-28T23:55:18.000+0800",
            "host": "WINFODB06HV11",
            "database_name": "oi_analytics_db",
            "session_id": "466",
            "wait_type": "",
            "command": "INSERT",
            "head_blocker": "1",
            "query_text": "SELECT * FROM dbo.log_activity WHERE system_id = @P0",
            "blocked_sessions_count": "9",
            "total_blocked_wait_time_ms": "522553",
        },
        {
            "_time": "2026-02-28T23:55:18.000+0800",
            "host": "WINFODB06HV11",
            "database_name": "oi_analytics_db",
            "session_id": "517",
            "wait_type": "",
            "command": "CONDITIONAL",
            "head_blocker": "1",
            "query_text": "SELECT * FROM dbo.ticket WHERE ticket_id = @P0",
            "blocked_sessions_count": "0",
            "total_blocked_wait_time_ms": "0",
        },
        {
            "_time": "2026-02-13T06:42:14.000+0800",   # different month doesn't appear here, same Feb
            "host": "QFMWDB1HV11",
            "database_name": "qfm_inbound_db",
            "session_id": "99",
            "wait_type": "PAGELATCH_EX",
            "command": "SELECT",
            "head_blocker": "1",
            "query_text": "SELECT id FROM dbo.queue WHERE status = @P0",
            "blocked_sessions_count": "3",
            "total_blocked_wait_time_ms": "15000",
        },
    ]

    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        return _write_csv(tmp_path / "blockersProdFeb26.csv", self._ROWS)

    def test_row_count_matches_csv(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        assert len(rows) == 3

    def test_month_year_derived_from_time(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        for r in rows:
            assert r["month_year"] == "2026-02", f"bad month_year: {r['month_year']!r}"

    def test_database_name_mapped_to_currentdbname(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        assert rows[0]["currentdbname"] == "oi_analytics_db"
        assert rows[2]["currentdbname"] == "qfm_inbound_db"

    def test_query_text_mapped_to_all_query(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        assert "log_activity" in rows[0]["all_query"]

    def test_session_id_mapped_to_victims(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        assert rows[0]["victims"] == "466"
        assert rows[1]["victims"] == "517"

    def test_wait_type_mapped_to_resources(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        assert rows[2]["resources"] == "PAGELATCH_EX"

    def test_command_mapped_to_lock_modes(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        assert rows[0]["lock_modes"] == "INSERT"

    def test_all_hashes_are_distinct(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        hashes = [tuple(r["_hash_parts"]) for r in rows]
        assert len(hashes) == len(set(hashes)), "duplicate _hash_parts found"

    def test_environment_detected_as_prod(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        for r in rows:
            assert r["environment"] == "prod"

    def test_table_type_is_blocker(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        for r in rows:
            assert r["table_type"] == "blocker"


class TestExtractTypedBlockerAggregated:
    """Aggregated/legacy CSV: currentdbname, victims, resources, lock_modes,
    count, latest, earliest, all_query"""

    _ROWS = [
        {
            "currentdbname": "wagering_db",
            "victims": "spid10 spid12",
            "resources": "PAGE:1:100",
            "lock_modes": "IX S",
            "count": "5",
            "latest":   "2026-02-15T10:00:00",
            "earliest": "2026-02-15T09:58:00",
            "all_query": "SELECT SUM(amount) FROM dbo.bet",
        },
        {
            "currentdbname": "accounts_db",
            "victims": "spid20",
            "resources": "KEY:1:200",
            "lock_modes": "X",
            "count": "2",
            "latest":   "2026-02-20T14:30:00",
            "earliest": "2026-02-20T14:28:00",
            "all_query": "UPDATE dbo.account SET balance = @P0 WHERE id = @P1",
        },
    ]

    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        return _write_csv(tmp_path / "blockersProdFeb26_legacy.csv", self._ROWS)

    def test_row_count_matches_csv(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        assert len(rows) == 2

    def test_month_year_from_earliest(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        for r in rows:
            assert r["month_year"] == "2026-02"

    def test_all_query_preserved(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        assert "dbo.bet" in rows[0]["all_query"]

    def test_all_hashes_distinct(self, csv_file: Path):
        rows = extract_typed_blocker(csv_file)
        hashes = [tuple(r["_hash_parts"]) for r in rows]
        assert len(hashes) == len(set(hashes))


# ---------------------------------------------------------------------------
# TestExtractTypedSlowSql
# ---------------------------------------------------------------------------

class TestExtractTypedSlowSql:
    _ROWS = [
        {
            "creation_time": "2026-02-01T08:00:00.000+0800",
            "last_execution_time": "2026-02-28T22:00:00.000+0800",
            "host": "WINDB01",
            "db_name": "wagering",
            "max_elapsed_time_s": "120.5",
            "avg_elapsed_time_s": "45.2",
            "total_elapsed_time_s": "9040",
            "total_worker_time_s": "8500",
            "avg_io": "5000",
            "avg_logical_reads": "4800",
            "avg_logical_writes": "100",
            "execution_count": "200",
            "query_final": "SELECT * FROM dbo.bet WHERE account_no = @P?",
        },
        {
            "creation_time": "2026-02-10T10:00:00.000+0800",
            "last_execution_time": "2026-02-25T12:00:00.000+0800",
            "host": "WINDB02",
            "db_name": "accounts",
            "max_elapsed_time_s": "60.0",
            "avg_elapsed_time_s": "30.0",
            "total_elapsed_time_s": "3000",
            "total_worker_time_s": "2900",
            "avg_io": "2000",
            "avg_logical_reads": "1900",
            "avg_logical_writes": "50",
            "execution_count": "100",
            "query_final": "UPDATE dbo.account SET balance = @P? WHERE id = @P?",
        },
    ]

    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        return _write_csv(tmp_path / "maxElapsedQueriesProdFeb26.csv", self._ROWS)

    def test_row_count(self, csv_file: Path):
        rows = extract_typed_slow_sql(csv_file)
        assert len(rows) == 2

    def test_month_year_from_execution_time(self, csv_file: Path):
        """month_year must be derived from last_execution_time (timezone-aware)."""
        rows = extract_typed_slow_sql(csv_file)
        for r in rows:
            assert r["month_year"] == "2026-02", f"got {r['month_year']!r}"

    def test_numeric_fields_converted(self, csv_file: Path):
        rows = extract_typed_slow_sql(csv_file)
        r = rows[0]
        assert isinstance(r["max_elapsed_time_s"], float)
        assert isinstance(r["execution_count"], int)
        assert r["max_elapsed_time_s"] == pytest.approx(120.5)
        assert r["execution_count"] == 200

    def test_query_final_placeholder_normalised(self, csv_file: Path):
        """_clean() should normalise @P0, @P1 → @P?"""
        rows = extract_typed_slow_sql(csv_file)
        assert "@P?" in rows[0]["query_final"]
        assert "@P0" not in rows[0]["query_final"]

    def test_environment_detected(self, csv_file: Path):
        rows = extract_typed_slow_sql(csv_file)
        for r in rows:
            assert r["environment"] == "prod"

    def test_table_type(self, csv_file: Path):
        rows = extract_typed_slow_sql(csv_file)
        for r in rows:
            assert r["table_type"] == "slow_sql"


# ---------------------------------------------------------------------------
# TestExtractTypedDeadlockRaw
# ---------------------------------------------------------------------------

_RAW_DEADLOCK = """\
2026-02-28 17:57:48.24 spid19s     deadlock-list
2026-02-28 17:57:48.24 spid19s      deadlock victim=processAAA
2026-02-28 17:57:48.24 spid19s       process-list
2026-02-28 17:57:48.24 spid19s        process id=processAAA waitresource=KEY: 8:1 (fe3f) waittime=3040 logused=100 transactionname=user_transaction lasttranstarted=2026-02-28T17:57:45.177 lockMode=X kpid=100 spid=10 trancount=1 clientapp=wcapp hostname=apphost01 loginname=DOM\\user1 isolationlevel=read committed (2) currentdb=8 currentdbname=my_db lockTimeout=4294967295
2026-02-28 17:57:48.24 spid19s         executionStack
2026-02-28 17:57:48.24 spid19s          frame procname=my_db.dbo.usp_insert line=5 sqlhandle=0xAAAA
2026-02-28 17:57:48.24 spid19s     INSERT INTO my_table (col) VALUES (@id)
2026-02-28 17:57:48.24 spid19s         inputbuf
2026-02-28 17:57:48.24 spid19s     Proc [Database Id = 8 Object Id = 1111]
2026-02-28 17:57:48.24 spid19s        process id=processBBB waitresource=KEY: 8:1 (157d) waittime=3040 logused=200 transactionname=user_transaction lasttranstarted=2026-02-28T17:57:45.163 lockMode=S kpid=200 spid=20 trancount=1 clientapp=wcapp hostname=apphost02 loginname=DOM\\user1 isolationlevel=read committed (2) currentdb=8 currentdbname=my_db lockTimeout=4294967295
2026-02-28 17:57:48.24 spid19s         executionStack
2026-02-28 17:57:48.24 spid19s          frame procname=my_db.dbo.usp_read line=10 sqlhandle=0xBBBB
2026-02-28 17:57:48.24 spid19s     SELECT col FROM my_table WHERE id = @id
2026-02-28 17:57:48.24 spid19s         inputbuf
2026-02-28 17:57:48.24 spid19s     Proc [Database Id = 8 Object Id = 2222]
2026-02-28 17:57:48.24 spid19s       resource-list
"""


class TestExtractTypedDeadlockRaw:
    _ROWS = [
        {
            "_time": "2026-02-28T17:57:48.000+0800",
            "host": "WINSQLDB01",
            "id": "processAAA|processBBB",
            "lockMode": "X",
            "transactionname": "user_transaction",
            "victim": "processAAA",
            "waittime": "3040",
            "_raw": _RAW_DEADLOCK,
        },
    ]

    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        return _write_csv(tmp_path / "deadlocksProdFeb26.csv", self._ROWS)

    def test_processes_expanded_to_separate_rows(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        assert len(rows) >= 2, "Two-way deadlock should yield at least 2 process rows"

    def test_month_year_on_every_row(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        assert rows, "no rows returned"
        for r in rows:
            assert r["month_year"] == "2026-02", f"row missing month_year: {r!r}"

    def test_db_name_extracted(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        assert any(r["db_name"] == "my_db" for r in rows)

    def test_table_type_is_deadlock(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        for r in rows:
            assert r["table_type"] == "deadlock"

    def test_is_victim_flag_set(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        victims = [r for r in rows if r.get("is_victim") == 1]
        assert len(victims) >= 1, "at least one victim expected"

    def test_hashes_are_distinct(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        hashes = [tuple(r["_hash_parts"]) for r in rows]
        assert len(hashes) == len(set(hashes))

    def test_environment_detected(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        for r in rows:
            assert r["environment"] == "prod"


class TestExtractTypedDeadlockLegacy:
    _ROWS = [
        {
            "_time": "2026-02-20T14:00:00.000+0800",
            "host": "WINSQLDB02",
            "currentdbname": "wagering",
            "victim": "spid44",
            "lockMode": "X",
            "waittime": "5000",
            "transactionname": "implicit_transaction",
            "clean_query": "UPDATE dbo.bet SET status = @P? WHERE id = @P?",
            "_raw": "",
        },
        {
            "_time": "2026-02-21T09:00:00.000+0800",
            "host": "WINSQLDB03",
            "currentdbname": "accounts",
            "victim": "spid55",
            "lockMode": "S",
            "waittime": "2000",
            "transactionname": "user_tran",
            "clean_query": "SELECT id FROM dbo.account WHERE account_no = @P?",
            "_raw": "",
        },
    ]

    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        return _write_csv(tmp_path / "deadlocksProdFeb26_legacy.csv", self._ROWS)

    def test_row_count(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        assert len(rows) == 2

    def test_month_year_from_time(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        for r in rows:
            assert r["month_year"] == "2026-02", f"got {r['month_year']!r}"

    def test_sql_text_from_clean_query(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        assert "dbo.bet" in rows[0]["sql_text"]
        assert "dbo.account" in rows[1]["sql_text"]

    def test_hashes_distinct(self, csv_file: Path):
        rows = extract_typed_deadlock(csv_file)
        hashes = [tuple(r["_hash_parts"]) for r in rows]
        assert len(hashes) == len(set(hashes))


# ---------------------------------------------------------------------------
# TestExtractTypedSlowMongo
# ---------------------------------------------------------------------------

class TestExtractTypedSlowMongo:
    _ROWS = [
        {
            "host": "WINMONGO01",
            "t.$date": "2026-02-15T03:00:00.000+0800",
            "attr.ns": "audit_db.log_event",
            "attr.durationMillis": "250",
            "attr.type": "remove",
            "attr.planSummary": "COLLSCAN",
            "attr.remote": "10.0.0.5:54321",
            "attr.queryShapeHash": "ABC123DEF456",
            "_raw": "",
        },
        {
            "host": "WINMONGO02",
            "t.$date": "2026-02-20T08:00:00.000+0800",
            "attr.ns": "wagering.bet",
            "attr.durationMillis": "1500",
            "attr.type": "find",
            "attr.planSummary": "IXSCAN { status: 1 }",
            "attr.remote": "10.0.0.6:12345",
            "attr.queryShapeHash": "XYZ789GHI000",
            "_raw": "",
        },
    ]

    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        return _write_csv(tmp_path / "mongodbSlowQueriesProdFeb26.csv", self._ROWS)

    def test_row_count(self, csv_file: Path):
        rows = extract_typed_slow_mongo(csv_file)
        assert len(rows) == 2

    def test_month_year_derived_from_event_time(self, csv_file: Path):
        rows = extract_typed_slow_mongo(csv_file)
        for r in rows:
            assert r["month_year"] == "2026-02", f"got {r['month_year']!r}"

    def test_db_and_collection_split(self, csv_file: Path):
        rows = extract_typed_slow_mongo(csv_file)
        assert rows[0]["db_name"] == "audit_db"
        assert rows[0]["collection"] == "log_event"
        assert rows[1]["db_name"] == "wagering"
        assert rows[1]["collection"] == "bet"

    def test_query_shape_hash_used_in_hash_parts(self, csv_file: Path):
        rows = extract_typed_slow_mongo(csv_file)
        assert "ABC123DEF456" in rows[0]["_hash_parts"]
        assert "XYZ789GHI000" in rows[1]["_hash_parts"]

    def test_hashes_distinct(self, csv_file: Path):
        rows = extract_typed_slow_mongo(csv_file)
        hashes = [tuple(r["_hash_parts"]) for r in rows]
        assert len(hashes) == len(set(hashes))

    def test_duration_ms_as_int(self, csv_file: Path):
        rows = extract_typed_slow_mongo(csv_file)
        assert rows[0]["duration_ms"] == 250
        assert rows[1]["duration_ms"] == 1500

    def test_table_type_is_slow_mongo(self, csv_file: Path):
        rows = extract_typed_slow_mongo(csv_file)
        for r in rows:
            assert r["table_type"] == "slow_mongo"

    def test_environment_detected_as_prod(self, csv_file: Path):
        rows = extract_typed_slow_mongo(csv_file)
        for r in rows:
            assert r["environment"] == "prod"


# ---------------------------------------------------------------------------
# TestHashUniqueness — cross-type: different files should never share hashes
# (regression: blocker per-session was sharing the same hash for all rows)
# ---------------------------------------------------------------------------

class TestHashUniqueness:
    def test_blocker_per_session_rows_all_unique(self, tmp_path: Path):
        rows_data = [
            {
                "_time": f"2026-02-{day:02d}T10:00:00.000+0800",
                "host": "WINDB01",
                "database_name": "wagering",
                "session_id": str(sid),
                "wait_type": "PAGELATCH_EX",
                "command": "SELECT",
                "head_blocker": "1",
                "query_text": "SELECT 1",   # intentionally identical SQL
                "blocked_sessions_count": "1",
                "total_blocked_wait_time_ms": "100",
            }
            for day, sid in [(1, 100), (1, 101), (2, 100), (3, 200)]
        ]
        csv_file = _write_csv(tmp_path / "blockersProdFeb26.csv", rows_data)
        rows = extract_typed_blocker(csv_file)
        hashes = [tuple(r["_hash_parts"]) for r in rows]
        assert len(hashes) == len(set(hashes)), "same SQL on different sessions/times must not collide"

    def test_slow_sql_same_query_different_hosts_unique(self, tmp_path: Path):
        rows_data = [
            {
                "creation_time": "2026-02-01T08:00:00",
                "last_execution_time": "2026-02-28T08:00:00",
                "host": f"WINDB0{i}",
                "db_name": "wagering",
                "max_elapsed_time_s": "10", "avg_elapsed_time_s": "5",
                "total_elapsed_time_s": "50", "total_worker_time_s": "40",
                "avg_io": "100", "avg_logical_reads": "90", "avg_logical_writes": "10",
                "execution_count": "5", "query_final": "SELECT 1",
            }
            for i in range(1, 4)
        ]
        csv_file = _write_csv(tmp_path / "maxElapsedQueriesProdFeb26.csv", rows_data)
        rows = extract_typed_slow_sql(csv_file)
        hashes = [tuple(r["_hash_parts"]) for r in rows]
        assert len(hashes) == len(set(hashes))
