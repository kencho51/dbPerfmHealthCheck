"""
Unit tests for the deadlock-specific extraction logic in
api/services/extractor.py.

Tests cover:
  TestIsRawDeadlockFormat      — column-based format detection helper
  TestProcessDeadlocksRaw      — raw-format CSV parsing (new SPL output)
  TestProcessDeadlocksLegacy   — legacy-format CSV parsing (old SPL output)
  TestExtractFromFileDeadlock  — extract_from_file() routing end-to-end

Run:
    uv run pytest tests/test_deadlock_extractor.py -v
"""
from __future__ import annotations

import io
import json
import textwrap
from pathlib import Path

import polars as pl
import pytest

from api.services.extractor import (
    _is_raw_deadlock_format,
    _process_deadlocks_legacy,
    _process_deadlocks_raw,
    extract_from_file,
)
from api.services.deadlock_parser import parse_raw


# ---------------------------------------------------------------------------
# Minimal _raw snippets used across tests
# ---------------------------------------------------------------------------

_RAW_TWO_WAY = """\
2026-02-28 17:57:48.24 spid19s     deadlock-list
2026-02-28 17:57:48.24 spid19s      deadlock victim=processAAA
2026-02-28 17:57:48.24 spid19s       process-list
2026-02-28 17:57:48.24 spid19s        process id=processAAA waitresource=KEY: 8:1 (fe3f) waittime=3040 logused=100 transactionname=user_transaction lasttranstarted=2026-02-28T17:57:45.177 lockMode=X kpid=100 spid=10 trancount=1 clientapp=wc_odds_req hostname=apphost01 loginname=DOM\\user1 isolationlevel=read committed (2) currentdb=8 currentdbname=my_db lockTimeout=4294967295
2026-02-28 17:57:48.24 spid19s         executionStack
2026-02-28 17:57:48.24 spid19s          frame procname=my_db.dbo.usp_do_work line=10 sqlhandle=0x0300080029AA
2026-02-28 17:57:48.24 spid19s     SELECT @x = col FROM my_table WHERE id = @id
2026-02-28 17:57:48.24 spid19s         inputbuf
2026-02-28 17:57:48.24 spid19s     Proc [Database Id = 8 Object Id = 12345]
2026-02-28 17:57:48.24 spid19s        process id=processBBB waitresource=KEY: 8:1 (157d) waittime=3040 logused=200 transactionname=user_transaction lasttranstarted=2026-02-28T17:57:45.163 lockMode=X kpid=200 spid=20 trancount=1 clientapp=wc_odds_req hostname=apphost02 loginname=DOM\\user1 isolationlevel=read committed (2) currentdb=8 currentdbname=my_db lockTimeout=4294967295
2026-02-28 17:57:48.24 spid19s         executionStack
2026-02-28 17:57:48.24 spid19s          frame procname=my_db.dbo.usp_do_work line=10 sqlhandle=0x0300080029AA
2026-02-28 17:57:48.24 spid19s     SELECT @x = col FROM my_table WHERE id = @id
2026-02-28 17:57:48.24 spid19s         inputbuf
2026-02-28 17:57:48.24 spid19s     Proc [Database Id = 8 Object Id = 12345]
2026-02-28 17:57:48.24 spid19s       resource-list
"""

_RAW_ADHOC_TWO_WAY = """\
2026-02-22 04:01:25.64 spid36s     deadlock-list
2026-02-22 04:01:25.64 spid36s      deadlock victim=processCCC
2026-02-22 04:01:25.64 spid36s       process-list
2026-02-22 04:01:25.64 spid36s        process id=processCCC waitresource=PAGE: 5:1:282896 waittime=450 logused=100 transactionname=implicit_transaction lasttranstarted=2026-02-22T04:01:25.173 lockMode=S kpid=300 spid=30 trancount=1 clientapp=JDBC hostname=appjdbc01 loginname=DOM\\svc1 isolationlevel=read committed (2) currentdb=5 currentdbname=sps_db lockTimeout=4294967295
2026-02-22 04:01:25.64 spid36s         executionStack
2026-02-22 04:01:25.64 spid36s          frame procname=adhoc line=1 sqlhandle=0xadhoc
2026-02-22 04:01:25.64 spid36s     unknown
2026-02-22 04:01:25.64 spid36s         inputbuf
2026-02-22 04:01:25.64 spid36s     SELECT id, name FROM dbo.events WHERE status = @P0
2026-02-22 04:01:25.64 spid36s        process id=processDDD waitresource=PAGE: 5:1:282897 waittime=449 logused=200 transactionname=implicit_transaction lasttranstarted=2026-02-22T04:01:25.173 lockMode=S kpid=400 spid=40 trancount=1 clientapp=JDBC hostname=appjdbc01 loginname=DOM\\svc1 isolationlevel=read committed (2) currentdb=5 currentdbname=sps_db lockTimeout=4294967295
2026-02-22 04:01:25.64 spid36s         executionStack
2026-02-22 04:01:25.64 spid36s          frame procname=adhoc line=1 sqlhandle=0xadhoc
2026-02-22 04:01:25.64 spid36s     unknown
2026-02-22 04:01:25.64 spid36s         inputbuf
2026-02-22 04:01:25.64 spid36s     SELECT id, name FROM dbo.events WHERE status = @P0
2026-02-22 04:01:25.64 spid36s       resource-list
"""


def _make_df(col_names: list[str], rows: list[dict]) -> pl.DataFrame:
    """Build a Polars DataFrame with all-string schema from column names and row dicts."""
    return pl.DataFrame(
        {col: [str(r.get(col, "")) for r in rows] for col in col_names}
    )


# ---------------------------------------------------------------------------
# TestIsRawDeadlockFormat
# ---------------------------------------------------------------------------

class TestIsRawDeadlockFormat:
    def test_raw_only_format_detected(self):
        assert _is_raw_deadlock_format(["_time", "host", "_raw", "id"]) is True

    def test_legacy_with_clean_query_not_raw(self):
        assert _is_raw_deadlock_format(["_time", "host", "_raw", "clean_query"]) is False

    def test_legacy_with_all_query_not_raw(self):
        assert _is_raw_deadlock_format(["_time", "host", "_raw", "all_query"]) is False

    def test_no_raw_column_is_not_raw_format(self):
        assert _is_raw_deadlock_format(["_time", "host", "clean_query"]) is False

    def test_column_names_are_case_insensitive(self):
        assert _is_raw_deadlock_format(["_TIME", "HOST", "_RAW"]) is True
        assert _is_raw_deadlock_format(["_TIME", "HOST", "_RAW", "CLEAN_QUERY"]) is False

    def test_extra_columns_do_not_affect_detection(self):
        assert _is_raw_deadlock_format(
            ["_time", "host", "id", "lockMode", "transactionname", "victim", "waittime", "_raw"]
        ) is True


# ---------------------------------------------------------------------------
# TestProcessDeadlocksRaw
# ---------------------------------------------------------------------------

class TestProcessDeadlocksRaw:
    @pytest.fixture
    def raw_df(self):
        return _make_df(
            ["_time", "host", "id", "lockMode", "victim", "waittime", "_raw"],
            [
                {
                    "_time":   "2026-02-28T17:57:48.240+0800",
                    "host":    "WGCSRV32",
                    "id":      "processAAA processBBB",
                    "lockMode": "X",
                    "victim":  "processAAA",
                    "waittime": "3040",
                    "_raw":    _RAW_TWO_WAY,
                },
            ],
        )

    def test_expands_one_row_to_two_processes(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        assert len(rows) == 2

    def test_each_row_has_required_keys(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        for row in rows:
            for key in ("time", "source", "host", "db_name", "environment",
                        "type", "query_details", "extra_metadata"):
                assert key in row, f"Missing key: {key}"

    def test_query_details_contains_actual_sql(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        for row in rows:
            assert "SELECT" in row["query_details"].upper()

    def test_extra_metadata_is_valid_json(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        for row in rows:
            meta = json.loads(row["extra_metadata"])
            assert isinstance(meta, dict)

    def test_extra_metadata_has_deadlock_id(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        for row in rows:
            meta = json.loads(row["extra_metadata"])
            assert "deadlock_id" in meta

    def test_extra_metadata_has_is_victim(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        victims = [r for r in rows if json.loads(r["extra_metadata"]).get("is_victim")]
        non_victims = [r for r in rows if not json.loads(r["extra_metadata"]).get("is_victim")]
        assert len(victims) == 1
        assert len(non_victims) == 1

    def test_db_name_populated_from_parser(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        for row in rows:
            assert row["db_name"] == "my_db"

    def test_environment_set_correctly(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        for row in rows:
            assert row["environment"] == "prod"

    def test_source_is_sql(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        for row in rows:
            assert row["source"] == "sql"

    def test_type_is_deadlock(self, raw_df):
        rows = _process_deadlocks_raw(raw_df, "prod", parse_raw)
        for row in rows:
            assert row["type"] == "deadlock"

    def test_fragment_row_with_empty_id_is_skipped(self):
        """Fragment rows (3-way deadlock overflow) with empty id + empty _raw are ignored."""
        df = _make_df(
            ["_time", "host", "id", "_raw"],
            [{"_time": "2026-02-28T10:11:36.050+0800", "host": "HOST", "id": "", "_raw": ""}],
        )
        rows = _process_deadlocks_raw(df, "prod", parse_raw)
        assert rows == []

    def test_two_splunk_rows_produce_four_process_rows(self):
        """Two separate deadlock events → 2 × 2 = 4 process rows."""
        df = _make_df(
            ["_time", "host", "id", "_raw"],
            [
                {"_time": "2026-02-28T17:57:48.240+0800", "host": "H1", "id": "A B", "_raw": _RAW_TWO_WAY},
                {"_time": "2026-02-22T04:01:25.640+0800", "host": "H2", "id": "C D", "_raw": _RAW_ADHOC_TWO_WAY},
            ],
        )
        rows = _process_deadlocks_raw(df, "sat", parse_raw)
        assert len(rows) == 4


# ---------------------------------------------------------------------------
# TestProcessDeadlocksLegacy
# ---------------------------------------------------------------------------

class TestProcessDeadlocksLegacy:
    @pytest.fixture
    def legacy_df(self):
        return _make_df(
            ["_time", "host", "hostname", "currentdbname", "id", "victim",
             "transactionname", "lockMode", "lockTimeout", "waittime",
             "count", "clean_query", "_raw"],
            [
                {
                    "_time":          "2026-02-28T17:57:48.240+0800",
                    "host":           "WGCSRV32",
                    "hostname":       "apphost01",
                    "currentdbname":  "my_db",
                    "id":             "processAAA processBBB",
                    "victim":         "processAAA",
                    "transactionname": "user_transaction",
                    "lockMode":       "X",
                    "lockTimeout":    "4294967295",
                    "waittime":       "3040",
                    "count":          "5",
                    "clean_query":    "SELECT @x = col FROM my_table WHERE id = @id",
                    "_raw":           _RAW_TWO_WAY,
                },
            ],
        )

    def test_one_legacy_row_produces_one_row(self, legacy_df):
        rows = _process_deadlocks_legacy(legacy_df, "prod", parse_raw)
        assert len(rows) == 1

    def test_query_details_from_clean_query(self, legacy_df):
        rows = _process_deadlocks_legacy(legacy_df, "prod", parse_raw)
        assert "SELECT" in rows[0]["query_details"].upper()

    def test_query_details_upgraded_if_parser_finds_better_sql(self, legacy_df):
        """Parser finds exec_sql with full DML — should replace frame-reference clean_query."""
        df = _make_df(
            ["_time", "host", "currentdbname", "count", "clean_query", "_raw"],
            [{
                "_time":         "2026-02-28T17:57:48.240+0800",
                "host":          "HOST",
                "currentdbname": "my_db",
                "count":         "1",
                "clean_query":   "frame procname=my_db.dbo.usp_do_work line=10 stmtstart=24158",
                "_raw":          _RAW_TWO_WAY,
            }],
        )
        rows = _process_deadlocks_legacy(df, "prod", parse_raw)
        # The parser finds actual SQL in exec_sql; it should replace the frame reference.
        assert not rows[0]["query_details"].startswith("frame procname=")

    def test_occurrence_count_from_count_column(self, legacy_df):
        rows = _process_deadlocks_legacy(legacy_df, "prod", parse_raw)
        assert rows[0]["occurrence_count"] == 5

    def test_extra_metadata_is_valid_json(self, legacy_df):
        rows = _process_deadlocks_legacy(legacy_df, "prod", parse_raw)
        meta = json.loads(rows[0]["extra_metadata"])
        assert isinstance(meta, dict)

    def test_extra_metadata_source_is_legacy_csv(self, legacy_df):
        rows = _process_deadlocks_legacy(legacy_df, "prod", parse_raw)
        meta = json.loads(rows[0]["extra_metadata"])
        assert meta.get("source") == "legacy_csv"

    def test_extra_metadata_has_structured_fields_from_parser(self, legacy_df):
        rows = _process_deadlocks_legacy(legacy_df, "prod", parse_raw)
        meta = json.loads(rows[0]["extra_metadata"])
        # Parser should enrich with pid, lockMode, etc.
        assert "deadlock_id" in meta
        assert "lockMode" in meta

    def test_fallback_metadata_when_raw_empty(self):
        """When _raw is absent, extra_metadata is built from CSV columns only."""
        df = _make_df(
            ["_time", "host", "currentdbname", "count", "clean_query",
             "victim", "lockMode", "lockTimeout", "waittime", "transactionname"],
            [{
                "_time":          "2026-02-22T04:01:25.640+0800",
                "host":           "HOST",
                "currentdbname":  "test_db",
                "count":          "2",
                "clean_query":    "INSERT INTO t VALUES (@P0)",
                "victim":         "proc_x",
                "lockMode":       "X",
                "lockTimeout":    "100",
                "waittime":       "50",
                "transactionname": "user_transaction",
            }],
        )
        rows = _process_deadlocks_legacy(df, "sat", parse_raw)
        assert len(rows) == 1
        meta = json.loads(rows[0]["extra_metadata"])
        assert meta["source"] == "legacy_csv"
        assert meta["lockMode"] == "X"
        assert meta["waittime"] == "50"

    def test_environment_set_correctly(self, legacy_df):
        rows = _process_deadlocks_legacy(legacy_df, "sat", parse_raw)
        assert rows[0]["environment"] == "sat"

    def test_host_from_host_column(self, legacy_df):
        rows = _process_deadlocks_legacy(legacy_df, "prod", parse_raw)
        assert rows[0]["host"] == "WGCSRV32"


# ---------------------------------------------------------------------------
# TestExtractFromFileDeadlock — end-to-end routing via extract_from_file()
# ---------------------------------------------------------------------------

class TestExtractFromFileDeadlock:
    def _write_csv(self, tmp_path: Path, filename: str, content: str) -> Path:
        p = tmp_path / filename
        p.write_text(textwrap.dedent(content).strip(), encoding="utf-8")
        return p

    def test_raw_format_filename_routed_correctly(self, tmp_path):
        """A CSV named deadlocksRaw* with _raw column uses the raw processor."""
        csv = self._write_csv(
            tmp_path,
            "deadlocksRawProdFeb26.csv",
            f"""_time,host,id,lockMode,victim,waittime,_raw
"2026-02-28T17:57:48.240+0800",WGCSRV32,"processAAA processBBB",X,processAAA,3040,"{_RAW_TWO_WAY.replace(chr(10), chr(10))}"
""",
        )
        rows = extract_from_file(csv)
        assert len(rows) >= 1
        assert all(r["type"] == "deadlock" for r in rows)
        assert all("extra_metadata" in r for r in rows)

    def test_legacy_format_detected_by_columns(self, tmp_path):
        """A CSV with clean_query column uses the legacy processor."""
        csv = self._write_csv(
            tmp_path,
            "deadlocksProdFeb26.csv",
            """_time,host,currentdbname,count,clean_query,_raw
2026-02-28T17:57:48.240+0800,WGCSRV32,my_db,3,SELECT 1,""",
        )
        rows = extract_from_file(csv)
        assert len(rows) == 1
        assert rows[0]["query_details"] == "SELECT 1"

    def test_rows_have_extra_metadata_field(self, tmp_path):
        csv = self._write_csv(
            tmp_path,
            "deadlocksProdFeb26.csv",
            """_time,host,currentdbname,count,clean_query
2026-02-28T17:57:48.240+0800,WGCSRV32,my_db,1,UPDATE t SET x=1""",
        )
        rows = extract_from_file(csv)
        assert rows[0].get("extra_metadata") is not None

    def test_environment_detected_from_prod_filename(self, tmp_path):
        csv = self._write_csv(
            tmp_path,
            "deadlocksProdFeb26.csv",
            "_time,host,currentdbname,count,clean_query\n2026-01-01,H,db,1,SELECT 1",
        )
        rows = extract_from_file(csv)
        assert rows[0]["environment"] == "prod"

    def test_environment_detected_from_sat_filename(self, tmp_path):
        csv = self._write_csv(
            tmp_path,
            "deadlocksSatFeb26.csv",
            "_time,host,currentdbname,count,clean_query\n2026-01-01,H,db,1,SELECT 1",
        )
        rows = extract_from_file(csv)
        assert rows[0]["environment"] == "sat"
