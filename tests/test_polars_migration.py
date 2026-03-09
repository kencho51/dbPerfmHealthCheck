"""
Tests verifying the pandas → Polars migration in extractor.py and validator.py.

Run from project root:
    uv run pytest tests/test_polars_migration.py -v

Each test writes minimal fixture CSVs via tmp_path so no real data files are needed.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import polars as pl
import pytest

from api.services.extractor import (
    _clean,
    _detect_file_category,
    detect_file_category,
    extract_from_file,
)
from api.services.ingestor import _derive_month_year
from api.services.validator import ValidationResult, validate_csv


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_csv(tmp_path: Path, filename: str, content: str) -> Path:
    """Write *content* to *tmp_path / filename* and return the path."""
    p = tmp_path / filename
    p.write_text(textwrap.dedent(content).strip(), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# _clean() — null-handling with Polars row dicts (Python None, not NaN)
# ---------------------------------------------------------------------------

class TestClean:
    def test_none_returns_empty_string(self):
        assert _clean(None) == ""

    def test_whitespace_stripped(self):
        assert _clean("  hello  ") == "hello"

    def test_normalises_internal_whitespace(self):
        assert _clean("foo  bar\tbaz") == "foo bar baz"

    def test_sql_placeholders_normalised(self):
        assert _clean("SELECT @P0, @P1") == "SELECT @P?, @P?"

    def test_integer_coerced_to_string(self):
        assert _clean(42) == "42"

    def test_float_nan_previously_from_pandas_now_none(self):
        # In Polars iter_rows(named=True), missing values → None (never float NaN)
        # Confirm _clean still handles None correctly after the migration
        assert _clean(None) == ""


# ---------------------------------------------------------------------------
# _detect_file_category()
# ---------------------------------------------------------------------------

class TestDetectFileCategory:
    @pytest.mark.parametrize("filename, expected", [
        ("maxElapsedQueriesProdJan26.csv",   "slow_query_sql"),
        ("blockersProdJan26.csv",            "blocker"),
        ("deadlocksProdJan26.csv",           "deadlock"),
        ("mongodbSlowQueriesSatJan26.csv",   "slow_query_mongo"),
        ("dataFileSizeProdJan26.csv",        "unknown"),
    ])
    def test_category_detection(self, filename: str, expected: str):
        assert _detect_file_category(filename) == expected

    def test_public_alias(self):
        assert detect_file_category("maxElapsedQueriesSatJan26.csv") == "slow_query_sql"


# ---------------------------------------------------------------------------
# extract_from_file — slow_query_sql
# ---------------------------------------------------------------------------

class TestExtractSlowQuerySQL:
    FILENAME = "maxElapsedQueriesProdJan26.csv"

    def test_returns_list_of_dicts(self, tmp_path: Path):
        p = write_csv(tmp_path, self.FILENAME, """\
            host,db_name,creation_time,query_final
            WINDB01,oi_analytics_db,1/26/2026 8:58:53 AM,SELECT * FROM big_table
        """)
        rows = extract_from_file(p)
        assert isinstance(rows, list)
        assert len(rows) == 1

    def test_expected_keys(self, tmp_path: Path):
        p = write_csv(tmp_path, self.FILENAME, """\
            host,db_name,creation_time,query_final
            WINDB01,oi_analytics_db,1/26/2026 8:58:53 AM,SELECT 1
        """)
        row = extract_from_file(p)[0]
        assert set(row) == {"time", "source", "host", "db_name", "environment", "type", "query_details"}

    def test_values_correct(self, tmp_path: Path):
        p = write_csv(tmp_path, self.FILENAME, """\
            host,db_name,creation_time,query_final
            WINDB01,oi_analytics_db,1/26/2026 8:58:53 AM,SELECT 1
        """)
        row = extract_from_file(p)[0]
        assert row["host"] == "WINDB01"
        assert row["db_name"] == "oi_analytics_db"
        assert row["source"] == "sql"
        assert row["type"] == "slow_query"
        assert row["environment"] == "prod"
        assert row["query_details"] == "SELECT 1"

    def test_null_query_becomes_empty_string(self, tmp_path: Path):
        p = write_csv(tmp_path, self.FILENAME, """\
            host,db_name,creation_time,query_final
            WINDB01,oi_analytics_db,1/26/2026 8:58:53 AM,
        """)
        row = extract_from_file(p)[0]
        assert row["query_details"] == ""


# ---------------------------------------------------------------------------
# extract_from_file — blockers
# ---------------------------------------------------------------------------

class TestExtractBlockers:
    FILENAME = "blockersProdJan26.csv"

    def test_environment_prod(self, tmp_path: Path):
        p = write_csv(tmp_path, self.FILENAME, """\
            host,database_name,_time,query_text
            WINDB01,fb_db_v2,1/26/2026 9:00:00 AM,EXEC sp_something
        """)
        row = extract_from_file(p)[0]
        assert row["environment"] == "prod"
        assert row["type"] == "blocker"
        assert row["source"] == "sql"

    def test_db_name_from_database_name_column(self, tmp_path: Path):
        p = write_csv(tmp_path, self.FILENAME, """\
            host,database_name,_time,query_text
            WINDB01,wagering_db,1/26/2026 9:00:00 AM,EXEC sp_something
        """)
        row = extract_from_file(p)[0]
        assert row["db_name"] == "wagering_db"


# ---------------------------------------------------------------------------
# extract_from_file — deadlocks
# ---------------------------------------------------------------------------

class TestExtractDeadlocks:
    FILENAME = "deadlocksProdJan26.csv"

    def test_occurrence_count_from_count_column(self, tmp_path: Path):
        p = write_csv(tmp_path, self.FILENAME, """\
            host,currentdbname,earliest,all_query,count
            WINDB01,wagering,1/26/2026 9:00:00 AM,UPDATE combination SET x=1,5
        """)
        row = extract_from_file(p)[0]
        assert row["occurrence_count"] == 5

    def test_missing_count_defaults_to_1(self, tmp_path: Path):
        p = write_csv(tmp_path, self.FILENAME, """\
            host,currentdbname,earliest,all_query,count
            WINDB01,wagering,1/26/2026 9:00:00 AM,UPDATE combination SET x=1,
        """)
        row = extract_from_file(p)[0]
        assert row["occurrence_count"] == 1

    def test_sat_environment(self, tmp_path: Path):
        fname = "deadlocksSatJan26.csv"
        p = write_csv(tmp_path, fname, """\
            host,currentdbname,earliest,all_query,count
            WINDB01,mydb,1/26/2026 9:00:00 AM,DELETE FROM log,3
        """)
        row = extract_from_file(p)[0]
        assert row["environment"] == "sat"


# ---------------------------------------------------------------------------
# extract_from_file — unknown type returns empty list
# ---------------------------------------------------------------------------

class TestExtractUnknown:
    def test_datafilesize_returns_empty(self, tmp_path: Path):
        p = write_csv(tmp_path, "dataFileSizeProdJan26.csv", """\
            host,size_mb
            WINDB01,1024
        """)
        assert extract_from_file(p) == []


# ---------------------------------------------------------------------------
# validator — validate_csv()
# ---------------------------------------------------------------------------

class TestValidateCSV:
    def test_valid_slow_query_sql(self, tmp_path: Path):
        p = write_csv(tmp_path, "maxElapsedQueriesProdJan26.csv", """\
            host,db_name,creation_time,query_final
            WINDB01,oi_analytics_db,1/26/2026 8:58:53 AM,SELECT 1
            WINDB02,fb_db_v2,1/26/2026 9:00:00 AM,SELECT 2
        """)
        result = validate_csv(p)
        assert isinstance(result, ValidationResult)
        assert result.is_valid is True
        assert result.file_type == "slow_query_sql"
        assert result.environment == "prod"
        assert result.row_count == 2
        assert result.errors == []

    def test_missing_required_column(self, tmp_path: Path):
        # 'query_final' is required for slow_query_sql but omitted here
        p = write_csv(tmp_path, "maxElapsedQueriesProdJan26.csv", """\
            host,db_name,creation_time
            WINDB01,oi_analytics_db,1/26/2026 8:58:53 AM
        """)
        result = validate_csv(p)
        assert result.is_valid is False
        assert any("query_final" in e for e in result.errors)

    def test_empty_csv_is_invalid(self, tmp_path: Path):
        p = write_csv(tmp_path, "maxElapsedQueriesProdJan26.csv", """\
            host,db_name,creation_time,query_final
        """)
        result = validate_csv(p)
        assert result.is_valid is False
        assert result.row_count == 0

    def test_unknown_filename_is_invalid(self, tmp_path: Path):
        p = write_csv(tmp_path, "dataFileSizeProdJan26.csv", """\
            host,size_mb
            WINDB01,1024
        """)
        result = validate_csv(p)
        assert result.is_valid is False
        assert result.file_type == "unknown"

    def test_null_rate_computed_correctly(self, tmp_path: Path):
        # 2 rows, 1 host is null → null_rate for 'host' should be 0.5
        p = write_csv(tmp_path, "maxElapsedQueriesProdJan26.csv", """\
            host,db_name,creation_time,query_final
            WINDB01,mssql_db,1/26/2026 8:00 AM,SELECT 1
            ,other_db,1/26/2026 9:00 AM,SELECT 2
        """)
        result = validate_csv(p)
        assert "host" in result.null_rates
        assert result.null_rates["host"] == 0.5

    def test_sample_rows_returned(self, tmp_path: Path):
        p = write_csv(tmp_path, "maxElapsedQueriesProdJan26.csv", """\
            host,db_name,creation_time,query_final
            WINDB01,oi_analytics_db,1/26/2026 8:58:53 AM,SELECT 1
        """)
        result = validate_csv(p)
        assert len(result.sample_rows) == 1
        assert result.sample_rows[0]["host"] == "WINDB01"

    def test_large_string_truncated_in_sample(self, tmp_path: Path):
        long_query = "X" * 600
        p = write_csv(tmp_path, "maxElapsedQueriesProdJan26.csv",
                      f"host,db_name,creation_time,query_final\nWINDB01,db,1/1/2026,{long_query}")
        result = validate_csv(p)
        assert len(result.sample_rows) == 1
        q = result.sample_rows[0]["query_final"]
        assert len(q) <= 514  # 500 chars + "…[truncated]"
        assert "truncated" in q

    def test_to_dict_interface_preserved(self, tmp_path: Path):
        p = write_csv(tmp_path, "maxElapsedQueriesProdJan26.csv", """\
            host,db_name,creation_time,query_final
            WINDB01,oi_analytics_db,1/26/2026 8:58:53 AM,SELECT 1
        """)
        result = validate_csv(p)
        d = result.to_dict()
        assert set(d.keys()) == {"is_valid", "file_type", "environment", "row_count",
                                  "warnings", "errors", "null_rates", "sample_rows"}

    def test_unknown_environment_adds_warning(self, tmp_path: Path):
        p = write_csv(tmp_path, "maxElapsedQueriesTestJan26.csv", """\
            host,db_name,creation_time,query_final
            WINDB01,mydb,1/26/2026 8:00:00 AM,SELECT 1
        """)
        result = validate_csv(p)
        assert result.environment == "unknown"
        assert any("environment" in w.lower() for w in result.warnings)


# ---------------------------------------------------------------------------
# ingestor — _derive_month_year() scalar function unchanged
# ---------------------------------------------------------------------------

class TestDeriveMonthYear:
    @pytest.mark.parametrize("time_str, expected", [
        ("1/26/2026 8:58:53 AM",   "2026-01"),
        ("2026-01-26T08:58:53",    "2026-01"),
        ("2026-01-26 08:58:53",    "2026-01"),
        ("Jan 26 2026  9:00AM",    "2026-01"),
    ])
    def test_known_formats(self, time_str: str, expected: str):
        assert _derive_month_year(time_str) == expected

    def test_none_returns_none(self):
        assert _derive_month_year(None) is None

    def test_bad_value_returns_none(self):
        assert _derive_month_year("not-a-date") is None

    def test_empty_string_returns_none(self):
        assert _derive_month_year("") is None
