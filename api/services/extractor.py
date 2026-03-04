"""
QueryExtractor service — promoted from scripts/extract_all_queries_refactored.py.

Key changes from the original:
  - Hardcoded PROJECT_ROOT / DATA_DIR / OUTPUT_DIR constants removed.
  - `process_single_csv` and `process_directory` now return rows directly
    instead of accumulating into `self.master_data`.
  - No CSV output logic (the API layer owns persistence).
  - Public helper `extract_from_path(path)` is the primary entry point for
    the upload and validate routers.

Usage inside FastAPI:
    from api.services.extractor import extract_from_path
    rows = extract_from_path(Path("/tmp/uploaded.csv"))
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Expected columns per file type (used by the validator service too)
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "slow_query_sql":  ["host", "db_name", "query_final"],
    "blocker":         ["host", "database_name", "query_text"],
    "deadlock":        ["host"],
    "slow_query_mongo": ["host", "_raw"],
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_environment(filename: str) -> str:
    name = filename.lower()
    if "prod" in name:
        return "prod"
    if "sat" in name:
        return "sat"
    return "unknown"


def _extract_query_type(filename: str) -> str:
    name = filename.lower()
    if "maxelapsed" in name or ("slow" in name and "mongodb" not in name):
        return "slow_query"
    if "blocker" in name:
        return "blocker"
    if "deadlock" in name:
        return "deadlock"
    return "unknown"


def _detect_file_category(filename: str) -> str:
    """Return internal category key used for routing to the right processor."""
    name = filename.lower()
    if "maxelapsed" in name:
        return "slow_query_sql"
    if "blocker" in name:
        return "blocker"
    if "deadlock" in name:
        return "deadlock"
    if "mongodb" in name and "slow" in name:
        return "slow_query_mongo"
    return "unknown"


def _clean(val: object) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    # normalise whitespace
    s = re.sub(r"\s+", " ", s)
    # normalise SQL param placeholders @P0, @P1 → @P?
    s = re.sub(r"@P\d+", "@P?", s)
    return s


def _get(row: pd.Series, *keys: str) -> str:
    """Return the first non-null value found among *keys*."""
    for k in keys:
        v = row.get(k, None)
        if v is not None and not (isinstance(v, float) and pd.isna(v)):
            return _clean(v)
    return ""


def _extract_mongodb_command(raw_json: str) -> str:
    try:
        if not raw_json:
            return ""
        data = json.loads(raw_json)
        if "attr" in data and "command" in data["attr"]:
            return json.dumps(data["attr"]["command"], separators=(",", ":"))
        if "attr" in data and "type" in data["attr"]:
            return f'{{"type":"{data["attr"]["type"]}"}}'
    except (json.JSONDecodeError, KeyError, TypeError):
        prefix = str(raw_json)[:100].replace('"', "'")
        return f'{{"error":"parse_failed","raw_prefix":"{prefix}"}}'
    return ""


# ---------------------------------------------------------------------------
# Per-type processors
# ---------------------------------------------------------------------------

def _process_slow_query_sql(file_path: Path) -> list[dict]:
    df = pd.read_csv(file_path, encoding="utf-8")
    env = _extract_environment(file_path.name)
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "time":          _get(row, "creation_time", "last_execution_time"),
            "source":        "sql",
            "host":          _get(row, "host"),
            "db_name":       _get(row, "db_name"),
            "environment":   env,
            "type":          "slow_query",
            "query_details": _clean(row.get("query_final", "")),
        })
    return rows


def _process_blockers(file_path: Path) -> list[dict]:
    df = pd.read_csv(file_path, encoding="utf-8")
    env = _extract_environment(file_path.name)
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "time":          _get(row, "_time"),
            "source":        "sql",
            "host":          _get(row, "host"),
            "db_name":       _get(row, "database_name"),
            "environment":   env,
            "type":          "blocker",
            "query_details": _clean(row.get("query_text", "")),
        })
    return rows


def _process_deadlocks(file_path: Path) -> list[dict]:
    df = pd.read_csv(file_path, encoding="utf-8")
    env = _extract_environment(file_path.name)
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "time":          _get(row, "earliest", "_time"),
            "source":        "sql",
            "host":          _get(row, "host"),
            "db_name":       _get(row, "currentdbname", "database_name", "db_name"),
            "environment":   env,
            "type":          "deadlock",
            "query_details": _get(row, "all_query", "query_text", "statement",
                                       "sql_text", "deadlock_graph"),
        })
    return rows


def _process_mongodb_slow(file_path: Path) -> list[dict]:
    df = pd.read_csv(file_path, encoding="utf-8")
    env = _extract_environment(file_path.name)
    rows = []
    for _, row in df.iterrows():
        # derive db from namespace  "dbname.collection"
        ns = _get(row, "attr.ns")
        db_name = ns.split(".")[0] if ns else ""

        raw = row.get("_raw", "")
        raw_str = "" if (raw is None or (isinstance(raw, float) and pd.isna(raw))) else str(raw)

        rows.append({
            "time":          _get(row, "t.$date"),
            "source":        "mongodb",
            "host":          _get(row, "host"),
            "db_name":       db_name,
            "environment":   env,
            "type":          "slow_query",
            "query_details": _extract_mongodb_command(raw_str),
        })
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_from_file(file_path: Path) -> list[dict]:
    """
    Extract rows from a single CSV file.
    Returns a list of dicts with keys:
        time, source, host, db_name, environment, type, query_details
    """
    category = _detect_file_category(file_path.name)
    if category == "slow_query_sql":
        return _process_slow_query_sql(file_path)
    if category == "blocker":
        return _process_blockers(file_path)
    if category == "deadlock":
        return _process_deadlocks(file_path)
    if category == "slow_query_mongo":
        return _process_mongodb_slow(file_path)
    return []   # unknown / unsupported type (e.g. datafilesize CSVs)


def extract_from_directory(directory: Path) -> list[dict]:
    """Extract from all recognisable CSV files in *directory* (non-recursive)."""
    all_rows: list[dict] = []
    for csv_file in sorted(directory.glob("*.csv")):
        category = _detect_file_category(csv_file.name)
        if category == "unknown":
            continue   # skip datafilesize etc.
        all_rows.extend(extract_from_file(csv_file))
    return all_rows


def extract_from_path(path: Path) -> list[dict]:
    """Unified entry point — accepts either a file or a directory."""
    if path.is_dir():
        return extract_from_directory(path)
    return extract_from_file(path)


def detect_file_category(filename: str) -> str:
    """Expose category detection for the validator service."""
    return _detect_file_category(filename)
