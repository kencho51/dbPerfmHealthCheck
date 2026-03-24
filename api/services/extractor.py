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

import polars as pl


# ---------------------------------------------------------------------------
# Expected columns per file type (used by the validator service too)
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "slow_query_sql":   ["host", "db_name", "query_final"],
    "blocker":          ["host", "database_name", "query_text"],
    # Deadlock accepts two CSV formats:
    #   Raw (new):    _time, host, id, lockMode, transactionname, victim, waittime, _raw
    #   Legacy (old): _time, host, hostname, currentdbname, id, victim, transactionname,
    #                 lockMode, lockTimeout, waittime, es_text, clean_query, _raw
    "deadlock":         ["host", "_raw"],
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
    if val is None:
        return ""
    s = str(val).strip()
    # normalise whitespace
    s = re.sub(r"\s+", " ", s)
    # normalise SQL param placeholders @P0, @P1 → @P?
    s = re.sub(r"@P\d+", "@P?", s)
    return s


def _get(row: dict, *keys: str) -> str:
    """Return the first non-null value found among *keys."""
    for k in keys:
        v = row.get(k)
        if v is not None:
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
    df = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    rows = []
    for row in df.iter_rows(named=True):
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
    df = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    rows = []
    for row in df.iter_rows(named=True):
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


def _is_raw_deadlock_format(columns: list[str]) -> bool:
    """
    Return True when the CSV was exported with the minimal-SPL raw format
    (``_raw`` present but no pre-extracted ``clean_query`` / ``all_query``).
    Return False for the legacy aggregated format that has ``clean_query`` or
    ``all_query`` already extracted by SPL.
    """
    col_set = set(c.strip().lower() for c in columns)
    has_raw   = "_raw" in col_set
    has_agg   = bool({"clean_query", "all_query"} & col_set)
    return has_raw and not has_agg


def _process_deadlocks(file_path: Path) -> list[dict]:
    """
    Extract deadlock rows from a Splunk CSV.

    Supports two formats:

    **Raw format** (new, minimal SPL)::

        _time, host, id, lockMode, transactionname, victim, waittime, _raw

    Each Splunk row contains a full deadlock graph in ``_raw``.  The parser
    expands it into one row *per process* (victim + waiter), extracting all
    structured attributes into ``extra_metadata``.

    **Legacy format** (old, aggregated SPL)::

        _time, host, hostname, currentdbname, id, victim, transactionname,
        lockMode, lockTimeout, waittime, es_text, clean_query, _raw

    ``clean_query`` / ``all_query`` are used directly as ``query_details``.  If
    ``_raw`` is present the parser is still run to enrich ``extra_metadata``
    (victim flag, waitresource, etc.).  Falls back gracefully when ``_raw``
    cannot be parsed.
    """
    from api.services.deadlock_parser import parse_raw

    df  = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    columns: list[str] = df.columns

    if _is_raw_deadlock_format(columns):
        return _process_deadlocks_raw(df, env, parse_raw)
    return _process_deadlocks_legacy(df, env, parse_raw)


def _process_deadlocks_raw(
    df: "pl.DataFrame",
    env: str,
    parse_raw,
) -> list[dict]:
    """
    Expand each Splunk row (containing a full deadlock graph in ``_raw``) into
    one dict per process using the DeadlockParser.
    """
    rows: list[dict] = []

    for row in df.iter_rows(named=True):
        raw        = row.get("_raw") or ""
        splunk_time = _get(row, "_time")
        splunk_host = _get(row, "host")

        # Skip fragment rows (3-way deadlock overflow with empty id).
        if not (row.get("id") or "").strip() and not raw.strip():
            continue

        processes = parse_raw(raw, splunk_time, splunk_host)

        if not processes:
            # Fallback: emit at least one row with whatever we have.
            rows.append({
                "time":           splunk_time,
                "source":         "sql",
                "host":           splunk_host,
                "db_name":        "",
                "environment":    env,
                "type":           "deadlock",
                "query_details":  _get(row, "id"),   # process IDs as last resort
                "extra_metadata": None,
            })
            continue

        for proc in processes:
            rows.append({
                "time":           proc.splunk_time,
                "source":         "sql",
                "host":           proc.splunk_host,
                "db_name":        proc.currentdbname,
                "environment":    env,
                "type":           "deadlock",
                "query_details":  proc.sql_text,
                "extra_metadata": proc.to_extra_metadata(),
            })

    return rows


def _process_deadlocks_legacy(
    df: "pl.DataFrame",
    env: str,
    parse_raw,
) -> list[dict]:
    """
    Process the legacy aggregated format.  ``clean_query`` / ``all_query`` are
    used as ``query_details``.  ``_raw`` (when present) is parsed to extract
    ``extra_metadata``; falls back to the columns already available.
    """
    import json

    rows: list[dict] = []

    for row in df.iter_rows(named=True):
        raw        = row.get("_raw") or ""
        splunk_time = _get(row, "earliest", "_time")
        splunk_host = _get(row, "host")

        # SQL text — prefer pre-extracted column, fall back to others.
        query_details = _get(row, "clean_query", "all_query", "query_text",
                                  "statement", "sql_text", "deadlock_graph")

        # Splunk aggregated occurrences.
        raw_count = row.get("count")
        occ = int(raw_count) if raw_count not in (None, "") else 1

        # Try to enrich with structured metadata from _raw.
        extra_metadata: str | None = None
        if raw:
            processes = parse_raw(raw, splunk_time, splunk_host)
            if processes:
                # Find the process whose sql_text best matches clean_query.
                matched = next(
                    (p for p in processes
                     if query_details and
                     p.sql_text and query_details[:60] in p.sql_text),
                    processes[0],   # fallback to first process
                )
                # If the parser found a better SQL (starts with a DML keyword
                # rather than a "frame procname=" reference), prefer it.
                _dml = re.compile(
                    r"^\s*(?:SELECT|INSERT|UPDATE|DELETE|EXEC|WITH|MERGE|@\w)", re.I
                )
                if matched.sql_text and _dml.match(matched.sql_text):
                    query_details = matched.sql_text
                # Merge any CSV-level fields into the parsed metadata.
                meta         = json.loads(matched.to_extra_metadata())
                meta["source"] = "legacy_csv"
                # Prefer CSV hostname over parsed (CSV is already extracted by SPL).
                if not meta.get("apphost"):
                    meta["apphost"] = _get(row, "hostname")
                extra_metadata = json.dumps(meta, ensure_ascii=False)

        # If _raw parsing failed, build best-effort metadata from CSV columns.
        if extra_metadata is None:
            meta = {
                "source":          "legacy_csv",
                "deadlock_victim": _get(row, "victim"),
                "lockMode":        _get(row, "lockMode"),
                "lockTimeout":     _get(row, "lockTimeout"),
                "waittime":        _get(row, "waittime"),
                "transactionname": _get(row, "transactionname"),
                "apphost":         _get(row, "hostname"),
            }
            extra_metadata = json.dumps(
                {k: v for k, v in meta.items() if v},
                ensure_ascii=False,
            )

        rows.append({
            "time":             splunk_time,
            "source":           "sql",
            "host":             splunk_host,
            "db_name":          _get(row, "currentdbname", "database_name", "db_name"),
            "environment":      env,
            "type":             "deadlock",
            "occurrence_count": occ,
            "query_details":    query_details,
            "extra_metadata":   extra_metadata,
        })

    return rows


def _process_mongodb_slow(file_path: Path) -> list[dict]:
    df = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    rows = []
    for row in df.iter_rows(named=True):
        # derive db from namespace  "dbname.collection"
        ns = _get(row, "attr.ns")
        db_name = ns.split(".")[0] if ns else ""

        raw = row.get("_raw")
        raw_str = "" if raw is None else str(raw)

        rows.append({
            "time":          _get(row, "t.$date"),
            "source":        "mongodb",
            "host":          _get(row, "host"),
            "db_name":       db_name,
            "environment":   env,
            "type":          "slow_query_mongo",
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


# ---------------------------------------------------------------------------
# Typed extractors — full native CSV columns for each file type
# These feed into the raw_query_* type-specific tables.
# ---------------------------------------------------------------------------

def _safe_float(val: object) -> Optional[float]:
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def _safe_int(val: object) -> Optional[int]:
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(float(str(val).strip().replace(",", "")))
    except (ValueError, TypeError):
        return None


def _detect_typed_table(filename: str) -> str:
    """Map filename → type-specific table key."""
    name = filename.lower()
    if "maxelapsed" in name:
        return "slow_sql"
    if "blocker" in name:
        return "blocker"
    if "deadlock" in name:
        return "deadlock"
    if "mongodbslowqueries" in name or ("mongodb" in name and "slow" in name and "datafile" not in name):
        return "slow_mongo"
    if "mongodbdatafilesize" in name or ("mongodb" in name and "datafile" in name):
        return "datafile_mongo"
    if "datafilesize" in name:
        return "datafile_sql"
    return "unknown"


def extract_typed_slow_sql(file_path: Path) -> list[dict]:
    """
    maxElapsedQueries*.csv — returns all native timing/IO columns.

    CSV header:
        "creation_time","last_execution_time",host,"db_name",
        "max_elapsed_time_s","avg_elapsed_time_s","total_elapsed_time_s",
        "total_worker_time_s","avg_io","avg_logical_reads","avg_logical_writes",
        "execution_count","query_final"
    """
    df = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    rows = []
    for row in df.iter_rows(named=True):
        query_final = _clean(row.get("query_final", ""))
        rows.append({
            "table_type":           "slow_sql",
            "host":                 _get(row, "host"),
            "db_name":              _get(row, "db_name"),
            "environment":          env,
            "creation_time":        _get(row, "creation_time"),
            "last_execution_time":  _get(row, "last_execution_time"),
            "max_elapsed_time_s":   _safe_float(row.get("max_elapsed_time_s")),
            "avg_elapsed_time_s":   _safe_float(row.get("avg_elapsed_time_s")),
            "total_elapsed_time_s": _safe_float(row.get("total_elapsed_time_s")),
            "total_worker_time_s":  _safe_float(row.get("total_worker_time_s")),
            "avg_io":               _safe_float(row.get("avg_io")),
            "avg_logical_reads":    _safe_float(row.get("avg_logical_reads")),
            "avg_logical_writes":   _safe_float(row.get("avg_logical_writes")),
            "execution_count":      _safe_int(row.get("execution_count")),
            "query_final":          query_final,
            # Dedup key fields
            "_hash_parts":          [
                _get(row, "host"),
                _get(row, "db_name"),
                env,
                query_final,
            ],
        })
    return rows


def extract_typed_blocker(file_path: Path) -> list[dict]:
    """
    blockers*.csv — returns all native blocker columns.

    CSV header:
        currentdbname, victims, resources, "lock_modes", count,
        latest, earliest, "all_query"
    """
    df = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    rows = []
    for row in df.iter_rows(named=True):
        all_query = _clean(row.get("all_query", ""))
        lock_modes = _get(row, "lock_modes")
        currentdbname = _get(row, "currentdbname")
        rows.append({
            "table_type":     "blocker",
            "environment":    env,
            "currentdbname":  currentdbname,
            "victims":        _get(row, "victims"),
            "resources":      _get(row, "resources"),
            "lock_modes":     lock_modes,
            "count":          _safe_int(row.get("count")),
            "latest":         _get(row, "latest"),
            "earliest":       _get(row, "earliest"),
            "all_query":      all_query,
            "_hash_parts":    [env, currentdbname, lock_modes, all_query],
        })
    return rows


def extract_typed_deadlock(file_path: Path) -> list[dict]:
    """
    deadlocks*.csv — returns per-process rows with all structured columns.
    Supports both raw SPL format and legacy aggregated format.
    """
    from api.services.deadlock_parser import parse_raw
    import json

    df  = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    columns: list[str] = df.columns
    rows: list[dict] = []

    if _is_raw_deadlock_format(columns):
        # Raw format: expand each Splunk row into one row per process
        for row in df.iter_rows(named=True):
            raw = row.get("_raw") or ""
            splunk_time = _get(row, "_time")
            splunk_host = _get(row, "host")

            processes = parse_raw(raw, splunk_time, splunk_host)
            if not processes:
                sql_text = _get(row, "id")
                rows.append({
                    "table_type":       "deadlock",
                    "host":             splunk_host,
                    "db_name":          "",
                    "environment":      env,
                    "event_time":       splunk_time,
                    "deadlock_id":      None,
                    "is_victim":        None,
                    "lock_mode":        None,
                    "wait_resource":    None,
                    "wait_time_ms":     None,
                    "transaction_name": None,
                    "app_host":         None,
                    "sql_text":         sql_text,
                    "raw_xml":          raw[:4000] if raw else None,
                    "_hash_parts":      [splunk_host, "", env, sql_text, ""],
                })
                continue
            for proc in processes:
                meta = json.loads(proc.to_extra_metadata()) if proc.to_extra_metadata() else {}
                sql_text = proc.sql_text or ""
                lock_mode = meta.get("lockMode", "")
                rows.append({
                    "table_type":       "deadlock",
                    "host":             proc.splunk_host,
                    "db_name":          proc.currentdbname,
                    "environment":      env,
                    "event_time":       proc.splunk_time,
                    "deadlock_id":      meta.get("deadlock_id"),
                    "is_victim":        1 if meta.get("is_victim") else 0,
                    "lock_mode":        lock_mode,
                    "wait_resource":    meta.get("waitresource"),
                    "wait_time_ms":     _safe_int(meta.get("waittime")),
                    "transaction_name": meta.get("transactionname"),
                    "app_host":         meta.get("apphost"),
                    "sql_text":         sql_text,
                    "raw_xml":          raw[:4000] if raw else None,
                    "_hash_parts":      [
                        proc.splunk_host,
                        proc.currentdbname,
                        env,
                        sql_text,
                        lock_mode,
                    ],
                })
    else:
        # Legacy aggregated format
        for row in df.iter_rows(named=True):
            raw = row.get("_raw") or ""
            splunk_time = _get(row, "earliest", "_time")
            splunk_host = _get(row, "host")
            query_details = _get(row, "clean_query", "all_query", "query_text",
                                  "statement", "sql_text", "deadlock_graph")
            lock_mode = _get(row, "lockMode", "lock_modes", "lock_mode")

            # Try to enrich from _raw
            extra: dict = {}
            if raw:
                processes = parse_raw(raw, splunk_time, splunk_host)
                if processes:
                    matched = next(
                        (p for p in processes
                         if query_details and p.sql_text
                         and query_details[:60] in p.sql_text),
                        processes[0],
                    )
                    extra = json.loads(matched.to_extra_metadata()) if matched.to_extra_metadata() else {}
                    if not query_details and matched.sql_text:
                        query_details = matched.sql_text

            rows.append({
                "table_type":       "deadlock",
                "host":             splunk_host,
                "db_name":          _get(row, "currentdbname", "database_name", "db_name"),
                "environment":      env,
                "event_time":       splunk_time,
                "deadlock_id":      extra.get("deadlock_id"),
                "is_victim":        1 if extra.get("is_victim") or _get(row, "victim") else 0,
                "lock_mode":        lock_mode,
                "wait_resource":    extra.get("waitresource"),
                "wait_time_ms":     _safe_int(extra.get("waittime") or row.get("waittime")),
                "transaction_name": extra.get("transactionname") or _get(row, "transactionname"),
                "app_host":         extra.get("apphost") or _get(row, "hostname"),
                "sql_text":         query_details,
                "raw_xml":          raw[:4000] if raw else None,
                "_hash_parts":      [
                    splunk_host,
                    _get(row, "currentdbname", "database_name", "db_name"),
                    env,
                    query_details,
                    lock_mode,
                ],
            })

    return rows


def extract_typed_slow_mongo(file_path: Path) -> list[dict]:
    """
    mongodbSlowQueries*.csv — returns all native MongoDB slow op columns.

    CSV columns include: host, t.$date, attr.ns, attr.durationMillis,
    attr.planSummary, attr.type, attr.remote, _raw
    """
    df = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    rows = []
    for row in df.iter_rows(named=True):
        ns = _get(row, "attr.ns")
        parts = ns.split(".", 1) if ns else ["", ""]
        db_name    = parts[0] if parts else ""
        collection = parts[1] if len(parts) > 1 else ""

        raw = row.get("_raw")
        raw_str = "" if raw is None else str(raw)
        command_json = _extract_mongodb_command(raw_str)
        op_type = _get(row, "attr.type")
        host    = _get(row, "host")

        rows.append({
            "table_type":    "slow_mongo",
            "host":          host,
            "db_name":       db_name,
            "collection":    collection,
            "environment":   env,
            "event_time":    _get(row, "t.$date"),
            "duration_ms":   _safe_int(row.get("attr.durationMillis")),
            "plan_summary":  _get(row, "attr.planSummary"),
            "op_type":       op_type,
            "remote_client": _get(row, "attr.remote"),
            "command_json":  command_json,
            "_hash_parts":   [host, db_name, env, op_type, command_json],
        })
    return rows


def extract_typed_datafile_sql(file_path: Path) -> list[dict]:
    """
    dataFileSize*.csv — returns all native data-file size columns.

    CSV header:
        updated, db, host, Path, trend, is_up, range_mb,
        "used_%", used_mb, allocated_mb, free, target_allocation_mb
    """
    df = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    rows = []
    for row in df.iter_rows(named=True):
        host       = _get(row, "host")
        db_name    = _get(row, "db")
        file_path_ = _get(row, "Path")
        updated    = _get(row, "updated")
        rows.append({
            "table_type":          "datafile_sql",
            "environment":         env,
            "host":                host,
            "db_name":             db_name,
            "file_path":           file_path_,
            "updated_at_source":   updated,
            "trend":               _get(row, "trend"),
            "is_up":               _get(row, "is_up"),
            "range_mb":            _safe_float(row.get("range_mb")),
            "used_pct":            _safe_float(row.get("used_%")),
            "used_mb":             _safe_float(row.get("used_mb")),
            "allocated_mb":        _safe_float(row.get("allocated_mb")),
            "free":                _get(row, "free"),
            "target_allocation_mb": _safe_float(row.get("target_allocation_mb")),
            "_hash_parts":         [host, db_name, env, file_path_, updated],
        })
    return rows


def extract_typed_datafile_mongo(file_path: Path) -> list[dict]:
    """
    mongodbDataFileSize*.csv (and aggregated variant) — returns all native
    MongoDB storage utilisation columns.

    CSV header:
        "host_mount", "max_storage", "avg_storage", "max_storage_free",
        "avg_storage_free", "max_storage_free_pct", "avg_storage_free_pct",
        "max_storage_used", "avg_storage_used", "max_used_percent",
        "avg_used_percent"
    """
    df = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    # Derive month_year hint from filename (e.g. "Jan26" → "2026-01")
    rows = []
    for row in df.iter_rows(named=True):
        host_mount = _get(row, "host_mount")
        rows.append({
            "table_type":          "datafile_mongo",
            "environment":         env,
            "host_mount":          host_mount,
            "max_storage_mb":      _safe_float(row.get("max_storage")),
            "avg_storage_mb":      _safe_float(row.get("avg_storage")),
            "max_storage_free_mb": _safe_float(row.get("max_storage_free")),
            "avg_storage_free_mb": _safe_float(row.get("avg_storage_free")),
            "max_storage_free_pct": _safe_float(row.get("max_storage_free_pct")),
            "avg_storage_free_pct": _safe_float(row.get("avg_storage_free_pct")),
            "max_storage_used_mb": _safe_float(row.get("max_storage_used")),
            "avg_storage_used_mb": _safe_float(row.get("avg_storage_used")),
            "max_used_percent":    _safe_float(row.get("max_used_percent")),
            "avg_used_percent":    _safe_float(row.get("avg_used_percent")),
            "_hash_parts":         [env, host_mount],
        })
    return rows


def extract_typed_from_file(file_path: Path) -> list[dict]:
    """
    Unified typed extractor — routes to the correct per-type function.
    Returns a list of dicts with a `table_type` key + all native CSV columns.
    Returns [] for unrecognised file types.
    """
    table_type = _detect_typed_table(file_path.name)
    if table_type == "slow_sql":
        return extract_typed_slow_sql(file_path)
    if table_type == "blocker":
        return extract_typed_blocker(file_path)
    if table_type == "deadlock":
        return extract_typed_deadlock(file_path)
    if table_type == "slow_mongo":
        return extract_typed_slow_mongo(file_path)
    if table_type == "datafile_sql":
        return extract_typed_datafile_sql(file_path)
    if table_type == "datafile_mongo":
        return extract_typed_datafile_mongo(file_path)
    return []

