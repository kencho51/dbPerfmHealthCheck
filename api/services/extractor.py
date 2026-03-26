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

# Shared month-year derivation used by typed extractors to resolve timezone-aware
# timestamps (e.g. '2026-02-28T23:55:18.000+0800') into 'YYYY-MM'.
from api.services.ingestor import _derive_month_year as _month_from_time  # noqa: E402


# ---------------------------------------------------------------------------
# Expected columns per file type (used by the validator service too)
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "slow_query_sql":   ["host", "db_name", "query_final"],
    "blocker":          ["host", "database_name", "query_text"],
    # Deadlock accepts three CSV formats:
    #   Raw (new):         _time, host, id, lockMode, transactionname, victim, waittime, _raw
    #   Legacy (old):      _time, host, hostname, currentdbname, id, victim, transactionname,
    #                      lockMode, lockTimeout, waittime, es_text, clean_query, _raw
    #   Summarised (Nov25):currentdbname, victims, resources, lock_modes, count,
    #                      latest, earliest, all_query  (no host, no _raw)
    "deadlock":         [],   # flexible — at least one of: _raw, all_query, clean_query
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
    df  = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    # Choose the best available time column; both are trimmed and nulls guarded.
    time_expr = (
        pl.when(_col_or(df, "last_execution_time").str.len_chars().gt(0))
          .then(_col_or(df, "last_execution_time"))
          .otherwise(_col_or(df, "creation_time"))
    )
    return df.select([
        time_expr.alias("time"),
        pl.lit("sql").alias("source"),
        _col_or(df, "host").alias("host"),
        _col_or(df, "db_name").alias("db_name"),
        pl.lit(env).alias("environment"),
        pl.lit("slow_query").alias("type"),
        _clean_expr(_col_or(df, "query_final")).alias("query_details"),
    ]).to_dicts()


def _process_blockers(file_path: Path) -> list[dict]:
    df  = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    # Support both per-session format (database_name, query_text) and
    # legacy aggregated format (each guarded with _col_or).
    time_expr = _col_or(df, "_time")
    db_expr   = (
        _col_or(df, "database_name")
        if "database_name" in df.columns
        else _col_or(df, "currentdbname")
    )
    query_expr = (
        _col_or(df, "query_text")
        if "query_text" in df.columns
        else _col_or(df, "all_query")
    )
    return df.select([
        time_expr.alias("time"),
        pl.lit("sql").alias("source"),
        _col_or(df, "host").alias("host"),
        db_expr.alias("db_name"),
        pl.lit(env).alias("environment"),
        pl.lit("blocker").alias("type"),
        _clean_expr(query_expr).alias("query_details"),
    ]).to_dicts()


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
    df  = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)

    # --- query key (pure Polars, zero Python per row) -------------------------
    # Prefer CSV column attr.queryShapeHash (set by MongoDB 7.0+) as the dedup
    # key.  Fall back to "ns:c" or "ns:type" — all Rust string ops.
    shape_col = _col_or(df, "attr.queryShapeHash")
    c_col     = _col_or(df, "c")
    ns_col    = _col_or(df, "attr.ns")
    type_col  = _col_or(df, "attr.type")

    query_key_expr = (
        pl.when(shape_col.str.len_chars().gt(0))
          .then(shape_col)
          .otherwise(
              pl.concat_str([
                  ns_col,
                  pl.lit(":"),
                  pl.when(c_col.str.len_chars().gt(0)).then(c_col).otherwise(type_col),
              ])
          )
    )

    # _query_key is ALWAYS non-empty: it is either queryShapeHash (len 64) or
    # the concat_str result which contains at least ":" (len 1).  Therefore
    # _cmd_json is never selected as query_details and we skip the entire
    # map_elements(_extract_mongodb_command) call — saving 1 768 json.loads
    # invocations and the associated GIL churn.
    df = df.with_columns([
        query_key_expr.alias("_query_key"),
        ns_col.str.split(".").list.get(0).fill_null("").alias("_db"),
    ])

    return df.select([
        _col_or(df, "t.$date").alias("time"),
        pl.lit("mongodb").alias("source"),
        _col_or(df, "host").alias("host"),
        pl.col("_db").alias("db_name"),
        pl.lit(env).alias("environment"),
        pl.lit("slow_query_mongo").alias("type"),
        pl.col("_query_key").alias("query_details"),
    ]).to_dicts()


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


# ---------------------------------------------------------------------------
# Polars expression helpers
# ---------------------------------------------------------------------------

def _clean_expr(expr: "pl.Expr") -> "pl.Expr":
    """
    Vectorised equivalent of _clean(): trim whitespace, collapse runs,
    normalise @P\\d+ → @P?  — runs entirely in Polars' Rust/Rayon thread pool.
    """
    return (
        expr
        .fill_null("")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.replace_all(r"@P\d+", "@P?")
    )


def _col_or(df: "pl.DataFrame", name: str, default: str = "") -> "pl.Expr":
    """Return pl.col(name).fill_null(default) when the column exists, else pl.lit(default).

    Guards against CSV exports that omit optional columns (e.g. attr.queryShapeHash
    absent in pre-MongoDB-7.0 exports).
    """
    return pl.col(name).fill_null(default) if name in df.columns else pl.lit(default)


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
    df  = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)

    # Best available execution time — used for month_year derivation.
    exec_time_expr = (
        pl.when(_col_or(df, "last_execution_time").str.len_chars().gt(0))
          .then(_col_or(df, "last_execution_time"))
          .otherwise(_col_or(df, "creation_time"))
    )
    qf_expr = _clean_expr(_col_or(df, "query_final"))

    # All casts run in Polars' Rust thread pool — zero Python GIL.
    rows = df.select([
        pl.lit("slow_sql").alias("table_type"),
        _col_or(df, "host").alias("host"),
        _col_or(df, "db_name").alias("db_name"),
        pl.lit(env).alias("environment"),
        _col_or(df, "creation_time").alias("creation_time"),
        _col_or(df, "last_execution_time").alias("last_execution_time"),
        _col_or(df, "max_elapsed_time_s").cast(pl.Float64, strict=False).alias("max_elapsed_time_s"),
        _col_or(df, "avg_elapsed_time_s").cast(pl.Float64, strict=False).alias("avg_elapsed_time_s"),
        _col_or(df, "total_elapsed_time_s").cast(pl.Float64, strict=False).alias("total_elapsed_time_s"),
        _col_or(df, "total_worker_time_s").cast(pl.Float64, strict=False).alias("total_worker_time_s"),
        _col_or(df, "avg_io").cast(pl.Float64, strict=False).alias("avg_io"),
        _col_or(df, "avg_logical_reads").cast(pl.Float64, strict=False).alias("avg_logical_reads"),
        _col_or(df, "avg_logical_writes").cast(pl.Float64, strict=False).alias("avg_logical_writes"),
        _col_or(df, "execution_count").cast(pl.Int64, strict=False).alias("execution_count"),
        qf_expr.alias("query_final"),
        exec_time_expr.alias("_exec_time"),
    ]).to_dicts()

    # _hash_parts is a list — cannot be a Polars column; add in one O(N) Python pass.
    for row in rows:
        exec_time = row.pop("_exec_time")
        row["month_year"]  = _month_from_time(exec_time)
        row["_hash_parts"] = [row["host"], row["db_name"], env, row["query_final"]]
    return rows


def extract_typed_blocker(file_path: Path) -> list[dict]:
    """
    blockers*.csv — returns all native blocker columns.

    Supports two Splunk export formats:

    **Per-session format** (current Splunk SPL export)::

        "_time", host, "database_name", "session_id", "wait_type",
        command, "head_blocker", "query_text",
        "blocked_sessions_count", "total_blocked_wait_time_ms"

    **Aggregated format** (legacy SPL export)::

        currentdbname, victims, resources, "lock_modes",
        count, latest, earliest, "all_query"

    Both are mapped to the same ``RawQueryBlocker`` columns to avoid a schema
    migration.  ``victims`` carries the session_id, ``resources`` the wait_type,
    and ``lock_modes`` the SQL command for per-session rows.
    """
    df  = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)
    col_set = {c.strip().lower() for c in df.columns}

    is_per_session = "query_text" in col_set and "session_id" in col_set

    if is_per_session:
        qf_expr = _clean_expr(_col_or(df, "query_text"))
        df = df.with_columns([
            qf_expr.alias("_query_text"),
            _col_or(df, "_time").alias("_event_time"),
            _col_or(df, "database_name").alias("_db_name"),
            _col_or(df, "session_id").alias("_session_id"),
        ])
        rows = df.select([
            pl.lit("blocker").alias("table_type"),
            pl.lit(env).alias("environment"),
            pl.col("_db_name").alias("currentdbname"),
            pl.col("_session_id").alias("victims"),
            _col_or(df, "wait_type").alias("resources"),
            _col_or(df, "command").alias("lock_modes"),
            _col_or(df, "blocked_sessions_count").cast(pl.Int64, strict=False).alias("count"),
            pl.col("_event_time").alias("latest"),
            pl.col("_event_time").alias("earliest"),
            pl.col("_query_text").alias("all_query"),
            # carry through for post-processing
            pl.col("_event_time"),
            pl.col("_query_text"),
            pl.col("_session_id"),
        ]).to_dicts()
        for row in rows:
            et  = row.pop("_event_time")
            qt  = row.pop("_query_text")
            sid = row.pop("_session_id")
            row["month_year"]  = _month_from_time(et)
            row["_hash_parts"] = [env, row["currentdbname"], qt, et, sid]
        return rows

    # Aggregated/legacy format — unchanged behaviour
    qf_expr = _clean_expr(_col_or(df, "all_query"))
    df = df.with_columns([
        qf_expr.alias("_all_query"),
        _col_or(df, "lock_modes").alias("_lock_modes"),
        _col_or(df, "currentdbname").alias("_currentdbname"),
        _col_or(df, "earliest").alias("_earliest"),
    ])
    rows = df.select([
        pl.lit("blocker").alias("table_type"),
        pl.lit(env).alias("environment"),
        pl.col("_currentdbname").alias("currentdbname"),
        _col_or(df, "victims").alias("victims"),
        _col_or(df, "resources").alias("resources"),
        pl.col("_lock_modes").alias("lock_modes"),
        _col_or(df, "count").cast(pl.Int64, strict=False).alias("count"),
        _col_or(df, "latest").alias("latest"),
        pl.col("_earliest").alias("earliest"),
        pl.col("_all_query").alias("all_query"),
        # carry through for post-processing
        pl.col("_all_query"),
        pl.col("_lock_modes"),
        pl.col("_currentdbname"),
        pl.col("_earliest"),
    ]).to_dicts()
    for row in rows:
        aq     = row.pop("_all_query")
        lm     = row.pop("_lock_modes")
        cdn    = row.pop("_currentdbname")
        earliest = row.pop("_earliest")
        row["month_year"]  = _month_from_time(earliest) or _month_from_time(row.get("latest", ""))
        row["_hash_parts"] = [env, cdn, lm, aq]
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
                    "month_year":       _month_from_time(splunk_time),
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
                    "month_year":       _month_from_time(proc.splunk_time),
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
                "month_year":       _month_from_time(splunk_time),
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
                    splunk_time,   # included so _derive_month_year_from_parts can set month_year
                ],
            })

    return rows


def _mongodb_query_key(row: dict, ns: str, op_type: str) -> str:
    """
    Return a stable query-shape key for MongoDB deduplication.

    MongoDB 7.0+ exports `attr.queryShapeHash` — a hash of the normalised
    query structure with literal values removed.  When present, use it.
    For write operations (remove/update/insert) queryShapeHash is absent;
    fall back to `ns:optype` which still groups all removes on the same
    collection together.
    """
    shape = _get(row, "attr.queryShapeHash")
    if shape:
        return shape
    # Writes: group by namespace + operation category column (c = WRITE/COMMAND)
    c_col = _get(row, "c") or op_type
    return f"{ns}:{c_col}"


def extract_typed_slow_mongo(file_path: Path) -> list[dict]:
    """
    mongodbSlowQueries*.csv — returns all native MongoDB slow op columns.

    Deduplication key: ``attr.queryShapeHash`` when present (MongoDB 7.0+),
    otherwise ``attr.ns:c`` (namespace + operation category).  This groups
    all executions of the same query shape into one row, matching the
    intent of the slow-query analysis rather than recording every execution.

    Performance: all field extraction except command_json runs in Polars'
    Rust/Rayon thread pool.  ``command_json`` still calls
    ``_extract_mongodb_command`` via ``map_elements`` (Python GIL-bound)
    but all other column reads (queryShapeHash, durationMillis, planSummary,
    remote, type, ns, host) are zero-copy Rust vector ops.
    """
    df  = pl.read_csv(file_path, encoding="utf-8", infer_schema_length=0)
    env = _extract_environment(file_path.name)

    # --- query key: queryShapeHash → ns:c → ns:type  (pure Rust) -------------
    shape_col = _col_or(df, "attr.queryShapeHash")
    c_col     = _col_or(df, "c")
    ns_col    = _col_or(df, "attr.ns")
    type_col  = _col_or(df, "attr.type")

    query_key_expr = (
        pl.when(shape_col.str.len_chars().gt(0))
          .then(shape_col)
          .otherwise(
              pl.concat_str([
                  ns_col,
                  pl.lit(":"),
                  pl.when(c_col.str.len_chars().gt(0)).then(c_col).otherwise(type_col),
              ])
          )
    )

    # --- command_json: requires Python json.loads; use map_elements -----------
    # All other fields are extracted with zero-GIL Polars ops above.
    df = df.with_columns([
        query_key_expr.alias("_query_key"),
        _col_or(df, "_raw").map_elements(
            _extract_mongodb_command, return_dtype=pl.Utf8
        ).alias("_command_json"),
        ns_col.str.split(".").list.get(0).fill_null("").alias("_db"),
        pl.when(ns_col.str.contains(".", literal=True))
          .then(ns_col.str.splitn(".", 2).struct.field("field_1"))
          .otherwise(pl.lit(""))
          .fill_null("").alias("_collection"),
    ])

    rows = df.select([
        pl.lit("slow_mongo").alias("table_type"),
        _col_or(df, "host").alias("host"),
        pl.col("_db").alias("db_name"),
        pl.col("_collection").alias("collection"),
        pl.lit(env).alias("environment"),
        _col_or(df, "t.$date").alias("event_time"),
        _col_or(df, "attr.durationMillis").cast(pl.Int64, strict=False).alias("duration_ms"),
        _col_or(df, "attr.planSummary").alias("plan_summary"),
        type_col.alias("op_type"),
        _col_or(df, "attr.remote").alias("remote_client"),
        pl.col("_command_json").alias("command_json"),
        pl.col("_query_key"),
    ]).to_dicts()

    # _hash_parts and month_year are list / derived — add in one O(N) Python pass.
    for row in rows:
        qk = row.pop("_query_key")
        row["month_year"]  = _month_from_time(row["event_time"])
        row["_hash_parts"] = [row["host"], row["db_name"], env, qk]
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

