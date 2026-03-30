"""
Validator service — dry-run CSV inspection without touching the database.

`validate_csv(path)` checks:
  - Required columns are present.
  - Null rates for critical fields.
  - Detects environment and query type from filename.
  - Samples up to SAMPLE_SIZE rows for a preview.

Returns a `ValidationResult` dataclass consumed by:
  - `api/routers/validate.py`   (POST /api/validate)
  - `scripts/validate_csv.py`   (standalone CLI)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from api.services.extractor import _extract_environment, detect_file_category

SAMPLE_SIZE = 50

# ---------------------------------------------------------------------------
# Required columns per file category
# ---------------------------------------------------------------------------

# Required columns per file category.
# Deadlock accepts two formats:
#   Raw format (new):    _time, host, _raw
#   Legacy format (old): _time, host, currentdbname, all_query/clean_query, _raw
# The common required column in BOTH is _raw + host.
_CRITICAL_FIELDS: dict[str, list[str]] = {
    "slow_query_sql": ["host", "db_name", "query_final"],
    "blocker": ["host", "database_name", "query_text"],
    "deadlock": [],  # flexible check in section 3b — formats vary
    "slow_query_mongo": ["host", "_raw"],
}

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    is_valid: bool
    file_type: str  # e.g. "slow_query_sql"
    environment: str  # "prod" | "sat" | "unknown"
    row_count: int
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    null_rates: dict[str, float] = field(default_factory=dict)
    sample_rows: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "is_valid": self.is_valid,
            "file_type": self.file_type,
            "environment": self.environment,
            "row_count": self.row_count,
            "warnings": self.warnings,
            "errors": self.errors,
            "null_rates": self.null_rates,
            "sample_rows": self.sample_rows,
        }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def validate_csv(path: Path) -> ValidationResult:
    """
    Validate a single CSV file.  Does NOT write to the database.
    """
    filename = path.name
    file_type = detect_file_category(filename)
    environment = _extract_environment(filename)

    errors: list[str] = []
    warnings: list[str] = []

    # -- 1. Unknown file type ----------------------------------------------------
    if file_type == "unknown":
        return ValidationResult(
            is_valid=False,
            file_type=file_type,
            environment=environment,
            row_count=0,
            errors=[
                f"Unrecognised filename '{filename}'. "
                "Expected: maxElapsed*, blockers*, deadlocks*, mongodbSlowQueries*."
            ],
        )

    # -- 2. Read CSV -------------------------------------------------------------
    try:
        df = pl.read_csv(path, encoding="utf-8", infer_schema_length=0)
    except Exception as exc:
        return ValidationResult(
            is_valid=False,
            file_type=file_type,
            environment=environment,
            row_count=0,
            errors=[f"Failed to read CSV: {exc}"],
        )

    row_count = df.height

    if row_count == 0:
        return ValidationResult(
            is_valid=False,
            file_type=file_type,
            environment=environment,
            row_count=0,
            errors=["CSV file is empty (no data rows)."],
        )

    # -- 3. Required columns check -----------------------------------------------
    required = _CRITICAL_FIELDS.get(file_type, [])
    missing = [col for col in required if col not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")

    # -- 3b. Deadlock format detection note (informational) ----------------------
    if file_type == "deadlock" and not errors:
        col_set = set(df.columns)
        _QUERY_TEXT_COLS = {
            "_raw",
            "all_query",
            "clean_query",
            "query_text",
            "statement",
            "sql_text",
        }
        if not (_QUERY_TEXT_COLS & col_set):
            errors.append(
                "Deadlock CSV must contain at least one query-text column: "
                "_raw, all_query, clean_query, or query_text."
            )
        if "host" not in col_set:
            warnings.append(
                "Column 'host' not found — host will be stored as null. "
                "Consider adding host to the Splunk export for better analytics."
            )
        if {"clean_query", "all_query"} & col_set:
            warnings.append(
                "Detected legacy deadlock format (pre-extracted clean_query / all_query). "
                "Rows will be ingested with occurrence_count from the 'count' column."
            )
        elif "_raw" in col_set:
            warnings.append(
                "Detected raw deadlock format (_raw only). "
                "Each row will be expanded into one row per process by the deadlock parser."
            )
        else:
            warnings.append(
                "Detected summarised deadlock format (aggregate columns only, no _raw). "
                "Rows will be ingested as-is using available query-text columns."
            )

    # -- 4. Null rates for required columns --------------------------------------
    null_rates: dict[str, float] = {}
    for col in required:
        if col in df.columns:
            null_rate = df[col].is_null().mean()
            null_rates[col] = round(float(null_rate), 4)
            if null_rate > 0.5:
                warnings.append(
                    f"Column '{col}' has {null_rate:.1%} null values — data quality may be poor."
                )

    # -- 5. Sample rows ----------------------------------------------------------
    try:
        sample_rows: list[dict] = [
            {
                k: (v[:500] + "\u2026[truncated]") if isinstance(v, str) and len(v) > 500 else v
                for k, v in rdict.items()
            }
            for rdict in df.head(SAMPLE_SIZE).to_dicts()
        ]
    except Exception:
        sample_rows = []

    # -- 6. Environment sanity ---------------------------------------------------
    if environment == "unknown":
        warnings.append(
            "Could not detect environment (prod/sat) from filename. "
            "Rows will be stored with environment='unknown'."
        )

    is_valid = len(errors) == 0

    return ValidationResult(
        is_valid=is_valid,
        file_type=file_type,
        environment=environment,
        row_count=row_count,
        warnings=warnings,
        errors=errors,
        null_rates=null_rates,
        sample_rows=sample_rows,
    )


def validate_directory(directory: Path) -> list[ValidationResult]:
    """Validate all CSV files in *directory* (non-recursive)."""
    results = []
    for csv_file in sorted(directory.glob("*.csv")):
        results.append(validate_csv(csv_file))
    return results
