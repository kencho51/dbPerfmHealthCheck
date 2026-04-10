"""
Phase 0 baseline benchmark — CSV ingest pipeline for data/Mar2026/.

Measures per-file timing for every stage of the ingest pipeline and records
DB + WAL sizes before and after, so post-optimization runs can show exact
improvement numbers.

Usage (from project root):

    uv run python scripts/perf/benchmark_ingest.py

What is timed per file
----------------------
  extract      – extract_from_file()  (Polars CSV parse → normalised dicts)
  extract_typed – extract_typed_from_file()  (Polars CSV parse → typed dicts)
  ingest       – ingest_rows()         (async SQLite upsert into raw_query)
  ingest_typed – ingest_typed_rows()   (async SQLite upsert into raw_query_*)
  total        – wall-clock for all four stages

Output
------
  Console table with per-file timings, row counts, and size delta.
  Writes a JSON snapshot to scripts/perf/baseline_ingest.json for later diff.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: ensure project root is on sys.path
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Set SQLITE_PATH env before any api import so it resolves to the real DB
os.environ.setdefault("SQLITE_PATH", str(PROJECT_ROOT / "db" / "master.db"))

# ---------------------------------------------------------------------------
# Imports (after sys.path fix)
# ---------------------------------------------------------------------------
from api.database import apply_pragmas  # noqa: E402
from api.services.extractor import (  # noqa: E402
    _detect_typed_table,
    extract_from_file,
    extract_typed_from_file,
)
from api.services.ingestor import ingest_rows  # noqa: E402
from api.services.typed_ingestor import ingest_typed_rows  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DB_PATH = PROJECT_ROOT / "db" / "master.db"
WAL_PATH = PROJECT_ROOT / "db" / "master.db-wal"
CSV_DIR = PROJECT_ROOT / "data" / "Feb2026"
OUTPUT_JSON = PROJECT_ROOT / "scripts" / "perf" / "baseline_ingest.json"


def _mb(path: Path) -> float:
    """Return file size in MB, or 0.0 if the file does not exist."""
    try:
        return round(path.stat().st_size / (1024 * 1024), 3)
    except FileNotFoundError:
        return 0.0


def _fmt_time(seconds: float) -> str:
    return f"{seconds:.3f}s"


def _print_header() -> None:
    print(
        f"\n{'File':<45} {'Ext':>7} {'ExtT':>7} {'Ingest':>7} {'IngT':>7} "
        f"{'Total':>7} {'Rows':>6} {'TypedRows':>10}"
    )
    print("-" * 110)


def _print_row(
    name: str,
    t_ext: float,
    t_ext_typed: float,
    t_ingest: float,
    t_ingest_typed: float,
    t_total: float,
    row_count: int,
    typed_count: int,
) -> None:
    print(
        f"{name:<45} {_fmt_time(t_ext):>7} {_fmt_time(t_ext_typed):>7} "
        f"{_fmt_time(t_ingest):>7} {_fmt_time(t_ingest_typed):>7} "
        f"{_fmt_time(t_total):>7} {row_count:>6} {typed_count:>10}"
    )


# ---------------------------------------------------------------------------
# Core benchmark
# ---------------------------------------------------------------------------


async def benchmark_file(csv_path: Path) -> dict:
    """Run the full pipeline for one CSV and return timing + count data."""
    table_type = _detect_typed_table(csv_path.name)

    # Stage 1: extract (sync, CPU-bound — call directly, not via to_thread, for
    # accurate isolation of extraction time)
    t0 = time.perf_counter()
    rows = extract_from_file(csv_path)
    t_extract = time.perf_counter() - t0

    # Stage 2: extract typed
    t0 = time.perf_counter()
    typed_rows = extract_typed_from_file(csv_path)
    t_extract_typed = time.perf_counter() - t0

    # Stage 3: ingest raw rows (async)
    t0 = time.perf_counter()
    ingest_result = await ingest_rows(rows)
    t_ingest = time.perf_counter() - t0

    # Stage 4: ingest typed rows (async)
    t0 = time.perf_counter()
    if table_type != "unknown" and typed_rows:
        typed_result = await ingest_typed_rows(typed_rows, table_type)
        typed_inserted = typed_result.inserted
        typed_updated = typed_result.updated
        typed_errors = typed_result.errors
    else:
        typed_inserted = typed_updated = 0
        typed_errors = [f"table_type={table_type!r} — skipped"]
    t_ingest_typed = time.perf_counter() - t0

    t_total = t_extract + t_extract_typed + t_ingest + t_ingest_typed

    return {
        "file": csv_path.name,
        "table_type": table_type,
        "row_count": len(rows),
        "typed_row_count": len(typed_rows),
        "ingest_inserted": ingest_result.inserted,
        "ingest_updated": ingest_result.updated,
        "typed_inserted": typed_inserted,
        "typed_updated": typed_updated,
        "typed_errors": typed_errors,
        "t_extract": round(t_extract, 4),
        "t_extract_typed": round(t_extract_typed, 4),
        "t_ingest": round(t_ingest, 4),
        "t_ingest_typed": round(t_ingest_typed, 4),
        "t_total": round(t_total, 4),
    }


async def main() -> None:
    # -- Initialise DB pragmas (WAL mode, cache_size, etc.) -------------------
    print("Initialising SQLite pragmas …")
    await apply_pragmas()

    # -- Discover CSV files ---------------------------------------------------
    csv_files = sorted(CSV_DIR.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {CSV_DIR}")
        return

    print(f"Found {len(csv_files)} CSV files in {CSV_DIR.relative_to(PROJECT_ROOT)}")

    # -- Record baseline sizes ------------------------------------------------
    db_size_before = _mb(DB_PATH)
    wal_size_before = _mb(WAL_PATH)

    print(f"\nDB size before : {db_size_before} MB")
    print(f"WAL size before: {wal_size_before} MB  ← target: < 4 MB after optimization")
    if wal_size_before > 20:
        print(
            f"  ⚠  WAL is {wal_size_before} MB — reads are degraded; "
            "Phase 1 WAL checkpoint will fix this."
        )

    # -- Run benchmark per file -----------------------------------------------
    results: list[dict] = []
    _print_header()

    grand_total_start = time.perf_counter()
    for csv_path in csv_files:
        result = await benchmark_file(csv_path)
        results.append(result)
        _print_row(
            name=result["file"],
            t_ext=result["t_extract"],
            t_ext_typed=result["t_extract_typed"],
            t_ingest=result["t_ingest"],
            t_ingest_typed=result["t_ingest_typed"],
            t_total=result["t_total"],
            row_count=result["row_count"],
            typed_count=result["typed_row_count"],
        )

    grand_total = time.perf_counter() - grand_total_start

    # -- Record post-ingest sizes ---------------------------------------------
    db_size_after = _mb(DB_PATH)
    wal_size_after = _mb(WAL_PATH)

    # -- Summary --------------------------------------------------------------
    print("-" * 110)
    total_rows = sum(r["row_count"] for r in results)
    total_typed = sum(r["typed_row_count"] for r in results)
    total_inserted = sum(r["ingest_inserted"] for r in results)
    total_updated = sum(r["ingest_updated"] for r in results)

    print(
        f"\n{'TOTAL':<45} "
        f"{'':>7} {'':>7} {'':>7} {'':>7} "
        f"{_fmt_time(grand_total):>7} {total_rows:>6} {total_typed:>10}"
    )

    print(
        f"\n  Raw rows   : {total_rows} extracted   ({total_inserted} inserted, {total_updated} updated)"
    )
    print(f"  Typed rows : {total_typed} extracted")

    print(f"\n  DB size before : {db_size_before} MB")
    print(
        f"  DB size after  : {db_size_after} MB  (delta: +{round(db_size_after - db_size_before, 3)} MB)"
    )
    print(f"  WAL size before: {wal_size_before} MB")
    print(
        f"  WAL size after : {wal_size_after} MB  (delta: +{round(wal_size_after - wal_size_before, 3)} MB)"
    )

    slowest = max(results, key=lambda r: r["t_total"])
    print(f"\n  Slowest file   : {slowest['file']} ({_fmt_time(slowest['t_total'])})")

    # -- Save JSON baseline ---------------------------------------------------
    snapshot = {
        "label": "Phase 0 baseline — Mar2026",
        "csv_dir": str(CSV_DIR.relative_to(PROJECT_ROOT)),
        "db_size_before_mb": db_size_before,
        "wal_size_before_mb": wal_size_before,
        "db_size_after_mb": db_size_after,
        "wal_size_after_mb": wal_size_after,
        "grand_total_s": round(grand_total, 4),
        "total_rows": total_rows,
        "total_typed_rows": total_typed,
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "files": results,
    }

    OUTPUT_JSON.write_text(json.dumps(snapshot, indent=2))
    print(f"\n  Baseline saved → {OUTPUT_JSON.relative_to(PROJECT_ROOT)}")
    print("\nDone. Run again after Phase 1–3 optimizations to compare.\n")


if __name__ == "__main__":
    asyncio.run(main())
