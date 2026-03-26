"""
End-to-end performance benchmark for the MongoDB slow query CSV pipeline.
Measures: extraction, normalization, upsert, and link-typed-to-raw timing.
"""
import sqlite3
import time
from pathlib import Path

# Project root is two levels above this file: scripts/perf/ -> scripts/ -> root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def measure_extractions():
    from api.services.extractor import extract_from_file, extract_typed_from_file
    from api.services.ingestor import _normalize_sync

    f = PROJECT_ROOT / "data/Feb2026/mongodbSlowQueriesProdFeb26.csv"

    t0 = time.perf_counter()
    rows = extract_from_file(f)
    t1 = time.perf_counter()
    print(f"  extract_from_file       : {t1-t0:.3f}s  ({len(rows)} rows)")

    t2 = time.perf_counter()
    typed_rows = extract_typed_from_file(f)
    t3 = time.perf_counter()
    print(f"  extract_typed_from_file : {t3-t2:.3f}s  ({len(typed_rows)} typed rows)")

    t4 = time.perf_counter()
    normalized = _normalize_sync(rows)
    t5 = time.perf_counter()
    print(f"  _normalize_sync (DuckDB): {t5-t4:.3f}s  ({len(normalized)} unique rows)")

    return rows, typed_rows, normalized


def measure_link_query():
    conn = sqlite3.connect(str(PROJECT_ROOT / "db/master.db"))
    # First ensure the composite index exists
    conn.execute("""
        CREATE INDEX IF NOT EXISTS ix_raw_query_link_key
        ON raw_query (type, host, db_name, environment, month_year)
    """)
    conn.commit()

    null_cnt = conn.execute(
        "SELECT count(*) FROM raw_query_slow_mongo WHERE raw_query_id IS NULL"
    ).fetchone()[0]
    print(f"\n  raw_query_slow_mongo rows with NULL fk: {null_cnt}")

    t0 = time.perf_counter()
    conn.execute("""
        UPDATE raw_query_slow_mongo
        SET raw_query_id = (
            SELECT rq.id FROM raw_query rq
            WHERE rq.type        = 'slow_query_mongo'
              AND rq.source      = 'mongodb'
              AND rq.environment IS raw_query_slow_mongo.environment
              AND rq.month_year  IS raw_query_slow_mongo.month_year
              AND rq.host        IS raw_query_slow_mongo.host
              AND rq.db_name     IS raw_query_slow_mongo.db_name
              AND rq.query_details IS raw_query_slow_mongo.command_json
            LIMIT 1
        )
        WHERE raw_query_id IS NULL
    """)
    conn.rollback()
    t1 = time.perf_counter()
    print(f"  _link_typed_to_raw (WITH composite idx): {t1-t0:.3f}s")
    conn.close()


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))

    print("=== Extraction + normalization ===")
    rows, typed_rows, normalized = measure_extractions()

    print("\n=== Link query (composite index applied) ===")
    measure_link_query()

    print("\nDone.")
