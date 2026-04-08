"""
Fast bulk backfill: links typed table rows to raw_query rows via text matching.
Uses Python-side lookup instead of a correlated SQL subquery to avoid O(N*M)
table scans without indexes.

Only handles tables with significant unlinked rows:
  raw_query_deadlock  (~661 unlinked, host='' vs NULL mismatch)
  raw_query_slow_mongo (~11k unlinked, month_year mismatch or NULL)
  raw_query_slow_sql  (~271 unlinked)
"""

import sqlite3
import time

conn = sqlite3.connect("db/master.db", timeout=60)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")

TABLES = [
    ("raw_query_slow_sql", "slow_query", "query_final"),
    ("raw_query_deadlock", "deadlock", "sql_text"),
    ("raw_query_slow_mongo", "slow_query_mongo", "command_json"),
]

for table, rq_type, text_col in TABLES:
    t0 = time.time()

    # Load all raw_query rows for this type into a dict keyed by query_details text
    # We want the canonical (lowest id) raw_query per unique text
    print(f"\n[{table}] building text lookup from raw_query...", flush=True)
    text_to_id: dict[str, int] = {}
    for rid, text in conn.execute(
        "SELECT id, query_details FROM raw_query WHERE type=? ORDER BY id", (rq_type,)
    ):
        if text and text not in text_to_id:
            text_to_id[text] = rid

    print(f"  {len(text_to_id)} unique query texts found", flush=True)

    # Load unlinked typed rows
    unlinked = conn.execute(
        f"SELECT id, {text_col} FROM {table} WHERE raw_query_id IS NULL"
    ).fetchall()
    print(f"  {len(unlinked)} unlinked typed rows", flush=True)

    # Match in Python
    updates: list[tuple[int, int]] = []
    for tid, text in unlinked:
        if text and text in text_to_id:
            updates.append((text_to_id[text], tid))

    print(f"  {len(updates)} matches found, applying UPDATE...", flush=True)

    if updates:
        conn.executemany(
            f"UPDATE {table} SET raw_query_id=? WHERE id=? AND raw_query_id IS NULL",
            updates,
        )
        conn.commit()

    elapsed = time.time() - t0
    print(f"  done in {elapsed:.1f}s", flush=True)

# Final summary
print("\n=== Final counts ===")
for tbl in (
    "raw_query_slow_sql",
    "raw_query_blocker",
    "raw_query_deadlock",
    "raw_query_slow_mongo",
):
    total = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    linked = conn.execute(f"SELECT COUNT(*) FROM {tbl} WHERE raw_query_id IS NOT NULL").fetchone()[
        0
    ]
    print(f"  {tbl}: {linked}/{total} linked ({total - linked} still unlinked)")

conn.close()
