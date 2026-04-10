"""Phase 1 verification — run from project root: uv run python scripts/perf/verify_phase1.py"""

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
db_path = ROOT / "db" / "master.db"
con = sqlite3.connect(str(db_path))

print("=== EXPLAIN QUERY PLAN: query_details LIKE search ===")
rows = con.execute(
    "EXPLAIN QUERY PLAN SELECT * FROM raw_query WHERE query_details LIKE '%SELECT%'"
).fetchall()
for r in rows:
    print(" ", r)

print("\n=== PRAGMA mmap_size ===")
print(" ", con.execute("PRAGMA mmap_size").fetchone())

print("\n=== New indexes present ===")
indexes = con.execute(
    "SELECT name FROM sqlite_master WHERE type='index' AND ("
    "name LIKE 'ix_raw_query_query%' OR name LIKE 'ix_upload_log_%')"
).fetchall()
for r in indexes:
    print(" ", r[0])

print()
wal_path = ROOT / "db" / "master.db-wal"
wal_mb = wal_path.stat().st_size / (1024 * 1024) if wal_path.exists() else 0.0
print(f"WAL size: {wal_mb:.3f} MB")

con.close()
