import sqlite3
from pathlib import Path

# Project root is two levels above this file: scripts/perf/ -> scripts/ -> root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
db = PROJECT_ROOT / "db/master.db"
conn = sqlite3.connect(str(db))
print(f"DB size: {db.stat().st_size / 1_048_576:.1f} MB")

for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall():
    cnt = conn.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0]
    print(f"  {t}: {cnt} rows")

print("\n--- Indexes ---")
for row in conn.execute("SELECT tbl_name, name, sql FROM sqlite_master WHERE type='index' ORDER BY tbl_name").fetchall():
    print(f"  {row[0]}.{row[1]}")

conn.close()
