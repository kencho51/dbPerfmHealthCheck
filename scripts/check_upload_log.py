import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "master.db"
print(f"DB: {DB_PATH}")
con = sqlite3.connect(str(DB_PATH))

print("=== duplicate filenames (uploaded > once) BEFORE cleanup ===")
dups = con.execute(
    "SELECT filename, COUNT(*) AS cnt, SUM(csv_row_count) AS total_rows "
    "FROM upload_log GROUP BY filename HAVING COUNT(*) > 1 ORDER BY cnt DESC"
).fetchall()
for r in dups:
    print(r)

if dups:
    print("\nDeduplicating upload_log — keeping latest row per filename...")
    # Delete all but the most recent upload entry for each filename.
    con.execute("""
        DELETE FROM upload_log
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM upload_log
            GROUP BY filename
        )
    """)
    con.commit()
    print(f"Deleted {con.total_changes} duplicate rows.")
else:
    print("No duplicates found.")

print("\n=== all months after cleanup ===")
final = con.execute(
    "SELECT month_year, SUM(csv_row_count) AS total_rows, COUNT(*) AS files "
    "FROM upload_log WHERE month_year IS NOT NULL "
    "GROUP BY month_year ORDER BY month_year"
).fetchall()
for r in final:
    print(r)

con.close()

