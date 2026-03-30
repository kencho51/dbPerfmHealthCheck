import sqlite3
from pathlib import Path

db = Path(__file__).resolve().parent.parent / "db" / "master.db"
con = sqlite3.connect(str(db))

print("=== raw_query counts by type+month ===")
rows = con.execute(
    "SELECT month_year, type, COUNT(*) as cnt "
    "FROM raw_query "
    "WHERE month_year IN ('2025-11','2025-12','2026-01','2026-02') "
    "GROUP BY month_year, type "
    "ORDER BY month_year, type"
).fetchall()
for r in rows:
    print(r)

print()
print("=== raw_query_deadlock row count by month ===")
rows = con.execute(
    "SELECT month_year, COUNT(*) as cnt "
    "FROM raw_query_deadlock "
    "GROUP BY month_year ORDER BY month_year"
).fetchall()
for r in rows:
    print(r)

print()
print("=== deadlock max occurrence_count for 2025-11 (detect re-upload multiplier) ===")
rows = con.execute(
    "SELECT occurrence_count, COUNT(*) as rows "
    "FROM raw_query "
    "WHERE type='deadlock' AND month_year='2025-11' "
    "GROUP BY occurrence_count ORDER BY occurrence_count"
).fetchall()
for r in rows:
    print(r)

print()
print("=== blocker: raw_query vs raw_query_blocker for 2025-11 ===")
rq = con.execute(
    "SELECT COUNT(*) FROM raw_query WHERE type='blocker' AND month_year='2025-11'"
).fetchone()[0]
rqb = con.execute("SELECT COUNT(*) FROM raw_query_blocker WHERE month_year='2025-11'").fetchone()[0]
print(f"  raw_query     blocker 2025-11: {rq}")
print(f"  raw_query_blocker 2025-11:     {rqb}")

print()
print("=== deadlock: raw_query vs raw_query_deadlock for 2025-11 ===")
rq = con.execute(
    "SELECT COUNT(*) FROM raw_query WHERE type='deadlock' AND month_year='2025-11'"
).fetchone()[0]
rqd = con.execute("SELECT COUNT(*) FROM raw_query_deadlock WHERE month_year='2025-11'").fetchone()[
    0
]
print(f"  raw_query     deadlock 2025-11: {rq}")
print(f"  raw_query_deadlock 2025-11:     {rqd}")

con.close()
