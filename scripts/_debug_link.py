import sqlite3

conn = sqlite3.connect("db/master.db")

print("=== slow_mongo month_year (unlinked) ===")
for r in conn.execute(
    "SELECT month_year, COUNT(1) FROM raw_query_slow_mongo WHERE raw_query_id IS NULL GROUP BY 1 ORDER BY 1"
).fetchall():
    print(r)

print("\n=== raw_query mongo month_year ===")
for r in conn.execute(
    "SELECT month_year, COUNT(1) FROM raw_query WHERE type='slow_query_mongo' GROUP BY 1 ORDER BY 1"
).fetchall():
    print(r)

print("\n=== deadlock month_year (unlinked) ===")
for r in conn.execute(
    "SELECT month_year, host, COUNT(1) FROM raw_query_deadlock WHERE raw_query_id IS NULL GROUP BY 1,2 ORDER BY 1 LIMIT 10"
).fetchall():
    print(r)

print("\n=== raw_query deadlock month_year ===")
for r in conn.execute(
    "SELECT month_year, host, COUNT(1) FROM raw_query WHERE type='deadlock' GROUP BY 1,2 ORDER BY 1 LIMIT 10"
).fetchall():
    print(r)

# Check: for unlinked slow_mongo, what's the month_year in raw_query for same text?
print("\n=== sample unlinked slow_mongo vs raw_query ===")
rows = conn.execute(
    "SELECT id, month_year, host, db_name, substr(command_json,1,60) FROM raw_query_slow_mongo WHERE raw_query_id IS NULL LIMIT 5"
).fetchall()
for row in rows:
    mid, my, host, db, cj = row
    candidates = conn.execute(
        "SELECT month_year, host, db_name FROM raw_query WHERE type='slow_query_mongo' AND query_details=? LIMIT 3",
        (cj[:60],),  # prefix match won't work, just for illustration
    ).fetchall()
    # Try exact
    exact = conn.execute(
        "SELECT month_year, host, db_name FROM raw_query WHERE type='slow_query_mongo' AND query_details=(SELECT command_json FROM raw_query_slow_mongo WHERE id=?) LIMIT 3",
        (mid,),
    ).fetchall()
    print(f"  typed id={mid} month_year={my!r} host={host!r}  ->  raw_query matches: {exact}")

conn.close()
