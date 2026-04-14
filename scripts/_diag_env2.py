import sqlite3

con = sqlite3.connect(r"C:\Users\kenlcho\Desktop\dbPerfmHealthCheck\db\master.db")

print("=== upload_log schema ===")
for r in con.execute("PRAGMA table_info(upload_log)").fetchall():
    print(f"  {r[1]} {r[2]}")

print()
print("=== upload_log rows for Prob file ===")
for r in con.execute(
    "SELECT id, filename, file_type, csv_row_count, inserted, updated FROM upload_log WHERE filename LIKE '%Prob%'"
).fetchall():
    print(r)

print()
print("=== typed tables that reference raw_query ===")
for tbl in [
    "raw_query_slow_sql",
    "raw_query_blocker",
    "raw_query_deadlock",
    "raw_query_slow_mongo",
]:
    try:
        cols = [r[1] for r in con.execute(f"PRAGMA table_info({tbl})").fetchall()]
        print(f"  {tbl}: {cols}")
    except Exception as e:
        print(f"  {tbl}: ERROR {e}")

con.close()
