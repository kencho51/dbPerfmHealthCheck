import sqlite3

conn = sqlite3.connect("db/master.db")

for table, type_val, text_col in [
    ("raw_query_slow_mongo", "slow_query_mongo", "command_json"),
    ("raw_query_deadlock", "deadlock", "sql_text"),
    ("raw_query_slow_sql", "slow_query", "query_final"),
]:
    sql = f"""
    SELECT COUNT(*) FROM {table} t
    JOIN raw_query rq ON rq.type = ? AND rq.query_details = t.{text_col}
    WHERE t.raw_query_id IS NULL
    """
    count = conn.execute(sql, (type_val,)).fetchone()[0]
    print(f"{table}: {count} unlinked rows matchable by text-only join")

# Also bulk-backfill with the text-only link
print("\nRunning bulk text-only backfill...")
for table, type_val, text_col in [
    ("raw_query_slow_mongo", "slow_query_mongo", "command_json"),
    ("raw_query_deadlock", "deadlock", "sql_text"),
    ("raw_query_slow_sql", "slow_query", "query_final"),
]:
    sql = f"""
    UPDATE {table}
    SET raw_query_id = (
        SELECT rq.id FROM raw_query rq
        WHERE rq.type = ?
          AND rq.query_details = {table}.{text_col}
        LIMIT 1
    )
    WHERE raw_query_id IS NULL
    """
    conn.execute(sql, (type_val,))

conn.commit()

print("\nAfter backfill:")
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
    print(f"  {tbl}: {total} total, {linked} linked, {total - linked} still unlinked")

conn.close()
