import sqlite3

conn = sqlite3.connect("db/master.db")

# Check query_details length distribution for slow_mongo
lens = conn.execute(
    "SELECT length(query_details), COUNT(*) FROM raw_query "
    "WHERE type='slow_query_mongo' GROUP BY 1 ORDER BY 1 DESC LIMIT 10"
).fetchall()
print("Top 10 query_details lengths for slow_mongo:")
for r in lens:
    print(f"  len={r[0]}  count={r[1]}")

# Compare query_details vs command_json lengths for linked rows
print("\nquery_details vs command_json length comparison (linked rows):")
mismatch = conn.execute("""
    SELECT rq.id, length(rq.query_details) AS rq_len, length(sm.command_json) AS sm_len
    FROM raw_query_slow_mongo sm
    JOIN raw_query rq ON rq.id = sm.raw_query_id
    WHERE length(rq.query_details) != length(sm.command_json)
    LIMIT 5
""").fetchall()
print(f"Rows where lengths differ: {len(mismatch)}")
for r in mismatch:
    print(f"  raw_query id={r[0]} query_details_len={r[1]} command_json_len={r[2]}")

# Confirm: for uncovered unique texts, what's raw_query.query_details look like?
print("\nSample raw_query rows NOT in typed table:")
samples = conn.execute("""
    SELECT rq.id, length(rq.query_details), substr(rq.query_details, 1, 100)
    FROM raw_query rq
    WHERE rq.type = 'slow_query_mongo'
      AND NOT EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.raw_query_id = rq.id)
    ORDER BY rq.id
    LIMIT 5
""").fetchall()
for r in samples:
    print(f"  id={r[0]} len={r[1]} text={r[2]!r}")

# Check upload_log to see upload history by type
print("\nUpload log by type/env:")
logs = conn.execute(
    "SELECT file_type, environment, COUNT(*) FROM upload_log GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20"
).fetchall()
for r in logs:
    print(f"  {r[0]:<25} env={r[1]:<8} count={r[2]}")

conn.close()
