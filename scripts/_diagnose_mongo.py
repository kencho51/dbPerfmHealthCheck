"""
Diagnose why slow_mongo typed-detail lookup still fails.
"""
import sqlite3

conn = sqlite3.connect("db/master.db")

# 1. How many raw_query slow_mongo rows exist vs typed rows?
rq_total = conn.execute("SELECT COUNT(*) FROM raw_query WHERE type='slow_query_mongo'").fetchone()[0]
typed_total = conn.execute("SELECT COUNT(*) FROM raw_query_slow_mongo").fetchone()[0]
typed_linked = conn.execute("SELECT COUNT(*) FROM raw_query_slow_mongo WHERE raw_query_id IS NOT NULL").fetchone()[0]
print(f"raw_query (slow_query_mongo):  {rq_total:,}")
print(f"raw_query_slow_mongo total:    {typed_total:,}")
print(f"raw_query_slow_mongo linked:   {typed_linked:,}")

# 2. For raw_query slow_mongo rows, how many have a typed row via FK?
rq_with_fk = conn.execute("""
    SELECT COUNT(*) FROM raw_query rq
    WHERE rq.type = 'slow_query_mongo'
      AND EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.raw_query_id = rq.id)
""").fetchone()[0]
print(f"\nraw_query rows that pointed to by FK: {rq_with_fk:,} / {rq_total:,}")

# 3. For raw_query slow_mongo rows with NO FK match, how many have a text match?
print("\nChecking text-match coverage (this may take a moment)...")
rq_with_text = conn.execute("""
    SELECT COUNT(*) FROM raw_query rq
    WHERE rq.type = 'slow_query_mongo'
      AND NOT EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.raw_query_id = rq.id)
      AND EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.command_json = rq.query_details)
""").fetchone()[0]
rq_no_match = conn.execute("""
    SELECT COUNT(*) FROM raw_query rq
    WHERE rq.type = 'slow_query_mongo'
      AND NOT EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.raw_query_id = rq.id)
      AND NOT EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.command_json = rq.query_details)
""").fetchone()[0]
print(f"raw_query rows with no FK but text match: {rq_with_text:,}")
print(f"raw_query rows with NO match at all:      {rq_no_match:,}")

# 4. Sample a couple of raw_query slow_mongo rows with no match
print("\n=== Sample raw_query rows with no typed match ===")
samples = conn.execute("""
    SELECT rq.id, rq.month_year, rq.host, rq.db_name, length(rq.query_details), substr(rq.query_details, 1, 80)
    FROM raw_query rq
    WHERE rq.type = 'slow_query_mongo'
      AND NOT EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.raw_query_id = rq.id)
      AND NOT EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.command_json = rq.query_details)
    LIMIT 5
""").fetchall()
for s in samples:
    print(f"  id={s[0]} month={s[1]} host={s[2]} db={s[3]} len={s[4]} text={s[5]!r}")

# 5. Check lengths: is query_details truncated vs command_json?
print("\n=== Length comparison for linked rows ===")
lengths = conn.execute("""
    SELECT 
        AVG(length(rq.query_details)),
        MIN(length(rq.query_details)),
        MAX(length(rq.query_details)),
        AVG(length(sm.command_json)),
        MIN(length(sm.command_json)),
        MAX(length(sm.command_json))
    FROM raw_query_slow_mongo sm
    JOIN raw_query rq ON rq.id = sm.raw_query_id
    LIMIT 1000
""").fetchone()
print(f"  raw_query.query_details:  avg={lengths[0]:.0f} min={lengths[1]} max={lengths[2]}")
print(f"  slow_mongo.command_json:  avg={lengths[3]:.0f} min={lengths[4]} max={lengths[5]}")

# 6. Are there rows where lengths differ?
mismatched_len = conn.execute("""
    SELECT COUNT(*) FROM raw_query_slow_mongo sm
    JOIN raw_query rq ON rq.id = sm.raw_query_id
    WHERE length(rq.query_details) != length(sm.command_json)
""").fetchone()[0]
print(f"\n  Rows where query_details length != command_json length: {mismatched_len}")

conn.close()
