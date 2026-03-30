import hashlib
import sqlite3
import time
from pathlib import Path

# Project root is two levels above this file: scripts/perf/ -> scripts/ -> root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

conn = sqlite3.connect(str(PROJECT_ROOT / "db/master.db"))

cnt = conn.execute("SELECT count(*) FROM raw_query WHERE type='slow_query_mongo'").fetchone()[0]
print(f"raw_query slow_query_mongo rows: {cnt}")

null_cnt = conn.execute(
    "SELECT count(*) FROM raw_query_slow_mongo WHERE raw_query_id IS NULL"
).fetchone()[0]
print(f"raw_query_slow_mongo NULL fk   : {null_cnt}")

t0 = time.perf_counter()
conn.execute("""
  UPDATE raw_query_slow_mongo
  SET raw_query_id = (
    SELECT rq.id FROM raw_query rq
    WHERE rq.type = 'slow_query_mongo' AND rq.source = 'mongodb'
      AND rq.environment IS raw_query_slow_mongo.environment
      AND rq.month_year  IS raw_query_slow_mongo.month_year
      AND rq.host        IS raw_query_slow_mongo.host
      AND rq.db_name     IS raw_query_slow_mongo.db_name
      AND rq.query_details IS raw_query_slow_mongo.command_json
    LIMIT 1
  )
  WHERE raw_query_id IS NULL
""")
conn.rollback()
t1 = time.perf_counter()
print(f"_link_typed_to_raw (mongo) time: {t1 - t0:.2f}s")

# Also time the upsert batch pattern (SELECT + individual UPDATE vs bulk)
t2 = time.perf_counter()
test_hashes = [hashlib.md5(str(i).encode()).hexdigest() for i in range(50)]
ph = ", ".join("?" for _ in test_hashes)
conn.execute(f"SELECT query_hash FROM raw_query WHERE query_hash IN ({ph})", test_hashes).fetchall()
t3 = time.perf_counter()
print(f"SELECT 50 hashes from 120K rows: {(t3 - t2) * 1000:.1f}ms")

conn.close()
