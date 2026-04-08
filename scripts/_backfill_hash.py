"""
Hash-based backfill for slow_mongo: links new-format raw_query rows
(query_details = queryShapeHash or ns:op_type) to raw_query_slow_mongo.

typed_ingestor hashes [host, db_name, env, query_key] to produce query_hash.
Since raw_query.query_details = query_key for new-format rows, we can
reconstruct the hash and find the typed row.
"""

import hashlib
import sqlite3
import time

conn = sqlite3.connect("db/master.db", timeout=60)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")

t0 = time.time()
print("Loading unlinked raw_query slow_mongo rows...")
# All raw_query slow_mongo rows not yet covered by FK
rq_rows = conn.execute(
    "SELECT rq.id, rq.host, rq.db_name, rq.environment, rq.query_details "
    "FROM raw_query rq "
    "WHERE rq.type='slow_query_mongo' AND rq.source='mongodb' "
    "AND NOT EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.raw_query_id = rq.id)"
).fetchall()
print(f"  {len(rq_rows):,} raw_query slow_mongo rows without a direct FK")

print("Loading typed query_hash → id map...")
hash_to_typed_id: dict[str, int] = {
    row[0]: row[1]
    for row in conn.execute("SELECT query_hash, id FROM raw_query_slow_mongo").fetchall()
    if row[0]
}
print(f"  {len(hash_to_typed_id):,} typed rows in hash map")

# Reconstruct hash and find matches
updates: list[tuple[int, int]] = []  # (raw_query_id, typed_id)
for rq_id, host, db_name, env, qd in rq_rows:
    if not qd:
        continue
    candidate = hashlib.md5(
        "|".join(str(p or "").strip() for p in [host, db_name, env, qd]).encode("utf-8")
    ).hexdigest()
    if candidate in hash_to_typed_id:
        updates.append((rq_id, hash_to_typed_id[candidate]))

print(f"\nFound {len(updates):,} new hash-based matches")

if updates:
    conn.executemany(
        "UPDATE raw_query_slow_mongo SET raw_query_id=? WHERE id=? AND raw_query_id IS NULL",
        [(rq_id, typed_id) for rq_id, typed_id in updates],
    )
    conn.commit()
    print(f"Applied {len(updates):,} FK updates to raw_query_slow_mongo")

elapsed = time.time() - t0
print(f"\nDone in {elapsed:.1f}s")

# Summary
total = conn.execute("SELECT COUNT(*) FROM raw_query_slow_mongo").fetchone()[0]
linked = conn.execute(
    "SELECT COUNT(*) FROM raw_query_slow_mongo WHERE raw_query_id IS NOT NULL"
).fetchone()[0]
print(f"raw_query_slow_mongo: {linked}/{total} linked")

# How many raw_query slow_mongo rows now have a typed row?
covered = conn.execute(
    "SELECT COUNT(*) FROM raw_query rq WHERE rq.type='slow_query_mongo' "
    "AND EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.raw_query_id=rq.id)"
).fetchone()[0]
total_rq = conn.execute("SELECT COUNT(*) FROM raw_query WHERE type='slow_query_mongo'").fetchone()[
    0
]
print(f"raw_query rows with typed FK: {covered:,}/{total_rq:,} ({100 * covered / total_rq:.1f}%)")

conn.close()
