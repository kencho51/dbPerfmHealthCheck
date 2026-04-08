"""Verify that the hash-based fallback in get_typed_detail works correctly."""

import asyncio
import hashlib
import sqlite3
import sys

sys.path.insert(0, ".")


async def test():

    # Find a new-format raw_query slow_mongo row (one NOT the FK target)
    # that DOES have a matching typed row via hash
    conn = sqlite3.connect("db/master.db")

    # Find a new-format raw_query slow_mongo row — query_details should be
    # queryShapeHash (64 hex) or ns:op_type, NOT command JSON (< 5 chars or no '{')
    sample = conn.execute("""
        SELECT rq.id, rq.host, rq.db_name, rq.environment, rq.query_details
        FROM raw_query rq
        WHERE rq.type = 'slow_query_mongo'
          AND NOT EXISTS (SELECT 1 FROM raw_query_slow_mongo sm WHERE sm.raw_query_id = rq.id)
          AND rq.query_details IS NOT NULL
          AND substr(rq.query_details, 1, 1) != '{'
        LIMIT 5
    """).fetchall()

    print("Checking hash-based fallback for 5 non-FK raw_query rows:")
    for rq_id, host, db_name, env, qd in sample:
        # Reconstruct expected hash
        candidate = hashlib.md5(
            "|".join(str(p or "").strip() for p in [host, db_name, env, qd]).encode("utf-8")
        ).hexdigest()
        # Check if typed row exists with this hash
        typed = conn.execute(
            "SELECT id, collection, op_type, duration_ms FROM raw_query_slow_mongo WHERE query_hash = ?",
            (candidate,),
        ).fetchone()
        print(f"  raw_query id={rq_id} query_details={qd[:50]!r}")
        if typed:
            print(
                f"    FOUND typed row id={typed[0]} col={typed[1]} op={typed[2]} dur={typed[3]}ms"
            )
        else:
            print(f"    MISS  no typed row for hash {candidate[:16]}...")

    conn.close()


asyncio.run(test())
