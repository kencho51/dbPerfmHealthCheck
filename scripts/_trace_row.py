"""Trace all 3 fallback paths for raw_query id 68209."""
import hashlib
import sqlite3

conn = sqlite3.connect("db/master.db")

rq_id = 68209
print(f"=== raw_query row {rq_id} ===")
row = conn.execute(
    "SELECT id, type, source, host, db_name, environment, month_year, length(query_details), substr(query_details,1,120) FROM raw_query WHERE id=?",
    (rq_id,)
).fetchone()
if not row:
    print("NOT FOUND")
    exit()
print(f"  type={row[1]} source={row[2]}")
print(f"  host={row[3]} db={row[4]} env={row[5]} month={row[6]}")
print(f"  query_details len={row[7]}: {row[8]!r}")

qd = conn.execute("SELECT query_details FROM raw_query WHERE id=?", (rq_id,)).fetchone()[0]
host, db_name, env = row[3], row[4], row[5]

print()
print("=== Fallback 1: FK lookup (raw_query_id = rq_id) ===")
fk = conn.execute("SELECT id, query_hash, collection, op_type FROM raw_query_slow_mongo WHERE raw_query_id=?", (rq_id,)).fetchone()
print(f"  Result: {fk}")

print()
print("=== Fallback 2: text match (command_json = query_details) ===")
text_match = conn.execute(
    "SELECT id, query_hash, collection, op_type FROM raw_query_slow_mongo WHERE command_json=?",
    (qd,)
).fetchone()
print(f"  Result: {text_match}")

print()
print("=== Fallback 3: hash reconstruction ===")
candidate = hashlib.md5(
    "|".join(str(p or "").strip() for p in [host, db_name, env, qd]).encode("utf-8")
).hexdigest()
print(f"  Reconstructed hash: {candidate}")
hash_match = conn.execute(
    "SELECT id, query_hash, collection, op_type FROM raw_query_slow_mongo WHERE query_hash=?",
    (candidate,)
).fetchone()
print(f"  Result: {hash_match}")

print()
print("=== Cross-check: does any typed row exist for same text/query_key? ===")
# Try with NULL-safe IS comparison
ns_match = conn.execute(
    "SELECT id, query_hash, host, db_name, environment, command_json IS ? FROM raw_query_slow_mongo WHERE command_json IS ? LIMIT 3",
    (qd, qd)
).fetchall()
print(f"  IS-comparison: {ns_match}")

# Check what format raw_query has (JSON or shape key)
is_json = qd.strip().startswith("{") if qd else False
print(f"  query_details looks like JSON: {is_json}")

if not is_json and qd:
    # New format - what would the hash be for ALL typed rows with same query_key in _hash_parts?
    # The typed_ingestor stores: MD5(host|db_name|env|query_key) where query_key == qd for new format
    # But the raw_query might have host/db_name NULL while typed has them set, or vice versa
    print()
    print("=== Checking hash variants (NULL/empty host/db_name combos) ===")
    for h in [host, None, ""]:
        for d in [db_name, None, ""]:
            variant = hashlib.md5(
                "|".join(str(p or "").strip() for p in [h, d, env, qd]).encode("utf-8")
            ).hexdigest()
            vrow = conn.execute("SELECT id, host, db_name FROM raw_query_slow_mongo WHERE query_hash=?", (variant,)).fetchone()
            if vrow:
                print(f"  host={h!r} db={d!r} -> hash {variant[:12]}... -> typed id={vrow[0]} host={vrow[1]!r} db={vrow[2]!r}")

# Also check what typed row is likely for the same operation
if qd:
    print()
    print("=== Searching typed table by ns prefix (first 40 chars of query_key) ===")
    # For shape keys, search by collection name if determinable
    parts = qd.split(":")
    if len(parts) >= 1:
        ns = parts[0]
        db_part = ns.split(".")[0] if "." in ns else ns
        col_part = ns.split(".")[1] if "." in ns else ""
        print(f"  Searching for collection={col_part!r} db_name={db_part!r}")
        candidates = conn.execute(
            "SELECT id, query_hash, host, db_name, collection, op_type, substr(command_json,1,60) FROM raw_query_slow_mongo WHERE collection=? LIMIT 5",
            (col_part,)
        ).fetchall()
        for c in candidates:
            print(f"    typed id={c[0]} hash={c[1][:12]} host={c[2]!r} db={c[3]!r} col={c[4]!r} op={c[5]!r}")

conn.close()
