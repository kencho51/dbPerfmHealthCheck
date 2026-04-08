import sqlite3

conn = sqlite3.connect("db/master.db")

print("Loading typed command_json texts...")
typed_texts = set(
    r[0] for r in conn.execute("SELECT command_json FROM raw_query_slow_mongo") if r[0]
)
print(f"Unique typed texts: {len(typed_texts):,}")

print("Checking raw_query coverage...")
rq_texts = set(
    r[0]
    for r in conn.execute("SELECT query_details FROM raw_query WHERE type='slow_query_mongo'")
    if r[0]
)
print(f"Unique raw_query texts: {len(rq_texts):,}")

covered = rq_texts & typed_texts
uncovered = rq_texts - typed_texts
print(f"Covered by typed text match:  {len(covered):,}")
print(f"NOT covered (no typed row):   {len(uncovered):,}")
print(f"Coverage: {100 * len(covered) / max(len(rq_texts), 1):.1f}%")

# Sample 3 uncovered texts
print("\nSample uncovered texts (won't find typed rows):")
for t in list(uncovered)[:3]:
    print(f"  {t[:100]!r}")

conn.close()
