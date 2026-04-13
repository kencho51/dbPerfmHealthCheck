"""
Diagnose and optionally delete all raw_query rows whose hashes match
mongodbSlowQueriesProbFeb26.csv.

Run in dry-run mode first (default), then pass --delete to remove.
"""
import argparse
import hashlib
import sqlite3
from pathlib import Path

DB_PATH = Path(r'C:\Users\kenlcho\Desktop\dbPerfmHealthCheck\db\master.db')

parser = argparse.ArgumentParser()
parser.add_argument('--delete', action='store_true', help='Actually delete rows (default is dry-run)')
args = parser.parse_args()

con = sqlite3.connect(str(DB_PATH))

# Confirm ALL unknown-env rows are slow_query_mongo (safe guard before deleting)
print('=== environment=unknown breakdown by type ===')
for r in con.execute(
    "SELECT type, COUNT(*) FROM raw_query WHERE environment='unknown' GROUP BY type"
).fetchall():
    print(f'  {r[0]}: {r[1]}')

total_unknown = con.execute("SELECT COUNT(*) FROM raw_query WHERE environment='unknown'").fetchone()[0]
typed_matches = con.execute(
    "SELECT COUNT(*) FROM raw_query_slow_mongo WHERE query_hash IN "
    "(SELECT query_hash FROM raw_query WHERE environment='unknown')"
).fetchone()[0]

print()
print(f'Total unknown-env rows in raw_query:         {total_unknown}')
print(f'Matching rows in raw_query_slow_mongo:       {typed_matches}')

if not args.delete:
    print()
    print('DRY RUN — no changes made.')
    print('Re-run with --delete to permanently remove these rows.')
else:
    print()
    print('=== DELETING ===')
    with con:
        c1 = con.execute(
            "DELETE FROM raw_query_slow_mongo WHERE query_hash IN "
            "(SELECT query_hash FROM raw_query WHERE environment='unknown')"
        ).rowcount
        c2 = con.execute("DELETE FROM raw_query WHERE environment='unknown'").rowcount
    print(f'  raw_query_slow_mongo: {c1} rows deleted')
    print(f'  raw_query:            {c2} rows deleted')
    remaining = con.execute("SELECT COUNT(*) FROM raw_query WHERE environment='unknown'").fetchone()[0]
    print(f'  Remaining unknown-env rows: {remaining}')
    print()
    print('Done. You may also want to delete the file:')
    print('  data/Feb2026/mongodbSlowQueriesProbFeb26.csv')

con.close()
