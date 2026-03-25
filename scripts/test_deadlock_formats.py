"""Quick smoke test for both deadlock CSV formats."""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api.services.extractor import extract_from_file

BASE = Path(__file__).resolve().parents[1] / "data" / "Feb2026"

# Legacy format (Prod — has clean_query column)
prod_rows = extract_from_file(BASE / "deadlocksProdFeb26.csv")
print(f"Prod (legacy) rows: {len(prod_rows)}")
r = prod_rows[0]
print(f"  db={r['db_name']}  sql={r['query_details'][:70]}")
m = json.loads(r["extra_metadata"])
print(f"  extra_metadata keys: {list(m.keys())}")
print(f"  is_victim={m.get('is_victim')}  lockMode={m.get('lockMode')}")

# Raw format (SAT)
sat_rows = extract_from_file(BASE / "deadlocksRawSatFeb26.csv")
print()
print(f"SAT (raw) rows: {len(sat_rows)}")
r = sat_rows[0]
print(f"  db={r['db_name']}  sql={r['query_details'][:70]}")
m = json.loads(r["extra_metadata"])
print(f"  extra_metadata keys: {list(m.keys())}")
print(f"  deadlock_id={m.get('deadlock_id')}  is_victim={m.get('is_victim')}  waitresource={m.get('waitresource')}")
