"""Apply alter_enum_to_varchar.sql against Neon via HTTP SQL endpoint."""
import json
import pathlib
import re
import urllib.error
import urllib.request

CONN_STR = (
    "postgresql://neondb_owner:npg_dnSNjM2XOw1e"
    "@ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech"
    "/perfmdb?sslmode=require"
)
HOST = "ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech"
URL  = f"https://{HOST}/sql"

sql_file = pathlib.Path(__file__).parent / "alter_enum_to_varchar.sql"
raw = sql_file.read_text(encoding="utf-8")

# Strip SQL comments, split into individual statements
lines = [re.sub(r"--.*", "", ln).strip() for ln in raw.splitlines()]
combined = " ".join(l for l in lines if l)
# Skip BEGIN/COMMIT — Neon HTTP SQL auto-wraps each request; explicit txn
# control statements are either no-ops or cause "already in transaction" errors.
_skip = {"begin", "commit", "rollback"}
statements = [
    s.strip() for s in combined.split(";")
    if s.strip() and s.strip().lower() not in _skip
]

print(f"Applying {len(statements)} statements to Neon …\n")
ok = err = 0
for stmt in statements:
    body = json.dumps({"query": stmt}).encode()
    req  = urllib.request.Request(
        URL, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Neon-Connection-String": CONN_STR},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            print(f"  OK  | {stmt[:90]}")
            ok += 1
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace") if e.fp else ""
        msg = json.loads(body_text).get("message", body_text) if body_text.startswith("{") else body_text
        print(f"  ERR | {stmt[:90]}\n       → {msg[:200]}")
        err += 1

print(f"\nDone — {ok} ok, {err} errors")
