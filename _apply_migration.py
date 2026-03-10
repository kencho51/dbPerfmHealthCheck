"""
Apply migration.sql to Neon perfmdb via REST API (HTTPS port 443).
Run with:  uv run python _apply_migration.py
"""
import urllib.request
import urllib.error
import json
import re
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent / "api" / ".env")

API_KEY  = os.environ["NEON_API_KEY"]
PROJECT  = "cold-union-77928175"
ENDPOINT = "ep-rough-morning-a1v4c224"
DB       = "perfmdb"
# DDL requires the database owner role; perfmdb_owner is the app runtime role
ROLE     = "neondb_owner"


def run_sql(sql: str):
    body = json.dumps({
        "query": sql,
        "db_name": DB,
        "endpoint_id": ENDPOINT,
        "role_name": ROLE,
    }).encode()
    req = urllib.request.Request(
        f"https://console.neon.tech/api/v2/projects/{PROJECT}/query",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
            # Neon returns 200 even for SQL errors; check for error field
            if "error" in data or "message" in data:
                return False, data
            return True, data
    except urllib.error.HTTPError as e:
        return False, e.read().decode()


def main():
    # utf-8-sig strips BOM that PowerShell adds to UTF-8 files
    sql = (Path(__file__).parent / "migration.sql").read_text(encoding="utf-8-sig")

    # Split on ';' and clean up each statement
    stmts = []
    for raw in sql.split(";"):
        s = re.sub(r"--[^\n]*", "", raw).strip()
        # Skip transaction control — each REST API call is its own auto-commit connection
        if s and s.upper() not in ("BEGIN", "COMMIT"):
            stmts.append(s)

    print(f"Applying {len(stmts)} statements to {DB} ...")
    all_ok = True
    for i, stmt in enumerate(stmts, 1):
        ok, result = run_sql(stmt)
        label = stmt[:80].replace("\n", " ")
        print(f"  [{i:02d}] {'OK  ' if ok else 'FAIL'} {label}")
        if not ok:
            print(f"         ERROR: {result}")
            all_ok = False

    print()
    if all_ok:
        print("All statements applied. Granting app role permissions ...")
        grants = [
            "GRANT USAGE ON SCHEMA public TO perfmdb_owner",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO perfmdb_owner",
            "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO perfmdb_owner",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO perfmdb_owner",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO perfmdb_owner",
        ]
        for g in grants:
            ok_g, res_g = run_sql(g)
            print(f"  {'OK  ' if ok_g else 'FAIL'} {g}")
            if not ok_g:
                print(f"       {res_g}")
        print()
        print("Verifying ...")
    else:
        print("Some statements FAILED. Running verification anyway ...")

    ok, result = run_sql(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
    )
    if ok:
        rows = result.get("rows", [])
        print(f"Tables in {DB} ({len(rows)}):", [list(r.values())[0] for r in rows])
    else:
        print("Verification query failed:", result)

    ok2, result2 = run_sql("SELECT version_num FROM alembic_version")
    if ok2:
        rows2 = result2.get("rows", [])
        print("Alembic version:", [list(r.values())[0] for r in rows2])


main()
