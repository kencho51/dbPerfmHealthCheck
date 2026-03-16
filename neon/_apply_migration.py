"""
Apply neon/migration.sql to Neon via the direct HTTP SQL endpoint (HTTPS port 443).

Uses neondb_owner (DDL privileges). Reads NEON_DDL_CONN_STR from api/.env.

Setup
-----
Add to api/.env:
    NEON_DDL_CONN_STR=postgresql://neondb_owner:<pass>@ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech/perfmdb?sslmode=require

Run
---
    uv run python neon/_apply_migration.py

Why not console.neon.tech/api/v2?
    Blocked by Zscaler IPS. The direct compute endpoint is NOT blocked.
"""
from __future__ import annotations
import json, os, re, urllib.error, urllib.request
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "api" / ".env")

EP       = "ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech"
SQL_URL  = f"https://{EP}/sql"
CONN_STR = os.environ.get("NEON_DDL_CONN_STR", "")

if not CONN_STR:
    raise RuntimeError(
        "NEON_DDL_CONN_STR is not set.\n"
        "Add to api/.env:\n"
        "  NEON_DDL_CONN_STR=postgresql://neondb_owner:<pass>@<endpoint>/perfmdb?sslmode=require"
    )


def run_sql(sql: str) -> dict:
    body = json.dumps({"query": sql, "params": []}).encode()
    req  = urllib.request.Request(SQL_URL, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "Neon-Connection-String": CONN_STR,
    })
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main() -> None:
    sql_file = Path(__file__).parent / "migration.sql"
    if not sql_file.exists():
        raise FileNotFoundError(
            f"{sql_file} not found.\n"
            "Generate it with:  uv run alembic upgrade head --sql > neon/migration.sql"
        )

    raw   = sql_file.read_text(encoding="utf-8-sig")
    stmts = []
    for chunk in raw.split(";"):
        s = re.sub(r"--[^\n]*", "", chunk).strip()
        if s and s.upper() not in ("BEGIN", "COMMIT"):
            stmts.append(s)

    print(f"Applying {len(stmts)} statements from {sql_file.name} ...")
    all_ok = True
    for i, stmt in enumerate(stmts, 1):
        label = stmt[:80].replace("\n", " ")
        try:
            run_sql(stmt)
            print(f"  [{i:02d}] OK   {label}")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace") if exc.fp else ""
            print(f"  [{i:02d}] FAIL {label}")
            print(f"         HTTP {exc.code}: {body[:300]}")
            all_ok = False
        except Exception as exc:
            print(f"  [{i:02d}] FAIL {label}")
            print(f"         {exc}")
            all_ok = False

    if not all_ok:
        print("\nSome statements FAILED.")
        return

    print("\nAll statements applied.")
    result = run_sql("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
    print("Tables:", [row["tablename"] for row in result.get("rows", [])])

    try:
        ver = run_sql("SELECT version_num FROM alembic_version")
        print("Alembic version:", [r["version_num"] for r in ver.get("rows", [])])
    except Exception:
        pass


if __name__ == "__main__":
    main()