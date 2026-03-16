"""
Neon PostgreSQL database management CLI.

All operations go via the direct HTTPS SQL endpoint (port 443).
Port 5432 is never used — safe behind corporate proxy.

Usage (run from project root):
    uv run python neon/manage.py status
    uv run python neon/manage.py create
    uv run python neon/manage.py drop
    uv run python neon/manage.py reset
    uv run python neon/manage.py migrate-up
    uv run python neon/manage.py migrate-down
    uv run python neon/manage.py truncate

Commands
--------
  status       Show current tables, row counts, and Alembic version
  create       Apply neon/migration.sql + neon/neon_schema.sql (full schema)
  drop         Drop all application tables and ENUM types
  reset        drop + create  (full wipe and re-apply)
  migrate-up   Generate SQL via `alembic upgrade head --sql` and apply it
  migrate-down Generate SQL via `alembic downgrade -1 --sql` and apply it
  truncate     Empty all data tables (keep schema), reset sequences

Prerequisites
-------------
  NEON_DDL_CONN_STR in api/.env — neondb_owner connection string (DDL privileges)
  NEON_SQL_PASS in api/.env     — perfmdb_owner password (used for status READ)
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

# ── Config ──────────────────────────────────────────────────────────────────

_ROOT    = Path(__file__).parent.parent          # project root (dbPerfmHealthCheck/)
_NEO_DIR = Path(__file__).parent                 # neon/

load_dotenv(_ROOT / "api" / ".env")

EP           = "ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech"
SQL_URL      = f"https://{EP}/sql"
DDL_CONN_STR = os.environ.get("NEON_DDL_CONN_STR", "")   # neondb_owner — DDL
APP_CONN_STR = os.environ.get("DATABASE_URL", "").replace(
    "postgresql+psycopg2://", "postgresql://"
)                                                           # perfmdb_owner — read-only status

# Tables in drop / truncate order (FK-safe: children first)
_TABLES = ["curated_query", "raw_query", "pattern_label", '"user"', "alembic_version"]
_ENUMS  = ["severitytype", "sourcetype", "environmenttype", "querytype", "labelsource"]


# ── Low-level HTTP SQL ───────────────────────────────────────────────────────

def _run_sql(sql: str, conn_str: str) -> dict:
    """POST a single SQL statement and return the Neon response dict."""
    body = json.dumps({"query": sql, "params": []}).encode()
    req  = urllib.request.Request(
        SQL_URL, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Neon-Connection-String": conn_str,
        },
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def _apply_statements(stmts: list[str], conn_str: str, label: str = "") -> bool:
    """Apply a list of SQL statements. Returns True if all succeeded."""
    if not stmts:
        print("  (nothing to apply)")
        return True
    all_ok = True
    for i, stmt in enumerate(stmts, 1):
        short = stmt[:90].replace("\n", " ")
        try:
            _run_sql(stmt, conn_str)
            print(f"  [{i:02d}] OK   {short}")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace") if exc.fp else ""
            print(f"  [{i:02d}] FAIL {short}")
            print(f"         HTTP {exc.code}: {body[:300]}")
            all_ok = False
        except Exception as exc:
            print(f"  [{i:02d}] FAIL {short}")
            print(f"         {exc}")
            all_ok = False
    return all_ok


def _parse_sql_file(path: Path) -> list[str]:
    """Split a SQL file into individual statements, drop transaction wrappers."""
    raw   = path.read_text(encoding="utf-8-sig")
    stmts = []
    for chunk in raw.split(";"):
        s = re.sub(r"--[^\n]*", "", chunk).strip()
        if s and s.upper() not in ("BEGIN", "COMMIT"):
            stmts.append(s)
    return stmts


# ── Commands ─────────────────────────────────────────────────────────────────

def cmd_status() -> None:
    """Show tables, row counts, and Alembic version."""
    print("── Status ──────────────────────────────")
    conn = DDL_CONN_STR or APP_CONN_STR
    if not conn:
        print("ERROR: neither NEON_DDL_CONN_STR nor DATABASE_URL is set in api/.env")
        sys.exit(1)

    # Tables
    try:
        res = _run_sql(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
            conn,
        )
        tables = [r["tablename"] for r in res.get("rows", [])]
        print(f"  Tables ({len(tables)}): {tables}")
    except Exception as exc:
        print(f"  Tables: ERROR — {exc}")

    # Row counts
    for t in ["raw_query", "curated_query", "pattern_label", '"user"']:
        try:
            res = _run_sql(f"SELECT COUNT(*) AS n FROM {t}", conn)
            n = res["rows"][0]["n"] if res.get("rows") else "?"
            print(f"  {t:<20} {n} rows")
        except Exception:
            print(f"  {t:<20} (table not found)")

    # Alembic version
    try:
        res = _run_sql("SELECT version_num FROM alembic_version", conn)
        vers = [r["version_num"] for r in res.get("rows", [])]
        print(f"  Alembic version: {vers or '(none)'}")
    except Exception:
        print("  Alembic version: (alembic_version table not found)")


def cmd_create() -> None:
    """Apply migration.sql + neon_schema.sql to create the full schema."""
    _require_ddl()
    print("── Create schema ───────────────────────")

    migration_sql = _NEO_DIR / "migration.sql"
    schema_sql    = _NEO_DIR / "neon_schema.sql"

    if not migration_sql.exists():
        print(f"ERROR: {migration_sql} not found.")
        print("Generate with:  uv run alembic upgrade head --sql > neon/migration.sql")
        sys.exit(1)

    stmts = _parse_sql_file(migration_sql)
    if schema_sql.exists():
        stmts += _parse_sql_file(schema_sql)

    print(f"Applying {len(stmts)} statements ...")
    ok = _apply_statements(stmts, DDL_CONN_STR)
    print(f"\n{'Schema created.' if ok else 'Some statements FAILED — check errors above.'}")


def cmd_drop() -> None:
    """Drop all application tables and ENUM types."""
    _require_ddl()
    print("── Drop schema ─────────────────────────")
    _confirm("This will DROP all tables and data. Continue?")

    stmts = [f"DROP TABLE IF EXISTS {t} CASCADE" for t in _TABLES]
    stmts += [f"DROP TYPE  IF EXISTS {e} CASCADE" for e in _ENUMS]

    print(f"Dropping {len(stmts)} objects ...")
    ok = _apply_statements(stmts, DDL_CONN_STR)
    print(f"\n{'All objects dropped.' if ok else 'Some drops FAILED.'}")


def cmd_reset() -> None:
    """Drop everything then re-apply the full schema (data will be lost)."""
    _require_ddl()
    print("── Reset database ──────────────────────")
    _confirm("This will WIPE ALL DATA and recreate the schema. Continue?")

    # Drop
    drop_stmts = [f"DROP TABLE IF EXISTS {t} CASCADE" for t in _TABLES]
    drop_stmts += [f"DROP TYPE  IF EXISTS {e} CASCADE" for e in _ENUMS]
    print(f"Step 1/2: Dropping {len(drop_stmts)} objects ...")
    _apply_statements(drop_stmts, DDL_CONN_STR)

    # Create
    migration_sql = _NEO_DIR / "migration.sql"
    schema_sql    = _NEO_DIR / "neon_schema.sql"
    if not migration_sql.exists():
        print(f"ERROR: {migration_sql} not found.")
        sys.exit(1)

    stmts = _parse_sql_file(migration_sql)
    if schema_sql.exists():
        stmts += _parse_sql_file(schema_sql)

    print(f"Step 2/2: Applying {len(stmts)} statements ...")
    ok = _apply_statements(stmts, DDL_CONN_STR)
    print(f"\n{'Reset complete.' if ok else 'Some statements FAILED.'}")


def cmd_migrate_up() -> None:
    """
    Generate incremental SQL via `alembic upgrade head --sql` and apply it.

    Alembic diffs the current model definitions against the Alembic version
    recorded in `alembic_version` and outputs only the NEW statements needed.
    Use this after adding/changing a model in api/models.py.

    Workflow:
        1. Edit api/models.py
        2. uv run alembic revision --autogenerate -m "describe change"
        3. uv run python neon/manage.py migrate-up
    """
    _require_ddl()
    print("── Migrate up ──────────────────────────")
    print("Generating SQL via alembic upgrade head --sql ...")

    result = subprocess.run(
        ["uv", "run", "alembic", "upgrade", "head", "--sql"],
        capture_output=True, text=True, cwd=_ROOT,
    )
    if result.returncode != 0:
        print("ERROR: alembic failed:")
        print(result.stderr)
        sys.exit(1)

    sql = result.stdout
    if not sql.strip():
        print("Nothing to migrate — already at head.")
        return

    stmts = []
    for chunk in sql.split(";"):
        s = re.sub(r"--[^\n]*", "", chunk).strip()
        if s and s.upper() not in ("BEGIN", "COMMIT"):
            stmts.append(s)

    print(f"Applying {len(stmts)} statements ...")
    ok = _apply_statements(stmts, DDL_CONN_STR)
    print(f"\n{'Migration applied.' if ok else 'Some statements FAILED.'}")


def cmd_migrate_down() -> None:
    """
    Rollback the last Alembic migration via `alembic downgrade -1 --sql`.

    This reverses the most recent migration: drops added columns/tables,
    restores removed columns/tables, moves `alembic_version` back one step.

    Use when you need to undo the last schema change.
    """
    _require_ddl()
    print("── Migrate down (rollback last migration) ──")
    _confirm("This will REVERSE the last schema migration. Continue?")
    print("Generating SQL via alembic downgrade -1 --sql ...")

    result = subprocess.run(
        ["uv", "run", "alembic", "downgrade", "-1", "--sql"],
        capture_output=True, text=True, cwd=_ROOT,
    )
    if result.returncode != 0:
        print("ERROR: alembic failed:")
        print(result.stderr)
        sys.exit(1)

    sql = result.stdout
    if not sql.strip():
        print("Nothing to downgrade — already at base.")
        return

    stmts = []
    for chunk in sql.split(";"):
        s = re.sub(r"--[^\n]*", "", chunk).strip()
        if s and s.upper() not in ("BEGIN", "COMMIT"):
            stmts.append(s)

    print(f"Applying {len(stmts)} statements ...")
    ok = _apply_statements(stmts, DDL_CONN_STR)
    print(f"\n{'Rollback applied.' if ok else 'Some statements FAILED.'}")


def cmd_truncate() -> None:
    """Empty all data tables but keep the schema intact."""
    _require_ddl()
    print("── Truncate data ───────────────────────")
    data_tables = ["curated_query", "raw_query", "pattern_label", '"user"']
    _confirm(f"This will DELETE ALL ROWS from {data_tables}. Continue?")

    stmt = f"TRUNCATE TABLE {', '.join(data_tables)} RESTART IDENTITY CASCADE"
    ok = _apply_statements([stmt], DDL_CONN_STR)
    print(f"\n{'All data cleared.' if ok else 'Truncate FAILED.'}")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _require_ddl() -> None:
    if not DDL_CONN_STR:
        print("ERROR: NEON_DDL_CONN_STR is not set in api/.env")
        print("  Set it to the neondb_owner connection string (DDL privileges required).")
        sys.exit(1)


def _confirm(msg: str) -> None:
    ans = input(f"\n⚠️  {msg} [y/N] ").strip().lower()
    if ans != "y":
        print("Aborted.")
        sys.exit(0)


# ── Entrypoint ────────────────────────────────────────────────────────────────

_COMMANDS = {
    "status":       cmd_status,
    "create":       cmd_create,
    "drop":         cmd_drop,
    "reset":        cmd_reset,
    "migrate-up":   cmd_migrate_up,
    "migrate-down": cmd_migrate_down,
    "truncate":     cmd_truncate,
}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in _COMMANDS:
        print("Usage: uv run python neon/manage.py <command>")
        print()
        print("Commands:")
        for name, fn in _COMMANDS.items():
            first_line = (fn.__doc__ or "").strip().splitlines()[0]
            print(f"  {name:<15} {first_line}")
        sys.exit(1)

    _COMMANDS[sys.argv[1]]()
