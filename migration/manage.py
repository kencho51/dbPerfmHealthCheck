"""
DB Performance Health Check - Database Management CLI
=====================================================

Single entry-point for all SQLite database operations.

Run from the project root:

    uv run python migration/manage.py <command> [--apply]

Dry-run mode (default)
----------------------
Without --apply, every mutating command prints what it would do and
exits without touching the database. This lets you preview changes safely.

    uv run python migration/manage.py create           # preview only
    uv run python migration/manage.py create --apply   # actually apply

Commands
--------
  status        Show DB path, tables, row counts, current Alembic revision
  create        Apply all migrations to create the full schema (idempotent)
  drop          Drop all tables (destructive - prompts for confirmation)
  reset         drop + create (full wipe and rebuild)
  migrate-up    Apply the next incremental schema change (alembic upgrade head)
  migrate-down  Reverse the last schema change (alembic downgrade -1)
  truncate      Delete all rows, keep schema (prompts for confirmation)
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path resolution - must be run from project root
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve().parent   # migration/
_ROOT = _HERE.parent                      # project root
sys.path.insert(0, str(_ROOT))

from api.database import SQLITE_PATH, SQLITE_URL  # noqa: E402

# Synchronous SQLite URL for Alembic CLI (strips aiosqlite driver)
_ALEMBIC_URL = str(SQLITE_URL).replace("sqlite+aiosqlite", "sqlite")

# Tables listed in FK-safe drop/truncate order
_DATA_TABLES = ["curated_query", "raw_query", "pattern_label", "spl_query", "user"]
_ALL_TABLES  = _DATA_TABLES + ["alembic_version"]

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _ok(msg: str)   -> None: print(f"  [OK]      {msg}")
def _err(msg: str)  -> None: print(f"  [ERROR]   {msg}")
def _info(msg: str) -> None: print(f"  [INFO]    {msg}")
def _warn(msg: str) -> None: print(f"  [WARN]    {msg}")
def _dry(msg: str)  -> None: print(f"  [DRY RUN] {msg}")

# ---------------------------------------------------------------------------
# Dry-run flag - set at entry-point, read by all commands
# ---------------------------------------------------------------------------

# True  = preview only (default)
# False = actually execute (requires --apply flag)
_DRY_RUN: bool = True


def _dry_banner(command: str) -> None:
    """Print a dry-run notice at the top of every mutating command."""
    print()
    print("  " + "-" * 58)
    print("  DRY RUN - no changes will be made.")
    print(f"  To apply, re-run with --apply:")
    print(f"    uv run python migration/manage.py {command} --apply")
    print("  " + "-" * 58)
    print()

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    """Return a synchronous SQLite connection to the current DB."""
    if not SQLITE_PATH.exists():
        _err(f"Database not found: {SQLITE_PATH}")
        _info("Run: uv run python migration/manage.py create --apply")
        sys.exit(1)
    return sqlite3.connect(SQLITE_PATH)


def _existing_tables(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def _row_count(con: sqlite3.Connection, table: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM \"{table}\"").fetchone()[0]  # noqa: S608
    except sqlite3.OperationalError:
        return -1


def _alembic_version(con: sqlite3.Connection) -> str:
    try:
        row = con.execute("SELECT version_num FROM alembic_version").fetchone()
        return row[0] if row else "(none)"
    except sqlite3.OperationalError:
        return "(table missing)"


# ---------------------------------------------------------------------------
# Alembic runner
# ---------------------------------------------------------------------------

def _alembic(args: list[str]) -> int:
    """Run an alembic sub-command with the correct DB URL."""
    env = os.environ.copy()
    # Ensure alembic.ini is resolved relative to the project root
    cmd = [sys.executable, "-m", "alembic"] + args
    result = subprocess.run(cmd, cwd=str(_ROOT), env=env)
    return result.returncode


# ---------------------------------------------------------------------------
# Confirmation prompt
# ---------------------------------------------------------------------------

def _confirm(prompt: str) -> bool:
    answer = input(f"\n  {prompt} [yes/N] ").strip().lower()
    return answer == "yes"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_status() -> None:
    """Read-only - dry-run flag has no effect."""
    print("\nDB Performance Health Check - Status")
    print(f"  DB path   : {SQLITE_PATH}")
    print(f"  DB exists : {'yes' if SQLITE_PATH.exists() else 'no (not created yet)'}\n")

    if not SQLITE_PATH.exists():
        return

    con = _connect()
    tables = _existing_tables(con)
    rev    = _alembic_version(con)

    print(f"  {'Table':<25} {'Rows':>8}")
    print(f"  {'-'*25} {'-'*8}")
    for t in _ALL_TABLES:
        if t in tables:
            n = _row_count(con, t)
            row_str = f"{n:>8}" if n >= 0 else f"{'err':>8}"
            print(f"  {t:<25} {row_str}")
        else:
            print(f"  {t:<25} {'(missing)':>8}")

    print(f"\n  Alembic revision : {rev}")
    con.close()
    print()


def cmd_create() -> None:
    print("\nCreate - apply all Alembic migrations to build the full schema.")

    if _DRY_RUN:
        _dry_banner("create")
        _dry("Would ensure DB directory exists: " + str(SQLITE_PATH.parent))
        _dry("Would run: alembic upgrade head")
        _info("Showing pending SQL (read-only preview):")
        print()
        _alembic(["upgrade", "head", "--sql"])
        return

    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    rc = _alembic(["upgrade", "head"])
    if rc == 0:
        _ok("Schema created / already up-to-date.")
    else:
        _err("Alembic exited with errors (see output above).")
        sys.exit(rc)
    print()


def cmd_drop() -> None:
    print("\nDrop - all tables and data will be permanently deleted.")

    if _DRY_RUN:
        _dry_banner("drop")
        if not SQLITE_PATH.exists():
            _warn("Database file does not exist - nothing to drop.")
            return
        con = sqlite3.connect(SQLITE_PATH)
        existing = _existing_tables(con)
        con.close()
        tables_to_drop = [
            t for t in list(reversed(_DATA_TABLES)) + ["alembic_version"]
            if t in existing
        ]
        if tables_to_drop:
            for t in tables_to_drop:
                _dry(f"Would drop table: {t}")
        else:
            _warn("No known tables found.")
        return

    if not _confirm("Type 'yes' to confirm DROP:"):
        _info("Cancelled.")
        return

    if not SQLITE_PATH.exists():
        _warn("Database file does not exist - nothing to drop.")
        return

    con = _connect()
    existing = _existing_tables(con)
    con.execute("PRAGMA foreign_keys = OFF")

    dropped = []
    for t in list(reversed(_DATA_TABLES)) + ["alembic_version"]:
        if t in existing:
            con.execute(f'DROP TABLE IF EXISTS "{t}"')
            dropped.append(t)

    con.commit()
    con.close()

    if dropped:
        for t in dropped:
            _ok(f"Dropped: {t}")
    else:
        _warn("No known tables found.")
    print()


def cmd_reset() -> None:
    print("\nReset - full drop + recreate (all data will be lost).")

    if _DRY_RUN:
        _dry_banner("reset")
        if SQLITE_PATH.exists():
            con = sqlite3.connect(SQLITE_PATH)
            existing = _existing_tables(con)
            con.close()
            for t in list(reversed(_DATA_TABLES)) + ["alembic_version"]:
                if t in existing:
                    _dry(f"Would drop table: {t}")
        else:
            _dry("DB file does not exist - drop phase would be skipped.")
        _dry("Would run: alembic upgrade head (recreate schema)")
        _info("Showing pending SQL (read-only preview):")
        print()
        _alembic(["upgrade", "head", "--sql"])
        return

    if not _confirm("Type 'yes' to confirm RESET:"):
        _info("Cancelled.")
        return

    if SQLITE_PATH.exists():
        con = sqlite3.connect(SQLITE_PATH)
        existing = _existing_tables(con)
        con.execute("PRAGMA foreign_keys = OFF")
        for t in list(reversed(_DATA_TABLES)) + ["alembic_version"]:
            if t in existing:
                con.execute(f'DROP TABLE IF EXISTS "{t}"')
                _ok(f"Dropped: {t}")
        con.commit()
        con.close()

    print("\nRecreating schema...")
    rc = _alembic(["upgrade", "head"])
    if rc == 0:
        _ok("Schema recreated.")
    else:
        _err("Alembic exited with errors.")
        sys.exit(rc)
    print()


def cmd_migrate_up() -> None:
    print("\nMigrate-up - apply pending migrations (alembic upgrade head).")

    if _DRY_RUN:
        _dry_banner("migrate-up")
        _dry("Would run: alembic upgrade head")
        _info("Showing pending SQL (read-only preview):")
        print()
        _alembic(["upgrade", "head", "--sql"])
        return

    rc = _alembic(["upgrade", "head"])
    if rc == 0:
        _ok("Migrations applied / already at head.")
    else:
        _err("Alembic exited with errors.")
        sys.exit(rc)
    print()


def cmd_migrate_down() -> None:
    print("\nMigrate-down - reverse the last schema migration.")

    if _DRY_RUN:
        _dry_banner("migrate-down")
        _dry("Would run: alembic downgrade -1")
        _info("Showing rollback SQL (read-only preview):")
        print()
        _alembic(["downgrade", "-1", "--sql"])
        return

    if not _confirm("Type 'yes' to confirm DOWNGRADE -1:"):
        _info("Cancelled.")
        return
    rc = _alembic(["downgrade", "-1"])
    if rc == 0:
        _ok("Downgrade applied.")
    else:
        _err("Alembic exited with errors.")
        sys.exit(rc)
    print()


def cmd_truncate() -> None:
    print("\nTruncate - delete all rows, keep schema.")

    if _DRY_RUN:
        _dry_banner("truncate")
        if not SQLITE_PATH.exists():
            _warn("Database file does not exist - nothing to truncate.")
            return
        con = sqlite3.connect(SQLITE_PATH)
        existing = _existing_tables(con)
        for t in reversed(_DATA_TABLES):
            if t in existing:
                n = _row_count(con, t)
                _dry(f"Would DELETE FROM {t} ({n} rows)")
            else:
                _warn(f"Would skip (not found): {t}")
        con.close()
        return

    if not _confirm("Type 'yes' to confirm TRUNCATE:"):
        _info("Cancelled.")
        return

    con = _connect()
    existing = _existing_tables(con)
    con.execute("PRAGMA foreign_keys = OFF")

    for t in reversed(_DATA_TABLES):
        if t in existing:
            con.execute(f'DELETE FROM "{t}"')
            _ok(f"Truncated: {t}")
        else:
            _warn(f"Skipped (not found): {t}")

    con.commit()
    con.close()
    print()


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------

_COMMANDS: dict[str, tuple[str, "function"]] = {  # type: ignore[type-arg]
    "status":       ("Show DB path, tables, row counts, Alembic revision",  cmd_status),
    "create":       ("Apply migrations - create the full schema",            cmd_create),
    "drop":         ("Drop all tables (WARNING: destroys data)",             cmd_drop),
    "reset":        ("drop + create - full wipe and rebuild (WARNING)",      cmd_reset),
    "migrate-up":   ("Apply next incremental schema change",                 cmd_migrate_up),
    "migrate-down": ("Reverse the last schema change (WARNING)",             cmd_migrate_down),
    "truncate":     ("Delete all rows, keep schema (WARNING)",               cmd_truncate),
}


def _usage() -> None:
    print("\nUsage:  uv run python migration/manage.py <command> [--apply]\n")
    print("  Default mode is dry-run (preview only). Add --apply to apply changes.\n")
    print(f"  {'Command':<15} Description")
    print(f"  {'-'*15} {'-'*50}")
    for cmd, (desc, _) in _COMMANDS.items():
        print(f"  {cmd:<15} {desc}")
    print()


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        _usage()
        sys.exit(0)

    # Consume --apply from anywhere in the argument list
    if "--apply" in args:
        args = [a for a in args if a != "--apply"]
        _DRY_RUN = False

    command = args[0].lower()
    if command not in _COMMANDS:
        _err(f"Unknown command: {command!r}")
        _usage()
        sys.exit(1)

    _COMMANDS[command][1]()
