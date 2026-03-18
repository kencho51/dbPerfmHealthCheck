"""
DB Performance Health Check — Database Management CLI
=====================================================

Single entry-point for all SQLite database operations.

Run from the **project root**:

    uv run python migration/manage.py <command>

Commands
--------
  status        Show DB path, tables, row counts, current Alembic revision
  create        Apply all migrations → create the full schema (idempotent)
  drop          Drop all tables (destructive — prompts for confirmation)
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
# Path resolution — must be run from project root
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve().parent          # migration/
_ROOT = _HERE.parent                             # project root
sys.path.insert(0, str(_ROOT))

from api.database import SQLITE_PATH, SQLITE_URL  # noqa: E402

# Synchronous SQLite URL for Alembic CLI (strips aiosqlite driver)
_ALEMBIC_URL = str(SQLITE_URL).replace("sqlite+aiosqlite", "sqlite")

# Tables that contain user data (truncate/drop order respects FK constraints)
_DATA_TABLES = ["curated_query", "raw_query", "pattern_label", "spl_query", "user"]
_ALL_TABLES  = _DATA_TABLES + ["alembic_version"]

# ---------------------------------------------------------------------------
# ANSI colours
# ---------------------------------------------------------------------------

_R = "\033[0m"
_G = "\033[92m"   # green
_Y = "\033[93m"   # yellow
_C = "\033[96m"   # cyan
_E = "\033[91m"   # red


def _ok(msg: str)  -> None: print(f"  {_G}✓{_R}  {msg}")
def _err(msg: str) -> None: print(f"  {_E}✗{_R}  {msg}")
def _info(msg: str)-> None: print(f"  {_C}»{_R}  {msg}")
def _warn(msg: str)-> None: print(f"  {_Y}!{_R}  {msg}")

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    """Return a synchronous SQLite connection to the current DB."""
    if not SQLITE_PATH.exists():
        _err(f"Database not found: {SQLITE_PATH}")
        _info("Run  uv run python migration/manage.py create  to initialise it.")
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
    answer = input(f"\n  {_Y}{prompt}{_R} [yes/N] ").strip().lower()
    return answer == "yes"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_status() -> None:
    print(f"\n{_C}DB Performance Health Check — Status{_R}")
    print(f"  DB path   : {SQLITE_PATH}")
    print(f"  DB exists : {'yes' if SQLITE_PATH.exists() else _Y + 'no (not created yet)' + _R}\n")

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
            indicator = _G if n >= 0 else _E
            count_str = f"{indicator}{n:>8}{_R}" if n >= 0 else f"{_E}{'err':>8}{_R}"
            print(f"  {t:<25} {count_str}")
        else:
            print(f"  {_Y}{t:<25}{_R} {'(missing)':>8}")

    print(f"\n  Alembic revision : {_C}{rev}{_R}")
    con.close()
    print()


def cmd_create() -> None:
    print(f"\n{_C}Creating schema via Alembic migrations…{_R}")
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    rc = _alembic(["upgrade", "head"])
    if rc == 0:
        _ok("Schema created / already up-to-date.")
    else:
        _err("Alembic exited with errors (see output above).")
        sys.exit(rc)
    print()


def cmd_drop() -> None:
    print(f"\n{_E}DROP — all tables and data will be permanently deleted.{_R}")
    if not _confirm("Type 'yes' to confirm DROP:"):
        _info("Cancelled.")
        return

    if not SQLITE_PATH.exists():
        _warn("Database file does not exist — nothing to drop.")
        return

    con = _connect()
    existing = _existing_tables(con)
    con.execute("PRAGMA foreign_keys = OFF")

    dropped = []
    for t in reversed(_DATA_TABLES) + ["alembic_version"]:
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
    print(f"\n{_E}RESET — full drop + recreate (all data will be lost).{_R}")
    if not _confirm("Type 'yes' to confirm RESET:"):
        _info("Cancelled.")
        return

    # Drop without second confirmation (already confirmed above)
    if SQLITE_PATH.exists():
        con = sqlite3.connect(SQLITE_PATH)
        existing = _existing_tables(con)
        con.execute("PRAGMA foreign_keys = OFF")
        for t in reversed(_DATA_TABLES) + ["alembic_version"]:
            if t in existing:
                con.execute(f'DROP TABLE IF EXISTS "{t}"')
                _ok(f"Dropped: {t}")
        con.commit()
        con.close()

    print(f"\n{_C}Recreating schema…{_R}")
    rc = _alembic(["upgrade", "head"])
    if rc == 0:
        _ok("Schema recreated.")
    else:
        _err("Alembic exited with errors.")
        sys.exit(rc)
    print()


def cmd_migrate_up() -> None:
    print(f"\n{_C}Applying pending migrations (alembic upgrade head)…{_R}")
    rc = _alembic(["upgrade", "head"])
    if rc == 0:
        _ok("Migrations applied / already at head.")
    else:
        _err("Alembic exited with errors.")
        sys.exit(rc)
    print()


def cmd_migrate_down() -> None:
    print(f"\n{_Y}MIGRATE-DOWN — reversing the last migration.{_R}")
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
    print(f"\n{_Y}TRUNCATE — all rows deleted, schema kept.{_R}")
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
    "status":       ("Show DB path, tables, row counts, Alembic revision",   cmd_status),
    "create":       ("Apply migrations — create the full schema",             cmd_create),
    "drop":         ("Drop all tables  ⚠ destroys data",                     cmd_drop),
    "reset":        ("drop + create — full wipe and rebuild  ⚠",             cmd_reset),
    "migrate-up":   ("Apply next incremental schema change",                  cmd_migrate_up),
    "migrate-down": ("Reverse the last schema change  ⚠",                    cmd_migrate_down),
    "truncate":     ("Delete all rows, keep schema  ⚠",                      cmd_truncate),
}


def _usage() -> None:
    print(f"\n{_C}Usage:{_R}  uv run python migration/manage.py <command>\n")
    print(f"  {'Command':<15} Description")
    print(f"  {'-'*15} {'-'*50}")
    for cmd, (desc, _) in _COMMANDS.items():
        print(f"  {cmd:<15} {desc}")
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        _usage()
        sys.exit(0)

    command = sys.argv[1].lower()
    if command not in _COMMANDS:
        _err(f"Unknown command: {command!r}")
        _usage()
        sys.exit(1)

    _COMMANDS[command][1]()
