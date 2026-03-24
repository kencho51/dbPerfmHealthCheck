"""
Run alembic upgrade head then verify the 6 new tables exist in master.db.
Usage:  uv run python scripts/run_migration.py

All output is written to scripts/run_migration.log so it can be read back
regardless of terminal capture issues.
"""
import sys
import sqlite3
import io
from pathlib import Path

ROOT = Path(__file__).parent.parent   # project root
LOG  = Path(__file__).parent / "run_migration.log"

lines: list[str] = []

def log(msg: str = "") -> None:
    lines.append(msg)

# ── Run Alembic ──────────────────────────────────────────────────────────────
try:
    from alembic.config import Config
    from alembic import command

    INI = ROOT / "alembic.ini"
    DB  = ROOT / "db" / "master.db"

    log(f"[migration] alembic.ini : {INI}")
    log(f"[migration] database    : {DB}")

    cfg = Config(str(INI))
    buf = io.StringIO()
    cfg.stdout = buf

    command.upgrade(cfg, "head")

    alembic_out = buf.getvalue().strip()
    if alembic_out:
        log("[alembic output]")
        for line in alembic_out.splitlines():
            log(f"  {line}")
    else:
        log("[alembic] upgrade head completed (no output = already at head or applied cleanly)")

except Exception as exc:
    log(f"[ERROR] Migration failed: {exc}")
    LOG.write_text("\n".join(lines))
    sys.exit(1)

# ── Verify tables ────────────────────────────────────────────────────────────
try:
    con = sqlite3.connect(str(DB))
    tables = sorted(
        r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    )
    ver_row = con.execute("SELECT version_num FROM alembic_version").fetchone()
    con.close()

    log(f"\n[migration] alembic version : {ver_row[0] if ver_row else 'none'}")
    log(f"[migration] tables ({len(tables)}):")
    for t in tables:
        log(f"  {t}")

    EXPECTED = {
        "raw_query_blocker",
        "raw_query_datafile_mongo",
        "raw_query_datafile_sql",
        "raw_query_deadlock",
        "raw_query_slow_mongo",
        "raw_query_slow_sql",
    }
    missing = EXPECTED - set(tables)
    if missing:
        log(f"\n[ERROR] Missing tables: {sorted(missing)}")
        LOG.write_text("\n".join(lines))
        sys.exit(1)
    else:
        log("\n[OK] All 6 typed tables are present.")

except Exception as exc:
    log(f"[ERROR] Verification failed: {exc}")
    LOG.write_text("\n".join(lines))
    sys.exit(1)

LOG.write_text("\n".join(lines))
