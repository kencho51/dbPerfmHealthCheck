"""
Migration tests — verify Alembic can fully upgrade and downgrade the schema.

These tests use a temporary file-based SQLite database (NOT the shared
in-memory test DB) so Alembic's synchronous engine and file paths work
correctly with the standard sqlite:// driver.

WHY NOT IN CI:
  These tests are explicitly excluded from the GitHub Actions workflow
  because they shell out to alembic via subprocess and write to disk,
  which is environment-sensitive and slow.  They are valuable for local
  verification of schema evolution before merging migration files.

Run locally:
    uv run pytest tests/test_migrations.py -v
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent


def _run_alembic(*args: str, db_path: Path) -> subprocess.CompletedProcess:
    """Invoke alembic as a subprocess with SQLITE_PATH set to db_path."""
    env = {**os.environ, "SQLITE_PATH": str(db_path)}
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    """Return a path to a fresh (non-existent) SQLite DB file for one test."""
    return tmp_path / "test_migration.db"


# ---------------------------------------------------------------------------
# Upgrade to head
# ---------------------------------------------------------------------------

class TestAlembicUpgradeHead:
    def test_upgrade_head_succeeds(self, tmp_db: Path):
        result = _run_alembic("upgrade", "head", db_path=tmp_db)
        assert result.returncode == 0, (
            f"alembic upgrade head failed\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    def test_db_file_created(self, tmp_db: Path):
        _run_alembic("upgrade", "head", db_path=tmp_db)
        assert tmp_db.exists(), "SQLite DB file should exist after upgrade head"

    def test_all_expected_tables_exist(self, tmp_db: Path):
        _run_alembic("upgrade", "head", db_path=tmp_db)
        conn = sqlite3.connect(str(tmp_db))
        try:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = {row[0] for row in cursor.fetchall()}
        finally:
            conn.close()

        # Only tables that Alembic migrations actually CREATE on a fresh DB.
        # pattern_label, curated_query, user, spl_query are created by
        # SQLModel.metadata.create_all() at app startup — they have no migration.
        expected = {
            "raw_query",
            "raw_query_slow_sql",
            "raw_query_blocker",
            "raw_query_deadlock",
            "raw_query_slow_mongo",
            "upload_log",
            "alembic_version",
        }
        missing = expected - tables
        assert not missing, (
            f"Missing tables after 'alembic upgrade head': {sorted(missing)}"
        )

    def test_alembic_version_recorded(self, tmp_db: Path):
        _run_alembic("upgrade", "head", db_path=tmp_db)
        conn = sqlite3.connect(str(tmp_db))
        try:
            cursor = conn.execute("SELECT version_num FROM alembic_version")
            versions = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
        assert len(versions) == 1, (
            f"Expected exactly 1 alembic version row, got: {versions}"
        )
        assert versions[0], "Alembic version_num must not be empty"

    def test_upgrade_head_is_idempotent(self, tmp_db: Path):
        """Running upgrade head twice should be a no-op on the second call."""
        r1 = _run_alembic("upgrade", "head", db_path=tmp_db)
        r2 = _run_alembic("upgrade", "head", db_path=tmp_db)
        assert r1.returncode == 0
        assert r2.returncode == 0

    def test_raw_query_columns(self, tmp_db: Path):
        """Spot-check that key columns added by later migrations exist."""
        _run_alembic("upgrade", "head", db_path=tmp_db)
        conn = sqlite3.connect(str(tmp_db))
        try:
            cursor = conn.execute("PRAGMA table_info(raw_query)")
            col_names = {row[1] for row in cursor.fetchall()}
        finally:
            conn.close()
        for col in ("id", "query_hash", "extra_metadata", "month_year",
                    "occurrence_count", "first_seen", "last_seen"):
            assert col in col_names, f"Column '{col}' missing from raw_query"

    def test_typed_tables_have_raw_query_id_fk(self, tmp_db: Path):
        """raw_query_id FK column should exist on all typed tables."""
        _run_alembic("upgrade", "head", db_path=tmp_db)
        conn = sqlite3.connect(str(tmp_db))
        try:
            for table in ("raw_query_slow_sql", "raw_query_blocker",
                          "raw_query_deadlock", "raw_query_slow_mongo"):
                cursor = conn.execute(f"PRAGMA table_info({table})")
                col_names = {row[1] for row in cursor.fetchall()}
                assert "raw_query_id" in col_names, (
                    f"raw_query_id column missing from {table}"
                )
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------

class TestAlembicDowngrade:
    def test_downgrade_one_step_succeeds(self, tmp_db: Path):
        _run_alembic("upgrade", "head", db_path=tmp_db)
        result = _run_alembic("downgrade", "-1", db_path=tmp_db)
        assert result.returncode == 0, (
            f"alembic downgrade -1 failed\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    def test_upgrade_after_downgrade_succeeds(self, tmp_db: Path):
        _run_alembic("upgrade", "head", db_path=tmp_db)
        _run_alembic("downgrade", "-1", db_path=tmp_db)
        result = _run_alembic("upgrade", "head", db_path=tmp_db)
        assert result.returncode == 0, (
            f"Re-upgrade after downgrade failed\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    def test_downgrade_to_base_succeeds(self, tmp_db: Path):
        """Full rollback to the initial state should complete without error."""
        _run_alembic("upgrade", "head", db_path=tmp_db)
        result = _run_alembic("downgrade", "base", db_path=tmp_db)
        assert result.returncode == 0, (
            f"alembic downgrade base failed\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    def test_full_cycle_up_down_up(self, tmp_db: Path):
        """Upgrade → downgrade to base → upgrade again must all succeed."""
        r1 = _run_alembic("upgrade", "head", db_path=tmp_db)
        r2 = _run_alembic("downgrade", "base", db_path=tmp_db)
        r3 = _run_alembic("upgrade", "head", db_path=tmp_db)
        assert r1.returncode == 0, f"First upgrade failed: {r1.stderr}"
        assert r2.returncode == 0, f"Downgrade base failed: {r2.stderr}"
        assert r3.returncode == 0, f"Final upgrade failed: {r3.stderr}"
