"""
Alembic environment script.

Key changes from the generated default:
  - Imports SQLModel metadata from api.models so autogenerate works.
  - Reads the DB URL from the .ini file (falls back to SQLITE_PATH env var).
  - Uses the synchronous SQLite driver for the Alembic CLI; the app uses
    aiosqlite at runtime (see api/database.py).
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

# ---------------------------------------------------------------------------
# Import models so SQLModel.metadata is populated for autogenerate
# ---------------------------------------------------------------------------
import api.models  # noqa: F401  — registers RawQuery + Pattern tables

# ---------------------------------------------------------------------------
# Alembic config object
# ---------------------------------------------------------------------------
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Point autogenerate at SQLModel's shared metadata
target_metadata = SQLModel.metadata


# ---------------------------------------------------------------------------
# Optionally override the URL from the environment (e.g. in CI)
# ---------------------------------------------------------------------------
def _get_url() -> str:
    env_path = os.getenv("SQLITE_PATH")
    if env_path:
        return f"sqlite:///{env_path}"
    return config.get_main_option("sqlalchemy.url")


# ---------------------------------------------------------------------------
# Offline mode — emit SQL to stdout without connecting
# ---------------------------------------------------------------------------
def run_migrations_offline() -> None:
    url = _get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
    )
    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Online mode — connect and run migrations
# ---------------------------------------------------------------------------
def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # Needed for ALTER TABLE support in SQLite (batch mode)
            render_as_batch=True,
        )
        with context.begin_transaction():
            context.run_migrations()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
