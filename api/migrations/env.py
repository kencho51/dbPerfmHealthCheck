"""
Alembic environment script.

Key changes from the generated default:
  - Imports SQLModel metadata from api.models so autogenerate works.
  - Loads DATABASE_URL from api/.env via python-dotenv.
  - Converts the URL scheme to postgresql+asyncpg for the Alembic CLI.
    asyncpg is already a project dependency; psycopg2-binary is not required.
"""
from __future__ import annotations

import os
from logging.config import fileConfig
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

from alembic import context

# ---------------------------------------------------------------------------
# Load env file before anything else
# ---------------------------------------------------------------------------
_ENV_FILE = Path(__file__).parent.parent / ".env"   # api/.env
load_dotenv(_ENV_FILE)

# ---------------------------------------------------------------------------
# Import models so SQLModel.metadata is populated for autogenerate
# ---------------------------------------------------------------------------
import api.models  # noqa: F401  — registers PatternLabel, CuratedQuery, RawQuery tables

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
    raw = os.environ.get("DATABASE_URL", "")
    if not raw:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Ensure api/.env exists with DATABASE_URL=postgresql://..."
        )
    # Use asyncpg dialect for Alembic CLI (psycopg2-binary is not a dependency).
    # In offline --sql mode no actual connection is made; only the dialect matters.
    url = raw.replace("postgresql+psycopg2://", "postgresql://", 1)
    url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


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

