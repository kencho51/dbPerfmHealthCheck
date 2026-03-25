"""Add raw_query_id FK to typed tables (Phase 1 — class table inheritance).

Each typed table gains a nullable `raw_query_id` FK column that references
`raw_query.id`.  This makes typed tables proper *extension* tables rather than
standalone tables with their own dedup key.

The `query_hash` UNIQUE constraint on each typed table is intentionally kept so
the existing upsert logic in `typed_ingestor.py` continues to work unchanged.
`raw_query_id` is populated post-ingest by a SQL UPDATE in the upload router.

Revision ID: c3d7f2a1b8e4
Revises: a1f3e9b2c8d5
Create Date: 2026-03-24
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c3d7f2a1b8e4"
down_revision = "a1f3e9b2c8d5"
branch_labels = None
depends_on = None

_TYPED_TABLES = [
    "raw_query_slow_sql",
    "raw_query_blocker",
    "raw_query_deadlock",
    "raw_query_slow_mongo",
]


def upgrade() -> None:
    for table in _TYPED_TABLES:
        with op.batch_alter_table(table) as batch_op:
            batch_op.add_column(
                sa.Column("raw_query_id", sa.Integer, nullable=True)
            )
            batch_op.create_index(f"ix_{table}_raw_query_id", ["raw_query_id"])
            batch_op.create_foreign_key(
                f"fk_{table}_raw_query_id",
                "raw_query",
                ["raw_query_id"],
                ["id"],
                ondelete="SET NULL",
            )


def downgrade() -> None:
    for table in _TYPED_TABLES:
        with op.batch_alter_table(table) as batch_op:
            batch_op.drop_constraint(f"fk_{table}_raw_query_id", type_="foreignkey")
            batch_op.drop_index(f"ix_{table}_raw_query_id")
            batch_op.drop_column("raw_query_id")
