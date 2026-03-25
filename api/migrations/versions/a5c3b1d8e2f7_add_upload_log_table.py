"""Add upload_log table to track per-upload CSV file row counts.

Each row records one successful upload: filename, type, environment,
month_year, and the actual CSV row count from the file.  This replaces
the misleading SUM(occurrence_count) metric in the monthly stats table
with the real number of rows found in each uploaded CSV file.

Revision ID: a5c3b1d8e2f7
Revises: c3d7f2a1b8e4
Create Date: 2026-03-25
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "a5c3b1d8e2f7"
down_revision = "c3d7f2a1b8e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "upload_log",
        sa.Column("id",            sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("filename",      sa.Text(),    nullable=False),
        sa.Column("file_type",     sa.Text(),    nullable=True),
        sa.Column("environment",   sa.Text(),    nullable=True),
        sa.Column("month_year",    sa.Text(),    nullable=True),
        sa.Column("csv_row_count", sa.Integer(), nullable=False),
        sa.Column("inserted",      sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated",       sa.Integer(), nullable=False, server_default="0"),
        sa.Column("uploaded_at",   sa.Text(),    nullable=False),
    )
    op.create_index("ix_upload_log_month_year", "upload_log", ["month_year"])


def downgrade() -> None:
    op.drop_index("ix_upload_log_month_year", "upload_log")
    op.drop_table("upload_log")
