"""Add missing indexes to raw_query and upload_log tables.

Adds four indexes that were absent from the schema but are used on hot
query paths:

  raw_query.query_details
    - Substring search in GET /api/queries/ (LIKE scan without index = full
      table scan over 130 k+ rows)
    - Correlated UPDATE in _link_typed_to_raw for all four typed tables
      (upload.py lines 84–126)

  upload_log.filename
    - DELETE FROM upload_log WHERE filename = :fn on every re-upload
      (upload.py line 355)

  upload_log.file_type
    - Analytics window queries grouping by file_type
      (analytics.py line ~434)

  upload_log.uploaded_at
    - Date-range filtering in analytics time-window queries

Revision ID: f1a2b3c4d5e6
Revises: a5c3b1d8e2f7
Create Date: 2026-04-10
"""

from __future__ import annotations

from alembic import op

revision = "f1a2b3c4d5e6"
down_revision = "a5c3b1d8e2f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_raw_query_query_details",
        "raw_query",
        ["query_details"],
    )
    op.create_index(
        "ix_upload_log_filename",
        "upload_log",
        ["filename"],
    )
    op.create_index(
        "ix_upload_log_file_type",
        "upload_log",
        ["file_type"],
    )
    op.create_index(
        "ix_upload_log_uploaded_at",
        "upload_log",
        ["uploaded_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_upload_log_uploaded_at", "upload_log")
    op.drop_index("ix_upload_log_file_type", "upload_log")
    op.drop_index("ix_upload_log_filename", "upload_log")
    op.drop_index("ix_raw_query_query_details", "raw_query")
