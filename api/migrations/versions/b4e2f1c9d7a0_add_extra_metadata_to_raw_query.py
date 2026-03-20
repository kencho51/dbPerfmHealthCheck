"""add_extra_metadata_to_raw_query

Adds the ``extra_metadata`` TEXT column to ``raw_query``.

This column stores type-specific structured data as a JSON string:

* **Deadlock rows**: deadlock_id, pid, is_victim, lockMode, waitresource,
  waittime, isolationlevel, loginname, clientapp, proc_name, exec_sql, …
* **Other types**: NULL (column is nullable, no change to existing rows).

Including deadlock-specific metadata in the hash (via the ingestor) ensures
that two processes involved in the same deadlock with identical SQL still
receive distinct ``query_hash`` values, so both (victim and waiter) are stored
as separate rows with their own context.

Revision ID: b4e2f1c9d7a0
Revises: 9db879faabd3
Create Date: 2026-03-20 00:00:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b4e2f1c9d7a0"
down_revision: Union[str, Sequence[str], None] = "9db879faabd3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add extra_metadata column to raw_query."""
    with op.batch_alter_table("raw_query", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "extra_metadata",
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=True,
            )
        )


def downgrade() -> None:
    """Remove extra_metadata column from raw_query."""
    with op.batch_alter_table("raw_query", schema=None) as batch_op:
        batch_op.drop_column("extra_metadata")
