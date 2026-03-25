"""drop_datafile_tables

Removes raw_query_datafile_sql and raw_query_datafile_mongo tables which are
no longer part of the typed ingestion scope.

Revision ID: a1f3e9b2c8d5
Revises: 2c081eeee9d3
Create Date: 2026-03-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'a1f3e9b2c8d5'
down_revision: Union[str, Sequence[str], None] = '2c081eeee9d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_S = sa.String


def upgrade() -> None:
    """Drop the datafile tables."""
    with op.batch_alter_table('raw_query_datafile_sql', schema=None) as batch_op:
        batch_op.drop_index('ix_raw_query_datafile_sql_query_hash')
        batch_op.drop_index('ix_raw_query_datafile_sql_month_year')
        batch_op.drop_index('ix_raw_query_datafile_sql_host')
        batch_op.drop_index('ix_raw_query_datafile_sql_environment')
        batch_op.drop_index('ix_raw_query_datafile_sql_db_name')

    op.drop_table('raw_query_datafile_sql')

    with op.batch_alter_table('raw_query_datafile_mongo', schema=None) as batch_op:
        batch_op.drop_index('ix_raw_query_datafile_mongo_query_hash')
        batch_op.drop_index('ix_raw_query_datafile_mongo_month_year')
        batch_op.drop_index('ix_raw_query_datafile_mongo_host_mount')
        batch_op.drop_index('ix_raw_query_datafile_mongo_environment')

    op.drop_table('raw_query_datafile_mongo')


def downgrade() -> None:
    """Re-create the datafile tables (rollback path)."""
    op.create_table('raw_query_datafile_mongo',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('query_hash', _S(), nullable=False),
        sa.Column('environment', _S(), nullable=False),
        sa.Column('month_year', _S(), nullable=True),
        sa.Column('host_mount', _S(), nullable=True),
        sa.Column('max_storage_mb', sa.Float(), nullable=True),
        sa.Column('avg_storage_mb', sa.Float(), nullable=True),
        sa.Column('max_storage_free_mb', sa.Float(), nullable=True),
        sa.Column('avg_storage_free_mb', sa.Float(), nullable=True),
        sa.Column('max_storage_free_pct', sa.Float(), nullable=True),
        sa.Column('avg_storage_free_pct', sa.Float(), nullable=True),
        sa.Column('max_storage_used_mb', sa.Float(), nullable=True),
        sa.Column('avg_storage_used_mb', sa.Float(), nullable=True),
        sa.Column('max_used_percent', sa.Float(), nullable=True),
        sa.Column('avg_used_percent', sa.Float(), nullable=True),
        sa.Column('occurrence_count', sa.Integer(), nullable=False),
        sa.Column('first_seen', sa.DateTime(), nullable=False),
        sa.Column('last_seen', sa.DateTime(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('raw_query_datafile_mongo', schema=None) as batch_op:
        batch_op.create_index('ix_raw_query_datafile_mongo_environment', ['environment'], unique=False)
        batch_op.create_index('ix_raw_query_datafile_mongo_host_mount', ['host_mount'], unique=False)
        batch_op.create_index('ix_raw_query_datafile_mongo_month_year', ['month_year'], unique=False)
        batch_op.create_index('ix_raw_query_datafile_mongo_query_hash', ['query_hash'], unique=True)

    op.create_table('raw_query_datafile_sql',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('query_hash', _S(), nullable=False),
        sa.Column('environment', _S(), nullable=False),
        sa.Column('month_year', _S(), nullable=True),
        sa.Column('host', _S(), nullable=True),
        sa.Column('db_name', _S(), nullable=True),
        sa.Column('file_path', _S(), nullable=True),
        sa.Column('updated_at_source', _S(), nullable=True),
        sa.Column('trend', _S(), nullable=True),
        sa.Column('is_up', _S(), nullable=True),
        sa.Column('range_mb', sa.Float(), nullable=True),
        sa.Column('used_pct', sa.Float(), nullable=True),
        sa.Column('used_mb', sa.Float(), nullable=True),
        sa.Column('allocated_mb', sa.Float(), nullable=True),
        sa.Column('free', _S(), nullable=True),
        sa.Column('target_allocation_mb', sa.Float(), nullable=True),
        sa.Column('occurrence_count', sa.Integer(), nullable=False),
        sa.Column('first_seen', sa.DateTime(), nullable=False),
        sa.Column('last_seen', sa.DateTime(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('raw_query_datafile_sql', schema=None) as batch_op:
        batch_op.create_index('ix_raw_query_datafile_sql_db_name', ['db_name'], unique=False)
        batch_op.create_index('ix_raw_query_datafile_sql_environment', ['environment'], unique=False)
        batch_op.create_index('ix_raw_query_datafile_sql_host', ['host'], unique=False)
        batch_op.create_index('ix_raw_query_datafile_sql_month_year', ['month_year'], unique=False)
        batch_op.create_index('ix_raw_query_datafile_sql_query_hash', ['query_hash'], unique=True)
