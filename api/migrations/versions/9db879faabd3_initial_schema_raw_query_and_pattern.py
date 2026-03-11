"""three_table_schema: pattern_label, raw_query, curated_query

Revision ID: 9db879faabd3
Revises:
Create Date: 2026-03-03 17:05:09.233409 (updated 2026-03-12)

Schema:
  pattern_label  — user-managed label library
  raw_query      — every Splunk CSV row (source of truth)
  curated_query  — one row per promoted raw_query, with optional label FK

Note: pattern table was removed; `querytype` gains slow_query_mongo;
      labelsource enum added for pattern_label.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = '9db879faabd3'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema — create the three production tables."""
    # ENUMs (PostgreSQL-specific; created explicitly before tables)
    severitytype = sa.Enum('critical', 'warning', 'info', name='severitytype')
    sourcetype   = sa.Enum('sql', 'mongodb', name='sourcetype')
    envtype      = sa.Enum('prod', 'sat', 'unknown', name='environmenttype')
    querytype    = sa.Enum('slow_query', 'slow_query_mongo', 'blocker', 'deadlock', 'unknown', name='querytype')
    labelsource  = sa.Enum('sql', 'mongodb', 'both', name='labelsource')

    # pattern_label (no FK deps)
    op.create_table(
        'pattern_label',
        sa.Column('id',          sa.Integer(),  nullable=False),
        sa.Column('name',        sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('severity',    severitytype,  nullable=False, server_default='warning'),
        sa.Column('description', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('source',      labelsource,   nullable=False, server_default='both'),
        sa.Column('created_at',  sa.DateTime(), nullable=False),
        sa.Column('updated_at',  sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('pattern_label', schema=None) as batch_op:
        batch_op.create_index('ix_pattern_label_name',     ['name'],     unique=False)
        batch_op.create_index('ix_pattern_label_severity', ['severity'], unique=False)
        batch_op.create_index('ix_pattern_label_source',   ['source'],   unique=False)

    # raw_query (source of truth — no pattern FK)
    op.create_table(
        'raw_query',
        sa.Column('id',               sa.Integer(),  nullable=False),
        sa.Column('query_hash',       sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('time',             sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('source',           sourcetype,    nullable=False),
        sa.Column('host',             sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('db_name',          sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('environment',      envtype,       nullable=False),
        sa.Column('type',             querytype,     nullable=False),
        sa.Column('query_details',    sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('month_year',       sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('occurrence_count', sa.Integer(),  nullable=False),
        sa.Column('first_seen',       sa.DateTime(), nullable=False),
        sa.Column('last_seen',        sa.DateTime(), nullable=False),
        sa.Column('created_at',       sa.DateTime(), nullable=False),
        sa.Column('updated_at',       sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('raw_query', schema=None) as batch_op:
        batch_op.create_index('ix_raw_query_query_hash',  ['query_hash'],  unique=True)
        batch_op.create_index('ix_raw_query_source',      ['source'],      unique=False)
        batch_op.create_index('ix_raw_query_host',        ['host'],        unique=False)
        batch_op.create_index('ix_raw_query_db_name',     ['db_name'],     unique=False)
        batch_op.create_index('ix_raw_query_environment', ['environment'], unique=False)
        batch_op.create_index('ix_raw_query_type',        ['type'],        unique=False)
        batch_op.create_index('ix_raw_query_month_year',  ['month_year'],  unique=False)

    # curated_query (FK → raw_query + pattern_label)
    op.create_table(
        'curated_query',
        sa.Column('id',           sa.Integer(), nullable=False),
        sa.Column('raw_query_id', sa.Integer(), nullable=False),
        sa.Column('label_id',     sa.Integer(), nullable=True),
        sa.Column('notes',        sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('created_at',   sa.DateTime(), nullable=False),
        sa.Column('updated_at',   sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['raw_query_id'], ['raw_query.id']),
        sa.ForeignKeyConstraint(['label_id'],     ['pattern_label.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('curated_query', schema=None) as batch_op:
        batch_op.create_index('uq_curated_query_raw_query_id', ['raw_query_id'], unique=True)
        batch_op.create_index('ix_curated_query_raw_query_id', ['raw_query_id'], unique=False)
        batch_op.create_index('ix_curated_query_label_id',     ['label_id'],     unique=False)


def downgrade() -> None:
    """Downgrade schema — drop all three tables and ENUMs."""
    op.drop_table('curated_query')
    op.drop_table('raw_query')
    op.drop_table('pattern_label')
    sa.Enum(name='labelsource').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='querytype').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='environmenttype').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='sourcetype').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='severitytype').drop(op.get_bind(), checkfirst=True)
        batch_op.drop_index(batch_op.f('ix_pattern_severity'))
        batch_op.drop_index(batch_op.f('ix_pattern_pattern_tag'))
        batch_op.drop_index(batch_op.f('ix_pattern_name'))
        batch_op.drop_index(batch_op.f('ix_pattern_environment'))

    op.drop_table('pattern')
    # ### end Alembic commands ###
