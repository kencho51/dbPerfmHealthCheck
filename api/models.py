"""
SQLModel table definitions  three-table schema:

  pattern_label  : user-managed label library (name, severity, description)
  curated_query  : one row per "promoted" raw_query; holds label FK + notes
  raw_query      : every row from Splunk CSVs (unchanged source of truth)

Declaration order matters:
  PatternLabel -> CuratedQuery -> RawQuery
  (CuratedQuery has FK -> pattern_label; RawQuery back-ref -> curated_query)

NOTE: `from __future__ import annotations` is intentionally NOT imported here.
SQLModel/SQLAlchemy evaluates Relationship type annotations at class-definition
time; string forward-refs ("RawQuery") are used only where required.
"""

from datetime import UTC, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import Column
from sqlalchemy import String as SAString
from sqlmodel import Field, Relationship, SQLModel

# ---------------------------------------------------------------------------
# Shared enums
# ---------------------------------------------------------------------------


class SourceType(str, Enum):
    sql = "sql"
    mongodb = "mongodb"


class QueryType(str, Enum):
    slow_query = "slow_query"
    slow_query_mongo = "slow_query_mongo"
    blocker = "blocker"
    deadlock = "deadlock"
    unknown = "unknown"


class EnvironmentType(str, Enum):
    prod = "prod"
    sat = "sat"
    unknown = "unknown"


class SeverityType(str, Enum):
    critical = "critical"
    warning = "warning"
    info = "info"


class LabelSource(str, Enum):
    sql = "sql"
    mongodb = "mongodb"
    both = "both"


class UserRole(str, Enum):
    admin = "admin"
    viewer = "viewer"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(tz=UTC)


# ---------------------------------------------------------------------------
# PatternLabel table  (declared first -- CuratedQuery holds FK to it)
# ---------------------------------------------------------------------------


class PatternLabel(SQLModel, table=True):
    __tablename__ = "pattern_label"

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True, description="Short human-readable label")
    severity: SeverityType = Field(
        default=SeverityType.warning,
        sa_column=Column(SAString, index=True, nullable=False, default=SeverityType.warning.value),
    )
    description: str | None = Field(default=None)
    source: LabelSource = Field(
        default=LabelSource.both,
        sa_column=Column(SAString, index=True, nullable=False, default=LabelSource.both.value),
    )

    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    curated_queries: list["CuratedQuery"] = Relationship(back_populates="label")  # noqa: UP037


# ---------------------------------------------------------------------------
# CuratedQuery table  (declared before RawQuery -- FK raw_query_id is a fwd ref)
# ---------------------------------------------------------------------------


class CuratedQuery(SQLModel, table=True):
    __tablename__ = "curated_query"

    id: int | None = Field(default=None, primary_key=True)

    # One curated row per raw query (enforced at DB level)
    raw_query_id: int = Field(foreign_key="raw_query.id", unique=True, index=True)

    # Optional label link -- can be set at creation or updated later
    label_id: int | None = Field(default=None, foreign_key="pattern_label.id", index=True)

    notes: str | None = Field(default=None)

    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    # Relationships
    raw_query: Optional["RawQuery"] = Relationship(back_populates="curated_entry")  # noqa: UP045, UP037
    label: PatternLabel | None = Relationship(back_populates="curated_queries")


# ---------------------------------------------------------------------------
# RawQuery table
# ---------------------------------------------------------------------------


class RawQuery(SQLModel, table=True):
    __tablename__ = "raw_query"

    id: int | None = Field(default=None, primary_key=True)

    # Deduplication key
    query_hash: str = Field(unique=True, index=True)

    # Core fields extracted from CSV
    time: str | None = Field(default=None)
    source: SourceType = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    host: str | None = Field(default=None, index=True)
    db_name: str | None = Field(default=None, index=True)
    environment: EnvironmentType = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    type: QueryType = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    query_details: str | None = Field(default=None, index=True)

    # Derived at ingest -- "YYYY-MM"
    month_year: str | None = Field(default=None, index=True)

    # Type-specific structured metadata (JSON string).
    # Deadlock rows: deadlock_id, pid, is_victim, lockMode, waitresource, etc.
    # Other types: None.
    extra_metadata: str | None = Field(default=None)

    # Occurrence tracking
    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)

    # Timestamps
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    # Back-reference to the single curated entry (if any)
    curated_entry: CuratedQuery | None = Relationship(back_populates="raw_query")


# ---------------------------------------------------------------------------
# User table  (authentication)
# ---------------------------------------------------------------------------


class User(SQLModel, table=True):
    __tablename__ = "user"

    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(unique=True, index=True)
    email: str = Field(unique=True, index=True)
    hashed_password: str
    role: UserRole = Field(
        default=UserRole.viewer,
        sa_column=Column(SAString, nullable=False, default=UserRole.viewer.value),
    )
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=_now)
    last_login: datetime | None = Field(default=None)


# ---------------------------------------------------------------------------
# UploadLog table  — one row per successful CSV upload
# ---------------------------------------------------------------------------


class UploadLog(SQLModel, table=True):
    __tablename__ = "upload_log"

    id: int | None = Field(default=None, primary_key=True)
    filename: str = Field(nullable=False, index=True)
    file_type: str | None = Field(default=None, index=True)
    environment: str | None = Field(default=None, index=True)
    month_year: str | None = Field(default=None, index=True)
    csv_row_count: int = Field(nullable=False)
    inserted: int = Field(default=0)
    updated: int = Field(default=0)
    uploaded_at: str = Field(nullable=False, index=True)


# ---------------------------------------------------------------------------
# Response / request schemas (not mapped to DB tables)
# ---------------------------------------------------------------------------

# ---- PatternLabel schemas --------------------------------------------------


class PatternLabelRead(SQLModel):
    id: int
    name: str
    severity: SeverityType
    description: str | None
    source: LabelSource
    created_at: datetime
    updated_at: datetime


class PatternLabelCreate(SQLModel):
    name: str
    severity: SeverityType = SeverityType.warning
    description: str | None = None
    source: LabelSource = LabelSource.both


class PatternLabelUpdate(SQLModel):
    name: str | None = None
    severity: SeverityType | None = None
    description: str | None = None
    source: LabelSource | None = None


# ---- CuratedQuery schemas --------------------------------------------------


class CuratedQueryRead(SQLModel):
    """Flat projection: curated row fields + embedded label + raw_query fields."""

    # curated_query fields
    id: int
    raw_query_id: int
    label_id: int | None
    label: PatternLabelRead | None
    notes: str | None
    created_at: datetime
    updated_at: datetime

    # raw_query fields (denormalised for display)
    query_hash: str
    time: str | None
    source: SourceType
    host: str | None
    db_name: str | None
    environment: EnvironmentType
    type: QueryType
    query_details: str | None
    month_year: str | None
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime


class CuratedQueryCreate(SQLModel):
    raw_query_id: int
    label_id: int | None = None
    notes: str | None = None


class CuratedQueryUpdate(SQLModel):
    label_id: int | None = None
    notes: str | None = None


# ---- RawQuery schemas ------------------------------------------------------


class RawQueryRead(SQLModel):
    id: int
    query_hash: str
    time: str | None
    source: SourceType
    host: str | None
    db_name: str | None
    environment: EnvironmentType
    type: QueryType
    query_details: str | None
    month_year: str | None
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime
    created_at: datetime
    updated_at: datetime
    # Injected at query time (None when no curated entry exists)
    curated_id: int | None = None


# ---------------------------------------------------------------------------
# SplQuery table  — SPL Library
# ---------------------------------------------------------------------------


class SplQuery(SQLModel, table=True):
    __tablename__ = "spl_query"

    id: int | None = Field(default=None, primary_key=True)

    # Human-readable name for this SPL (e.g. "SQL Slow Queries – Prod")
    name: str = Field(index=True)

    # Free-form query type tag — not a fixed enum so users can add new types.
    # Seeded defaults: slow_query, slow_query_mongo, blocker, deadlock.
    query_type: str = Field(index=True)

    # prod | sat | both  (not enforced at DB level so future values are possible)
    environment: str = Field(default="both")

    # Optional markdown description / notes
    description: str | None = Field(default=None)

    # The actual Splunk Processing Language query
    spl: str = Field(sa_column=Column("spl", SAString, nullable=False))

    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


# ---- SplQuery schemas ------------------------------------------------------


class SplQueryRead(SQLModel):
    id: int
    name: str
    query_type: str
    environment: str
    description: str | None
    spl: str
    created_at: datetime
    updated_at: datetime


class SplQueryCreate(SQLModel):
    name: str
    query_type: str
    environment: str = "both"
    description: str | None = None
    spl: str


class SplQueryUpdate(SQLModel):
    name: str | None = None
    query_type: str | None = None
    environment: str | None = None
    description: str | None = None
    spl: str | None = None


# ---------------------------------------------------------------------------
# Type-specific raw_query_* tables
# ---------------------------------------------------------------------------
# These tables store every native CSV column for each file type.
# They are written in parallel with `raw_query` during upload so that:
#   - curated_query / labelling workflow keeps using raw_query (unchanged)
#   - DuckDB analytics can query full-fidelity columns per type
# ---------------------------------------------------------------------------


class RawQuerySlowSql(SQLModel, table=True):
    """maxElapsedQueries*.csv — SQL slow queries with all timing/IO metrics."""

    __tablename__ = "raw_query_slow_sql"

    id: int | None = Field(default=None, primary_key=True)
    query_hash: str = Field(unique=True, index=True)
    # FK to raw_query.id — set post-ingest by the upload router
    raw_query_id: int | None = Field(default=None, foreign_key="raw_query.id", index=True)

    host: str | None = Field(default=None, index=True)
    db_name: str | None = Field(default=None, index=True)
    environment: str = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    month_year: str | None = Field(default=None, index=True)

    # Timing columns (native CSV)
    creation_time: str | None = Field(default=None)
    last_execution_time: str | None = Field(default=None)
    max_elapsed_time_s: float | None = Field(default=None)
    avg_elapsed_time_s: float | None = Field(default=None)
    total_elapsed_time_s: float | None = Field(default=None)
    total_worker_time_s: float | None = Field(default=None)
    avg_io: float | None = Field(default=None)
    avg_logical_reads: float | None = Field(default=None)
    avg_logical_writes: float | None = Field(default=None)
    execution_count: int | None = Field(default=None)
    query_final: str | None = Field(default=None)

    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class RawQueryBlocker(SQLModel, table=True):
    """blockers*.csv — SQL blocking events with victim/resource/lock details."""

    __tablename__ = "raw_query_blocker"

    id: int | None = Field(default=None, primary_key=True)
    query_hash: str = Field(unique=True, index=True)
    raw_query_id: int | None = Field(default=None, foreign_key="raw_query.id", index=True)

    environment: str = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    month_year: str | None = Field(default=None, index=True)

    currentdbname: str | None = Field(default=None, index=True)
    victims: str | None = Field(default=None)  # space-separated process IDs
    resources: str | None = Field(default=None)  # space-separated PAGE/KEY locks
    lock_modes: str | None = Field(default=None)  # e.g. "IX S"
    count: int | None = Field(default=None)  # occurrences in SPL window
    latest: str | None = Field(default=None)  # latest timestamp in window
    earliest: str | None = Field(default=None)  # earliest timestamp in window
    all_query: str | None = Field(default=None)  # SQL text

    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class RawQueryDeadlock(SQLModel, table=True):
    """deadlocks*.csv — SQL deadlock events (both raw and legacy formats)."""

    __tablename__ = "raw_query_deadlock"

    id: int | None = Field(default=None, primary_key=True)
    query_hash: str = Field(unique=True, index=True)
    raw_query_id: int | None = Field(default=None, foreign_key="raw_query.id", index=True)

    host: str | None = Field(default=None, index=True)
    db_name: str | None = Field(default=None, index=True)
    environment: str = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    month_year: str | None = Field(default=None, index=True)

    event_time: str | None = Field(default=None)
    deadlock_id: str | None = Field(default=None)  # process ID from _raw
    is_victim: int | None = Field(default=None)  # 0 / 1
    lock_mode: str | None = Field(default=None)  # e.g. "S", "IX"
    wait_resource: str | None = Field(default=None)
    wait_time_ms: int | None = Field(default=None)
    transaction_name: str | None = Field(default=None)
    app_host: str | None = Field(default=None)  # application hostname
    sql_text: str | None = Field(default=None)  # cleaned query text
    raw_xml: str | None = Field(default=None)  # original _raw XML (optional)

    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class RawQuerySlowMongo(SQLModel, table=True):
    """mongodbSlowQueries*.csv — MongoDB slow operation events."""

    __tablename__ = "raw_query_slow_mongo"

    id: int | None = Field(default=None, primary_key=True)
    query_hash: str = Field(unique=True, index=True)
    raw_query_id: int | None = Field(default=None, foreign_key="raw_query.id", index=True)

    host: str | None = Field(default=None, index=True)
    db_name: str | None = Field(default=None, index=True)  # extracted from attr.ns
    collection: str | None = Field(default=None, index=True)  # extracted from attr.ns
    environment: str = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    month_year: str | None = Field(default=None, index=True)

    event_time: str | None = Field(default=None)  # t.$date
    duration_ms: int | None = Field(default=None)  # attr.durationMillis
    plan_summary: str | None = Field(default=None)  # attr.planSummary
    op_type: str | None = Field(default=None)  # attr.type
    remote_client: str | None = Field(default=None)  # attr.remote
    command_json: str | None = Field(default=None)  # extracted command from _raw

    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


# ---------------------------------------------------------------------------
# Read schemas for type-specific tables
# ---------------------------------------------------------------------------


class RawQuerySlowSqlRead(SQLModel):
    id: int
    query_hash: str
    raw_query_id: int | None
    host: str | None
    db_name: str | None
    environment: str
    month_year: str | None
    creation_time: str | None
    last_execution_time: str | None
    max_elapsed_time_s: float | None
    avg_elapsed_time_s: float | None
    total_elapsed_time_s: float | None
    total_worker_time_s: float | None
    avg_io: float | None
    avg_logical_reads: float | None
    avg_logical_writes: float | None
    execution_count: int | None
    query_final: str | None
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime


class RawQueryBlockerRead(SQLModel):
    id: int
    query_hash: str
    raw_query_id: int | None
    environment: str
    month_year: str | None
    currentdbname: str | None
    victims: str | None
    resources: str | None
    lock_modes: str | None
    count: int | None
    latest: str | None
    earliest: str | None
    all_query: str | None
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime


class RawQueryDeadlockRead(SQLModel):
    id: int
    query_hash: str
    raw_query_id: int | None
    host: str | None
    db_name: str | None
    environment: str
    month_year: str | None
    event_time: str | None
    deadlock_id: str | None
    is_victim: int | None
    lock_mode: str | None
    wait_resource: str | None
    wait_time_ms: int | None
    transaction_name: str | None
    app_host: str | None
    sql_text: str | None
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime


class RawQuerySlowMongoRead(SQLModel):
    id: int
    query_hash: str
    raw_query_id: int | None
    host: str | None
    db_name: str | None
    collection: str | None
    environment: str
    month_year: str | None
    event_time: str | None
    duration_ms: int | None
    plan_summary: str | None
    op_type: str | None
    remote_client: str | None
    command_json: str | None
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime
