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

from datetime import datetime, timezone
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
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# PatternLabel table  (declared first -- CuratedQuery holds FK to it)
# ---------------------------------------------------------------------------

class PatternLabel(SQLModel, table=True):
    __tablename__ = "pattern_label"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, description="Short human-readable label")
    severity: SeverityType = Field(
        default=SeverityType.warning,
        sa_column=Column(SAString, index=True, nullable=False, default=SeverityType.warning.value),
    )
    description: Optional[str] = Field(default=None)
    source: LabelSource = Field(
        default=LabelSource.both,
        sa_column=Column(SAString, index=True, nullable=False, default=LabelSource.both.value),
    )

    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    curated_queries: list["CuratedQuery"] = Relationship(back_populates="label")


# ---------------------------------------------------------------------------
# CuratedQuery table  (declared before RawQuery -- FK raw_query_id is a fwd ref)
# ---------------------------------------------------------------------------

class CuratedQuery(SQLModel, table=True):
    __tablename__ = "curated_query"

    id: Optional[int] = Field(default=None, primary_key=True)

    # One curated row per raw query (enforced at DB level)
    raw_query_id: int = Field(foreign_key="raw_query.id", unique=True, index=True)

    # Optional label link -- can be set at creation or updated later
    label_id: Optional[int] = Field(default=None, foreign_key="pattern_label.id", index=True)

    notes: Optional[str] = Field(default=None)

    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    # Relationships
    raw_query: Optional["RawQuery"] = Relationship(back_populates="curated_entry")
    label: Optional[PatternLabel] = Relationship(back_populates="curated_queries")


# ---------------------------------------------------------------------------
# RawQuery table
# ---------------------------------------------------------------------------

class RawQuery(SQLModel, table=True):
    __tablename__ = "raw_query"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Deduplication key
    query_hash: str = Field(unique=True, index=True)

    # Core fields extracted from CSV
    time: Optional[str] = Field(default=None)
    source: SourceType = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    host: Optional[str] = Field(default=None, index=True)
    db_name: Optional[str] = Field(default=None, index=True)
    environment: EnvironmentType = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    type: QueryType = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    query_details: Optional[str] = Field(default=None)

    # Derived at ingest -- "YYYY-MM"
    month_year: Optional[str] = Field(default=None, index=True)

    # Type-specific structured metadata (JSON string).
    # Deadlock rows: deadlock_id, pid, is_victim, lockMode, waitresource, etc.
    # Other types: None.
    extra_metadata: Optional[str] = Field(default=None)

    # Occurrence tracking
    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)

    # Timestamps
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    # Back-reference to the single curated entry (if any)
    curated_entry: Optional[CuratedQuery] = Relationship(back_populates="raw_query")


# ---------------------------------------------------------------------------
# User table  (authentication)
# ---------------------------------------------------------------------------

class User(SQLModel, table=True):
    __tablename__ = "user"

    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(unique=True, index=True)
    email: str = Field(unique=True, index=True)
    hashed_password: str
    role: UserRole = Field(
        default=UserRole.viewer,
        sa_column=Column(SAString, nullable=False, default=UserRole.viewer.value),
    )
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=_now)
    last_login: Optional[datetime] = Field(default=None)


# ---------------------------------------------------------------------------
# UploadLog table  — one row per successful CSV upload
# ---------------------------------------------------------------------------

class UploadLog(SQLModel, table=True):
    __tablename__ = "upload_log"

    id:            Optional[int] = Field(default=None, primary_key=True)
    filename:      str           = Field(nullable=False)
    file_type:     Optional[str] = Field(default=None)
    environment:   Optional[str] = Field(default=None, index=True)
    month_year:    Optional[str] = Field(default=None, index=True)
    csv_row_count: int           = Field(nullable=False)
    inserted:      int           = Field(default=0)
    updated:       int           = Field(default=0)
    uploaded_at:   str           = Field(nullable=False)


# ---------------------------------------------------------------------------
# Response / request schemas (not mapped to DB tables)
# ---------------------------------------------------------------------------

# ---- PatternLabel schemas --------------------------------------------------

class PatternLabelRead(SQLModel):
    id: int
    name: str
    severity: SeverityType
    description: Optional[str]
    source: LabelSource
    created_at: datetime
    updated_at: datetime


class PatternLabelCreate(SQLModel):
    name: str
    severity: SeverityType = SeverityType.warning
    description: Optional[str] = None
    source: LabelSource = LabelSource.both


class PatternLabelUpdate(SQLModel):
    name: Optional[str] = None
    severity: Optional[SeverityType] = None
    description: Optional[str] = None
    source: Optional[LabelSource] = None


# ---- CuratedQuery schemas --------------------------------------------------

class CuratedQueryRead(SQLModel):
    """Flat projection: curated row fields + embedded label + raw_query fields."""
    # curated_query fields
    id: int
    raw_query_id: int
    label_id: Optional[int]
    label: Optional[PatternLabelRead]
    notes: Optional[str]
    created_at: datetime
    updated_at: datetime

    # raw_query fields (denormalised for display)
    query_hash: str
    time: Optional[str]
    source: SourceType
    host: Optional[str]
    db_name: Optional[str]
    environment: EnvironmentType
    type: QueryType
    query_details: Optional[str]
    month_year: Optional[str]
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime


class CuratedQueryCreate(SQLModel):
    raw_query_id: int
    label_id: Optional[int] = None
    notes: Optional[str] = None


class CuratedQueryUpdate(SQLModel):
    label_id: Optional[int] = None
    notes: Optional[str] = None


# ---- RawQuery schemas ------------------------------------------------------

class RawQueryRead(SQLModel):
    id: int
    query_hash: str
    time: Optional[str]
    source: SourceType
    host: Optional[str]
    db_name: Optional[str]
    environment: EnvironmentType
    type: QueryType
    query_details: Optional[str]
    month_year: Optional[str]
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime
    created_at: datetime
    updated_at: datetime
    # Injected at query time (None when no curated entry exists)
    curated_id: Optional[int] = None


# ---------------------------------------------------------------------------
# SplQuery table  — SPL Library
# ---------------------------------------------------------------------------

class SplQuery(SQLModel, table=True):
    __tablename__ = "spl_query"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Human-readable name for this SPL (e.g. "SQL Slow Queries – Prod")
    name: str = Field(index=True)

    # Free-form query type tag — not a fixed enum so users can add new types.
    # Seeded defaults: slow_query, slow_query_mongo, blocker, deadlock.
    query_type: str = Field(index=True)

    # prod | sat | both  (not enforced at DB level so future values are possible)
    environment: str = Field(default="both")

    # Optional markdown description / notes
    description: Optional[str] = Field(default=None)

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
    description: Optional[str]
    spl: str
    created_at: datetime
    updated_at: datetime


class SplQueryCreate(SQLModel):
    name: str
    query_type: str
    environment: str = "both"
    description: Optional[str] = None
    spl: str


class SplQueryUpdate(SQLModel):
    name: Optional[str] = None
    query_type: Optional[str] = None
    environment: Optional[str] = None
    description: Optional[str] = None
    spl: Optional[str] = None


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

    id: Optional[int] = Field(default=None, primary_key=True)
    query_hash: str = Field(unique=True, index=True)
    # FK to raw_query.id — set post-ingest by the upload router
    raw_query_id: Optional[int] = Field(default=None, foreign_key="raw_query.id", index=True)

    host: Optional[str] = Field(default=None, index=True)
    db_name: Optional[str] = Field(default=None, index=True)
    environment: str = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    month_year: Optional[str] = Field(default=None, index=True)

    # Timing columns (native CSV)
    creation_time: Optional[str] = Field(default=None)
    last_execution_time: Optional[str] = Field(default=None)
    max_elapsed_time_s: Optional[float] = Field(default=None)
    avg_elapsed_time_s: Optional[float] = Field(default=None)
    total_elapsed_time_s: Optional[float] = Field(default=None)
    total_worker_time_s: Optional[float] = Field(default=None)
    avg_io: Optional[float] = Field(default=None)
    avg_logical_reads: Optional[float] = Field(default=None)
    avg_logical_writes: Optional[float] = Field(default=None)
    execution_count: Optional[int] = Field(default=None)
    query_final: Optional[str] = Field(default=None)

    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class RawQueryBlocker(SQLModel, table=True):
    """blockers*.csv — SQL blocking events with victim/resource/lock details."""
    __tablename__ = "raw_query_blocker"

    id: Optional[int] = Field(default=None, primary_key=True)
    query_hash: str = Field(unique=True, index=True)
    raw_query_id: Optional[int] = Field(default=None, foreign_key="raw_query.id", index=True)

    environment: str = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    month_year: Optional[str] = Field(default=None, index=True)

    currentdbname: Optional[str] = Field(default=None, index=True)
    victims: Optional[str] = Field(default=None)        # space-separated process IDs
    resources: Optional[str] = Field(default=None)      # space-separated PAGE/KEY locks
    lock_modes: Optional[str] = Field(default=None)     # e.g. "IX S"
    count: Optional[int] = Field(default=None)          # occurrences in SPL window
    latest: Optional[str] = Field(default=None)         # latest timestamp in window
    earliest: Optional[str] = Field(default=None)       # earliest timestamp in window
    all_query: Optional[str] = Field(default=None)      # SQL text

    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class RawQueryDeadlock(SQLModel, table=True):
    """deadlocks*.csv — SQL deadlock events (both raw and legacy formats)."""
    __tablename__ = "raw_query_deadlock"

    id: Optional[int] = Field(default=None, primary_key=True)
    query_hash: str = Field(unique=True, index=True)
    raw_query_id: Optional[int] = Field(default=None, foreign_key="raw_query.id", index=True)

    host: Optional[str] = Field(default=None, index=True)
    db_name: Optional[str] = Field(default=None, index=True)
    environment: str = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    month_year: Optional[str] = Field(default=None, index=True)

    event_time: Optional[str] = Field(default=None)
    deadlock_id: Optional[str] = Field(default=None)    # process ID from _raw
    is_victim: Optional[int] = Field(default=None)      # 0 / 1
    lock_mode: Optional[str] = Field(default=None)      # e.g. "S", "IX"
    wait_resource: Optional[str] = Field(default=None)
    wait_time_ms: Optional[int] = Field(default=None)
    transaction_name: Optional[str] = Field(default=None)
    app_host: Optional[str] = Field(default=None)       # application hostname
    sql_text: Optional[str] = Field(default=None)       # cleaned query text
    raw_xml: Optional[str] = Field(default=None)        # original _raw XML (optional)

    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class RawQuerySlowMongo(SQLModel, table=True):
    """mongodbSlowQueries*.csv — MongoDB slow operation events."""
    __tablename__ = "raw_query_slow_mongo"

    id: Optional[int] = Field(default=None, primary_key=True)
    query_hash: str = Field(unique=True, index=True)
    raw_query_id: Optional[int] = Field(default=None, foreign_key="raw_query.id", index=True)

    host: Optional[str] = Field(default=None, index=True)
    db_name: Optional[str] = Field(default=None, index=True)   # extracted from attr.ns
    collection: Optional[str] = Field(default=None, index=True) # extracted from attr.ns
    environment: str = Field(
        sa_column=Column(SAString, index=True, nullable=False),
    )
    month_year: Optional[str] = Field(default=None, index=True)

    event_time: Optional[str] = Field(default=None)     # t.$date
    duration_ms: Optional[int] = Field(default=None)    # attr.durationMillis
    plan_summary: Optional[str] = Field(default=None)   # attr.planSummary
    op_type: Optional[str] = Field(default=None)        # attr.type
    remote_client: Optional[str] = Field(default=None)  # attr.remote
    command_json: Optional[str] = Field(default=None)   # extracted command from _raw

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
    raw_query_id: Optional[int]
    host: Optional[str]
    db_name: Optional[str]
    environment: str
    month_year: Optional[str]
    creation_time: Optional[str]
    last_execution_time: Optional[str]
    max_elapsed_time_s: Optional[float]
    avg_elapsed_time_s: Optional[float]
    total_elapsed_time_s: Optional[float]
    total_worker_time_s: Optional[float]
    avg_io: Optional[float]
    avg_logical_reads: Optional[float]
    avg_logical_writes: Optional[float]
    execution_count: Optional[int]
    query_final: Optional[str]
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime


class RawQueryBlockerRead(SQLModel):
    id: int
    query_hash: str
    raw_query_id: Optional[int]
    environment: str
    month_year: Optional[str]
    currentdbname: Optional[str]
    victims: Optional[str]
    resources: Optional[str]
    lock_modes: Optional[str]
    count: Optional[int]
    latest: Optional[str]
    earliest: Optional[str]
    all_query: Optional[str]
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime


class RawQueryDeadlockRead(SQLModel):
    id: int
    query_hash: str
    raw_query_id: Optional[int]
    host: Optional[str]
    db_name: Optional[str]
    environment: str
    month_year: Optional[str]
    event_time: Optional[str]
    deadlock_id: Optional[str]
    is_victim: Optional[int]
    lock_mode: Optional[str]
    wait_resource: Optional[str]
    wait_time_ms: Optional[int]
    transaction_name: Optional[str]
    app_host: Optional[str]
    sql_text: Optional[str]
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime


class RawQuerySlowMongoRead(SQLModel):
    id: int
    query_hash: str
    raw_query_id: Optional[int]
    host: Optional[str]
    db_name: Optional[str]
    collection: Optional[str]
    environment: str
    month_year: Optional[str]
    event_time: Optional[str]
    duration_ms: Optional[int]
    plan_summary: Optional[str]
    op_type: Optional[str]
    remote_client: Optional[str]
    command_json: Optional[str]
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime



