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
    severity: SeverityType = Field(default=SeverityType.warning, index=True)
    description: Optional[str] = Field(default=None)
    source: LabelSource = Field(default=LabelSource.both, index=True)

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
    source: SourceType = Field(index=True)
    host: Optional[str] = Field(default=None, index=True)
    db_name: Optional[str] = Field(default=None, index=True)
    environment: EnvironmentType = Field(index=True)
    type: QueryType = Field(index=True)
    query_details: Optional[str] = Field(default=None)

    # Derived at ingest -- "YYYY-MM"
    month_year: Optional[str] = Field(default=None, index=True)

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
