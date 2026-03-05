"""
SQLModel table definitions for the two-table schema:
  - RawQuery  : every row extracted from Splunk CSVs (source of truth for analytics)
  - Pattern   : curated / auto-detected recurring or suspicious patterns

NOTE: `from __future__ import annotations` is intentionally NOT imported here.
SQLModel/SQLAlchemy needs to evaluate relationship type annotations at class-
definition time (not lazily), so that ForwardRef('RawQuery') can be resolved
by the mapper during initialisation.  String forward refs ("RawQuery") are used
only for the one cross-reference that must be forward-declared.
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


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Pattern table (declared first — RawQuery holds FK to it)
# ---------------------------------------------------------------------------

class Pattern(SQLModel, table=True):
    __tablename__ = "pattern"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Identity
    name: str = Field(index=True, description="Short human-readable label")
    description: Optional[str] = Field(default=None)
    pattern_tag: str = Field(
        index=True,
        description="Category key, e.g. 'missing_index', 'bulk_delete', 'deadlock_hotspot'",
    )
    severity: SeverityType = Field(default=SeverityType.warning, index=True)

    # Representative raw query
    example_query_hash: Optional[str] = Field(
        default=None,
        description="query_hash of the canonical RawQuery example",
    )

    # Inherited context (denormalised for quick filtering without a join)
    source: Optional[SourceType] = Field(default=None, index=True)
    environment: Optional[EnvironmentType] = Field(default=None, index=True)
    type: Optional[QueryType] = Field(default=None, index=True)

    # Observation window (denormalised; recomputed when raw rows are linked)
    first_seen: Optional[datetime] = Field(default=None)
    last_seen: Optional[datetime] = Field(default=None)
    total_occurrences: int = Field(default=0)

    # Curation notes
    notes: Optional[str] = Field(default=None)

    # Timestamps
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    # Relationship back to raw queries
    raw_queries: list["RawQuery"] = Relationship(back_populates="pattern")


# ---------------------------------------------------------------------------
# RawQuery table
# ---------------------------------------------------------------------------

class RawQuery(SQLModel, table=True):
    __tablename__ = "raw_query"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Deduplication key — MD5 of (source + host + db_name + environment + type + normalised query)
    query_hash: str = Field(unique=True, index=True)

    # Core fields extracted from CSV
    time: Optional[str] = Field(default=None)
    source: SourceType = Field(index=True)
    host: Optional[str] = Field(default=None, index=True)
    db_name: Optional[str] = Field(default=None, index=True)
    environment: EnvironmentType = Field(index=True)
    type: QueryType = Field(index=True)
    query_details: Optional[str] = Field(default=None)

    # Derived at ingest — "YYYY-MM" — avoids computing in GROUP BY queries
    month_year: Optional[str] = Field(default=None, index=True)

    # Occurrence tracking (dedup merges repeated uploads into these counters)
    occurrence_count: int = Field(default=1)
    first_seen: datetime = Field(default_factory=_now)
    last_seen: datetime = Field(default_factory=_now)

    # Link to curated pattern (optional; set when a DBA promotes this query)
    pattern_id: Optional[int] = Field(default=None, foreign_key="pattern.id", index=True)
    pattern: Optional[Pattern] = Relationship(back_populates="raw_queries")

    # Timestamps
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


# ---------------------------------------------------------------------------
# Response / request schemas (not mapped to DB tables)
# ---------------------------------------------------------------------------

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
    pattern_id: Optional[int]
    created_at: datetime
    updated_at: datetime


class PatternRead(SQLModel):
    id: int
    name: str
    description: Optional[str]
    pattern_tag: str
    severity: SeverityType
    example_query_hash: Optional[str]
    source: Optional[SourceType]
    environment: Optional[EnvironmentType]
    type: Optional[QueryType]
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    total_occurrences: int
    notes: Optional[str]
    created_at: datetime
    updated_at: datetime


class PatternCreate(SQLModel):
    name: str
    pattern_tag: Optional[str] = None
    severity: SeverityType = SeverityType.warning
    description: Optional[str] = None
    example_query_hash: Optional[str] = None
    notes: Optional[str] = None


class PatternUpdate(SQLModel):
    name: Optional[str] = None
    description: Optional[str] = None
    pattern_tag: Optional[str] = None
    severity: Optional[SeverityType] = None
    notes: Optional[str] = None
