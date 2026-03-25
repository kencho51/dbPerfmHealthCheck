# Raw Query Tables — Per-Type Ingestion Design

## Problem

The current `raw_query` table is a **lowest-common-denominator** schema:

```
id, query_hash, time, source, host, db_name, environment, type,
query_details, month_year, extra_metadata, occurrence_count,
first_seen, last_seen, created_at, updated_at
```

Each CSV type has **distinct, important columns** that are being silently dropped:

| CSV Type | Dropped columns |
|---|---|
| `maxElapsedQueries*` (SQL slow) | `max_elapsed_time_s`, `avg_elapsed_time_s`, `total_elapsed_time_s`, `total_worker_time_s`, `avg_io`, `avg_logical_reads`, `avg_logical_writes`, `execution_count`, `creation_time`, `last_execution_time` |
| `blockers*` | `victims`, `resources`, `lock_modes`, `count`, `latest`, `earliest`, `currentdbname` |
| `deadlocks*` | `lockMode`, `transactionname`, `victim`, `waittime`, `waitresource`, `lockTimeout` (partially saved to `extra_metadata` JSON — lossy) |
| `mongodbSlowQueries*` | `attr.ns`, `attr.durationMillis`, `attr.planSummary`, `attr.type`, `attr.remote`, `t.$date` |

---

## Proposed Solution: `raw_query_*` per-type tables

Replace the single `raw_query` table with **4 type-specific tables**, each preserving all native CSV columns, plus shared bookkeeping columns.

### Shared columns (all tables)
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `query_hash` | TEXT UNIQUE | MD5 dedup key |
| `environment` | TEXT | `prod` / `sat` |
| `month_year` | TEXT | `YYYY-MM` derived from date |
| `occurrence_count` | INTEGER | Upsert counter |
| `first_seen` | DATETIME | |
| `last_seen` | DATETIME | |
| `created_at` | DATETIME | |
| `updated_at` | DATETIME | |

---

### Table 1 — `raw_query_slow_sql`

Source files: `maxElapsedQueries*.csv`

CSV columns:
```
"creation_time", "last_execution_time", host, "db_name",
"max_elapsed_time_s", "avg_elapsed_time_s", "total_elapsed_time_s",
"total_worker_time_s", "avg_io", "avg_logical_reads", "avg_logical_writes",
"execution_count", "query_final"
```

Table columns:
```sql
CREATE TABLE raw_query_slow_sql (
    id                  INTEGER PRIMARY KEY,
    query_hash          TEXT UNIQUE NOT NULL,
    host                TEXT,
    db_name             TEXT,
    environment         TEXT NOT NULL,            -- prod | sat
    month_year          TEXT,                     -- YYYY-MM
    creation_time       TEXT,
    last_execution_time TEXT,
    max_elapsed_time_s  REAL,
    avg_elapsed_time_s  REAL,
    total_elapsed_time_s REAL,
    total_worker_time_s  REAL,
    avg_io              REAL,
    avg_logical_reads   REAL,
    avg_logical_writes  REAL,
    execution_count     INTEGER,
    query_final         TEXT,
    occurrence_count    INTEGER DEFAULT 1,
    first_seen          DATETIME NOT NULL,
    last_seen           DATETIME NOT NULL,
    created_at          DATETIME NOT NULL,
    updated_at          DATETIME NOT NULL
);
```

Dedup key hash inputs: `host | db_name | environment | query_final`

---

### Table 2 — `raw_query_blocker`

Source files: `blockers*.csv`

CSV columns:
```
currentdbname, victims, resources, "lock_modes", count, latest, earliest, "all_query"
```

Table columns:
```sql
CREATE TABLE raw_query_blocker (
    id               INTEGER PRIMARY KEY,
    query_hash       TEXT UNIQUE NOT NULL,
    environment      TEXT NOT NULL,
    month_year       TEXT,
    currentdbname    TEXT,
    victims          TEXT,                        -- space-separated process IDs
    resources        TEXT,                        -- space-separated PAGE/KEY locks
    lock_modes       TEXT,                        -- e.g. "IX S"
    count            INTEGER,                     -- number of occurrences in window
    latest           TEXT,                        -- latest timestamp in window
    earliest         TEXT,                        -- earliest timestamp in window
    all_query        TEXT,                        -- the SQL text
    occurrence_count INTEGER DEFAULT 1,
    first_seen       DATETIME NOT NULL,
    last_seen        DATETIME NOT NULL,
    created_at       DATETIME NOT NULL,
    updated_at       DATETIME NOT NULL
);
```

Dedup key hash inputs: `environment | currentdbname | lock_modes | all_query`

---

### Table 3 — `raw_query_deadlock`

Source files: `deadlocks*.csv` (both raw and legacy formats)

CSV columns (legacy aggregated format):
```
currentdbname, victims, resources, "lock_modes", count, latest, earliest, "all_query"
```

CSV columns (raw SPL format):
```
_time, host, id, lockMode, transactionname, victim, waittime, _raw
```

Table columns:
```sql
CREATE TABLE raw_query_deadlock (
    id               INTEGER PRIMARY KEY,
    query_hash       TEXT UNIQUE NOT NULL,
    environment      TEXT NOT NULL,
    month_year       TEXT,
    host             TEXT,
    db_name          TEXT,
    event_time       TEXT,
    deadlock_id      TEXT,                        -- process ID from _raw
    is_victim        INTEGER,                     -- 0/1
    lock_mode        TEXT,                        -- e.g. "S", "IX"
    wait_resource    TEXT,
    wait_time_ms     INTEGER,
    transaction_name TEXT,
    app_host         TEXT,                        -- application hostname
    sql_text         TEXT,                        -- cleaned query text
    raw_xml          TEXT,                        -- original _raw XML (optional)
    occurrence_count INTEGER DEFAULT 1,
    first_seen       DATETIME NOT NULL,
    last_seen        DATETIME NOT NULL,
    created_at       DATETIME NOT NULL,
    updated_at       DATETIME NOT NULL
);
```

Dedup key hash inputs: `host | db_name | environment | sql_text | lock_mode`

---

### Table 4 — `raw_query_slow_mongo`

Source files: `mongodbSlowQueries*.csv`

CSV columns:
```
host, t.$date, attr.ns, attr.durationMillis, attr.planSummary,
attr.type, attr.remote, _raw
```

Table columns:
```sql
CREATE TABLE raw_query_slow_mongo (
    id                  INTEGER PRIMARY KEY,
    query_hash          TEXT UNIQUE NOT NULL,
    host                TEXT,
    db_name             TEXT,                     -- extracted from attr.ns (before ".")
    collection          TEXT,                     -- extracted from attr.ns (after ".")
    environment         TEXT NOT NULL,
    month_year          TEXT,
    event_time          TEXT,                     -- t.$date
    duration_ms         INTEGER,                  -- attr.durationMillis
    plan_summary        TEXT,                     -- attr.planSummary
    op_type             TEXT,                     -- attr.type (query/update/etc.)
    remote_client       TEXT,                     -- attr.remote
    command_json        TEXT,                     -- extracted command from _raw
    occurrence_count    INTEGER DEFAULT 1,
    first_seen          DATETIME NOT NULL,
    last_seen           DATETIME NOT NULL,
    created_at          DATETIME NOT NULL,
    updated_at          DATETIME NOT NULL
);
```

Dedup key hash inputs: `host | db_name | environment | op_type | command_json`

---

## Migration strategy

### Option A — Keep `raw_query`, add new tables (Recommended)

Keep the existing `raw_query` table as the unified **analytics/curated** anchor. Add the 6 type-specific tables alongside it.

**Benefits:**
- `curated_query` FK (`raw_query_id`) stays intact — no migration of curated data.
- Existing API endpoints (`/api/queries`, `/api/curated`) continue working.
- New tables added via new Alembic migration — zero downtime.

**How it works:**
- Upload pipeline writes to **both** `raw_query` (for curated/labelling workflow) AND the appropriate `raw_query_*` table (for full-fidelity analytics).
- DuckDB analytics queries use the `raw_query_*` tables for metrics (elapsed time, lock count, duration, etc.).
- The `raw_query` table drops `extra_metadata` (or keeps it as a convenience cache).

### Option B — Replace `raw_query` with type-specific tables

Drop `raw_query`, make `curated_query` point to whichever type-specific table it needs via a polymorphic FK (e.g., `raw_table_name TEXT`, `raw_row_id INTEGER`).

**Drawbacks:** Breaks all existing API endpoints, requires complex polymorphic JOIN logic.

---

## Implementation plan (Option A)

### Step 1 — Add new SQLModel table classes to `models.py`

Add 4 new `SQLModel` table classes (`RawQuerySlowSql`, `RawQueryBlocker`, `RawQueryDeadlock`, `RawQuerySlowMongo`).

### Step 2 — Alembic migration

```bash
cd api
alembic revision --autogenerate -m "add_raw_query_typed_tables"
alembic upgrade head
```

### Step 3 — Extend `extractor.py`

Add a `extract_typed_rows(file_path)` function per type that returns the **full native columns** (not just the 7-column normalised dict).

```python
def extract_slow_sql_rows(file_path: Path) -> list[dict]:
    """Returns full columns: host, db_name, creation_time, last_execution_time,
       max_elapsed_time_s, avg_elapsed_time_s, ..., query_final"""
    ...

def extract_blocker_rows(file_path: Path) -> list[dict]:
    """Returns: currentdbname, victims, resources, lock_modes, count,
       latest, earliest, all_query"""
    ...
```

### Step 4 — Extend `ingestor.py`

Add `ingest_typed_rows(rows, table_class)` that does the same MD5-dedup upsert pattern as the existing `ingest_rows()` but targets the type-specific table.

### Step 5 — Update `upload.py`

After the existing ingest, also call `ingest_typed_rows()` for the appropriate table type.

### Step 6 — DuckDB analytics queries

Update `analytics_db.py` to query `raw_query_slow_sql` for elapsed-time histograms, `raw_query_blocker` for lock contention analysis, etc.

---

## Deduplication hash design

Each table uses an MD5 hash of its **semantically meaningful columns** (not time-based columns, since the same query may appear across multiple report windows).

| Table | Hash inputs |
|---|---|
| `raw_query_slow_sql` | `host \| db_name \| environment \| query_final` |
| `raw_query_blocker` | `environment \| currentdbname \| lock_modes \| all_query` |
| `raw_query_deadlock` | `host \| db_name \| environment \| sql_text \| lock_mode` |
| `raw_query_slow_mongo` | `host \| db_name \| environment \| op_type \| command_json` |

---

## File naming → table routing

```python
def detect_typed_table(filename: str) -> str:
    name = filename.lower()
    if "maxelapsed" in name:              return "slow_sql"
    if "blocker" in name:                  return "blocker"
    if "deadlock" in name:                 return "deadlock"
    if "mongodbslowqueries" in name:       return "slow_mongo"
    return "unknown"
```

---

## Summary

| File pattern | Current fate | After this change |
|---|---|---|
| `maxElapsed*.csv` | 7 cols stored, 9 dropped | All 13 cols in `raw_query_slow_sql` |
| `blockers*.csv` | 7 cols stored, 1 dropped | All 8 cols in `raw_query_blocker` |
| `deadlocks*.csv` | 7 cols + partial `extra_metadata` | All cols in `raw_query_deadlock` |
| `mongodbSlowQueries*.csv` | 4 cols stored, many dropped | All cols in `raw_query_slow_mongo` |

