# Neon PostgreSQL Implementation Guide

> **Branch**: `migrate-to-neon-psql-db`
> **Status**: Schema applied ✅ | App role configured ✅ | All SQL via HTTPS port 443 ✅

---

## Overview

The `dbPerfmHealthCheck` FastAPI backend runs entirely over **Neon's HTTPS REST API**.
The corporate proxy resets PostgreSQL wire-protocol SSL on port 5432, so `asyncpg`
and `psycopg2` are **not used at runtime**. All SQL statements — SELECT, INSERT,
UPDATE, DELETE — are sent as JSON payloads over HTTPS port 443.

```
Next.js (web/)
    │  HTTP only (no direct DB access)
    ▼
FastAPI (api/)
    │  NeonSession.exec/execute() → urllib.request (HTTPS POST)
    ▼
Neon REST API  →  POST /api/v2/projects/{id}/query   (port 443)
    ▼
Neon PostgreSQL (perfmdb)
    └── Branch: production  →  ep-rough-morning-a1v4c224
```

---

## Neon Project Details

| Field | Value |
|---|---|
| Project name | `hkjc-db-perfm` |
| Project ID | `cold-union-77928175` |
| Region | `aws-ap-southeast-1` |
| Organisation ID | `org-bitter-tree-45878282` |

### Branches & Endpoints

| Branch | Branch ID | Endpoint ID | Purpose |
|---|---|---|---|
| `production` | `br-icy-night-a14gn0lz` | `ep-rough-morning-a1v4c224` | Stable data |
| `develop` | `br-floral-feather-a1ns5yhw` | `ep-orange-meadow-a1p2p3mi` | Safe to reset |

### Database

| Field | Value |
|---|---|
| Database | `perfmdb` |
| DDL owner | `neondb_owner` (used for migrations) |
| App runtime role | `perfmdb_owner` (SELECT/INSERT/UPDATE/DELETE only) |

Connection uses the **pooler endpoint** (`*-pooler.ap-southeast-1.aws.neon.tech`) for connection multiplexing.

---

## Environment Variables (`api/.env`)

```dotenv
# App runtime connection — perfmdb_owner (DML only, not DDL)
DATABASE_URL=postgresql://perfmdb_owner:<password>@ep-rough-morning-a1v4c224-pooler.ap-southeast-1.aws.neon.tech/perfmdb?sslmode=require&channel_binding=require

# PG* vars for psql CLI / tooling
PGHOST=ep-rough-morning-a1v4c224-pooler.ap-southeast-1.aws.neon.tech
PGDATABASE=perfmdb
PGUSER=perfmdb_owner
PGPASSWORD=<password>

# Neon REST API (HTTPS port 443 — bypasses corporate proxy port 5432 block)
NEON_ORG_ID=org-bitter-tree-45878282
NEON_API_KEY=<api_key>

# Branch labels (informational)
NEON_DEV_BRANCH=develop
NEON_PROD_BRANCH=production
```

> `api/.env` is gitignored. Never commit credentials.

### Role Separation

| Variable | Role | Why |
|---|---|---|
| `DATABASE_URL` | `perfmdb_owner` | App runtime — least-privilege DML |
| `_apply_migration.py` | `neondb_owner` | DDL requires database owner |
| Alembic CLI | `neondb_owner` (via `DATABASE_URL` swap) | `CREATE TABLE`, `ALTER` need owner |

---

## Driver Stack

```
  FastAPI runtime  →  NeonHTTPSession   (urllib.request HTTPS port 443)
  Alembic CLI      →  psycopg2-binary   (postgresql+psycopg2://, needs port 5432)
  Schema scripts   →  urllib.request    (HTTPS port 443 — no CLI needed)
```

`asyncpg` is **not used**.  All runtime SQL goes through `api/neon_http.py`.

---

## Key Files Changed

### `api/database.py`

Provides `NeonSession` — the sole session abstraction used by all FastAPI routers.
No SQLAlchemy engine or connection pool is created at startup.

```python
class NeonSession:
    """Routes all SQL through NeonHTTPSession (HTTPS port 443)."""

    def add(self, obj)         → queues INSERT/UPDATE for commit()
    async def delete(self, obj)    → queues DELETE for commit()
    async def refresh(self, obj)   → no-op (object complete after INSERT RETURNING)
    async def commit(self)         → flushes pending ops via HTTPS
    async def exec(self, stmt)     → SELECT wrapper → _NeonResult
    async def execute(self, stmt)  → DML wrapper → _NeonExecResult
    async def get(self, model, pk) → model instance or None

Psycopg2Session = NeonSession  # backward-compat alias
```

Public generators: `get_session()` (FastAPI Depends), `open_session()` (context manager for scripts/tests).

### `api/neon_http.py`

Low-level HTTP executor. `_sync_http_sql(sql)` tries the direct `/sql` Neon endpoint first
(HTTP SQL API), then falls back to the management REST API.

```python
class NeonHTTPSession:
    async def exec(self, stmt)     → _NeonResult    # SELECT
    async def execute(self, stmt)  → _NeonExecResult   # DML
    async def get(self, model, pk) → model instance or None  # PK lookup
```

`get()` maps the returned row tuple to a proper model instance using
`sa_inspect(model).mapper.columns` ordering — routers can call `setattr()` safely.

### `api/services/ingestor.py`

Batch upsert: **50 rows per HTTPS call** instead of 1 call per row.

```python
BATCH_SIZE = 50

stmt = (
    pg_insert(RawQuery)
    .values(chunk)                          # 50 rows at once
    .on_conflict_do_update(
        index_elements=["query_hash"],
        set_={"occurrence_count": RawQuery.occurrence_count + 1, ...},
    )
)
await session.execute(stmt)
```

1000 rows → 20 HTTPS calls ≈ 16 s  (vs 1000 calls ≈ 13 min previously).

### `api/main.py`

- Removed `create_db_and_tables()` call from lifespan (was triggering a port-5432 attempt at startup and logging a WARNING)
- Lifespan is now a simple log + yield — no DB I/O at startup
- Schema is managed externally via `migration.sql` applied through the Neon REST API

### `api/migrations/env.py`

```python
# Loads api/.env so Alembic CLI picks up DATABASE_URL
load_dotenv(Path(__file__).parent.parent / ".env")

def _get_url() -> str:
    raw = os.environ.get("DATABASE_URL", "")
    # Alembic CLI needs psycopg2 (synchronous) scheme
    return raw.replace("postgresql://", "postgresql+psycopg2://", 1)
```

Removed `render_as_batch=True` (SQLite-only batch migration mode).

### `alembic.ini`

```ini
# URL is set dynamically in api/migrations/env.py from DATABASE_URL env var
sqlalchemy.url =
```

---

## Schema (applied via `migration.sql`)

Migration revision: `9db879faabd3`

```sql
-- ENUM types
CREATE TYPE severitytype    AS ENUM ('critical', 'warning', 'info');
CREATE TYPE sourcetype      AS ENUM ('sql', 'mongodb');
CREATE TYPE environmenttype AS ENUM ('prod', 'sat', 'unknown');
CREATE TYPE querytype       AS ENUM ('slow_query', 'slow_query_mongo', 'blocker', 'deadlock', 'unknown');
CREATE TYPE labelsource     AS ENUM ('sql', 'mongodb', 'both');

-- Tables (declaration order matches FK dependencies)
CREATE TABLE pattern_label  (id SERIAL PK, name, severity severitytype DEFAULT 'warning',
                             description, source labelsource DEFAULT 'both',
                             created_at, updated_at);

CREATE TABLE raw_query      (id SERIAL PK, query_hash UNIQUE, time, source sourcetype,
                             host, db_name, environment environmenttype, type querytype,
                             query_details, month_year, occurrence_count,
                             first_seen, last_seen, created_at, updated_at);

CREATE TABLE curated_query  (id SERIAL PK,
                             raw_query_id INT UNIQUE FK→raw_query.id,
                             label_id INT FK→pattern_label.id,
                             notes, created_at, updated_at);
```

Indexes on: `pattern_label(name, severity, source)`, `raw_query(query_hash UNIQUE, source, host, db_name, environment, type, month_year)`, `curated_query(raw_query_id UNIQUE, label_id)`.

---

## Neon REST API Usage

Because the **corporate proxy blocks the PostgreSQL wire protocol on port 5432** (SSL handshake is reset after TCP connect), all management operations use the Neon REST API over **HTTPS port 443**.

### Base URL

```
https://console.neon.tech/api/v2
```

Authentication: `Authorization: Bearer <NEON_API_KEY>`

### Query Execution

```python
body = {
    "query":       "SELECT ...",
    "db_name":     "perfmdb",
    "endpoint_id": "ep-rough-morning-a1v4c224",
    "role_name":   "neondb_owner",
}
# POST https://console.neon.tech/api/v2/projects/{project_id}/query
```

**Response format** (always HTTP 200, SQL errors return `success: false`):

```json
{
  "success": true,
  "duration": 1206729,
  "response": [
    {
      "query": "#1: SELECT ...",
      "data": {
        "fields": ["col1", "col2"],
        "rows":   [["val1", "val2"]],
        "truncated": false
      }
    }
  ]
}
```

> ⚠️ Rows are accessed at `response[0]["data"]["rows"]` — **not** the top-level `rows`.
> Empty results return `None` for the `rows` key — handled with `.get("rows") or []`.

### Project & Branch Management

```python
# List projects (org_id required for org-scoped API keys)
GET /projects?org_id=org-bitter-tree-45878282

# List branches
GET /projects/{project_id}/branches

# List databases on a branch
GET /projects/{project_id}/branches/{branch_id}/databases

# Delete a database
DELETE /projects/{project_id}/branches/{branch_id}/databases/{db_name}
```

---

## Utility Scripts

### `_test_neon.py` — API Connectivity Test

```bash
uv run python _test_neon.py
```

Verifies HTTPS connectivity to the Neon REST API. Lists projects, branches, and databases. Does **not** use port 5432.

### `_apply_migration.py` — Apply Schema via REST API

```bash
uv run python _apply_migration.py
```

Reads `migration.sql`, splits on `;`, strips `BEGIN`/`COMMIT`, and POSTs each statement individually as `neondb_owner`. Then GRANTs DML to `perfmdb_owner`. Used instead of `alembic upgrade head` when port 5432 is blocked.

### `migration.sql` — Generated DDL

```bash
# Regenerate if models change
uv run alembic upgrade head --sql > migration.sql
```

---

## Running Alembic (when port 5432 is accessible)

```powershell
# Generate a new migration from model changes
uv run alembic revision --autogenerate -m "add column x"

# Apply to Neon (requires port 5432)
uv run alembic upgrade head

# Preview SQL without applying
uv run alembic upgrade head --sql
```

`DATABASE_URL` is loaded from `api/.env` automatically by `api/migrations/env.py`. No shell export needed.

When port 5432 is blocked, use the `_apply_migration.py` script instead.

---

## Branch Workflow

```
Develop new feature
        │
        ▼
Local dev → connects to: develop branch (ep-orange-meadow)
        │
uv run alembic upgrade head  (or _apply_migration.py)
        │
        ▼
Test against develop branch data
        │
Neon Console → Branches → develop → Reset from parent  (to undo)
        │
When ready → apply same migration to production branch
```

---

## Corporate Proxy Workaround Summary

| Method | Port | Status |
|---|---|---|
| asyncpg direct | 5432 | ❌ SSL reset by proxy |
| psycopg2 / Alembic CLI | 5432 | ❌ SSL reset by proxy |
| **NeonHTTPSession (app runtime)** | **443 HTTPS** | **✅ All SQL works** |
| Neon REST API (schema scripts) | 443 HTTPS | ✅ Works |
| Neon Console SQL Editor (browser) | 443 HTTPS | ✅ Works |

The FastAPI app is **fully operational** using `NeonHTTPSession`.
No code changes are needed if/when port 5432 is eventually unblocked —
simply validate the connection, but the HTTPS path remains as a fallback.
