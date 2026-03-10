# Neon PostgreSQL Implementation Guide

> **Branch**: `migrate-to-neon-psql-db`
> **Status**: Schema applied ✅ | App role configured ✅ | Port 5432 blocked by corporate proxy ⚠️

---

## Overview

The `dbPerfmHealthCheck` FastAPI backend was migrated from SQLite (`aiosqlite`) to Neon PostgreSQL (`asyncpg`). The architecture is:

```
Next.js (web/)
    │  HTTP only (no direct DB access)
    ▼
FastAPI (api/)
    │  asyncpg over postgresql+asyncpg://
    ▼
Neon PostgreSQL
    ├── Branch: production  →  ep-rough-morning-a1v4c224
    └── Branch: develop     →  ep-orange-meadow-a1p2p3mi
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
  FastAPI runtime       →  asyncpg          (postgresql+asyncpg://)
  Alembic CLI           →  psycopg2-binary  (postgresql+psycopg2://)
  Neon REST API         →  urllib.request   (HTTPS port 443)
```

asyncpg **does not** accept `sslmode=` or `channel_binding=` as URL query parameters. These are stripped in `api/database.py` and SSL is passed via `connect_args={"ssl": True}`.

psycopg2 (used only by the Alembic CLI) accepts them in the URL directly.

---

## Key Files Changed

### `api/database.py`

Replaced the SQLite engine with a PostgreSQL async engine:

```python
def _build_async_url(raw: str) -> tuple[str, dict]:
    # 1. Change scheme: postgresql:// → postgresql+asyncpg://
    # 2. Strip sslmode= and channel_binding= (asyncpg rejects them)
    # 3. Return connect_args={"ssl": True} when sslmode=require
    ...

engine = create_async_engine(
    ASYNC_DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,   # drops stale Neon connections after auto-suspend
    connect_args=_connect_args,
)
```

Public interface is unchanged — `get_session()`, `open_session()`, `create_db_and_tables()` work identically to the SQLite version.

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

### `api/main.py`

- Removed `apply_pragmas` import and call (SQLite-only PRAGMAs)
- Health endpoint masks credentials in the logged URL

---

## Schema (applied via `migration.sql`)

Migration revision: `9db879faabd3`

```sql
-- ENUM types
CREATE TYPE severitytype     AS ENUM ('critical', 'warning', 'info');
CREATE TYPE sourcetype       AS ENUM ('sql', 'mongodb');
CREATE TYPE environmenttype  AS ENUM ('prod', 'sat', 'unknown');
CREATE TYPE querytype        AS ENUM ('slow_query', 'blocker', 'deadlock', 'unknown');

-- Tables
CREATE TABLE pattern   (id SERIAL PK, name, description, pattern_tag, severity,
                        source, environment, type, first_seen, last_seen,
                        total_occurrences, notes, created_at, updated_at);

CREATE TABLE raw_query (id SERIAL PK, query_hash UNIQUE, time, source, host,
                        db_name, environment, type, query_details, month_year,
                        occurrence_count, first_seen, last_seen,
                        pattern_id FK→pattern.id, created_at, updated_at);
```

Indexes on: `pattern(name, pattern_tag, severity, source, environment, type)` and `raw_query(query_hash UNIQUE, host, db_name, environment, source, type, month_year, pattern_id)`.

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
| asyncpg direct (app runtime) | 5432 | ❌ SSL reset by proxy |
| psycopg2 / Alembic CLI | 5432 | ❌ SSL reset by proxy |
| Neon REST API (`_apply_migration.py`, `_test_neon.py`) | 443 HTTPS | ✅ Works |
| Neon Console SQL Editor (browser) | 443 HTTPS | ✅ Works |

Until port 5432 is unblocked, the FastAPI app cannot connect to Neon at runtime. The schema is correctly applied. The app will work once the network restriction is lifted.
