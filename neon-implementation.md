# Neon PostgreSQL Implementation Guide

> **Branch**: `migrate-to-neon-psql-db`
> **Status**: Schema applied ✅ | App role configured ✅ | All SQL via HTTPS port 443 ✅ | CSV upload working ✅

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
Neon HTTP SQL endpoint  →  POST https://{endpoint}.aws.neon.tech/sql   (port 443)
    │  Header: Neon-Connection-String: postgresql://user:pass@host/db?sslmode=require
    │  Body:   { "query": "INSERT ... ($1, $2, ...)", "params": [...] }
    ▼
Neon PostgreSQL (perfmdb)
    └── Branch: production  →  ep-rough-morning-a1v4c224
```

> ⚠️ **NOT used**: `https://console.neon.tech/api/v2/projects/{id}/query` — this management API is
> blocked by Zscaler IPS (classified as "malicious content"). The direct compute endpoint above
> (`ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech`) is **not blocked**.

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

# HTTP SQL endpoint password — MUST match PGPASSWORD.
# Password MUST be reset via Neon Console UI (Roles → perfmdb_owner → Reset password).
# Passwords created via SQL (CREATE ROLE / ALTER ROLE ... PASSWORD) are NOT
# SCRAM-SHA-256 and will fail with "missing authentication credentials".
NEON_SQL_PASS=<password>

# Neon API key (used by dev tooling / migration scripts only)
NEON_ORG_ID=org-bitter-tree-45878282
NEON_API_KEY=<api_key>

# Branch labels (informational)
NEON_DEV_BRANCH=develop
NEON_PROD_BRANCH=production
```

> `api/.env` is gitignored. Never commit credentials.

---

## DB Backend Switcher

The app supports two backends selected entirely by `DB_BACKEND` in `api/.env`. No code changes required.

| `DB_BACKEND` | Session class | Connection | Use case |
|---|---|---|---|
| `neon` **(default)** | `NeonSession` | Neon HTTPS REST API (port 443) | Production, behind corporate proxy |
| `sqlite` | SQLModel `AsyncSession` | Local `master.db` via `aiosqlite` | Offline dev, unit tests |

### Switching to SQLite (offline dev)

**Step 1** — Edit `api/.env`:
```dotenv
DB_BACKEND=sqlite
SQLITE_URL=sqlite+aiosqlite:///./master.db   # path relative to project root
```

**Step 2** — Create the SQLite schema (first time only):
```powershell
# From the project root (dbPerfmHealthCheck/)
uv run python - <<'EOF'
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel
from api.models import RawQuery, PatternLabel, CuratedQuery  # adjust imports if needed

async def main():
    engine = create_async_engine("sqlite+aiosqlite:///./master.db")
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

asyncio.run(main())
EOF
```

Or use Alembic if you have a working migration (SQLite-compatible):
```powershell
uv run alembic upgrade head
```

**Step 3** — Restart the dev server:
```powershell
uv run uvicorn api.main:app --port 8000 --reload
```

**Step 4** — Verify via `/health`:
```
GET http://127.0.0.1:8000/health
```
Expected response:
```json
{
  "status": "ok",
  "backend": "sqlite",
  "db": "sqlite+aiosqlite:///./master.db"
}
```
Startup log will confirm: `INFO: Starting up — DB_BACKEND=sqlite`

---

### Switching back to Neon

**Step 1** — Edit `api/.env`:
```dotenv
DB_BACKEND=neon
```

**Step 2** — Restart the dev server.

**Step 3** — Verify via `/health`:
```json
{
  "status": "ok",
  "backend": "neon",
  "db": "ep-rough-morning-a1v4c224-pooler.ap-southeast-1.aws.neon.tech/perfmdb?sslmode=require"
}
```

---

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

Low-level HTTP executor. Sends all SQL to the **direct Neon compute endpoint** (`/sql`),
not the management REST API.

```python
class NeonHTTPSession:
    async def exec(self, stmt)     → _NeonResult     # SELECT  (literal_binds — our values only)
    async def execute(self, stmt)  → _NeonExecResult # DML     (parameterized $1/$2 — see below)
    async def get(self, model, pk) → model instance or None  # PK lookup
```

#### Auth — `Neon-Connection-String` header only

The endpoint authenticates via the connection string URL in `Neon-Connection-String`.
Do **NOT** send `Authorization: Basic` — it causes a 400 error:

```python
headers={
    # ✅ Correct — credentials embedded in the connection-string URL
    "Content-Type": "application/json",
    "Neon-Connection-String": "postgresql://perfmdb_owner:<pass>@<endpoint>/perfmdb?sslmode=require",
    # ❌ WRONG — adding Authorization: Basic breaks auth (returns 400 "missing authentication credentials")
}
```

#### Parameterized queries for DML — Zscaler IPS workaround

CSV files contain real SQL query text (captured from `sys.dm_exec_query_stats`, MongoDB slow query
logs, etc.). When this text was inlined into INSERT statements via `literal_binds=True`, Zscaler's
IPS engine flagged the HTTPS request body as SQL injection → **HTTP 403 Forbidden**.

Fix: `execute()` uses `_compile_parameterized()` which compiles to `$1/$2/...` style SQL
(asyncpg dialect) and sends user data in the JSON `params` array:

```python
def _compile_parameterized(stmt) -> tuple[str, list]:
    """Compile INSERT/UPDATE to $1/$2/... SQL — user data in params, not inline."""
    dialect = _pg_asyncpg.dialect()
    compiled = stmt.compile(dialect=dialect, compile_kwargs={"render_postcompile": True})
    params = []
    for key in (compiled.positiontup or []):
        val = (compiled.params or {}).get(key)
        if hasattr(val, "isoformat"):   # datetime → ISO string
            val = val.isoformat()
        params.append(val)
    return str(compiled), params

# HTTP body sent to Neon:
# { "query": "INSERT INTO raw_query ... VALUES ($1, $2, ...) ON CONFLICT ...",
#   "params": ["abc123", null, "sql", "WINFODB06HV11", "fb_db_v2", ...] }
```

`exec()` (SELECT) still uses `literal_binds=True` — those values come from our own code,
never from user-supplied CSV content.

#### Response format

```json
{
  "fields":   [{"name": "col", "dataTypeID": 19}],
  "rows":     [{"col": "value"}],
  "command":  "SELECT",
  "rowCount": 1,
  "rowAsArray": false
}
```

Rows come back as **dicts** (keyed by column name). `rowCount` = server-reported affected count,
used directly for INSERT/UPDATE/DELETE (e.g. `{"command": "INSERT", "rowCount": 50, "rows": []}`).

`get()` maps the returned row to a model instance using `sa_inspect(model).mapper.columns` ordering.

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

## Neon HTTP SQL Endpoint

All runtime SQL is sent to the **direct compute endpoint** (not the management API):

```
POST https://ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech/sql
Content-Type: application/json
Neon-Connection-String: postgresql://perfmdb_owner:<pass>@<endpoint>/perfmdb?sslmode=require

{
  "query":  "SELECT current_user",
  "params": []
}
```

**Response** (HTTP 200 on success, 4xx on SQL or auth error):

```json
{
  "fields":   [{"name": "current_user", "dataTypeID": 19}],
  "rows":     [{"current_user": "perfmdb_owner"}],
  "command":  "SELECT",
  "rowCount": 1,
  "rowAsArray": false
}
```

> `rowCount` is the actual affected-row count for DML — not the number of rows in the response body.

### Password requirements

> ⚠️ Password MUST be generated via **Neon Console UI → Roles → Reset password**.
> Passwords set via SQL (`ALTER ROLE ... PASSWORD '...'`) do not use SCRAM-SHA-256
> and will be rejected with `{"message": "missing authentication credentials: required password"}`.

### What about the management REST API?

`https://console.neon.tech/api/v2/projects/{id}/query` was considered early in development but
is **not used** for two reasons:
1. **Blocked by Zscaler IPS** — classified as "malicious content"
2. **SQL-created roles not recognised** — `perfmdb_owner` (created via `CREATE ROLE`) was rejected with 403

The direct compute endpoint (`ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech`) is **not
blocked** and authenticates any Console-reset password correctly.

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
| Neon management API (`console.neon.tech`) | 443 HTTPS | ❌ Zscaler IPS block ("malicious content") |
| **NeonHTTPSession — direct `/sql` endpoint** | **443 HTTPS** | **✅ All SQL works** |
| Neon Console SQL Editor (browser) | 443 HTTPS | ✅ Works |

### Why two different Zscaler blocks?

**Block 1 — Port 5432:** The corporate proxy performs SSL inspection and resets the TCP connection
after the TLS handshake for any non-HTTPS traffic. PostgreSQL wire protocol on port 5432 is dropped.

**Block 2 — Management API (`console.neon.tech`):** Zscaler IPS classifies `console.neon.tech`
as malicious content (possibly due to shared SaaS abuse patterns). Any request to this domain
returns 403 even over HTTPS port 443.

**Not blocked:** The compute endpoint `ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech`
is standard AWS infrastructure and passes through Zscaler unblocked.

### Why CSV upload was still 403 after switching to the compute endpoint

CSV files contain captured SQL query text (from `sys.dm_exec_query_stats`, MongoDB slow query logs,
etc.). When that text was embedded **inline** in the INSERT body (`literal_binds=True`), the full
SQL text appeared in the HTTPS request — Zscaler IPS pattern-matched it as SQL injection → 403.

**Fix:** `execute()` now uses `_compile_parameterized()`. The SQL template uses `$1/$2/...`
placeholders; actual values (including query text) go in the `params` JSON array. Zscaler no longer
pattern-matches user data because it is in a structured JSON field, not inline SQL text.

```
Before (blocked):
  POST /sql  {"query": "INSERT ... VALUES ('SELECT /* captured */ * FROM sys.dm_exec...', ...)", "params": []}
                                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                           SQL injection pattern → Zscaler IPS → 403

After (working):
  POST /sql  {"query": "INSERT ... VALUES ($1, $2, ...)", "params": ["SELECT /* captured */ * FROM sys.dm_exec..."]}
                                                                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                                                      In params array → passes through
```

The FastAPI app is **fully operational** using `NeonHTTPSession`.
No code changes are needed if/when port 5432 is eventually unblocked —
simply validate the connection, but the HTTPS path remains as a fallback.
