# Neon PostgreSQL — Implementation & Operations Guide

> **Branch**: `migrate-to-neon-psql-db` | **Project**: `hkjc-db-perfm` (`cold-union-77928175`)

---

## Architecture

```
Next.js (web/)
    
FastAPI (api/)
      NeonSession  urllib.request (HTTPS POST, port 443)
    
POST https://ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech/sql
    Header: Neon-Connection-String: postgresql://user:pass@host/db?sslmode=require
    Body:   {"query": "SELECT ... WHERE id = $1", "params": [42]}
    
Neon PostgreSQL (perfmdb)
```

**Port 5432 is never used.** The corporate Zscaler proxy resets any non-HTTPS SSL handshake, killing the PostgreSQL wire protocol before authentication.

---

## Project Reference

| Field | Value |
|---|---|
| Project | `hkjc-db-perfm` |
| Project ID | `cold-union-77928175` |
| Region | `aws-ap-southeast-1` |
| Database | `perfmdb` |
| DDL role | `neondb_owner` (owner  CREATE/DROP/ALTER) |
| App role | `perfmdb_owner` (DML only  SELECT/INSERT/UPDATE/DELETE) |

### Branches & Endpoints

| Branch | Endpoint ID | Use |
|---|---|---|
| `production` | `ep-rough-morning-a1v4c224` | Stable data |
| `develop` | `ep-orange-meadow-a1p2p3mi` | Safe to reset |

---

## Environment Variables (`api/.env`)

```dotenv
DB_BACKEND=neon

# App runtime (perfmdb_owner  DML only)
DATABASE_URL=postgresql://perfmdb_owner:<pass>@ep-rough-morning-a1v4c224-pooler.ap-southeast-1.aws.neon.tech/perfmdb?sslmode=require&channel_binding=require
PGHOST=ep-rough-morning-a1v4c224-pooler.ap-southeast-1.aws.neon.tech
PGDATABASE=perfmdb
PGUSER=perfmdb_owner
PGPASSWORD=<pass>

# Must equal PGPASSWORD. MUST be set via Neon Console -> Roles -> Reset password.
# SQL-created passwords (ALTER ROLE ... PASSWORD) fail with "missing authentication credentials".
NEON_SQL_PASS=<pass>

# DDL migrations  neondb_owner (owner role)
NEON_DDL_CONN_STR=postgresql://neondb_owner:<pass>@ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech/perfmdb?sslmode=require

# Management API (used only by _test_neon.py to list projects/branches)
NEON_API_KEY=<api_key>
NEON_ORG_ID=org-bitter-tree-45878282
```

> `api/.env` is gitignored. Never commit credentials.

---

## Schema Tables

| Table | Purpose | Owner role |
|---|---|---|
| `raw_query` | Every CSV row (source of truth) | `neondb_owner` |
| `curated_query` | Promoted rows with labels | `neondb_owner` |
| `pattern_label` | Label library | `neondb_owner` |
| `"user"` | Auth users (quoted  reserved word) | `neondb_owner` |
| `alembic_version` | Migration tracking | `neondb_owner` |

Full DDL: [`neon/migration.sql`](neon/migration.sql) + [`neon/neon_schema.sql`](neon/neon_schema.sql)

---

## Operations

All operations use a single management script. Run from the **project root**:

```powershell
uv run python neon/manage.py <command>
```

| Command | What it does |
|---|---|
| `status` | Show tables, row counts, current Alembic version |
| `create` | Apply `migration.sql` + `neon_schema.sql` to create the full schema |
| `drop` | Drop all tables and ENUM types (data destroyed) |
| `reset` | `drop` + `create` — full wipe and rebuild |
| `migrate-up` | Apply the **next** incremental schema change (new column, new table, etc.) |
| `migrate-down` | Reverse the **last** schema change (rollback one step) |
| `truncate` | Delete all rows, keep schema, reset sequences |

Destructive commands (`drop`, `reset`, `truncate`, `migrate-down`) prompt for confirmation.

---

### What Alembic does here

Alembic tracks schema **versions**. Each migration file has two functions:

```python
def upgrade():   # what to do going forward  (add column, create table)
def downgrade(): # how to reverse it         (drop column, drop table)
```

Normally `alembic upgrade head` connects to the DB directly. Since **port 5432 is blocked**, `manage.py` uses Alembic in **offline mode** (`--sql` flag) — it generates SQL without touching the DB, then `manage.py` POSTs that SQL to Neon via HTTPS.

```
alembic upgrade head --sql   →  outputs incremental DDL statements to stdout
manage.py                    →  captures that output, POSTs each statement via HTTPS
```

**migrate-up** and **migrate-down** are for **incremental changes after initial setup** — adding a column, renaming a field, adding an index. They only apply the *delta* since the last recorded version. They are not the same as `create` (full schema from scratch).

---

### 1. Check Status

```powershell
uv run python neon/manage.py status
```

---

### 2. Create Schema (first time / after reset)

```powershell
uv run python neon/manage.py create
```

Applies `neon/migration.sql` (all tables, indexes, ENUM types, grants) then `neon/neon_schema.sql` (`"user"` table + grants).

If `migration.sql` is out of date, regenerate it first:

```powershell
uv run alembic upgrade head --sql > neon/migration.sql
```

---

### 3. Migrate Up (add a column / table / index)

Use this when you've changed `api/models.py` and need to push the schema change to Neon.

```powershell
# Step 1: edit api/models.py (add field, new model, etc.)

# Step 2: create the migration file (offline — no DB needed)
uv run alembic revision --autogenerate -m "add field x to raw_query"

# Step 3: apply it to Neon via HTTPS
uv run python neon/manage.py migrate-up
```

What happens internally:
1. `manage.py` runs `alembic upgrade head --sql` → gets incremental DDL
2. POSTs each statement to Neon as `neondb_owner`
3. `alembic_version` row is updated to the new revision ID

---

### 4. Migrate Down (rollback last change)

```powershell
uv run python neon/manage.py migrate-down
```

Reverses the most recent migration — drops the column/table/index that was added, moves `alembic_version` back one step. Prompts for confirmation.

---

### 5. Drop Schema

```powershell
uv run python neon/manage.py drop
```

Drops all tables (`curated_query`, `raw_query`, `pattern_label`, `"user"`, `alembic_version`) and all ENUM types. **All data is lost.**

---

### 6. Reset Database

```powershell
uv run python neon/manage.py reset
```

Equivalent to `drop` + `create`. Use to start completely fresh.

For the **`develop` branch** only, you can also reset via Neon Console:
Branches → `develop` → **Reset from parent** (restores the `production` snapshot).

---

### 7. Truncate (clear data, keep schema)

```powershell
uv run python neon/manage.py truncate
```

Deletes all rows from all data tables and resets auto-increment sequences. Schema and `alembic_version` are untouched.

---

### 8. Create the First Admin User

Requires the FastAPI server to be running and the `"user"` table to exist.

```powershell
uv run python scripts/create_admin.py
```

Prompts for username / email / password. POSTs to `POST /api/auth/register`. Subsequent registrations are locked — use the admin UI to add more users.

---

### 9. Switch Backend (offline dev)

Edit `api/.env`:

```dotenv
DB_BACKEND=sqlite
SQLITE_URL=sqlite+aiosqlite:///./master.db
```

Create schema (first time):

```powershell
uv run alembic upgrade head
```

Verify via `GET /health` — response will show `"backend": "sqlite"`.

Switch back to Neon: set `DB_BACKEND=neon` and restart.

---

## Neon HTTP SQL Endpoint  Technical Details

### Auth

Only `Neon-Connection-String` header is required. Do **NOT** send `Authorization: Basic`  it causes 400.

```python
headers={
    "Content-Type": "application/json",
    "Neon-Connection-String": "postgresql://user:pass@host/db?sslmode=require",
}
```

### Request / Response

```json
// Request
{"query": "SELECT * FROM raw_query WHERE host = $1", "params": ["WINFODB06HV11"]}

// Response (200 on success, 4xx on SQL/auth error)
{
  "fields":   [{"name": "id", "dataTypeID": 23}, ...],
  "rows":     [{"id": 1, "host": "WINFODB06HV11", ...}],
  "command":  "SELECT",
  "rowCount": 1
}
```

Rows come back as **objects keyed by column name**. `rowCount` = affected rows for DML.

### Why parameterized queries ($1/$2)?

CSV files contain captured SQL text. When embedded inline (`literal_binds=True`), Zscaler IPS pattern-matched INSERT bodies as SQL injection  403. Parameterized queries keep user data in the `params` array, which Zscaler does not inspect.

### Password requirements

Passwords **must** be reset via **Neon Console UI  Roles  Reset password**.  
SQL-created passwords (`ALTER ROLE ... PASSWORD`) are not SCRAM-SHA-256 and return `{"message":"missing authentication credentials"}`.

---

## Proxy Block Matrix

| Method | Port | Status |
|---|---|---|
| asyncpg / psycopg2 direct | 5432 | ❌ Proxy resets SSL |
| Neon management API (`console.neon.tech`) | 443 | ❌ Zscaler IPS block |
| **`NeonHTTPSession`  direct `/sql` endpoint** | **443** | **✅ Works** |
| Neon Console SQL Editor (browser) | 443 | ✅ Works |
| Alembic `--sql` (offline generation) | none | ✅ Works |

---

## Key Files

| File | Purpose |
|---|---|
| `api/neon_http.py` | Low-level HTTP SQL executor (`_sync_http_sql`, `NeonHTTPSession`) |
| `api/database.py` | `NeonSession` — FastAPI session abstraction over `NeonHTTPSession` |
| `api/models.py` | SQLModel table definitions (use `sa_column=Column(SAString)` for enums) |
| `neon/manage.py` | **Management CLI** — create, drop, reset, migrate-up/down, truncate, status |
| `neon/migration.sql` | Full schema DDL (Alembic-generated, applied by `manage.py create`) |
| `neon/neon_schema.sql` | `"user"` table DDL + grants (applied by `manage.py create`) |
| `neon/_apply_migration.py` | Lower-level: apply any SQL file via HTTPS (used internally by older scripts) |
| `neon/_ddl_user.py` | One-shot: create/grant `"user"` table (reads `NEON_DDL_CONN_STR`) |
| `neon/_test_neon.py` | Verify HTTPS API connectivity — lists projects/branches via management API |
| `scripts/create_admin.py` | Create first admin user via the running FastAPI server |