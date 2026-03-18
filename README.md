# DB Performance Health Check

Ingests Splunk-exported database performance CSV files, deduplicates and stores them in SQLite, exposes a FastAPI REST API for analysis, and renders a Next.js dashboard for interactive exploration and pattern curation.

---

## Tech Stack

| Layer | Technology |
|---|---|
| **API** | Python 3.14, FastAPI, SQLModel, Alembic |
| **Database** | SQLite (`db/master.db`) via `aiosqlite` async driver |
| **Analytics** | DuckDB (in-memory OLAP), Polars (DataFrame transforms) |
| **Auth** | JWT (`python-jose`), bcrypt password hashing |
| **Frontend** | Next.js 15 (App Router), Tailwind CSS, shadcn/ui |
| **Tests** | pytest, pytest-asyncio, httpx (async test client) |
| **Package mgr** | `uv` |

---

## Project Structure

```
dbPerfmHealthCheck/
├── api/                        # FastAPI application
│   ├── main.py                 # App factory, lifespan, router registration
│   ├── database.py             # SQLite engine, get_session(), open_session()
│   ├── models.py               # SQLModel table definitions + Pydantic schemas
│   ├── analytics_db.py         # DuckDB + Polars bridge (OLAP layer)
│   ├── routers/
│   │   ├── auth.py             # POST /api/auth/login|register, GET /api/auth/me
│   │   ├── queries.py          # GET /api/queries, /api/queries/count, /distinct
│   │   ├── labels.py           # GET/POST/DELETE /api/labels
│   │   ├── curated.py          # GET/POST/PUT/DELETE /api/curated
│   │   ├── spl.py              # GET/POST/PUT/DELETE /api/spl, /api/spl/types
│   │   ├── analytics.py        # GET /api/analytics/summary|by-host|by-month|…
│   │   ├── upload.py           # POST /api/upload
│   │   ├── validate.py         # POST /api/validate
│   │   └── export.py           # GET /api/export/csv|json
│   ├── services/
│   │   ├── auth_service.py     # JWT creation/verification, bcrypt hashing
│   │   ├── extractor.py        # CSV → RawQuery row conversion (Polars)
│   │   ├── ingestor.py         # Deduplication, hash computation, DB upsert
│   │   └── validator.py        # CSV schema validation
│   └── migrations/             # Alembic migration scripts (versions/)
├── migration/
│   └── manage.py               # ← DB management CLI (see below)
├── scripts/
│   ├── create_admin.py         # Create first admin user (requires running server)
│   └── validate_csv.py         # Standalone CSV validation CLI
├── tests/
│   ├── conftest.py             # In-memory SQLite engine patch + table fixture
│   ├── test_api_auth.py
│   ├── test_api_queries.py
│   ├── test_api_labels.py
│   ├── test_api_analytics.py
│   ├── test_api_performance.py
│   ├── test_db_connection.py   # analytics_db helpers (load_table, get_duck)
│   ├── test_upload.py
│   ├── test_ui_smoke.py
│   └── test_polars_migration.py
├── web/                        # Next.js 15 frontend
│   ├── app/
│   │   ├── dashboard/          # Analytics charts, query counts
│   │   ├── login/              # JWT login
│   │   ├── account/            # Profile, password change
│   │   ├── admin/users/        # User management (admin only)
│   │   └── spl/                # SPL library (tab per query type)
│   ├── components/             # Shared UI components
│   ├── lib/
│   │   ├── api.ts              # Typed fetch wrappers
│   │   └── auth-client.ts      # JWT storage / session helpers
│   └── middleware.ts           # Route protection (redirect to /login if unauth)
├── data/                       # Raw CSV input files (gitignored)
├── db/                         # SQLite database file (gitignored)
├── alembic.ini                 # Alembic config (sqlalchemy.url = sqlite:///db/master.db)
└── pyproject.toml
```

---

## Getting Started

### 1. Install dependencies

```powershell
# Corporate proxy with SSL inspection — use native TLS once to populate cache
uv sync --native-tls
```

### 2. Configure environment

Create `api/.env` (gitignored):

```dotenv
# SQLite path (default: db/master.db relative to project root)
SQLITE_PATH=db/master.db

# JWT secret — change before production use
SECRET_KEY=change-me-in-production

# Frontend URL (used by CORS middleware)
FRONTEND_URL=http://localhost:3000
```

### 3. Create the database

```powershell
uv run python migration/manage.py create
```

### 4. Create the first admin user

Start the API server first (step 5), then in a separate terminal:

```powershell
uv run python scripts/create_admin.py
```

Prompts for username / email / password. The `/api/auth/register` endpoint is locked after the first user — subsequent users are added via the admin panel.

### 5. Start the API server

```powershell
uv run uvicorn api.main:app --port 8000 --reload
# Health check: http://localhost:8000/health
# Swagger UI:   http://localhost:8000/docs
```

### 6. Start the frontend

```powershell
cd web
npm install
npm run dev
# http://localhost:3000
```

---

## Database Management

All database operations go through a single management script:

```powershell
uv run python migration/manage.py <command>
```

| Command | What it does |
|---|---|
| `status` | Show DB path, table names, row counts, current Alembic revision |
| `create` | Run `alembic upgrade head` — create full schema (idempotent) |
| `drop` | Drop all tables ⚠ destroys all data |
| `reset` | `drop` + `create` — full wipe and rebuild ⚠ |
| `migrate-up` | Apply the next incremental schema change (`alembic upgrade head`) |
| `migrate-down` | Reverse the last schema change (`alembic downgrade -1`) ⚠ |
| `truncate` | Delete all rows, keep schema ⚠ |

Destructive commands (`drop`, `reset`, `truncate`, `migrate-down`) require typing `yes` at the confirmation prompt.

### Adding a new column / table

```powershell
# 1. Edit api/models.py (add field or new SQLModel class)

# 2. Auto-generate the migration file
uv run alembic revision --autogenerate -m "add field x to raw_query"

# 3. Apply it
uv run python migration/manage.py migrate-up
```

---

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Server + DB status |
| `POST` | `/api/auth/login` | Returns JWT access token |
| `POST` | `/api/auth/register` | Register first user (locked after first) |
| `GET` | `/api/auth/me` | Current user profile |
| `POST` | `/api/upload` | Upload CSV files (multipart) |
| `POST` | `/api/validate` | Validate CSV without persisting |
| `GET` | `/api/queries` | Paginated query list with filters |
| `GET` | `/api/queries/count` | Row count with optional type/env filter |
| `GET` | `/api/queries/distinct` | Distinct field values (host, env, type…) |
| `GET` | `/api/labels` | Pattern label library |
| `POST` | `/api/labels` | Create a label |
| `GET` | `/api/curated` | Curated query patterns |
| `POST` | `/api/curated` | Promote a raw query to curated |
| `GET` | `/api/spl` | SPL query library |
| `GET` | `/api/spl/types` | Distinct SPL query types (for combobox) |
| `POST` | `/api/spl` | Create SPL query |
| `PUT` | `/api/spl/{id}` | Update SPL query |
| `DELETE` | `/api/spl/{id}` | Delete SPL query |
| `GET` | `/api/analytics/summary` | Aggregate counts by type/env |
| `GET` | `/api/analytics/by-host` | Top-N hosts by query count |
| `GET` | `/api/analytics/by-month` | Monthly trend |
| `GET` | `/api/analytics/by-db` | Top-N databases |
| `GET` | `/api/analytics/curation-coverage` | % of queries curated |
| `GET` | `/api/export/csv` | Export filtered rows as CSV |
| `GET` | `/api/export/json` | Export filtered rows as JSON |

Full interactive docs at `http://localhost:8000/docs` once the server is running.

---

## Database Schema

| Table | Purpose |
|---|---|
| `raw_query` | Every ingested CSV row (source of truth, deduplicated by `query_hash`) |
| `curated_query` | Promoted rows with assigned pattern labels |
| `pattern_label` | Pattern label library (name, category, severity) |
| `spl_query` | SPL (Splunk Processing Language) query library |
| `user` | Auth users (username, hashed password, role) |
| `alembic_version` | Schema migration tracking |

---

## Tests

```powershell
# Run all tests
uv run pytest tests/ -v

# Single file
uv run pytest tests/test_api_queries.py -v

# With coverage
uv run pytest tests/ --cov=api --cov-report=term-missing
```

### How tests work

`tests/conftest.py` patches `api.database.engine` at import time with a shared in-memory SQLite engine (`sqlite+aiosqlite:///file:testmemdb?mode=memory&cache=shared&uri=true`). A session-scoped `autouse` fixture drops and recreates all tables once per run. No real DB file is touched.

### Validate CSVs without the server

```powershell
# Single file
uv run python scripts/validate_csv.py --file data/Jan2026/maxElapsedQueriesProdJan26.csv

# Entire directory
uv run python scripts/validate_csv.py --directory data/Jan2026
```

---

## Frontend Pages

| Route | Description |
|---|---|
| `/login` | JWT login |
| `/dashboard` | Analytics charts, summary cards, monthly trend |
| `/spl` | SPL library — tab per query type, full SPL display |
| `/account` | Profile and password change |
| `/admin/users` | User management (admin role only) |

Unauthenticated requests to protected routes are redirected to `/login` by `web/middleware.ts`.

---

## Development Notes

- **Corporate SSL proxy**: `uv sync --native-tls` once; subsequent installs use the cache and don't need it.
- **SQLite WAL mode**: enabled automatically on startup via `PRAGMA journal_mode=WAL`. Improves concurrent read/write performance.
- **DuckDB analytics**: each analytics request opens its own in-memory DuckDB connection — reads all rows from SQLite into Polars DataFrames, registers them as virtual tables, then runs OLAP SQL. No shared state between requests.
- **SPL query types**: free-form text field (not an ENUM). `GET /api/spl/types` returns DB-stored types merged with four built-in defaults (`slow_query`, `slow_query_mongo`, `blocker`, `deadlock`).
