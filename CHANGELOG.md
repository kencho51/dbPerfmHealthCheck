# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-03-30

### Added
- GitHub Actions CI workflow: ruff lint + pytest on every push/PR
- 113+ new tests covering previously untested routers (`curated`, `spl`, `export`, `validate`), analytics endpoints (`by-hour`, `host-stats`, `co-occurrence`, `by-month-type`), and auth scenarios (`DELETE /users`, `POST /register/admin`, `PATCH /me`)
- Migration test suite (`tests/test_migrations.py`) — runs locally, excluded from CI

### Fixed
- Resolved all ruff lint errors across `api/`, `scripts/`, and `tests/` (E402, UP042, E501, UP045, UP037)
- Restored SQLModel forward-reference `Relationship` annotations in `models.py` to prevent mapper initialisation failure
- Moved deferred dotenv imports above `_write_lock` in `database.py` to fix E402

### Changed
- Updated GitHub Actions to opt in to Node.js 24 (`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`) to suppress Node 20 deprecation warnings
- Added `per-file-ignores` in `pyproject.toml`: suppress `E501` for `tests/*` and `scripts/*` (raw SQL trace strings), and `UP042` for `api/models.py` (SQLModel requires `str+Enum` pattern)

## [0.5.0] - 2026-03-27

### Added
- `perf/` scripts for profiling upload and DB throughput
- `.gitignore` updates for standalone repo use

### Changed
- Updated Next.js and npm packages to latest versions
- Increased ingestor batch size to 1 000 rows for better throughput
- Updated monthly trend board UI layout and number formatting
- Ruff config updated to stricter settings

### Added
- Per-query-type DB tables (`raw_query_slow_sql`, `raw_query_blocker`, `raw_query_deadlock`, `raw_query_slow_mongo`) with `raw_query_id` FK
- `TypedIngestor` service to populate typed tables on upload
- Alembic migrations for new typed tables and FK columns

## [0.4.0] - 2026-03-23

### Added
- Week filter on analytics heatmap (`week_start` / `week_end` query params)
- Auto logout on JWT expiry

### Changed
- Refresh button now works correctly on dashboard
- Exact SQL query text extraction improved in deadlock parser
- Top-query descriptions added to pattern label seed data

## [0.3.0] - 2026-03-18

### Added
- Auth router: JWT login, user registration (admin-only after first user), role management, user deletion, `PATCH /me` password change
- SPL query library: full CRUD at `/api/spl` with `query_type` filtering and `/types` endpoint
- Analytics router: DuckDB-backed endpoints for `by-hour`, `host-stats`, `co-occurrence`, `by-month-type`
- Migration CLI (`migration/manage.py`) with `upgrade`, `downgrade`, `history`, `current` sub-commands
- Serialised DuckDB connection pool to prevent concurrent-write race conditions
- `upload_log` table tracking every CSV ingest (filename, file type, environment, row counts)

### Changed
- Migrated fully from Neon/PostgreSQL to SQLite + aiosqlite backend
- DB backend configurable via `DB_BACKEND` env var in `api/.env`
- `conftest.py` refactored to use a named shared in-memory SQLite (`testmemdb_pytest`) with a holder connection to keep it alive across the test session
- Deprecated ASGI middleware removed; login expiry verification added

### Fixed
- Test suite repaired: `client`, `admin_token`, and `auth_headers` fixtures added to `conftest.py`
- `api/.env` loaded via python-dotenv; `DB_BACKEND` exposed in `/health` endpoint

## [0.2.0] - 2026-03-09

### Added
- Polars migration replacing pandas for all CSV parsing (faster, lower memory)
- `month_year` column derived and back-filled from upload timestamps
- Systems filter and database systems mapping dictionary
- Top-query board with host/DB filters and monthly trend breakdown
- Heatmap by query type on analytics dashboard
- Ingest error surfaced in the UI

### Changed
- Dashboard filters made more robust; additional filter dimensions added
- `month_year` null values fixed during ingest

## [0.1.0] - 2026-03-04

### Added
- Initial FastAPI backend with SQLite + SQLModel schema (`raw_query`, `pattern_label`, `curated_query`)
- CSV upload endpoint supporting multiple files and large uploads (batch insert)
- Pattern labelling UI: create/edit labels, assign to curated queries, confirm before unassign
- Monthly trend chart with filter-by-type support
- Sort by ID and large CSV upload support
- Export and validate endpoints
- Alembic migration baseline
- Next.js frontend scaffolded with upload preview, dashboard, and patterns pages
- Playwright smoke test (`test_ui_smoke.py`)

