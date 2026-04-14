# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),

## [0.8.0] - 2026-04-13

### Added
- Alembic migration `add_perf_indexes`: `ix_raw_query_query_details`, `ix_upload_log_filename`, `ix_upload_log_file_type`, `ix_upload_log_uploaded_at`
- `PRAGMA mmap_size=536870912` (512 MB memory-mapped I/O) on both async and sync SQLite engines
- `PRAGMA optimize=0x10002` (targeted auto-ANALYZE) on every new SQLite connection
- `PRAGMA wal_checkpoint(PASSIVE)` after each bulk ingest in `ingestor.py`, `typed_ingestor.py`, and the background link function in `upload.py`
- Thread-local DuckDB singleton in `analytics_db.py` — tables re-registered only when the 60 s TTL cache refreshes; eliminates DataFrame→Arrow copy on warm requests
- `Cache-Control: max-age=60, stale-while-revalidate=120` ASGI middleware on all `GET /api/analytics/*` responses
- `LIMIT` cap (default 200, max 1000) on `GET /api/analytics/co-occurrence`
- `asyncio.Semaphore(3)` guard on background link tasks in `upload.py`
- `@tanstack/react-query@^5`; `web/app/providers.tsx` with SSR-safe `QueryClientProvider` (`staleTime: 60 s`)
- `scripts/perf/compare_benchmarks.py` — side-by-side diff of ingest or endpoint JSON snapshots with % improvement markers
- `--output` flag on `benchmark_ingest.py` and `benchmark_endpoints.py` to avoid overwriting Phase 0 baselines

### Changed
- `valid_cols` computation moved outside the batch loop in `typed_ingestor.py` (computed once per ingest call)
- `pl.read_csv` → `pl.scan_csv(...).select([cols]).collect()` (lazy column projection) in `extractor.py` for `_process_slow_query_sql`, `_process_mongodb_slow`, and `extract_typed_slow_mongo`
- Full in-memory file read in `upload.py` replaced with 1 MB chunked streaming write to `NamedTemporaryFile`
- Row-by-row slow_mongo UPDATE loop replaced with `executemany` bulk UPDATE
- `web/app/layout.tsx` root layout wrapped in `<Providers>`
- `cache: "no-store"` → `cache: "default"` in `apiFetch()` (`web/lib/api.ts`) so browsers honour `Cache-Control: max-age=60`
- Labels fetch in `QueryDetailDrawer` converted from `useEffect` + `api.labels.list()` to `useQuery({ queryKey: ['labels'], staleTime: Infinity })`
- `seeded_raw_query` test fixture now calls `invalidate_cache("raw_query")` before and after yield
- `TestGetDuck.test_returns_duckdb_connection` updated to assert `_DuckNoClose` proxy type

### Fixed
- `KeyError: 'raw_query'` in `get_duck()` when `_load_table` is mocked in tests or `invalidate_cache()` races between the call and cache read — replaced bare `_df_cache[table][0]` with `.get()` fallback that backfills the cache entry

## [0.7.1] - 2026-04-08

### Added
- 117 new tests for `GET /api/queries/{id}/typed-detail` slow_mongo paths: hash-reconstruction fallback and collection-fuzzy fallback (`tests/test_api_queries.py`)

### Changed
- CI workflow refactored: split into two parallel jobs (`lint` and `test`); added `concurrency` group to cancel stale runs; upgraded to `setup-uv@v5` with `enable-cache: true`; switched to `uv sync --frozen` to enforce lockfile integrity; added `--tb=short` to pytest output; added `permissions: contents: read`
- `QueryDetailDrawer` — tracks active query ID in a local variable with an abort flag to prevent stale fetch results overwriting fresher ones

### Fixed
- CodeQL alert "Use of broken or weak cryptographic hashing algorithm on sensitive data" — replaced raw `hashlib.md5()` call in test helper with `hashlib.md5(..., usedforsecurity=False)`
- `ruff format` violations across 11 files (`api/analytics_db.py`, `api/routers/analytics.py`, `api/routers/queries.py`, `migration/manage.py`, and 7 `scripts/` files) caused by multi-line E501 refactors not being subsequently auto-formatted

### Chores
- Removed `instructions.md` from Git tracking (`git rm --cached`); added to `.gitignore`

## [0.7.0] - 2026-04-08

### Added
- `GET /api/queries/{id}/typed-detail` endpoint — returns the full native-column row from the matching `raw_query_*` typed table (joined via `raw_query_id` FK with text-hash fallback)
- 5 new tests in `TestGetTypedDetail` covering: 404, null data when no typed row, FK fast path, text fallback for slow SQL, and response shape
- `migration/manage.py` `partial-reset` command — truncates the 6 ingestion tables (`raw_query`, `upload_log`, and all 4 typed tables) while preserving `pattern_label`, `spl_query`, and `user`

### Changed
- **`raw_query` ingest is no longer deduplicated by time** — `_process_blockers` now includes `session_id`, `wait_type`, `command`, `head_blocker`, and `blocked_sessions_count` in `extra_metadata` so each blocked session produces a distinct hash; aggregated-format rows include `victims`, `resources`, `count`, and `lock_modes`
- `_process_blockers` aggregated path now prefers `database_name` column when `currentdbname` is absent, restoring correct `db_name` extraction for per-session format CSVs without `session_id`
- `migration/manage.py` `_DATA_TABLES` updated to include all 10 tables (`raw_query_slow_sql`, `raw_query_blocker`, `raw_query_deadlock`, `raw_query_slow_mongo`, `raw_query`, `upload_log`, `curated_query`, `pattern_label`, `spl_query`, `user`)
- Queries page (`/queries`) trimmed to 9 display columns: Env, Type, Src, Host, Database, Occ, Month, Curated, Query Details — removing Hash, Time, First/Last Seen, Created, Updated from the table view
- `QueryDetailDrawer` extended with a "Full CSV Detail" section that fetches and renders all typed-table columns when a row is clicked

### Fixed
- **Upload timeout** — `_link_typed_to_raw` background task rewrote to use `asyncio.to_thread` with a plain `sqlite3.connect(timeout=600)` connection; the heavy correlated UPDATE now runs in a worker thread using SQLite's C-level busy-wait, leaving the asyncio event loop free to accept new uploads
- **`database is locked` on `DELETE FROM upload_log`** — root cause was concurrent `open_session()` writers; resolved by the `asyncio.to_thread` approach above combined with bumping the aiosqlite engine `connect_args["timeout"]` from 30 s → 120 s
- **DuckDB `sum(VARCHAR)` BinderException on empty tables** — `analytics_db._load_table()` now reads `PRAGMA table_info` to map declared SQLite column types to correct Polars dtypes instead of defaulting all columns to `pl.Utf8` for empty tables
- **Dashboard "Total Queries" KPI ignoring filters** — switched source from `upload_log.csv_row_count` to `kpi.coverage.total_rows` (i.e. `COUNT(*) FROM raw_query WHERE <all active filters>`)
- **Monthly Trend "All types" bar using unfiltered upload_log counts** — "All types" view now uses `initialData` from the filter-aware `byMonth(filters)` call instead of `monthTypeData`

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

