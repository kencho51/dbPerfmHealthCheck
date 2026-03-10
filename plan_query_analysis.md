## Plan: Splunk Query Audit & Analysis Full-Stack App

**TL;DR** — Build a local-only full-stack app on top of the existing `dbPerfmHealthCheck` project. FastAPI serves the backend (CSV ingestion, dedup, REST API), SQLite replaces the CSV master table (better querying, dedup via hash, indexing), and Next.js + shadcn/ui delivers the dashboard. The existing `QueryExtractor` class is promoted to a shared service. The schema is designed to be ML-export-ready from day one.

> **Changelog (2026-03-03)**: Added CSV validator, Context7 integration, two-table schema design, and performance/refactoring suggestions — see sections below.

---

### Tech Stack Decision

| Layer | Choice | Reasoning |
|---|---|---|
| **Backend** | FastAPI + SQLModel + aiosqlite | Type-safe ORM, async SQLite driver, fits uv/Python 3.14 |
| **DB** | SQLite (`db/master.db`) | Zero infrastructure, file-based, handles millions of rows locally |
| **DB migrations** | Alembic | Schema versioning for future ML columns |
| **CSV parsing** | Existing `QueryExtractor` (promoted to service) | Avoid rewrite; already handles all 4 CSV types |
| **CSV validation** | `scripts/validate_csv.py` (CLI) + `POST /api/validate` (API) | Dry-run preview before ingestion |
| **Frontend** | Next.js 15 (App Router) + shadcn/ui | Rich components, RSC, type-safe |
| **Charts** | Recharts (via shadcn chart primitives) | Already bundled with shadcn |
| **Data grid** | TanStack Table v8 | Filtering, sorting, virtual scroll |
| **HTTP client** | Native `fetch` (Next.js RSC) | No extra library needed |
| **Doc / AI assist** | `@upstash/context7-sdk` (Next.js) | Pulls live library docs for in-app query suggestion panel |

---

### New Directory Layout

```
dbPerfmHealthCheck/
├── plan_query_analysis.md      ← This file
├── api/                        ← FastAPI backend
│   ├── main.py                 ← App factory, CORS, lifespan
│   ├── database.py             ← SQLite engine + session factory
│   ├── models.py               ← SQLModel: RawQuery + Pattern tables
│   ├── routers/
│   │   ├── upload.py           ← POST /api/upload (multipart CSV)
│   │   ├── validate.py         ← POST /api/validate (dry-run preview)
│   │   ├── queries.py          ← GET/PATCH /api/queries
│   │   ├── patterns.py         ← GET/POST/PATCH /api/patterns
│   │   └── analytics.py        ← GET /api/analytics/*
│   └── services/
│       ├── extractor.py        ← QueryExtractor (promoted from scripts/)
│       ├── validator.py        ← CSV schema/content validation logic
│       └── ingestor.py         ← Dedup + upsert into raw_queries table
├── web/                        ← Next.js 15 frontend
│   ├── app/
│   │   ├── page.tsx            ← Redirect → /dashboard
│   │   ├── dashboard/page.tsx  ← KPI cards + trend/distribution charts
│   │   ├── upload/page.tsx     ← Validate preview → confirm → ingest
│   │   ├── queries/page.tsx    ← Filterable raw query explorer
│   │   └── patterns/page.tsx   ← Curated pattern library + Context7 panel
│   ├── components/
│   │   ├── ui/                 ← shadcn components
│   │   └── context7-panel.tsx  ← Context7 doc/suggestion widget
│   ├── lib/
│   │   └── context7.ts         ← @upstash/context7-sdk client init
│   └── .env.local              ← CONTEXT7_API_KEY
├── db/                         ← SQLite database
│   └── master.db
├── data/                       ← Existing (Splunk CSV inputs by month)
├── output/                     ← Existing (legacy CSV export kept)
├── scripts/                    ← Existing CLI tools
│   ├── extract_all_queries_refactored.py
│   └── validate_csv.py         ← New: standalone CSV validator CLI
├── pyproject.toml              ← + fastapi, sqlmodel, aiosqlite, alembic
└── web/package.json            ← Next.js, shadcn, recharts, tanstack-table,
                                   @upstash/context7-sdk
```

---

### Implementation Steps

#### Phase 0 — Foundation

1. **Extend `pyproject.toml`** — `uv add --native-tls fastapi sqlmodel aiosqlite alembic python-multipart`

2. **Alembic setup** — `alembic init api/migrations`; create initial migration generating both tables; future ML/embedding columns via additional migrations

#### Phase 1 — Two-Table SQLite Schema (`api/models.py`)

3. **`RawQuery` table** — stores every individual row extracted from Splunk CSVs; is the source of truth for analytics:
   - `id` (int PK)
   - `query_hash` (str, unique index) — MD5 of `(source + host + db_name + environment + type + normalized_query_details)`; dedup key
   - `time` (str), `source` (str), `host` (str), `db_name` (str), `environment` (str), `type` (str)
   - `query_details` (text)
   - `month_year` (str, e.g. `"2026-01"`) — derived at ingest time for fast grouping
   - `first_seen` (datetime), `last_seen` (datetime), `occurrence_count` (int default 1)
   - `pattern_id` (int FK → `Pattern.id`, nullable) — link to curated pattern once identified
   - `created_at`, `updated_at` (datetime auto)

4. **`Pattern` table** — human-curated or auto-detected recurring/suspicious patterns; analytics and ML training use this:
   - `id` (int PK)
   - `name` (str) — short label, e.g. `"COLLSCAN on audit_log"`
   - `description` (text, nullable)
   - `pattern_tag` (str, indexed) — category, e.g. `"missing_index"`, `"bulk_delete"`, `"deadlock_hotspot"`
   - `severity` (str) — `"critical"`, `"warning"`, `"info"`
   - `example_query_hash` (str FK → `RawQuery.query_hash`) — canonical example row
   - `source` (str), `environment` (str), `type` (str) — inherited from representative raw query
   - `first_seen` (datetime), `last_seen` (datetime), `total_occurrences` (int, denormalized from joined RawQuery)
   - `notes` (text, nullable)
   - `created_at`, `updated_at`

   **Why two tables?**: `analytics.py` needs the full dataset (all raw rows) to produce correct trend lines, host breakdowns, and month-over-month comparisons — collapsing to patterns-only would lose that fidelity. The `Pattern` table is the curation layer on top, populated by promoting rows from `RawQuery` either manually via the UI or automatically by a future clustering job.

#### Phase 2 — CSV Validation

5. **Build `api/services/validator.py`** — `validate_csv(path, file_type)` function that:
   - Detects file type from filename (same logic as `QueryExtractor`)
   - Checks required columns are present (one expected-columns dict per type)
   - Samples up to 50 rows and reports: row count, null % per required column, detected `environment`, detected `type`, any rows with empty critical fields (`query_details`, `host`, `db_name`)
   - Returns a `ValidationResult` dataclass: `{is_valid, file_type, row_count, warnings: list[str], errors: list[str], sample_rows: list[dict]}`

6. **Add `scripts/validate_csv.py`** — standalone CLI wrapping `validator.py`; usable without the API server:
   ```
   uv run scripts/validate_csv.py --file data/Jan2026/maxElapsedQueriesProdJan26.csv
   uv run scripts/validate_csv.py --directory data/Jan2026
   ```
   Prints a colour-coded summary table per file + exits with code 1 if any errors

7. **Add `POST /api/validate`** router in `api/routers/validate.py` — same multipart upload as `/api/upload` but calls `validator.py` only (no DB writes); returns `ValidationResult[]`; used by the upload page's preview step

#### Phase 3 — Ingestion

8. **Promote `QueryExtractor` to `api/services/extractor.py`** — copy unchanged; remove hardcoded `PROJECT_ROOT` constants (pass paths explicitly in the API context)

9. **Build `api/services/ingestor.py`** — per extracted row: compute `query_hash` → `INSERT OR IGNORE` + increment `occurrence_count`/update `last_seen` on conflict; return `{inserted, updated}` counts

10. **Build `api/routers/upload.py`** — `POST /api/upload`: (1) run validation, reject if errors, (2) extract via `QueryExtractor`, (3) ingest; return `{filename, validated, inserted, updated, skipped}`

#### Phase 4 — Query & Pattern API

11. **Build `api/routers/queries.py`**:
    - `GET /api/queries` — filter by `environment`, `type`, `host`, `db_name`, `month_year` (multi), `pattern_id`, `search` (SQLite FTS5 on `query_details`); paginated
    - `PATCH /api/queries/{id}` — set `pattern_id` (promote to a pattern)

12. **Build `api/routers/patterns.py`**:
    - `GET /api/patterns` — filter by `severity`, `pattern_tag`, `environment`; paginated
    - `POST /api/patterns` — create a new pattern (optionally from an existing `raw_query_hash`)
    - `PATCH /api/patterns/{id}` — update `name`, `description`, `severity`, `notes`, `pattern_tag`
    - `GET /api/patterns/{id}/queries` — list all `RawQuery` rows linked to this pattern

13. **Build `api/routers/analytics.py`** — all queries run against `RawQuery` (full dataset):
    - `GET /api/analytics/summary` — counts by `{environment, type}`
    - `GET /api/analytics/by-host` — top N hosts ranked by `occurrence_count` sum
    - `GET /api/analytics/by-month` — row count per `month_year` (trend line)
    - `GET /api/analytics/by-db` — top databases by count
    - `GET /api/analytics/pattern-coverage` — % of raw rows with a linked `pattern_id`

14. **Add `GET /api/export`** — streams full `RawQuery` + joined `Pattern.name/tag/severity` as UTF-8 CSV; ML-ready

#### Phase 5 — Frontend

15. **Scaffold `web/`** — `npx create-next-app@latest web --ts --app --tailwind`; `npx shadcn@latest init`; `npm i @upstash/context7-sdk @tanstack/react-table recharts`

16. **Build `web/app/upload/page.tsx`** — two-step flow:
    - Step 1: drag-drop → `POST /api/validate` → show preview table (row count, warnings, errors per file) with pass/fail badge
    - Step 2: confirm button → `POST /api/upload` → show `{inserted, updated}` result

17. **Build `web/app/dashboard/page.tsx`** — RSC fetching `/api/analytics/*`:
    - KPI row: total raw queries, distinct hosts, months covered, patterns curated
    - Bar chart: raw query count by type
    - Pie chart: by environment
    - Line chart: by month (trend)
    - Top-10 databases table
    - Pattern coverage donut (curated vs uncurated)

18. **Build `web/app/queries/page.tsx`** — TanStack Table, server-side pagination; filter sidebar; expandable row for full `query_details`; "Promote to Pattern" action button per row

19. **Build `web/app/patterns/page.tsx`** — pattern library with severity badge, tag chip, occurrence count; click-through to linked raw queries; edit panel (name, description, severity, notes, pattern_tag)

20. **Build `web/components/context7-panel.tsx`** + `web/lib/context7.ts`:
    - Initialise `@upstash/context7-sdk` client using `CONTEXT7_API_KEY` from `.env.local`
    - Panel shown on the `patterns/` page alongside each pattern edit form
    - On `pattern_tag` change (e.g. `"missing_index"`), call `client.searchLibrary(pattern_tag, "sql")` → pick top library → `client.getContext(query_details_snippet, libraryId, { type: "txt" })` → render returned documentation/suggestions in a read-only `<pre>` block
    - Use case: when curating a pattern tagged `"COLLSCAN"`, the panel can surface relevant MongoDB indexing docs automatically
    - Note: SDK is marked WIP by Upstash — wrap all calls in `try/catch`; fail silently if unavailable; gate behind a feature flag env var `NEXT_PUBLIC_CONTEXT7_ENABLED=true`

---

**Phase 6 — pandas → Polars Migration**

> **Background**: The current codebase uses pandas in `api/services/extractor.py` and `api/services/validator.py` for CSV reading, column detection, row iteration, and null-rate sampling. While pandas is serviceable for small monthly CSV uploads (~1–20 k rows), it exhibits known bottlenecks at scale: single-threaded execution, slow `iterrows()`, high memory overhead from NumPy-backed columns, and no lazy evaluation.
>
> Polars is written in Rust, uses the Apache Arrow memory format, parallelises column operations automatically, and provides a query optimizer through its `LazyFrame` API. Migration is additive — Polars DataFrames are interoperable with the rest of the stack (SQLite via Connector-X, pandas via `to_pandas()`/`from_pandas()`, Arrow zero-copy).
>
> **Reference**: [docs.pola.rs](https://docs.pola.rs/) · [Python API reference](https://docs.pola.rs/api/python/stable/reference/index.html) · [Coming from pandas](https://docs.pola.rs/user-guide/migration/pandas/)

#### Why Polars Gains for This Project

| Area | pandas (current) | Polars (target) | Gain |
|---|---|---|---|
| **CSV read** | `pd.read_csv()` — single-threaded, eager, full load | `pl.scan_csv()` — lazy, only reads needed columns | ~3–8× faster; lower peak RAM |
| **Row iteration** | `df.iterrows()` — Python loop, ~2 µs/row | Expression API — vectorised Rust kernel | ~50–100× faster per column op |
| **Column derivation** | `df.assign(col=lambda df_: ...)` — sequential | `df.with_columns(expr1, expr2)` — parallel | All new columns computed in one pass |
| **Null-rate sampling** | `df.isnull().mean()` — materialises full boolean frame | `df.select(pl.all().null_count() / pl.len())` — Arrow kernel | Less memory, same O(n) work |
| **Type safety** | Loose — integers silently cast to float on null | Strict — nulls stay as `null`; column type unchanged | Fewer silent conversion bugs |
| **Memory** | NumPy: ~8 bytes/int regardless of value | Arrow: bit-packed, dictionary-encoded strings | 30–60% smaller in-memory footprint for string-heavy CSVs |
| **streaming** | `chunksize` iterator — manual chunk management | `LazyFrame.collect(streaming=True)` — automatic | Simple API, handles files larger than RAM |
| **Validation sampling** | `df.sample(50)` — random, eager | `lf.limit(50).collect()` — lazy head | No need to read entire file for preview |

#### Key Conceptual Differences to Know

- **No index** — Polars has no `.loc`/`.iloc`; use `.filter(pl.col(...) == ...)` and `.select(...)` instead.
- **Expressions are lazy by default** — `pl.col("host").str.to_uppercase()` describes intent; nothing runs until `.collect()` or used inside `.with_columns()`/`.filter()`.
- **`iterrows()` is an anti-pattern** — replace row-by-row Python loops with vectorised expressions or `map_elements()` as a last resort.
- **`null` not `NaN`** — Polars uses `null` for missing data in all types; `NaN` is a floating-point value, not a missing marker.
- **Expressions run in parallel** — multiple expressions inside a single `.with_columns(...)` call are computed concurrently across CPU cores.

#### Migration Steps

21. **Install Polars**:
    ```bash
    uv add polars
    # Optional: connectorx for zero-copy SQLite → Polars reads (useful for export)
    uv add connectorx
    ```
    Remove pandas if no other dependency needs it after migration:
    ```bash
    uv remove pandas
    ```

22. **Migrate `api/services/extractor.py`** — replace `pd.read_csv` with `pl.scan_csv` and rewrite column operations:

    ```python
    # BEFORE (pandas)
    import pandas as pd
    df = pd.read_csv(path)
    for _, row in df.iterrows():
        yield {
            "host": row.get("host"),
            "query_details": str(row.get("sql_text", "")),
            ...
        }

    # AFTER (polars)
    import polars as pl
    lf = pl.scan_csv(path, infer_schema_length=500)
    df = lf.select(["host", "sql_text", ...]).collect()   # only needed cols
    for row in df.iter_rows(named=True):
        yield {
            "host": row["host"],
            "query_details": str(row["sql_text"] or ""),
            ...
        }
    ```

    For column renaming and transformations, replace `df.rename(columns={...})` with:
    ```python
    df = lf.rename({"sql_text": "query_details"}).with_columns(
        pl.col("query_details").str.strip_chars(),
        pl.col("host").str.to_lowercase(),
    ).collect()
    ```

23. **Migrate `api/services/validator.py`** — replace the pandas `chunksize` sample approach with Polars lazy head:

    ```python
    # BEFORE (pandas)
    df_sample = pd.read_csv(path, nrows=50)
    null_rates = df_sample.isnull().mean().to_dict()
    row_count  = sum(1 for _ in pd.read_csv(path, chunksize=1000))

    # AFTER (polars)
    lf = pl.scan_csv(path, infer_schema_length=200)
    # Fast null rates over the whole file — no materialisation until .collect()
    null_rates = (
        lf.select(
            (pl.all().null_count() / pl.len()).name.suffix("_null_rate")
        ).collect()
        .to_dicts()[0]
    )
    # Row count without loading entire file
    row_count = lf.select(pl.len()).collect().item()
    # Sample rows for human preview
    sample_rows = lf.limit(50).collect().to_dicts()
    ```

24. **Replace `df.assign(...)` chains with `with_columns`** — any sequence of `df.assign(a=...).assign(b=...)` should collapse to:
    ```python
    df = df.with_columns(
        pl.col("time").str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p", strict=False).alias("parsed_time"),
        pl.col("host").str.to_lowercase().alias("host"),
    )
    ```
    Both expressions execute in parallel; a sequential `assign` chain would be two serial Python calls.

25. **Replace `df.apply()`/`df.iterrows()` in `ingestor.py`** — the `_derive_month_year()` call that runs per-row is the primary bottleneck candidate. Vectorise with `str.strptime` and `dt.strftime`:
    ```python
    # Vectorised month_year derivation — replaces per-row Python function
    df = df.with_columns(
        pl.col("time")
          .str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p", strict=False)
          .dt.strftime("%Y-%m")
          .alias("month_year")
    )
    # Rows where time format doesn't match remain null — handled gracefully
    ```
    This replaces the entire `_derive_month_year()` Python function with a single Rust kernel call across all rows simultaneously.

26. **Use streaming for large files** — if monthly CSV uploads exceed ~100 k rows, switch to streaming collect to avoid peak RAM spikes:
    ```python
    # streaming=True processes data in batches — RAM stays bounded regardless of file size
    df = pl.scan_csv(path).with_columns(...).collect(engine="streaming")
    ```
    Polars automatically picks block size; no manual `chunksize` management needed.

27. **Update `pyproject.toml` dependency group** — record intent:
    ```toml
    [project]
    dependencies = [
        "fastapi",
        "sqlmodel",
        "aiosqlite",       # or asyncpg after Phase 7
        "alembic",
        "python-multipart",
        "polars>=1.0",     # replaces pandas
        "connectorx",      # optional: SQLite → Polars zero-copy reads
    ]
    ```

28. **Verify with existing tests** — run `uv run pytest` after each file migration; the `ValidationResult` dataclass interface is unchanged so FastAPI response schemas stay the same. Key assertions to add:
    ```python
    import polars as pl
    from polars.testing import assert_frame_equal

    def test_extractor_yields_correct_columns(tmp_path):
        # write a minimal test CSV, run extractor, assert output dicts
        ...

    def test_derive_month_year_vectorised():
        df = pl.DataFrame({"time": ["1/26/2026 8:58:53 AM", None, "bad-value"]})
        result = df.with_columns(
            pl.col("time")
              .str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p", strict=False)
              .dt.strftime("%Y-%m")
              .alias("month_year")
        )
        assert result["month_year"].to_list() == ["2026-01", None, None]
    ```

#### Expected Gains Summary

| Metric | Before (pandas) | After (polars) | Notes |
|---|---|---|---|
| Ingest 10 k-row CSV | ~2–4 s | ~0.3–0.6 s | `scan_csv` + vectorised hash |
| Ingest 100 k-row CSV | ~20–40 s | ~2–4 s | streaming mode |
| Validation preview (50 rows) | ~0.8 s (full load) | ~0.05 s (`limit(50)`) | Lazy head |
| Memory peak (20 k rows, 10 cols) | ~80 MB | ~25 MB | Arrow vs NumPy |
| `_derive_month_year` (10 k rows) | ~0.5 s (Python loop) | ~3 ms (Rust kernel) | `str.strptime` vectorised |

---

**Phase 7 — Neon PostgreSQL Integration**

> **Context**: Neon project `hkjc-db-perfm` already exists (ap-southeast-1). Two branches are configured: `main` (production) and `dev` (development). The architecture keeps all DB access through the FastAPI backend — Next.js never connects to Neon directly. The FastAPI backend swaps `aiosqlite` (SQLite) → `asyncpg` (PostgreSQL).

**Branch strategy**

| Neon branch | FastAPI env | Purpose |
|---|---|---|
| `main` | `DATABASE_URL` (production) | Stable, deployed data |
| `dev` | `DATABASE_URL` (local dev) | Safe to reset / experiment |

Each branch has its own independent connection string and compute endpoint. The `dev` branch is a copy of `main` at branch time, so schemas stay in sync.

---

29. **Install VS Code Neon extension**
    - Search `Neon` in Extensions marketplace → install **Neon Database Explorer**
    - Sign in and confirm project `hkjc-db-perfm` appears with both branches

30. **Install Python PostgreSQL driver** — replace `aiosqlite` with `asyncpg`:
    ```bash
    uv remove aiosqlite
    uv add asyncpg
    ```
    `pyproject.toml` dependencies become: `fastapi`, `sqlmodel`, `asyncpg`, `alembic`, `python-multipart`

31. **Update `api/database.py`** — swap SQLite URL for PostgreSQL:
    ```python
    from sqlmodel import create_engine
    from sqlmodel.ext.asyncio.session import AsyncSession
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    import os

    DATABASE_URL = os.environ["DATABASE_URL"]
    # asyncpg requires postgresql+asyncpg:// scheme
    ASYNC_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(ASYNC_URL, echo=False, pool_size=5, max_overflow=10)
    AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def get_session():
        async with AsyncSessionLocal() as session:
            yield session
    ```

32. **Update Alembic to target Neon** — edit `alembic.ini`:
    ```ini
    # Leave blank — we set it dynamically in env.py
    sqlalchemy.url =
    ```
    In `api/migrations/env.py`, read from environment:
    ```python
    import os
    from api.models import SQLModel   # so metadata is populated

    config.set_main_option(
        "sqlalchemy.url",
        os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+psycopg2://", 1),
    )
    target_metadata = SQLModel.metadata
    ```
    Install sync driver for Alembic (Alembic doesn't support asyncpg in the CLI):
    ```bash
    uv add psycopg2-binary
    ```

33. **Set environment variables**

    **Backend** — create `api/.env` (gitignored):
    ```dotenv
    # DEV branch (local development)
    DATABASE_URL=postgresql://neondb_owner:npg_zxZFetn6S3rP@ep-orange-meadow-a1p2p3mi.ap-southeast-1.aws.neon.tech/neondb?sslmode=require
    ```
    Load in `api/main.py` lifespan:
    ```python
    from dotenv import load_dotenv
    load_dotenv("api/.env")
    ```
    Add `python-dotenv` dependency: `uv add python-dotenv`

    **Frontend** — `web/.env.local` (already gitignored by Next.js):
    ```dotenv
    NEXT_PUBLIC_API_BASE=http://localhost:8000
    ```
    The frontend never connects to Neon directly — it calls FastAPI only.

34. **Run Alembic migration against Neon** — generate and apply the initial schema:
    ```bash
    # Ensure DATABASE_URL is set in shell for the Alembic CLI
    $env:DATABASE_URL = "postgresql://neondb_owner:npg_zxZFetn6S3rP@ep-orange-meadow-a1p2p3mi.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

    # Generate first migration from current SQLModel metadata
    uv run alembic revision --autogenerate -m "initial schema"

    # Apply to Neon dev branch
    uv run alembic upgrade head
    ```
    Verify in the Neon Console → **Tables** tab that `raw_query`, `pattern_label`, and `curated_query` tables exist.

35. **Seed labels into Neon**:
    ```bash
    uv run python -m api.seed_labels
    ```

36. **Install Neon serverless driver in Next.js** (optional, for future direct queries from Server Components):
    ```bash
    cd web
    npm install @neondatabase/serverless
    ```
    Create `web/lib/neon.ts`:
    ```typescript
    import { neon } from "@neondatabase/serverless";

    // Only used server-side (Server Components / Server Actions)
    export const sql = neon(process.env.DATABASE_URL!);
    ```
    Add to `web/.env.local` if direct queries are needed:
    ```dotenv
    DATABASE_URL=postgresql://neondb_owner:npg_zxZFetn6S3rP@ep-orange-meadow-a1p2p3mi.ap-southeast-1.aws.neon.tech/neondb?sslmode=require
    ```
    > **Note**: Keep `DATABASE_URL` server-only (no `NEXT_PUBLIC_` prefix). All data mutations go through FastAPI; `@neondatabase/serverless` is only used for lightweight read-only Server Component queries if needed.

37. **Update `next.config.ts`** — add the Neon serverless driver WebSocket config required for Edge/serverless runtimes:
    ```typescript
    import type { NextConfig } from "next";

    const nextConfig: NextConfig = {
      async rewrites() {
        return [{ source: "/api/:path*", destination: "http://localhost:8000/api/:path*" }];
      },
      serverExternalPackages: ["@neondatabase/serverless"],
    };

    export default nextConfig;
    ```

38. **Branch workflow for ongoing development**:
    - All local development runs against the `dev` Neon branch — safe to run migrations and seed without affecting `main`
    - When a new Alembic migration is ready:
      1. Test against `dev`: `uv run alembic upgrade head`
      2. Verify app behaviour
      3. In Neon Console → **Branches**, merge or apply the same migration to `main` before production deploy
    - To reset `dev` to match `main`: Neon Console → **Branches** → `dev` → **Reset from parent**

39. **Keep SQLite artifacts for future development** (once Neon is confirmed working):
    - Update `README.md` to reflect PostgreSQL/Neon setup

---

**Phase 8 — Advanced Analytics & Query Pattern Intelligence**

> **Background**: With Polars powering the ingestion layer and the full `raw_query` dataset available in the DB, the next analytics tier focuses on *time-based patterns* and *query structural similarity* — the two questions DBAs ask most: "When does load peak?" and "Which query keeps reappearing?". All new endpoints are added to `api/routers/analytics.py` and consume existing ingested data with zero schema changes.

#### 8A — Peak Hour Heatmap

The primary use case: identify whether slow queries cluster around morning batch jobs, end-of-day reporting, midnight maintenance windows, or real-time wagering load. A 24×7 grid (hour × weekday) makes this instantly visible.

| Endpoint | Description |
|---|---|
| `GET /api/analytics/by-hour` | Returns `{hour, weekday, count}` cells; Polars parses mixed `time` formats server-side |

**Frontend**: `HourHeatmap` component — CSS grid, no extra library. Color scale: white (0 events) → deep indigo (max). Tooltip shows exact day + hour + count on hover.

**Time parsing strategy**: `raw_query.time` stores raw Splunk strings in 3+ formats (`ISO+TZ`, `M/D/YYYY h:MM:SS AM/PM`, ISO without TZ). The endpoint:
1. Fetches all `(time, occurrence_count)` rows matching filters
2. Strips trailing `+HHMM` / `+HH:MM` timezone offsets via Polars `str.replace`
3. Tries 7 datetime formats in order using `pl.coalesce(str.strptime(..., strict=False), ...)`
4. Groups by `(hour, weekday)` and sums `occurrence_count`

40. **Add `GET /api/analytics/by-hour`** in `api/routers/analytics.py`:
    - Accepts same filter params as other analytics endpoints
    - Uses Polars in-process to parse `time` strings and extract `dt.hour()` + `dt.weekday()`
    - Returns `list[{hour: int, weekday: int, count: int}]` (168 cells max, sparse — missing cells mean zero)

41. **Add `HourHeatmap` component** in `web/components/HourHeatmap.tsx`:
    - Fetches `/api/analytics/by-hour` with current dashboard filters
    - Renders a 24 (rows) × 7 (cols) CSS grid
    - Color intensity: `count / max_count` mapped to 7 opacity levels (Tailwind `bg-indigo-{100..700}`)
    - Row labels: `0h`–`23h` on left; column headers: Mon–Sun
    - Tooltip: `{Day}, {Hour}:00 — {N} events` on hover

42. **Wire into dashboard** — add below the monthly trend row; accepts `filters` prop identical to `MonthlyTrendCard`

#### 8B — Query Fingerprint Top-N

Identical query structures called with different parameters (different IDs, dates, amounts) appear as hundreds of distinct rows in `raw_query`. Fingerprinting normalises literals away to reveal the true repeat-offender queries.

**Fingerprint algorithm** (server-side, Polars):
```python
pl.col("query_details")
  .str.replace_all(r"'[^']*'", "'?'")      # string literals
  .str.replace_all(r"\b\d+\b", "?")        # numeric literals
  .str.replace_all(r"@P\?+", "@P?")        # param placeholders (already normalised)
  .str.replace_all(r"\s+", " ")            # collapse whitespace
  .str.to_lowercase()
  .str.slice(0, 300)                        # cap fingerprint length
  .alias("fingerprint")
```

43. ✅ **Add `GET /api/analytics/top-fingerprints`** — fetches `(query_details, occurrence_count, type, source, host, db_name)`, normalises to fingerprints in Polars, groups by fingerprint, returns top-N `{fingerprint, count, row_count, by_type, example_host, example_db}` sorted by `count` desc. Supports all standard filters (`environment`, `source`, `host`, `db_name`, `month_year`, `type`) plus `top_n` (default 20, max 200).

44. ✅ **Add `TopFingerprintsTable`** component — full-width card with `<select>` type filter + Top-N picker (10/20/50). Table shows rank, fingerprint (mono, line-clamp-2, click to expand full text + type breakdown), type badges, occurrence count, row count, example host, example database. Uses `React.Fragment key={idx}` for expand/collapse detail row pairs.

#### 8C — P50/P95 Occurrence Distribution per Host

Averages hide heavy-tail problems. A host with P50=1 but P95=800 has a few catastrophically repeated queries that average out to look normal. Polars `quantile()` makes this a single aggregation.

45. **Add `GET /api/analytics/host-stats`** — returns `{host, p50, p95, p99, max, total_occurrences, row_count}` per host; Polars aggregation over `occurrence_count` grouped by `host`; no new DB columns needed

46. **Add host stats row to dashboard** — small table below Top Hosts bar chart; cells coloured by P95 threshold (green < 10, amber 10–100, red > 100)

#### 8D — Blocker + Deadlock Co-occurrence

Hosts with both blocking and deadlocking events within the same clock-hour are the most critical infrastructure problems — they indicate resource contention building to actual lock cycles.

47. **Add `GET /api/analytics/co-occurrence`** — self-join `raw_query` on `host` where one row is `type=blocker` and another is `type=deadlock` within the same `month_year`; returns `{host, month_year, blocker_count, deadlock_count, combined_score}` sorted by combined_score desc

48. **Add `CoOccurrenceTable`** component — compact host × event-type matrix; red badge when both > 0 in same month

#### 8E — Month-over-Month Delta

Absolute counts are less actionable than trend direction. A database with 500 slow queries/month is fine if it was 800 last month; alarming if it was 200.

49. **Extend `GET /api/analytics/by-month`** — add a `delta` field: `row_count - previous_month_row_count`; computed as a window function `LAG(row_count, 1)` over `month_year`; or computed in Polars with `df.sort("month_year").with_columns(pl.col("row_count").diff().alias("delta"))`

50. **Update `MonthlyTrendCard`** — overlay delta as a dashed secondary line or add colour coding to bars (green = decreasing, red = increasing vs prior month)

#### Implementation Priority

| Feature | Effort | DBA Value | Build Order |
|---|---|---|---|
| Peak hour heatmap (8A) | Medium | High — scheduling insight | **1st** (done below) |
| Query fingerprint top-N (8B) | Medium | Very high — finds repeat offenders | 2nd |
| P50/P95 per host (8C) | Low | Medium — needs explanation | 3rd |
| MoM delta (8E) | Low | Medium — trend direction | 4th |
| Co-occurrence (8D) | High | High — critical infra signal | 5th |



- `uv run scripts/validate_csv.py --file data/Jan2026/maxElapsedQueriesProdJan26.csv` → shows row count, column check, sample rows; no errors
- Upload same file via UI validate step → preview table shows pass badge
- Confirm upload → `inserted` count matches CSV row count; re-upload → `inserted=0, updated=N`
- Dashboard trend chart shows Jan 2026 bar; host breakdown includes `WINDB11ST01N`
- Promote a raw query row to a pattern → it appears in `/patterns` with correct tag and severity
- Context7 panel on a pattern tagged `"COLLSCAN"` surfaces MongoDB index documentation (if `NEXT_PUBLIC_CONTEXT7_ENABLED=true`)
- `GET /api/export` CSV includes `pattern_name`, `pattern_tag`, `severity`, `occurrence_count` columns

---

**Phase 9 — Cloudflare Workers / Pages Migration**

> **Feasibility verdict: Workable with targeted rewrites.** Both FastAPI and Next.js have official first-party Cloudflare support. The two non-trivial blockers are (1) **Polars** — which uses native Rust extensions incompatible with Pyodide/WebAssembly — and (2) **asyncpg** — which requires native binaries. Both are solvable by pushing aggregation logic into SQL (D1 or Neon HTTP) and using Neon's HTTPS query API respectively. Python Workers are in **open beta** — appropriate for an internal tool but not a public production service.

#### Architecture After Migration

Two database options are viable — choose based on whether you want to keep the Neon PostgreSQL investment from Phase 7:

```
┌──────────────────────────────────────────────────────────────────────┐
│                       Cloudflare Edge Network                         │
│                                                                        │
│  ┌──────────────────────────┐  ┌────────────────────────────────────┐  │
│  │  Workers (Next.js)        │  │  Workers (FastAPI Python)           │  │
│  │  @opennextjs/cloudflare   │  │  WorkerEntrypoint + asgi.fetch()   │  │
│  │  App Router / RSC / SSR   │  │  pywrangler CLI                    │  │
│  │  → /api/* (Service Bind)  │──▶  POST /api/upload                  │  │
│  └──────────────────────────┘  │  GET  /api/analytics/*              │  │
│                                 │  GET  /api/queries                  │  │
│                                 └──────────┬──────────────────────────┘  │
│                                            │                              │
│  Option A (simpler)           Option B (keep Neon from Phase 7)          │
│  ┌──────────────────┐         ┌────────────▼───────────────────────┐     │
│  │  Cloudflare D1   │         │  Cloudflare Hyperdrive              │     │
│  │  (SQLite-compat) │         │  (edge connection pool → Neon PG)  │     │
│  │  raw_query       │         │  pg8000 pure-Python driver          │     │
│  │  pattern_label   │         └────────────┬───────────────────────┘     │
│  │  curated_query   │                      │                              │
│  └──────────────────┘              ┌───────▼──────────────────────┐      │
│                                    │  Neon PostgreSQL (ap-se-1)    │      │
│                                    │  hkjc-db-perfm / dev branch  │      │
│                                    └──────────────────────────────┘      │
└──────────────────────────────────────────────────────────────────────────┘
```

#### Compatibility Matrix

| Component | Current | After Migration | Notes |
|---|---|---|---|
| **Next.js frontend** | `npm run dev` locally | `@opennextjs/cloudflare` Worker | App Router, RSC, SSR all ✅ supported |
| **FastAPI backend** | `uvicorn` process | Python Worker + `asgi.fetch()` | FastAPI is officially supported package |
| **SQLite / aiosqlite** | Local file `db/master.db` | Cloudflare **D1** binding | D1 is SQLite-wire-compatible; schema unchanged |
| **asyncpg (Neon)** | Native binary driver | **Hyperdrive + `pg8000`** (Option B) or replace with D1 (Option A) | `asyncpg` needs native extension → blocked; `pg8000` is pure Python |
| **Polars** | In-process analytics | **Replaced by D1 SQL** | Native Rust extension → Pyodide blocked ❌ |
| **SQLModel ORM** | Full async engine | Models/schemas only; D1 calls raw | SQLite engine replaced by D1 binding |
| **Alembic** | Schema migrations | Option A: `wrangler d1 execute` scripts · Option B: Alembic still runs locally against Neon (unchanged from Phase 7) | Alembic CLI can't run inside a Worker |
| **python-multipart** | Multipart CSV uploads | Pure-Python ✅ | No native extensions |
| **Pydantic** | Request/response validation | ✅ Officially supported | |
| **R2** | n/a (uploads direct to DB) | Optional: buffer large CSVs | Workers have 100 MB request limit |

#### Key Blockers and Mitigations

**Blocker 1 — Polars** (hard)
Python Workers run on Pyodide, a WebAssembly port of CPython. Packages with native compiled extensions (Rust, C) that are not pre-compiled for WASM cannot be loaded. Polars is Rust-compiled and is not in Pyodide's supported package list.

*Mitigation*: Move all aggregation currently done in Polars **into D1 SQL**:
- `by-hour` time parsing → use SQLite `strftime('%H', time)` with a `CASE` for the AM/PM format variant (or pre-derive `hour`/`weekday` columns at ingest time)
- Fingerprinting → SQLite `REPLACE(REPLACE(...))` expressions or a lightweight pure-Python regex normaliser
- `by-month`, `by-host`, `by-db` → already plain `GROUP BY` queries; no Polars needed today
- Null-rate sampling in validator → pure Python with stdlib `csv` module (`itertools.islice`)

**Blocker 2 — asyncpg** (medium)
asyncpg requires `libpq` native binary, not available in Pyodide.

*Mitigation (Option A — D1)*: Replace Neon/asyncpg entirely with Cloudflare D1. The SQLite-compatible wire protocol means the existing schema and queries work largely as-is. Simpler setup, no external dependency.

*Mitigation (Option B — Neon + Hyperdrive + pg8000)*: **Recommended if you want to keep the Neon PostgreSQL investment from Phase 7.** Cloudflare Hyperdrive acts as an edge-local connection pool that speaks standard PostgreSQL wire protocol — it exposes a regular `postgres://` connection string to your Worker. The Python Worker connects to it using **`pg8000`**, a fully pure-Python PostgreSQL driver with zero native extensions (Pyodide-compatible):
```python
import pg8000.native, os

def get_conn():
    # env.HYPERDRIVE.connectionString is injected by the Hyperdrive binding
    # Format: postgres://user:pass@hyperdrive-host:5432/dbname
    return pg8000.native.Connection(
        host=..., port=5432, database=...,
        user=..., password=..., ssl_context=True,
    )
```
Hyperdrive maintains a warm connection pool across Cloudflare's PoPs, so each Worker request avoids the full TLS + PostgreSQL handshake overhead. Neon's branch strategy (Phase 7) is fully preserved.

> **Note**: Cloudflare recommends `node-postgres` (`pg`) for JS Workers — for Python Workers use `pg8000` against the same Hyperdrive connection string. The Hyperdrive binding is database-driver-agnostic.

**Blocker 3 — Python Workers open beta** (low risk for internal tool)
The `python_workers` compatibility flag is required and the feature is explicitly beta. Cloudflare's own FastAPI example works correctly; the risk is API churn before GA.

---

#### Migration Steps

**Sub-phase 9A — Frontend (Next.js → Cloudflare Workers)**

51. **Install OpenNext Cloudflare adapter**:
    ```bash
    cd web
    npm i @opennextjs/cloudflare@latest
    npm i -D wrangler@latest
    ```

52. **Create `web/wrangler.jsonc`**:
    ```jsonc
    {
      "$schema": "./node_modules/wrangler/config-schema.json",
      "name": "hkjc-dbperfm-web",
      "main": ".open-next/worker.js",
      "compatibility_date": "2026-03-10",
      "compatibility_flags": ["nodejs_compat"],
      "assets": {
        "directory": ".open-next/assets",
        "binding": "ASSETS"
      },
      "services": [
        {
          "binding": "API",
          "service": "hkjc-dbperfm-api"
        }
      ]
    }
    ```
    The `services` binding wires the frontend Worker directly to the backend Python Worker — no public internet hop between them, no CORS required.

53. **Create `web/open-next.config.ts`**:
    ```typescript
    import { defineCloudflareConfig } from "@opennextjs/cloudflare";
    export default defineCloudflareConfig();
    ```

54. **Update `web/package.json` scripts**:
    ```json
    {
      "scripts": {
        "dev":     "next dev",
        "build":   "next build",
        "preview": "opennextjs-cloudflare build && opennextjs-cloudflare preview",
        "deploy":  "opennextjs-cloudflare build && opennextjs-cloudflare deploy",
        "cf-typegen": "wrangler types --env-interface CloudflareEnv cloudflare-env.d.ts"
      }
    }
    ```

55. **Update `web/next.config.ts`** — remove the local `uvicorn` rewrite; in Workers, service bindings handle routing instead:
    ```typescript
    import type { NextConfig } from "next";
    const nextConfig: NextConfig = {
      // Rewrites only needed for local dev (npm run dev)
      // In Cloudflare preview/deploy, the service binding routes /api/*
      ...(process.env.NODE_ENV === "development" ? {
        async rewrites() {
          return [{ source: "/api/:path*", destination: "http://localhost:8000/api/:path*" }];
        },
      } : {}),
    };
    export default nextConfig;
    ```

56. **Test locally with Cloudflare adapter**:
    ```bash
    npm run preview   # builds + starts miniflare (Cloudflare local sim)
    ```
    Verify all dashboard pages load and `/api/*` calls reach the backend.

57. **Deploy frontend Worker**:
    ```bash
    npm run deploy
    ```
    Note the deployed URL (e.g. `hkjc-dbperfm-web.workers.dev`).

---

**Sub-phase 9B — Database** (choose Option A or Option B)

---

**Option A — Cloudflare D1** (simpler, recommended if dropping Neon)

58a. **Create D1 database**:
    ```bash
    npx wrangler d1 create hkjc-dbperfm-db
    ```
    Note the `database_id` from the output. Add to both `web/wrangler.jsonc` and the API `wrangler.jsonc` (step 62):
    ```jsonc
    "d1_databases": [
      { "binding": "DB", "database_name": "hkjc-dbperfm-db", "database_id": "<id>" }
    ]
    ```

59a. **Export existing SQLite schema** from `db/master.db`:
    ```bash
    sqlite3 db/master.db .schema > db/schema.sql
    ```
    Apply to D1:
    ```bash
    npx wrangler d1 execute hkjc-dbperfm-db --file db/schema.sql
    ```

60a. **Migrate existing data** (one-time):
    ```bash
    sqlite3 db/master.db .dump > db/data.sql
    grep "^INSERT" db/data.sql > db/inserts.sql
    npx wrangler d1 execute hkjc-dbperfm-db --file db/inserts.sql
    ```

61a. **Local development** — D1's local simulation:
    ```bash
    npx wrangler d1 execute hkjc-dbperfm-db --local --file db/schema.sql
    ```
    Wrangler stores the local D1 as a SQLite file in `.wrangler/state/` — identical behaviour to production. Skip to Sub-phase 9C after this step.

---

**Option B — Neon PostgreSQL + Cloudflare Hyperdrive** (recommended if keeping Phase 7 Neon investment)

58b. **Create a dedicated Hyperdrive user in Neon**:
    - In Neon Console → your project → **Roles** → **New Role** → name it `hyperdrive-user`, copy the password
    - Go to **Dashboard** → **Connection Details** → select branch (`dev` or `main`), database `neondb`, role `hyperdrive-user` → uncheck **Connection pooling** → copy the `postgres://` connection string

59b. **Create a Hyperdrive configuration** in the Cloudflare dashboard:
    - Go to **Workers & Pages** → **Hyperdrive** → **Create Configuration**
    - Paste the Neon connection string and name it `hkjc-neon`
    - Note the `hyperdrive_id` shown after creation

    Or via CLI:
    ```bash
    npx wrangler hyperdrive create hkjc-neon \
      --connection-string "postgres://hyperdrive-user:<password>@<neon-host>/neondb?sslmode=require"
    ```

60b. **Add Hyperdrive binding to the API Worker's `wrangler.jsonc`**:
    ```jsonc
    {
      "name": "hkjc-dbperfm-api",
      "compatibility_flags": ["python_workers", "nodejs_compat"],
      "hyperdrive": [
        { "binding": "HYPERDRIVE", "id": "<your-hyperdrive-id>" }
      ]
    }
    ```

61b. **Install `pg8000`** (pure-Python PostgreSQL driver, Pyodide-compatible):
    ```bash
    uv add pg8000
    ```
    Replace `api/database.py`'s asyncpg engine with a `pg8000.native` connection factory:
    ```python
    # api/database.py — Hyperdrive + pg8000 version
    import pg8000.native
    from urllib.parse import urlparse
    from fastapi import Request

    def get_conn(request: Request):
        """Dependency: opens a pg8000 connection via Hyperdrive."""
        # Hyperdrive injects a standard postgres:// connection string
        cs = request.scope["env"].HYPERDRIVE.connectionString
        p = urlparse(cs)
        return pg8000.native.Connection(
            host=p.hostname, port=p.port or 5432,
            database=p.path.lstrip("/"),
            user=p.username, password=p.password,
            ssl_context=True,
        )
    ```
    All existing router SQL stays unchanged (Neon is PostgreSQL; queries are identical). Replace `session.exec(select(...))` calls with `conn.run(sql, params)` returning lists of tuples:
    ```python
    # BEFORE (SQLModel)
    rows = (await session.exec(select(RawQuery).where(...))).all()

    # AFTER (pg8000)
    rows = conn.run("SELECT * FROM raw_query WHERE environment = :env", env=environment)
    ```
    Neon's schema from Phase 7 (Alembic migrations) is already applied — **no data migration needed**.

61c. **Local development** with Hyperdrive:
    Hyperdrive has no local emulation — during local `pywrangler dev`, connect directly to the Neon `dev` branch instead:
    ```python
    # In pywrangler dev, HYPERDRIVE binding is absent; fall back to direct Neon URL
    import os
    cs = getattr(getattr(request.scope.get("env"), "HYPERDRIVE", None), "connectionString", None) \
        or os.environ["DATABASE_URL"]
    ```
    Set `DATABASE_URL` in `api/.env` pointing to the Neon `dev` branch (already configured in Phase 7).

---

**Sub-phase 9C — Backend (FastAPI → Python Worker)**

62. **Initialise Python Worker project**:
    ```bash
    # From project root (not web/)
    uv tool install workers-py
    uv run pywrangler init
    ```
    This creates a `wrangler.toml` for the Python Worker. Rename/edit to `wrangler.jsonc`:
    ```jsonc
    {
      "name": "hkjc-dbperfm-api",
      "main": "api/worker.py",
      "compatibility_date": "2026-03-10",
      "compatibility_flags": ["python_workers"],
      "d1_databases": [
        { "binding": "DB", "database_name": "hkjc-dbperfm-db", "database_id": "<id>" }
      ]
    }
    ```

63. **Create `api/worker.py`** — the Worker entrypoint that bridges FastAPI via ASGI:
    ```python
    from workers import WorkerEntrypoint
    import asgi
    # Import the existing FastAPI app — no changes needed to app definition
    from api.main import app

    class Default(WorkerEntrypoint):
        async def fetch(self, request):
            return await asgi.fetch(app, request, self.env)
    ```
    The existing `api/main.py` FastAPI app, routers, and models are reused unchanged. Only the database access layer changes (see step 64).

64. **Replace `api/database.py`** — swap `aiosqlite`/`asyncpg` engine for D1 binding access:
    ```python
    # api/database.py — D1 version
    # D1 is accessed via the Workers binding (self.env.DB) injected into the ASGI scope.
    # FastAPI dependencies retrieve it from request.scope["env"].

    from fastapi import Request

    def get_db(request: Request):
        """Dependency: returns the D1 binding from the Worker environment."""
        return request.scope["env"].DB

    # Usage in routers:
    # async def my_route(db = Depends(get_db)):
    #     result = await db.prepare("SELECT * FROM raw_query WHERE id = ?").bind(42).first()
    ```

65. **Rewrite DB calls in all routers** — replace SQLModel `session.exec(select(...))` with D1 binding calls. Pattern per router:

    ```python
    # BEFORE (SQLModel + aiosqlite)
    stmt = select(RawQuery).where(RawQuery.environment == environment)
    rows = (await session.exec(stmt)).all()

    # AFTER (D1 binding)
    sql = "SELECT * FROM raw_query WHERE environment = ?"
    result = await db.prepare(sql).bind(environment).all()
    rows = result.results  # list of dicts
    ```

    D1 Python binding API mirrors the JS API:
    - `db.prepare(sql)` → `PreparedStatement`
    - `.bind(*params)` → parameterised
    - `.first()` → single row dict or None
    - `.all()` → `{results: list[dict], meta: dict}`
    - `.run()` → for INSERT/UPDATE/DELETE (returns `{meta}`)

66. **Replace Polars analytics with SQL aggregations** — the `analytics.py` `by-hour` endpoint is the main rewrite. Push all date extraction and grouping into D1 SQL:

    ```python
    # BEFORE (Polars in-process parsing)
    # — fetched raw time strings, parsed in Polars, grouped in Python

    # AFTER (D1 SQL with SQLite strftime)
    # SQLite can parse ISO timestamps natively with strftime.
    # For Splunk's US AM/PM format, add a computed column at ingest time (see step 67).

    sql = """
        SELECT
            CAST(strftime('%H', parsed_time) AS INTEGER) AS hour,
            CAST((strftime('%w', parsed_time) + 6) % 7 AS INTEGER) AS weekday,
            SUM(occurrence_count) AS count
        FROM raw_query
        WHERE parsed_time IS NOT NULL
        GROUP BY hour, weekday
        ORDER BY hour, weekday
    """
    result = await db.prepare(sql).all()
    ```

67. **Add `parsed_time` column at ingest time** — store a normalised ISO timestamp alongside the raw `time` string so D1's `strftime` can use it reliably. This replaces the Polars multi-format parsing:

    ```python
    # api/services/ingestor.py — add normalised timestamp at ingest
    import re
    from datetime import datetime

    _FORMATS = [
        "%m/%d/%Y %I:%M:%S %p",   # Splunk US: 1/26/2026 8:58:53 AM
        "%Y-%m-%dT%H:%M:%S",       # ISO without TZ
        "%Y-%m-%d %H:%M:%S",       # ISO space-separated
    ]

    def parse_time(raw: str | None) -> str | None:
        if not raw:
            return None
        # Strip trailing TZ offset (+0800, +08:00, -05:00)
        clean = re.sub(r"[+-]\d{2}:?\d{2}$", "", raw.strip())
        for fmt in _FORMATS:
            try:
                return datetime.strptime(clean, fmt).isoformat()
            except ValueError:
                continue
        return None
    ```
    Add `parsed_time TEXT` column to the schema and populate it during `INSERT` in `ingestor.py`. This is a one-time schema migration (step 59 already runs the schema, so add the column there).

68. **Handle `by_type`/`top_hosts`/`top_dbs` breakdown** for heatmap tooltip — replace the Polars groupby approach with separate SQL queries per dimension:
    ```python
    # Fetch all three breakdowns in parallel using D1 batch()
    batch_result = await db.batch([
        db.prepare(hour_weekday_total_sql),
        db.prepare(by_type_sql),
        db.prepare(top_hosts_sql),
    ])
    ```
    D1's `db.batch()` sends multiple statements in a single HTTP round-trip to the D1 edge node.

69. **Remove Polars from `pyproject.toml`**:
    ```bash
    uv remove polars
    ```
    The D1 approach is actually simpler — no in-process computation, all aggregation happens at the database layer where it belongs.

70. **Rewrite `api/services/validator.py`** — replace Polars lazy CSV reading with stdlib:
    ```python
    import csv, io
    from itertools import islice

    def validate_csv(content: bytes, filename: str) -> ValidationResult:
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
        sample = list(islice(reader, 50))
        # null rates over sample
        if sample:
            null_rates = {
                col: sum(1 for r in sample if not r.get(col)) / len(sample)
                for col in sample[0].keys()
            }
        ...
    ```
    Row count requires reading the whole file — acceptable for typical CSV sizes (< 5 MB); Workers have a 30-second CPU time limit on the paid tier.

71. **Test Python Worker locally**:
    ```bash
    uv run pywrangler dev
    ```
    Verify all API endpoints respond correctly against the local D1 simulation. Check `/api/analytics/by-hour`, `/api/upload`, and `/api/queries`.

72. **Deploy Python Worker**:
    ```bash
    uv run pywrangler deploy
    ```

---

**Sub-phase 9D — Wiring, Environment Variables, CI/CD**

73. **Set Worker secrets** — add Neon or other secrets (if kept) via Wrangler:
    ```bash
    # For the API Worker
    npx wrangler secret put NEON_API_KEY      # if using Neon HTTP fallback
    # For the frontend Worker
    npx wrangler secret put CONTEXT7_API_KEY   # if Context7 enabled
    ```
    D1 is accessed via binding (no credential needed — it's scoped to your Cloudflare account).

74. **Configure service binding** — the frontend Worker calls the API Worker directly (no public URL needed):
    In `web/wrangler.jsonc`, the `"services"` binding (step 52) already routes `API.*` calls to the Python Worker. Update `web/lib/api.ts` base URL logic:
    ```typescript
    // In Cloudflare Workers environment, NEXT_PUBLIC_API_BASE is not needed —
    // the service binding handles it. Keep the env var only for local dev.
    const CLIENT_BASE = process.env.NEXT_PUBLIC_API_BASE
      ? `${process.env.NEXT_PUBLIC_API_BASE}/api`
      : "/api";
    ```

75. **Set up Workers Builds (CI/CD)** — connect the GitHub repo in the Cloudflare dashboard:
    - **Frontend**: Cloudflare Pages/Workers Builds → root dir `web/` → build command `npm run deploy` → env vars `NEXT_PUBLIC_API_BASE` (blank — uses service binding in prod)
    - **Backend**: Workers Builds → root dir `.` → build command `uv run pywrangler deploy`
    - D1 migrations run as a pre-deploy step: `npx wrangler d1 execute hkjc-dbperfm-db --file db/schema.sql`

76. **Local development workflow post-migration**:
    ```bash
    # Terminal 1: API Worker (local D1 simulation)
    uv run pywrangler dev

    # Terminal 2: Next.js dev server (proxies /api/* to localhost:8787)
    cd web && npm run dev
    ```
    This keeps the fast Next.js HMR dev experience while testing against real Worker behaviour.

    For full end-to-end Cloudflare simulation (both Workers + D1):
    ```bash
    cd web && npm run preview    # builds OpenNext + starts miniflare
    # Also start: uv run pywrangler dev  (in another terminal)
    ```

---

#### Limitations and Remaining Risks

| Item | Risk | Mitigation |
|---|---|---|
| **Python Workers open beta** | API may change before GA | Pin `pywrangler` version; follow #python-workers Discord channel |
| **D1 row limits** | D1 free tier: 100k writes/day, 5M reads/day; Paid: 50M writes | Batch upserts (single `executeBatch`); acceptable for internal monthly uploads |
| **Worker CPU time** | 10ms free / 30s paid (per request) | Paid plan needed; CSV validator with 10k-row file well within 30s |
| **Bundle size** | Python Worker packages counted toward CPU startup time | `pywrangler` pre-compiles packages via Pyodide; FastAPI cold start ~200ms |
| **No Polars** | Analytics expressiveness reduced | D1 SQL `GROUP BY` + `strftime` covers all current use cases; fingerprinting via pure Python |
| **Alembic** | Can't run inside a Worker | Option A: `wrangler d1 execute` for schema changes · Option B: Alembic runs locally against Neon unchanged (Phase 7 workflow preserved) |
| **File uploads > 100 MB** | Workers request body limit | Monthly Splunk CSVs are typically < 10 MB — not an issue in practice |
| **miniflare vs Pyodide differences** | Local sim may not catch all Pyodide-specific failures | Use `pywrangler dev` for Python, `npm run preview` only for frontend integration |

---

### Decisions

- **SQLite over CSV**: CSV has no dedup, no indexing, no concurrent-safe writes. SQLite with `query_hash` unique constraint handles all three; still a single file, easy to back up/copy.
- **Two tables over one**: `RawQuery` preserves every ingested row; `Pattern` is the curated intelligence layer. Analytics runs on `RawQuery` for full fidelity; Pattern is the output that is exported for ML training.
- **`occurrence_count` instead of duplicate rows**: Repeated Splunk exports of the same query increment the counter rather than bloating the table.
- **shadcn chart primitives (Recharts) over Tremor**: Tremor is deprecated in favour of shadcn/Recharts natively.
- **Existing `QueryExtractor` unchanged**: CLI scripts (`uv run scripts/extract_all_queries_refactored.py`) still work standalone; API reuses same class — no duplication.
- **Context7 gated behind a feature flag**: The SDK is explicitly marked WIP by Upstash; wrapping it in `NEXT_PUBLIC_CONTEXT7_ENABLED` prevents breakage if the API changes.
- **ML-readiness deferred but structured**: `Pattern` table has `pattern_tag`, `severity`, `total_occurrences`. `GET /api/export` yields a clean, labelled dataset. Embedding/cluster columns added via Alembic migration at ML phase.

---

### Performance & Refactoring Suggestions

#### SQLite
- **Enable FTS5** on `RawQuery.query_details` — allows full-text `MATCH` queries instead of slow `LIKE '%...%'`; create via Alembic: `CREATE VIRTUAL TABLE raw_query_fts USING fts5(query_details, content=raw_query, content_rowid=id)`
- **Add indexes** on `(environment, type, month_year)` and `(host, db_name)` — covers the most common dashboard filter combinations
- **WAL mode** — `PRAGMA journal_mode=WAL` in the SQLite lifespan hook; allows concurrent reads while a write is in progress
- **`month_year` computed at ingest** — never compute it in SQL aggregation queries; derive once from `time` in `ingestor.py` so analytics group-bys hit a plain indexed column

#### FastAPI / Python
- **Batch ingest with `INSERT OR IGNORE ... ON CONFLICT DO UPDATE`** — use SQLite's `UPSERT` syntax in a single parameterised batch statement instead of per-row Python loops; reduces round-trips by 100×
- **Stream CSV validation** using Polars lazy API (`pl.scan_csv(...).limit(50).collect()`) — avoids loading the full file for previews; use `collect(engine="streaming")` for large-file row counts. See Phase 6 for full migration steps.
- **`QueryExtractor` refactor** — replace the per-handler `for _, row in df.iterrows()` loops with Polars vectorised expressions (`with_columns`, `filter`, `select`) for ~50–100× speed improvement on large files. `iterrows()` is an anti-pattern in both pandas and Polars; the Polars expression API eliminates it entirely.
- **Typed `ValidationResult` with Pydantic** — share the same model between FastAPI response schema and the CLI `validate_csv.py` output; single source of truth
- **`asynccontextmanager` lifespan** in `api/main.py` — create DB tables and run `PRAGMA` settings once at startup; avoid per-request overhead

#### Next.js / Frontend
- **SWR or React Query** for client-side data fetching on filter/paginate interactions — avoids full page reloads; keeps RSC for initial dashboard paint
- **Debounce** the free-text search input (300 ms) before firing `GET /api/queries?search=...` to avoid hammering the API
- **Virtual scroll** (`@tanstack/react-virtual`) on the query explorer table — `raw_queries` can grow to hundreds of thousands of rows; never render all at once
- **`next.config.ts` rewrites** for `/api → http://localhost:8000` — keeps the frontend and backend on the same origin during development; no CORS preflight overhead
- **`loading.tsx` + `Suspense`** per route segment — dashboard analytics endpoints may be slow on first load; boundaries prevent the whole page from blocking

#### Codebase Structure
- **Shared `QueryType` and `EnvironmentType` enums** in `api/models.py` — currently string literals scattered across extractor, ingestor, and validators; centralise to one source
- **`conftest.py` + `pytest` with `tmp_path`** — unit test `validator.py` and `ingestor.py` against real sample CSV files in `data/`; run via `uv run pytest`
- **`.env` / `.env.local` convention** — FastAPI reads `SQLITE_PATH`, `CORS_ORIGINS` from `.env`; Next.js reads `NEXT_PUBLIC_API_BASE`, `NEXT_PUBLIC_CONTEXT7_ENABLED`, `CONTEXT7_API_KEY` from `.env.local`
