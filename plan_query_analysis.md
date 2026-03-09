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

39. **Remove SQLite artifacts** (once Neon is confirmed working):
    - Delete `db/master.db`
    - Remove `add_source_col.py` migration script
    - Remove `aiosqlite` from any remaining references
    - Update `README.md` to reflect PostgreSQL/Neon setup

---

### Verification

- `uv run scripts/validate_csv.py --file data/Jan2026/maxElapsedQueriesProdJan26.csv` → shows row count, column check, sample rows; no errors
- Upload same file via UI validate step → preview table shows pass badge
- Confirm upload → `inserted` count matches CSV row count; re-upload → `inserted=0, updated=N`
- Dashboard trend chart shows Jan 2026 bar; host breakdown includes `WINDB11ST01N`
- Promote a raw query row to a pattern → it appears in `/patterns` with correct tag and severity
- Context7 panel on a pattern tagged `"COLLSCAN"` surfaces MongoDB index documentation (if `NEXT_PUBLIC_CONTEXT7_ENABLED=true`)
- `GET /api/export` CSV includes `pattern_name`, `pattern_tag`, `severity`, `occurrence_count` columns

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
