# Performance Profiling Scripts

Standalone scripts for measuring pipeline performance against the live SQLite
database (`db/master.db`).  Run them from the **project root** with `uv run`:

```bash
# from: hkjc/dbPerfmHealthCheck/
uv run python scripts/perf/profile_db.py
uv run python scripts/perf/profile_bottleneck.py
uv run python scripts/perf/profile_after.py
```

All scripts resolve the project root from their own `__file__` path, so they
are safe to run from any working directory.

---

## Scripts

### `profile_db.py` — Database inventory

Prints the live DB file size, row counts for every table, and all index names.
Use this to quickly check how many rows have accumulated and whether expected
indexes are present.

**Sample output**

```
DB size: 182.6 MB
  alembic_version: 1 rows
  raw_query: 120488 rows
  raw_query_slow_mongo: 11126 rows
  ...

--- Indexes ---
  raw_query.ix_raw_query_link_key
  raw_query.ix_raw_query_query_hash
  ...
```

---

### `profile_bottleneck.py` — Link-query + hash-lookup timing

Times the two database operations that were the original performance
bottlenecks before the March 2026 optimisations:

| Operation | Before fix | After fix |
|-----------|-----------|-----------|
| `_link_typed_to_raw` correlated UPDATE | **> 10 min** | ~0.08 s |
| `SELECT 50 hashes` from 120 K rows | — | ~1 ms |

The UPDATE is always rolled back — the script is **read-only** with respect to
committed data.

**What it measures**

1. Row count of `raw_query WHERE type='slow_query_mongo'` (outer scan size).
2. Row count of `raw_query_slow_mongo WHERE raw_query_id IS NULL` (unlinked rows).
3. Wall-clock time of the correlated subquery UPDATE.
4. Wall-clock time of a 50-hash `SELECT … IN (…)` lookup on `raw_query`.

---

### `profile_after.py` — Full pipeline end-to-end benchmark

The main regression benchmark.  Measures every stage of a single-file upload
for `mongodbSlowQueriesProdFeb26.csv` (1 768 rows, ~2 MB):

| Stage | Typical time |
|-------|-------------|
| `extract_from_file` (Polars, raw) | ~0.06 s |
| `extract_typed_from_file` (Polars + `map_elements`) | ~0.25 s |
| `_normalize_sync` (pure-Python hashlib) | ~0.08 s |
| `_link_typed_to_raw` (composite index) | ~0.09 s |
| **Total per file** | **~0.5 s** |

Run this after any change to `extractor.py`, `ingestor.py`, or the upload
router to catch regressions before they reach production.

---

## Background — what was fixed (March 2026)

### Problem
Uploading the 8 Feb 2026 CSV files took **> 20 minutes**.

### Root causes

1. **`_link_typed_to_raw` — correlated full-table scan**  
   The UPDATE that sets `raw_query_id` on `raw_query_slow_mongo` did a nested
   loop: 11 062 outer rows × 78 716 `raw_query` rows = **870 M string
   comparisons** with no index on the inner table.

2. **`_normalize_sync` — DuckDB process spin-up per file**  
   Each file launched a DuckDB in-process instance taking ~3 s for startup
   alone, before processing a single row.

3. **`BATCH_SIZE = 50` + per-row `UPDATE` loop**  
   The ingestor issued up to 1 700 individual async DB round-trips for 1 655
   normalised rows.

4. **Redundant `json.loads` in `_process_mongodb_slow`**  
   `_extract_mongodb_command` was called once per row via `map_elements` even
   though `_query_key` (queryShapeHash or `ns:op`) was always non-empty and
   was always preferred as `query_details`.

### Fixes

| Fix | File | Speedup |
|-----|------|---------|
| Composite index `ix_raw_query_link_key` on `raw_query(type, host, db_name, environment, month_year)` + `CREATE INDEX IF NOT EXISTS` before every link query | `upload.py` | 870 M ops → ~11 K index probes |
| Replace DuckDB normalisation with pure-Python `hashlib.md5` loop | `ingestor.py` | 3.5 s → 0.08 s |
| `BATCH_SIZE` 50 → 1 000, single `INSERT … ON CONFLICT` per batch | `ingestor.py` | ~1 700 round-trips → 1-2 per file |
| Remove `map_elements(_extract_mongodb_command)` from raw extractor path | `extractor.py` | 1 768 `json.loads` calls eliminated |
| `asyncio.gather` for parallel extraction | `upload.py` | extraction overlaps with typed extraction |
