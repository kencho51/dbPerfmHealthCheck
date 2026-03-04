"""
Phase 3 end-to-end integration test.
Run from project root:
    uv run python _e2e_test.py
"""
import asyncio
from pathlib import Path

from api.database import apply_pragmas, create_db_and_tables, open_session
from api.models import RawQuery
from api.services.extractor import extract_from_directory
from api.services.ingestor import compute_hash, ingest_rows
from api.services.validator import validate_directory
from sqlmodel import func, select

DATA = Path("data/Jan2026")


async def main() -> None:
    await apply_pragmas()
    await create_db_and_tables()

    # ---- Validation dry-run --------------------------------------------------
    print("=" * 60)
    print("PHASE 3 — End-to-End Integration Test")
    print("=" * 60)
    print("\n[1] Validator dry-run")
    results = validate_directory(DATA)
    for r in results:
        tag    = "OK " if r.is_valid else "ERR"
        warns  = f"warnings={len(r.warnings)}" if r.warnings else ""
        errors = f"ERRORS={len(r.errors)}" if r.errors else ""
        note   = f"  {warns} {errors}".strip()
        print(f"  [{tag}] {r.environment:<5} {r.file_type:<22} rows={r.row_count:>5}  {note}")

    # ---- Extraction ----------------------------------------------------------
    print("\n[2] Extraction")
    rows = extract_from_directory(DATA)
    print(f"  Total rows extracted : {len(rows)}")
    by_type: dict[str, int] = {}
    by_source: dict[str, int] = {}
    for row in rows:
        by_type[row["type"]]     = by_type.get(row["type"], 0) + 1
        by_source[row["source"]] = by_source.get(row["source"], 0) + 1
    for k, v in sorted(by_type.items()):
        print(f"    type={k:20}  count={v}")
    for k, v in sorted(by_source.items()):
        print(f"    source={k:18}  count={v}")

    # ---- Hash verification ---------------------------------------------------
    print("\n[3] Hash sample (first 5 rows)")
    seen: set[str] = set()
    collisions = 0
    for row in rows:
        h = compute_hash(row)
        if h in seen:
            collisions += 1
        seen.add(h)
    print(f"  Unique hashes : {len(seen)}")
    print(f"  Collisions    : {collisions}")
    for row in rows[:5]:
        h = compute_hash(row)
        host = (row["host"] or "-")[:32]
        print(f"    {h[:16]}...  host={host:<32} env={row['environment']}")

    # ---- First ingest --------------------------------------------------------
    print("\n[4] First ingest")
    async with open_session() as session:
        r1 = await ingest_rows(rows, session)
    print(f"  Inserted : {r1.inserted}")
    print(f"  Updated  : {r1.updated}")
    print(f"  Skipped  : {r1.skipped}")
    if r1.errors:
        print(f"  Errors ({len(r1.errors)}):")
        for e in r1.errors[:5]:
            print(f"    {e}")

    # ---- Second ingest (idempotency check) -----------------------------------
    print("\n[5] Re-ingest (idempotency — all should be updates)")
    async with open_session() as session:
        r2 = await ingest_rows(rows, session)
    inserted_ok = r2.inserted == 0
    updated_ok  = r2.updated  == len(rows)
    print(f"  Inserted : {r2.inserted}  {'✓' if inserted_ok else '✗ expected 0'}")
    print(f"  Updated  : {r2.updated}  {'✓' if updated_ok else f'✗ expected {len(rows)}'}")
    print(f"  Skipped  : {r2.skipped}")

    # ---- DB row count --------------------------------------------------------
    print("\n[6] DB state after ingestion")
    async with open_session() as session:
        total      = (await session.exec(select(func.count(RawQuery.id)))).one()
        max_occ    = (await session.exec(select(func.max(RawQuery.occurrence_count)))).one()
        prod_count = (await session.exec(
            select(func.count(RawQuery.id)).where(RawQuery.environment == "prod")
        )).one()
        sat_count  = (await session.exec(
            select(func.count(RawQuery.id)).where(RawQuery.environment == "sat")
        )).one()

    print(f"  Total unique rows in DB : {total}")
    print(f"  Max occurrence_count    : {max_occ}")
    print(f"  Prod rows               : {prod_count}")
    print(f"  SAT rows                : {sat_count}")
    print(f"\n  {'ALL TESTS PASSED ✓' if not r1.errors and inserted_ok and updated_ok else 'SOME CHECKS FAILED — review above'}")


if __name__ == "__main__":
    asyncio.run(main())
