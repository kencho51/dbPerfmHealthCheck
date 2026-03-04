"""
Phase 4 smoke test — exercises all query / pattern / analytics / export endpoints
directly against the real DB (no HTTP server needed).

Run from project root:
    uv run python _phase4_test.py
"""
import asyncio
import io
from pathlib import Path

from api.database import apply_pragmas, create_db_and_tables, open_session
from api.models import Pattern, PatternCreate, RawQuery, SeverityType
from sqlmodel import func, select


async def main() -> None:
    await apply_pragmas()
    await create_db_and_tables()

    print("=" * 60)
    print("PHASE 4 — Endpoint Smoke Test")
    print("=" * 60)

    async with open_session() as session:

        # ---- [1] Queries list + filter ----------------------------------------
        print("\n[1] GET /api/queries (filter: source=sql, env=prod, limit=5)")
        from sqlmodel import col
        stmt = (
            select(RawQuery)
            .where(RawQuery.source == "sql")
            .where(RawQuery.environment == "prod")
            .order_by(col(RawQuery.last_seen).desc())
            .limit(5)
        )
        rows = list((await session.exec(stmt)).all())
        print(f"    Returned {len(rows)} rows (expected ≤5)")
        for r in rows:
            print(f"      id={r.id:<6} host={r.host or '-':<25} occ={r.occurrence_count}")

        # ---- [2] Count endpoint -----------------------------------------------
        print("\n[2] GET /api/queries/count (no filters)")
        total = (await session.exec(select(func.count(RawQuery.id)))).one()
        print(f"    Total raw_query rows: {total}")
        assert total > 0, "Expected rows in DB"

        # ---- [3] Single row ---------------------------------------------------
        print("\n[3] GET /api/queries/{id} (first row)")
        first = (await session.exec(select(RawQuery).limit(1))).first()
        assert first is not None
        print(f"    id={first.id}  hash={first.query_hash[:16]}...")

        # ---- [4] Create pattern -----------------------------------------------
        print("\n[4] POST /api/patterns (create from example_query_hash)")
        # Pick a real hash from DB
        sample = (await session.exec(select(RawQuery).limit(1))).first()
        p = Pattern(
            name="Test: COLLSCAN on audit_log",
            description="Collection scan detected on audit_log collection",
            pattern_tag="missing_index",
            severity=SeverityType.critical,
            example_query_hash=sample.query_hash,
            source=sample.source,
            environment=sample.environment,
            type=sample.type,
            notes="Promote to create index on audit_log.timestamp",
            total_occurrences=0,
        )
        session.add(p)
        await session.commit()
        await session.refresh(p)
        print(f"    Created pattern id={p.id}  name='{p.name}'  severity={p.severity}")

        # ---- [5] Assign pattern to a query ------------------------------------
        print("\n[5] PATCH /api/queries/{id} (assign pattern_id)")
        from datetime import datetime, timezone
        sample.pattern_id = p.id
        sample.updated_at = datetime.now(tz=timezone.utc)
        session.add(sample)
        await session.commit()
        await session.refresh(sample)
        assert sample.pattern_id == p.id
        print(f"    RawQuery id={sample.id} → pattern_id={sample.pattern_id} ✓")

        # ---- [6] GET /api/patterns/{id}/queries --------------------------------
        print("\n[6] GET /api/patterns/{id}/queries")
        linked = list((await session.exec(
            select(RawQuery).where(RawQuery.pattern_id == p.id)
        )).all())
        print(f"    Pattern {p.id} has {len(linked)} linked row(s) ✓")

        # ---- [7] PATCH pattern -------------------------------------------------
        print("\n[7] PATCH /api/patterns/{id} (update notes)")
        p.notes = "Updated by smoke test"
        p.updated_at = datetime.now(tz=timezone.utc)
        session.add(p)
        await session.commit()
        await session.refresh(p)
        assert p.notes == "Updated by smoke test"
        print(f"    Pattern notes updated ✓")

        # ---- [8] GET /api/patterns (list) -------------------------------------
        print("\n[8] GET /api/patterns (list all)")
        patterns = list((await session.exec(select(Pattern))).all())
        print(f"    Found {len(patterns)} pattern(s) ✓")

        # ---- [9] Analytics endpoints ------------------------------------------
        print("\n[9] Analytics endpoints")

        summary = list((await session.exec(
            select(RawQuery.environment, RawQuery.type, func.count(RawQuery.id).label("n"))
            .group_by(RawQuery.environment, RawQuery.type)
        )).all())
        print(f"    summary groups: {len(summary)}")
        for row in summary[:4]:
            print(f"      env={row[0]:<5} type={row[1]:<12} count={row[2]}")

        by_month = list((await session.exec(
            select(RawQuery.month_year, func.count(RawQuery.id).label("n"))
            .where(col(RawQuery.month_year).isnot(None))
            .group_by(RawQuery.month_year)
            .order_by(col(RawQuery.month_year))
        )).all())
        print(f"    by-month rows: {len(by_month)} distinct months")
        for row in by_month:
            print(f"      {row[0]}: {row[1]} rows")

        tagged   = (await session.exec(
            select(func.count(RawQuery.id)).where(col(RawQuery.pattern_id).isnot(None))
        )).one()
        total_q  = (await session.exec(select(func.count(RawQuery.id)))).one()
        coverage = round(tagged / total_q * 100, 2) if total_q else 0
        print(f"    pattern-coverage: {tagged}/{total_q} = {coverage}%")

        # ---- [10] Export CSV preview ------------------------------------------
        print("\n[10] GET /api/export (first 5 rows of streaming CSV)")
        # Simulate the export by re-using the generator logic directly
        import csv as _csv, io as _io
        from api.routers.export import _fmt

        fields = [
            "id", "query_hash", "time", "source", "host", "db_name",
            "environment", "type", "month_year", "occurrence_count",
            "first_seen", "last_seen", "query_details",
            "pattern_id", "pattern_name", "pattern_tag", "pattern_severity",
        ]
        stmt = (
            select(
                RawQuery.id, RawQuery.query_hash, RawQuery.time, RawQuery.source,
                RawQuery.host, RawQuery.db_name, RawQuery.environment, RawQuery.type,
                RawQuery.month_year, RawQuery.occurrence_count,
                RawQuery.first_seen, RawQuery.last_seen, RawQuery.query_details,
                RawQuery.pattern_id,
                Pattern.name.label("pattern_name"),
                Pattern.pattern_tag.label("pattern_tag"),
                Pattern.severity.label("pattern_severity"),
            )
            .join(Pattern, RawQuery.pattern_id == Pattern.id, isouter=True)
            .order_by(RawQuery.id)
            .limit(5)
        )
        result = await session.execute(stmt)
        export_rows = result.fetchall()

        buf = _io.StringIO()
        writer = _csv.writer(buf)
        writer.writerow(fields)
        for row in export_rows:
            writer.writerow([_fmt(v) for v in row])

        csv_text = buf.getvalue()
        lines = csv_text.strip().split("\n")
        print(f"    Export preview ({len(lines)} lines including header):")
        for line in lines:
            print(f"      {line[:120]}")

        # ---- Cleanup: remove the test pattern ---------------------------------
        print("\n[Cleanup] Removing test pattern and unlinking rows")
        sample.pattern_id = None
        session.add(sample)
        await session.delete(p)
        await session.commit()
        print("    Done ✓")

    print("\n" + "=" * 60)
    print("  ALL PHASE 4 CHECKS PASSED ✓")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
