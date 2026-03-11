"""
Backfill month_year for any raw_query rows where it is NULL
but a parseable time string is present.

Run with:
    uv run python -m api.backfill_month_year
"""
from __future__ import annotations

import asyncio

from sqlmodel import select

from api.database import open_session
from api.models import RawQuery
from api.services.ingestor import _derive_month_year


async def backfill() -> None:
    async with open_session() as session:
        # Fetch only rows missing month_year that have a non-null time
        stmt = (
            select(RawQuery)
            .where(RawQuery.month_year.is_(None))   # type: ignore[union-attr]
            .where(RawQuery.time.isnot(None))        # type: ignore[union-attr]
        )
        rows = (await session.exec(stmt)).all()

        if not rows:
            print("Nothing to backfill — all rows already have month_year set.")
            return

        fixed = skipped = 0
        for row in rows:
            derived = _derive_month_year(row.time)
            if derived:
                row.month_year = derived
                session.add(row)
                fixed += 1
            else:
                skipped += 1

        await session.commit()
        print(
            f"Backfilled {fixed} row(s).  "
            f"{skipped} row(s) still unparseable (time format not recognised)."
        )


if __name__ == "__main__":
    asyncio.run(backfill())
