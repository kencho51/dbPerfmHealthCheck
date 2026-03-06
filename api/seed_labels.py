"""
Seed default pattern labels into pattern_label table.
Run with: uv run python -m api.seed_labels
Idempotent — skips labels that already exist (matched by name).
"""

import asyncio
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from api.database import engine
from api.models import PatternLabel

DEFAULT_LABELS = [
    # ── SQL Server ────────────────────────────────────────────────────────────
    {
        "name": "Full Table Scan",
        "severity": "critical",
        "source": "sql",
        "description": (
            "No usable index; the optimizer reads every row in the table. "
            "Look for SCAN operators on large tables in the execution plan."
        ),
    },
    {
        "name": "Missing Index",
        "severity": "warning",
        "source": "sql",
        "description": (
            "Optimizer suggests an index that doesn't exist. "
            "Check sys.dm_db_missing_index_details for recommendations."
        ),
    },
    {
        "name": "Key Lookup",
        "severity": "warning",
        "source": "sql",
        "description": (
            "Non-covering index forces a secondary lookup into the clustered index per row. "
            "Fix by adding INCLUDEd columns to the non-clustered index."
        ),
    },
    {
        "name": "Implicit Conversion",
        "severity": "warning",
        "source": "sql",
        "description": (
            "Data type mismatch (e.g. VARCHAR vs NVARCHAR, INT vs BIGINT) prevents index seek "
            "and forces a scan. Fix by aligning parameter and column types."
        ),
    },
    {
        "name": "Parameter Sniffing",
        "severity": "warning",
        "source": "sql",
        "description": (
            "Cached plan was optimised for an atypical parameter value and performs poorly "
            "for the current input. Symptoms: fast ad-hoc, slow via stored proc. "
            "Fix with OPTION(OPTIMIZE FOR), OPTION(RECOMPILE), or local variables."
        ),
    },
    {
        "name": "Blocking Chain",
        "severity": "critical",
        "source": "sql",
        "description": (
            "A long-running transaction holds locks that block other sessions. "
            "Identify the head blocker via sys.dm_exec_requests / sys.dm_os_waiting_tasks."
        ),
    },
    {
        "name": "Deadlock",
        "severity": "critical",
        "source": "sql",
        "description": (
            "Two sessions in a circular lock wait; SQL Server kills one as victim. "
            "Capture via system_health XE session or trace flag 1222. "
            "Fix by enforcing consistent object access order or using READ COMMITTED SNAPSHOT."
        ),
    },
    {
        "name": "Long-Running Transaction",
        "severity": "critical",
        "source": "sql",
        "description": (
            "Transaction open far longer than expected, holding locks and growing the log. "
            "Check sys.dm_tran_active_transactions and sys.dm_exec_sessions."
        ),
    },
    {
        "name": "Excessive Recompilation",
        "severity": "warning",
        "source": "sql",
        "description": (
            "Plan cache thrashing due to schema changes, SET option changes, or overuse of "
            "OPTION(RECOMPILE). Monitor SQL:StmtRecompile with Extended Events."
        ),
    },
    {
        "name": "TempDB Spill",
        "severity": "warning",
        "source": "sql",
        "description": (
            "Sort, hash join, or hash aggregate spills to disk in TempDB because the memory "
            "grant was insufficient. Shown as yellow warning icon on Sort/Hash operators."
        ),
    },
    {
        "name": "Large Result Set",
        "severity": "info",
        "source": "sql",
        "description": (
            "Query returns far more rows than the application uses (SELECT *, no TOP/filter). "
            "Add filtering, pagination, or column projection."
        ),
    },
    {
        "name": "RBAR",
        "severity": "warning",
        "source": "sql",
        "description": (
            "Row-By-Agonising-Row: cursor or while-loop processing one record at a time "
            "instead of a set-based operation. Rewrite as a single DML statement."
        ),
    },
    {
        "name": "N+1 Query",
        "severity": "warning",
        "source": "both",
        "description": (
            "Application issues one parent query then one child query per parent row. "
            "Fix with a JOIN or batched IN-list on the application side."
        ),
    },
    {
        "name": "Aggregation without Index",
        "severity": "warning",
        "source": "sql",
        "description": (
            "GROUP BY / SUM / COUNT on non-indexed columns forces a large sort or hash aggregate. "
            "Add a composite or covering index that matches the GROUP BY + WHERE columns."
        ),
    },
    {
        "name": "Bulk Delete / Archive",
        "severity": "info",
        "source": "sql",
        "description": (
            "Large DELETE without row batching holds a schema-level or table lock for an extended period. "
            "Use DELETE TOP (N) in a loop with short transactions."
        ),
    },
    {
        "name": "Ad-hoc / Unparameterized",
        "severity": "info",
        "source": "sql",
        "description": (
            "Query text is unique per call (literal values embedded), causing plan cache bloat. "
            "Fix by parameterizing queries or enabling Optimize for Ad hoc Workloads."
        ),
    },
    {
        "name": "Statistics Out of Date",
        "severity": "warning",
        "source": "sql",
        "description": (
            "Row count estimates are wrong because statistics haven't been updated after "
            "large data changes, leading to bad plan choices. Run UPDATE STATISTICS."
        ),
    },
    {
        "name": "Report / Analytics Query",
        "severity": "info",
        "source": "both",
        "description": (
            "Complex aggregation query for reporting purposes. Long runtime is expected "
            "but should be isolated to a read replica or off-peak schedule."
        ),
    },
    # ── MongoDB ───────────────────────────────────────────────────────────────
    {
        "name": "COLLSCAN",
        "severity": "critical",
        "source": "mongodb",
        "description": (
            "No index used; MongoDB scans every document in the collection. "
            "Check explain() output for COLLSCAN stage and add an appropriate index."
        ),
    },
    {
        "name": "Missing Index (Mongo)",
        "severity": "warning",
        "source": "mongodb",
        "description": (
            "High docsExamined vs docsReturned ratio (> 10:1) indicates the query "
            "touches far more documents than needed. Add an index on the filter fields."
        ),
    },
    {
        "name": "Inefficient Pipeline",
        "severity": "warning",
        "source": "mongodb",
        "description": (
            "$match placed after $project or $unwind prevents index use. "
            "Always put $match (and $sort on indexed fields) as the first pipeline stage."
        ),
    },
    {
        "name": "In-Memory Sort",
        "severity": "warning",
        "source": "mongodb",
        "description": (
            "sort stage has no supporting index, or result set exceeds the 100 MB "
            "in-memory sort limit and spills to disk. Add an index matching the sort key."
        ),
    },
    {
        "name": "Unbounded Query",
        "severity": "warning",
        "source": "mongodb",
        "description": (
            "Query has no limit() and may return the entire collection. "
            "Add limit() or ensure a selective filter is always present."
        ),
    },
    {
        "name": "Lookup without Index",
        "severity": "warning",
        "source": "mongodb",
        "description": (
            "Lookup join on a field with no index on the foreign collection causes "
            "a full scan of the joined collection per document. Add an index on the joined field."
        ),
    },
    {
        "name": "Array Field without Multikey Index",
        "severity": "warning",
        "source": "mongodb",
        "description": (
            "Querying or sorting on an array field with no multikey index causes a COLLSCAN "
            "or inefficient fetch. Create a multikey index on the array field."
        ),
    },
    {
        "name": "Log / Cleanup Operation",
        "severity": "info",
        "source": "both",
        "description": (
            "Scheduled TTL expiry, log deletion, or maintenance aggregation. "
            "Long scan is expected; review if it overlaps with peak traffic windows."
        ),
    },
]


async def seed() -> None:
    async with AsyncSession(engine) as session:
        # Build lookup: name -> existing row
        result = await session.exec(select(PatternLabel))
        existing: dict[str, PatternLabel] = {r.name: r for r in result.all()}

        inserted = updated = 0
        for row in DEFAULT_LABELS:
            if row["name"] in existing:
                # Upsert: update source field (and other fields) on existing rows
                lbl = existing[row["name"]]
                lbl.source = row["source"]
                lbl.severity = row["severity"]
                lbl.description = row["description"]
                session.add(lbl)
                updated += 1
            else:
                session.add(PatternLabel(**row))
                inserted += 1

        await session.commit()
        print(f"Seeded: {inserted} inserted, {updated} updated.")


if __name__ == "__main__":
    asyncio.run(seed())
