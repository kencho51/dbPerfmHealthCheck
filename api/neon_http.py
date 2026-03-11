"""
Neon HTTPS SQL executor — bypasses PostgreSQL port 5432.

Uses Neon's native HTTP SQL endpoint:
    POST https://{endpoint_hostname}/sql
    Authorization: Basic base64(user:password)
    Content-Type: application/json
    {"query": "SELECT ...", "params": [...]}

This endpoint runs on HTTPS port 443 and is completely separate from the
PostgreSQL wire protocol, so it works even when a corporate proxy blocks
port 5432 SSL handshakes.

The NeonHTTPSession class provides a duck-type compatible subset of
SQLModel AsyncSession so routers don't change:
    result = await session.exec(stmt)
    rows   = result.all()
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import types
import urllib.error
import urllib.request
from typing import Any

from sqlalchemy.dialects import postgresql as pg_dialect


# ---------------------------------------------------------------------------
# Type coercion — Neon REST API returns every value as a JSON string/null
# ---------------------------------------------------------------------------

def _coerce(value, col):
    """Convert a raw REST-API value to the Python type dictated by col.type."""
    if value is None:
        return None
    import sqlalchemy as sa
    t = col.type
    try:
        if isinstance(t, (sa.Integer, sa.SmallInteger, sa.BigInteger)):
            return int(value)
        if isinstance(t, (sa.Float, sa.Numeric)):
            return float(value)
        if isinstance(t, sa.Boolean):
            return value if isinstance(value, bool) else str(value).lower() in ("true", "1", "t")
        if isinstance(t, sa.DateTime):
            if isinstance(value, str):
                from datetime import datetime
                for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
    except Exception:
        pass
    return value


# ---------------------------------------------------------------------------
# Config — resolved lazily so dotenv is loaded first by database.py
# ---------------------------------------------------------------------------
def _cfg():
    pghost     = os.environ.get("PGHOST",    "ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech")
    pguser     = os.environ.get("PGUSER",    "perfmdb_owner")
    pgpassword = os.environ.get("PGPASSWORD","")
    pgdatabase = os.environ.get("PGDATABASE","perfmdb")
    http_host  = pghost.replace("-pooler", "")
    return {
        "http_url":  f"https://{http_host}/sql",
        "auth":      base64.b64encode(f"{pguser}:{pgpassword}".encode()).decode(),
        "conn_str":  f"postgresql://{pguser}:{pgpassword}@{http_host}/{pgdatabase}?sslmode=require",
        "api_key":   os.environ.get("NEON_API_KEY", ""),
        "project":   "cold-union-77928175",
        "endpoint":  "ep-rough-morning-a1v4c224",
        "pguser":    pguser,
        "pgdatabase":pgdatabase,
    }


# ---------------------------------------------------------------------------
# Result wrapper — compatible with SQLModel/SQLAlchemy result interface
# ---------------------------------------------------------------------------
class _NeonResult:
    """Wraps rows returned from the Neon HTTP endpoint."""

    def __init__(self, rows: list[list[Any]]) -> None:
        # rows come back as lists from the JSON response
        self._rows = [tuple(r) for r in rows]

    def all(self) -> list[tuple]:
        return self._rows

    def first(self) -> tuple | None:
        return self._rows[0] if self._rows else None

    def one_or_none(self) -> tuple | None:
        return self._rows[0] if len(self._rows) == 1 else None

    def one(self) -> tuple:
        if len(self._rows) != 1:
            raise ValueError(
                f"Expected exactly one row, got {len(self._rows)}"
            )
        return self._rows[0]

    def scalars(self) -> "_NeonResult":
        # Return first column value only
        self._rows = [(r[0],) if r else () for r in self._rows]
        return self

    def __iter__(self):
        return iter(self._rows)


# ---------------------------------------------------------------------------
# Low-level sync HTTP call (run in thread pool for async compatibility)
# ---------------------------------------------------------------------------
def _sync_http_sql(sql: str) -> list[list[Any]]:
    """
    Execute SQL via Neon native HTTP endpoint.
    Falls back to management API if the direct endpoint returns an error.
    """
    c = _cfg()

    # --- Try direct compute HTTP endpoint first ---
    body = json.dumps({"query": sql, "params": []}).encode()
    req = urllib.request.Request(
        c["http_url"],
        data=body,
        method="POST",
        headers={
            "Authorization": f"Basic {c['auth']}",
            "Content-Type": "application/json",
            "Neon-Connection-String": c["conn_str"],
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
            # Direct endpoint returns {"rows": [...], "fields": [...]}
            # Rows come back as dicts: [{"col": val}]
            if isinstance(data, dict) and "rows" in data:
                fields = [f["name"] for f in data.get("fields", [])]
                return [[row.get(f) for f in fields] for row in data["rows"]]
    except Exception:
        pass  # Fall through to management API

    # --- Fallback: Neon management REST API (confirmed working over HTTPS) ---
    body2 = json.dumps({
        "query":       sql,
        "db_name":     c["pgdatabase"],
        "endpoint_id": c["endpoint"],
        "role_name":   c["pguser"],
    }).encode()
    req2 = urllib.request.Request(
        f"https://console.neon.tech/api/v2/projects/{c['project']}/query",
        data=body2,
        method="POST",
        headers={
            "Authorization": f"Bearer {c['api_key']}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req2, timeout=30) as r2:
        data2 = json.loads(r2.read())
        if not data2.get("success"):
            raise RuntimeError(f"Neon query failed: {data2}")
        resp_data = data2["response"][0]["data"]
        # rows may be missing (empty result) or present as list-of-lists
        return resp_data.get("rows") or []


async def _async_sql(sql: str) -> list[list[Any]]:
    """Run _sync_http_sql in a thread pool to avoid blocking the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_http_sql, sql)


# ---------------------------------------------------------------------------
# SQLModel-compatible async session over HTTPS
# ---------------------------------------------------------------------------
def _compile(stmt) -> str:
    """Compile a SQLAlchemy/SQLModel statement to a PostgreSQL SQL string."""
    dialect = pg_dialect.dialect()
    compiled = stmt.compile(
        dialect=dialect,
        compile_kwargs={"literal_binds": True},
    )
    return str(compiled)


class _NeonExecResult:
    """Result for DML statements (INSERT, UPDATE, DELETE) — exposes .rowcount."""

    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class NeonHTTPSession:
    """
    Drop-in replacement for SQLModel AsyncSession that runs SQL over HTTPS.

    Supports the subset used by our routers:
        result = await session.exec(select(...))
        rows   = result.all()
        result = await session.execute(insert_stmt)
        result.rowcount
    """

    async def exec(self, stmt) -> _NeonResult:
        """
        Execute a SELECT and return a _NeonResult.

        Row mapping strategy (determined from column_descriptions):

        • select(Model)          → model instances  (r.attr works)
        • select(col1, col2, …)  → SimpleNamespace  (r.col_name works)
        • select(single_col)     → scalar values    (result.one() returns int/str/…)
        • INSERT … RETURNING col → plain tuples      (row[0] works — no column_descriptions)
        """
        model     = None
        col_names = None
        scalar    = False
        try:
            descs = getattr(stmt, "column_descriptions", None)
            if descs:
                if len(descs) == 1:
                    entity = descs[0].get("entity")
                    expr   = descs[0].get("expr")
                    # select(SomeModel) → expr IS the class itself
                    # select(func.count(Model.id)) → entity=Model but expr is a Function
                    if isinstance(entity, type) and expr is entity:
                        model = entity
                    else:
                        scalar = True           # single col / aggregate
                else:
                    names = [d.get("name") for d in descs]
                    if all(isinstance(n, str) and n for n in names):
                        col_names = names       # select(col1, col2, …)
        except Exception:
            pass

        sql  = _compile(stmt)
        rows = await _async_sql(sql)
        result = _NeonResult(rows)

        if model is not None:
            from sqlalchemy import inspect as sa_inspect
            cols = list(sa_inspect(model).mapper.columns)
            result._rows = [
                model(**{col.key: _coerce(row[i], col) for i, col in enumerate(cols)})
                for row in result._rows
            ]
        elif scalar:
            # Unwrap single-element tuple and best-effort numeric cast.
            # func.count() / func.sum() return strings from the REST API.
            def _cast_scalar(val):
                if not isinstance(val, str):
                    return val
                try:
                    return int(val)
                except (ValueError, TypeError):
                    pass
                try:
                    return float(val)
                except (ValueError, TypeError):
                    pass
                return val
            result._rows = [_cast_scalar(row[0]) if row else None for row in result._rows]
        elif col_names is not None:
            # Build SA column objects for coercion where the expr is an
            # InstrumentedAttribute (i.e. RawQuery.occurrence_count, not an aggregate).
            sa_cols: list = []
            for d in descs:
                expr = d.get("expr")
                try:
                    sa_cols.append(expr.property.columns[0])
                except Exception:
                    sa_cols.append(None)
            result._rows = [
                types.SimpleNamespace(**{
                    name: (_coerce(val, sa_cols[i]) if sa_cols[i] is not None else val)
                    for i, (name, val) in enumerate(zip(col_names, row))
                })
                for row in result._rows
            ]

        return result

    async def execute(self, stmt) -> _NeonExecResult:
        """Run DML (INSERT / UPDATE / DELETE) over HTTPS and return rowcount."""
        sql = _compile(stmt)
        rows = await _async_sql(sql)
        # Management API returns empty rows list for DML; rowcount is unknown.
        # Return 1 if at least one row was returned, else 0.
        return _NeonExecResult(rowcount=len(rows) if rows else 0)

    async def get(self, model, pk):
        """Return a model instance (or None) for the given primary key."""
        from sqlmodel import select
        stmt = select(model).where(model.id == pk)
        result = await self.exec(stmt)
        return result.first()  # exec() maps tuples → model instances for select(Model)

    async def commit(self)   -> None: pass
    async def rollback(self) -> None: pass
    async def close(self)    -> None: pass

    async def __aenter__(self)        -> "NeonHTTPSession": return self
    async def __aexit__(self, *args)  -> None: pass
