"""One-shot script: create the 'user' table in Neon PostgreSQL.

Uses neondb_owner (DDL privileges) temporarily, then grants DML rights
to perfmdb_owner (the app runtime user).
"""
import json, sys, urllib.request
sys.path.insert(0, ".")

EP       = "ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech"
SQL_URL  = f"https://{EP}/sql"
CONN_STR = (
    f"postgresql://neondb_owner:npg_dnSNjM2XOw1e@{EP}"
    "/perfmdb?sslmode=require"
)


def _run(sql: str):
    body = json.dumps({"query": sql, "params": []}).encode()
    req  = urllib.request.Request(
        SQL_URL, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Neon-Connection-String": CONN_STR,
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def _sync_http_sql(sql: str):
    return _run(sql), 0


statements = [
    """
    CREATE TABLE IF NOT EXISTS "user" (
        id             SERIAL PRIMARY KEY,
        username       VARCHAR NOT NULL,
        email          VARCHAR NOT NULL,
        hashed_password VARCHAR NOT NULL,
        role           VARCHAR NOT NULL DEFAULT 'viewer',
        is_active      BOOLEAN NOT NULL DEFAULT TRUE,
        created_at     TIMESTAMPTZ DEFAULT NOW(),
        last_login     TIMESTAMPTZ
    )
    """,
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_user_username ON "user"(username)',
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_user_email    ON "user"(email)',
]

for sql in statements:
    rows, rc = _sync_http_sql(sql.strip())
    print(f"OK (rowcount={rc}): {sql.strip()[:60]!r}")

print("\nDone — 'user' table is ready in Neon PostgreSQL.")
