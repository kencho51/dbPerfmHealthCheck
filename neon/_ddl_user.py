"""
One-shot DDL: create the 'user' table in Neon PostgreSQL.

Reads NEON_DDL_CONN_STR from the environment or from api/.env.
Set it to the neondb_owner connection string before running:

    $env:NEON_DDL_CONN_STR = "postgresql://neondb_owner:<pass>@<ep>/perfmdb?sslmode=require"
    uv run --native-tls python neon/_ddl_user.py

Or uncomment DATABASE_URL_DDL in api/.env and rename it NEON_DDL_CONN_STR.
"""
import json
import os
import urllib.request
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / "api" / ".env")

EP      = "ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech"
SQL_URL = f"https://{EP}/sql"

# NEON_DDL_CONN_STR must be set to a superuser connection string (neondb_owner)
CONN_STR = os.environ.get("NEON_DDL_CONN_STR", "")
if not CONN_STR:
    raise RuntimeError(
        "Set NEON_DDL_CONN_STR to the neondb_owner connection string before running."
    )

STATEMENTS = [
    (
        'CREATE TABLE IF NOT EXISTS "user" ('
        'id SERIAL PRIMARY KEY, '
        'username VARCHAR NOT NULL, '
        'email VARCHAR NOT NULL, '
        'hashed_password VARCHAR NOT NULL, '
        "role VARCHAR NOT NULL DEFAULT 'viewer', "
        'is_active BOOLEAN NOT NULL DEFAULT TRUE, '
        'created_at TIMESTAMPTZ DEFAULT NOW(), '
        'last_login TIMESTAMPTZ'
        ')'
    ),
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_user_username ON "user"(username)',
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_user_email    ON "user"(email)',
    'GRANT SELECT,INSERT,UPDATE,DELETE ON "user" TO perfmdb_owner',
    'GRANT USAGE,SELECT ON SEQUENCE user_id_seq TO perfmdb_owner',
]


def run(sql: str) -> dict:
    body = json.dumps({"query": sql, "params": []}).encode()
    req  = urllib.request.Request(
        SQL_URL,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Neon-Connection-String": CONN_STR,
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


if __name__ == "__main__":
    for stmt in STATEMENTS:
        try:
            run(stmt)
            print(f"OK  | {stmt[:90]}")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace") if exc.fp else ""
            print(f"ERR | {stmt[:90]}")
            print(f"    | HTTP {exc.code}: {body[:200]}")
        except Exception as exc:
            print(f"ERR | {stmt[:90]}")
            print(f"    | {exc}")
    print("\nScript complete.")

    (
        'CREATE TABLE IF NOT EXISTS "user" ('
        'id SERIAL PRIMARY KEY, '
        'username VARCHAR NOT NULL, '
        'email VARCHAR NOT NULL, '
        'hashed_password VARCHAR NOT NULL, '
        "role VARCHAR NOT NULL DEFAULT 'viewer', "
        'is_active BOOLEAN NOT NULL DEFAULT TRUE, '
        'created_at TIMESTAMPTZ DEFAULT NOW(), '
        'last_login TIMESTAMPTZ'
        ')'
    ),
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_user_username ON "user"(username)',
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_user_email    ON "user"(email)',
    'GRANT SELECT,INSERT,UPDATE,DELETE ON "user" TO perfmdb_owner',
    'GRANT USAGE,SELECT ON SEQUENCE user_id_seq TO perfmdb_owner',
]


def run(sql: str) -> dict:
    body = json.dumps({"query": sql, "params": []}).encode()
    req  = urllib.request.Request(
        SQL_URL,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Neon-Connection-String": CONN_STR,
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


if __name__ == "__main__":
    for stmt in STATEMENTS:
        try:
            result = run(stmt)
            print(f"OK  | {stmt[:90]}")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace") if exc.fp else ""
            print(f"ERR | {stmt[:90]}")
            print(f"    | HTTP {exc.code}: {body[:200]}")
        except Exception as exc:
            print(f"ERR | {stmt[:90]}")
            print(f"    | {exc}")
    print("\nScript complete.")
