"""Create spl_query table in Neon via HTTP SQL API."""
import json
import urllib.error
import urllib.request

CONN_STR = (
    "postgresql://neondb_owner:npg_dnSNjM2XOw1e"
    "@ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech"
    "/perfmdb?sslmode=require"
)
HOST = "ep-rough-morning-a1v4c224.ap-southeast-1.aws.neon.tech"
URL  = f"https://{HOST}/sql"

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS spl_query (
        id          SERIAL      PRIMARY KEY,
        name        VARCHAR     NOT NULL,
        query_type  VARCHAR     NOT NULL,
        environment VARCHAR     NOT NULL DEFAULT 'both',
        description VARCHAR,
        spl         TEXT        NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_spl_query_query_type ON spl_query (query_type)",
    "CREATE INDEX IF NOT EXISTS ix_spl_query_name       ON spl_query (name)",
    "GRANT SELECT, INSERT, UPDATE, DELETE ON spl_query TO perfmdb_owner",
    "GRANT USAGE, SELECT ON SEQUENCE spl_query_id_seq TO perfmdb_owner",
]


def run(stmt: str) -> None:
    body = json.dumps({"query": stmt.strip()}).encode()
    req = urllib.request.Request(
        URL, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Neon-Connection-String": CONN_STR},
    )
    try:
        with urllib.request.urlopen(req, timeout=30):
            print(f"  OK  | {stmt.strip()[:90]}")
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace") if e.fp else ""
        msg = json.loads(body_text).get("message", body_text) if body_text.startswith("{") else body_text
        print(f"  ERR | {stmt.strip()[:90]}\n       → {msg[:200]}")


print(f"Creating spl_query table in Neon …\n")
for s in STATEMENTS:
    run(s)
print("\nDone")
