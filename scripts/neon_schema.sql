-- =============================================================================
-- Neon PostgreSQL DDL — run this in the Neon Console SQL editor
-- (logged in as neondb_owner, which has CREATE privileges on public schema)
-- =============================================================================

-- "user" is a reserved word in PostgreSQL; always quoted.

CREATE TABLE IF NOT EXISTS "user" (
    id              SERIAL          PRIMARY KEY,
    username        VARCHAR         NOT NULL UNIQUE,
    email           VARCHAR         NOT NULL UNIQUE,
    hashed_password VARCHAR         NOT NULL,
    role            VARCHAR         NOT NULL DEFAULT 'viewer',
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ              DEFAULT NOW(),
    last_login      TIMESTAMPTZ
);

-- Grant the app role access to the table and its sequence.
GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE "user"
    TO perfmdb_owner;

GRANT USAGE, SELECT
    ON SEQUENCE user_id_seq
    TO perfmdb_owner;
