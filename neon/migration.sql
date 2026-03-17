BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 9db879faabd3

-- ---------------------------------------------------------------------------
-- pattern_label  (no FK dependencies -- declared first)
-- ---------------------------------------------------------------------------
-- NOTE: Columns that were originally ENUM types are VARCHAR + CHECK constraints.
-- This avoids "operator does not exist: enumtype = character varying" errors
-- when SQLAlchemy (using SAString) sends bound params as character varying.

CREATE TABLE pattern_label (
    id          SERIAL NOT NULL,
    name        VARCHAR NOT NULL,
    severity    VARCHAR NOT NULL DEFAULT 'warning'
                    CONSTRAINT chk_pattern_label_severity
                    CHECK (severity IN ('critical', 'warning', 'info')),
    description VARCHAR,
    source      VARCHAR NOT NULL DEFAULT 'both'
                    CONSTRAINT chk_pattern_label_source
                    CHECK (source IN ('sql', 'mongodb', 'both')),
    created_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ix_pattern_label_name     ON pattern_label (name);
CREATE INDEX ix_pattern_label_severity ON pattern_label (severity);
CREATE INDEX ix_pattern_label_source   ON pattern_label (source);

-- ---------------------------------------------------------------------------
-- raw_query  (source of truth -- every row from Splunk CSVs)
-- ---------------------------------------------------------------------------

CREATE TABLE raw_query (
    id               SERIAL NOT NULL,
    query_hash       VARCHAR NOT NULL,
    time             VARCHAR,
    source           VARCHAR NOT NULL
                         CONSTRAINT chk_raw_query_source
                         CHECK (source IN ('sql', 'mongodb')),
    host             VARCHAR,
    db_name          VARCHAR,
    environment      VARCHAR NOT NULL
                         CONSTRAINT chk_raw_query_environment
                         CHECK (environment IN ('prod', 'sat', 'unknown')),
    "type"           VARCHAR NOT NULL
                         CONSTRAINT chk_raw_query_type
                         CHECK ("type" IN ('slow_query', 'slow_query_mongo', 'blocker', 'deadlock', 'unknown')),
    query_details    VARCHAR,
    month_year       VARCHAR,
    occurrence_count INTEGER NOT NULL,
    first_seen       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_seen        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX ix_raw_query_query_hash ON raw_query (query_hash);
CREATE INDEX ix_raw_query_source            ON raw_query (source);
CREATE INDEX ix_raw_query_host              ON raw_query (host);
CREATE INDEX ix_raw_query_db_name           ON raw_query (db_name);
CREATE INDEX ix_raw_query_environment       ON raw_query (environment);
CREATE INDEX ix_raw_query_type              ON raw_query (type);
CREATE INDEX ix_raw_query_month_year        ON raw_query (month_year);

-- ---------------------------------------------------------------------------
-- curated_query  (one row per "promoted" raw_query; holds label FK + notes)
-- ---------------------------------------------------------------------------

CREATE TABLE curated_query (
    id           SERIAL NOT NULL,
    raw_query_id INTEGER NOT NULL,
    label_id     INTEGER,
    notes        VARCHAR,
    created_at   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (raw_query_id) REFERENCES raw_query (id),
    FOREIGN KEY (label_id)     REFERENCES pattern_label (id)
);

CREATE UNIQUE INDEX uq_curated_query_raw_query_id ON curated_query (raw_query_id);
CREATE INDEX ix_curated_query_raw_query_id        ON curated_query (raw_query_id);
CREATE INDEX ix_curated_query_label_id            ON curated_query (label_id);

-- ---------------------------------------------------------------------------
-- Grants  (app role: perfmdb_owner has DML only, not DDL)
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- spl_query  (SPL Library — store/edit Splunk queries per type)
-- ---------------------------------------------------------------------------

CREATE TABLE spl_query (
    id          SERIAL      PRIMARY KEY,
    name        VARCHAR     NOT NULL,
    query_type  VARCHAR     NOT NULL,
    environment VARCHAR     NOT NULL DEFAULT 'both',
    description VARCHAR,
    spl         TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ix_spl_query_query_type ON spl_query (query_type);
CREATE INDEX ix_spl_query_name       ON spl_query (name);

-- ---------------------------------------------------------------------------
-- Grants  (app role: perfmdb_owner has DML only, not DDL)
-- ---------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE, DELETE ON pattern_label TO perfmdb_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON raw_query     TO perfmdb_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON curated_query TO perfmdb_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON spl_query     TO perfmdb_owner;

GRANT USAGE, SELECT ON SEQUENCE pattern_label_id_seq TO perfmdb_owner;
GRANT USAGE, SELECT ON SEQUENCE raw_query_id_seq     TO perfmdb_owner;
GRANT USAGE, SELECT ON SEQUENCE curated_query_id_seq TO perfmdb_owner;
GRANT USAGE, SELECT ON SEQUENCE spl_query_id_seq     TO perfmdb_owner;

-- ---------------------------------------------------------------------------
-- "user"  (auth table — managed outside Alembic; merged here for completeness)
-- "user" is a reserved word in PostgreSQL; always quoted.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS "user" (
    id              SERIAL      PRIMARY KEY,
    username        VARCHAR     NOT NULL UNIQUE,
    email           VARCHAR     NOT NULL UNIQUE,
    hashed_password VARCHAR     NOT NULL,
    role            VARCHAR     NOT NULL DEFAULT 'viewer',
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ          DEFAULT NOW(),
    last_login      TIMESTAMPTZ
);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "user"   TO perfmdb_owner;
GRANT USAGE, SELECT                  ON SEQUENCE user_id_seq TO perfmdb_owner;

-- ---------------------------------------------------------------------------

INSERT INTO alembic_version (version_num) VALUES ('9db879faabd3')
    RETURNING alembic_version.version_num;

COMMIT;
