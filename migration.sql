BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 9db879faabd3

-- ---------------------------------------------------------------------------
-- ENUM types
-- ---------------------------------------------------------------------------

CREATE TYPE severitytype AS ENUM ('critical', 'warning', 'info');

CREATE TYPE sourcetype AS ENUM ('sql', 'mongodb');

CREATE TYPE environmenttype AS ENUM ('prod', 'sat', 'unknown');

-- slow_query_mongo added vs the original migration (MongoDB slow query variant)
CREATE TYPE querytype AS ENUM ('slow_query', 'slow_query_mongo', 'blocker', 'deadlock', 'unknown');

CREATE TYPE labelsource AS ENUM ('sql', 'mongodb', 'both');

-- ---------------------------------------------------------------------------
-- pattern_label  (no FK dependencies -- declared first)
-- ---------------------------------------------------------------------------

CREATE TABLE pattern_label (
    id          SERIAL NOT NULL,
    name        VARCHAR NOT NULL,
    severity    severitytype NOT NULL DEFAULT 'warning',
    description VARCHAR,
    source      labelsource NOT NULL DEFAULT 'both',
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
    source           sourcetype NOT NULL,
    host             VARCHAR,
    db_name          VARCHAR,
    environment      environmenttype NOT NULL,
    type             querytype NOT NULL,
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

GRANT SELECT, INSERT, UPDATE, DELETE ON pattern_label TO perfmdb_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON raw_query     TO perfmdb_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON curated_query TO perfmdb_owner;

GRANT USAGE, SELECT ON SEQUENCE pattern_label_id_seq TO perfmdb_owner;
GRANT USAGE, SELECT ON SEQUENCE raw_query_id_seq     TO perfmdb_owner;
GRANT USAGE, SELECT ON SEQUENCE curated_query_id_seq TO perfmdb_owner;

-- ---------------------------------------------------------------------------

INSERT INTO alembic_version (version_num) VALUES ('9db879faabd3')
    RETURNING alembic_version.version_num;

COMMIT;
