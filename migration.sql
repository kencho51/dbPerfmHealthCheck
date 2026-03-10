BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 9db879faabd3

CREATE TYPE severitytype AS ENUM ('critical', 'warning', 'info');

CREATE TYPE sourcetype AS ENUM ('sql', 'mongodb');

CREATE TYPE environmenttype AS ENUM ('prod', 'sat', 'unknown');

CREATE TYPE querytype AS ENUM ('slow_query', 'blocker', 'deadlock', 'unknown');

CREATE TABLE pattern (
    id SERIAL NOT NULL, 
    name VARCHAR NOT NULL, 
    description VARCHAR, 
    pattern_tag VARCHAR NOT NULL, 
    severity severitytype NOT NULL, 
    example_query_hash VARCHAR, 
    source sourcetype, 
    environment environmenttype, 
    type querytype, 
    first_seen TIMESTAMP WITHOUT TIME ZONE, 
    last_seen TIMESTAMP WITHOUT TIME ZONE, 
    total_occurrences INTEGER NOT NULL, 
    notes VARCHAR, 
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    PRIMARY KEY (id)
);

CREATE INDEX ix_pattern_environment ON pattern (environment);

CREATE INDEX ix_pattern_name ON pattern (name);

CREATE INDEX ix_pattern_pattern_tag ON pattern (pattern_tag);

CREATE INDEX ix_pattern_severity ON pattern (severity);

CREATE INDEX ix_pattern_source ON pattern (source);

CREATE INDEX ix_pattern_type ON pattern (type);

CREATE TABLE raw_query (
    id SERIAL NOT NULL, 
    query_hash VARCHAR NOT NULL, 
    time VARCHAR, 
    source sourcetype NOT NULL, 
    host VARCHAR, 
    db_name VARCHAR, 
    environment environmenttype NOT NULL, 
    type querytype NOT NULL, 
    query_details VARCHAR, 
    month_year VARCHAR, 
    occurrence_count INTEGER NOT NULL, 
    first_seen TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    last_seen TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    pattern_id INTEGER, 
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(pattern_id) REFERENCES pattern (id)
);

CREATE INDEX ix_raw_query_db_name ON raw_query (db_name);

CREATE INDEX ix_raw_query_environment ON raw_query (environment);

CREATE INDEX ix_raw_query_host ON raw_query (host);

CREATE INDEX ix_raw_query_month_year ON raw_query (month_year);

CREATE INDEX ix_raw_query_pattern_id ON raw_query (pattern_id);

CREATE UNIQUE INDEX ix_raw_query_query_hash ON raw_query (query_hash);

CREATE INDEX ix_raw_query_source ON raw_query (source);

CREATE INDEX ix_raw_query_type ON raw_query (type);

INSERT INTO alembic_version (version_num) VALUES ('9db879faabd3') RETURNING alembic_version.version_num;

COMMIT;

