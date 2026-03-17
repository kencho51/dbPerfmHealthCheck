-- ---------------------------------------------------------------------------
-- Migration: convert native ENUM columns → VARCHAR
-- ---------------------------------------------------------------------------
-- WHY: SQLAlchemy models use SAString (VARCHAR) for cross-backend compat.
--      PostgreSQL native ENUMs reject VARCHAR params with:
--        "operator does not exist: querytype = character varying"
-- FIX: Convert ENUM columns to VARCHAR with CHECK constraints for the same
--      validation that ENUMs provided.  No data loss — USING casts to text.
--
-- Run ONCE against the live Neon DB (neondb_owner or superuser required):
--   psql "$NEON_DDL_CONN_STR" -f neon/alter_enum_to_varchar.sql
--   -- or paste into the Neon SQL Editor
-- ---------------------------------------------------------------------------

BEGIN;

-- pattern_label ---------------------------------------------------------
ALTER TABLE pattern_label
    ALTER COLUMN severity TYPE VARCHAR USING severity::text,
    ALTER COLUMN source   TYPE VARCHAR USING source::text;

ALTER TABLE pattern_label
    ADD CONSTRAINT chk_pattern_label_severity
        CHECK (severity IN ('critical', 'warning', 'info')),
    ADD CONSTRAINT chk_pattern_label_source
        CHECK (source IN ('sql', 'mongodb', 'both'));

-- raw_query -------------------------------------------------------------
ALTER TABLE raw_query
    ALTER COLUMN source      TYPE VARCHAR USING source::text,
    ALTER COLUMN environment TYPE VARCHAR USING environment::text,
    ALTER COLUMN "type"      TYPE VARCHAR USING "type"::text;

ALTER TABLE raw_query
    ADD CONSTRAINT chk_raw_query_source
        CHECK (source IN ('sql', 'mongodb')),
    ADD CONSTRAINT chk_raw_query_environment
        CHECK (environment IN ('prod', 'sat', 'unknown')),
    ADD CONSTRAINT chk_raw_query_type
        CHECK ("type" IN ('slow_query', 'slow_query_mongo', 'blocker', 'deadlock', 'unknown'));

-- Drop the now-unused ENUM types (safe once no columns reference them)  ----
DROP TYPE IF EXISTS severitytype;
DROP TYPE IF EXISTS labelsource;
DROP TYPE IF EXISTS sourcetype;
DROP TYPE IF EXISTS environmenttype;
DROP TYPE IF EXISTS querytype;

COMMIT;
