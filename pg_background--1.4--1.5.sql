-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION pg_background UPDATE TO '1.5'" to load this file. \quit

-- Create the task state enum type
CREATE TYPE pg_background_task_state AS ENUM (
    'pending',
    'running',
    'retrying',
    'failed',
    'finished'
);

-- Create the tasks management table
CREATE TABLE pg_background_tasks (
    id BIGSERIAL PRIMARY KEY,
    sql_statement TEXT NOT NULL,
    topic TEXT,
    joined_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    closed_at TIMESTAMP,
    state pg_background_task_state NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    retry_delay_in_sec INTEGER NOT NULL DEFAULT 0,
    errors TEXT
);

-- Create indexes
CREATE INDEX idx_pg_background_tasks_queue 
    ON pg_background_tasks(joined_at, id) 
    WHERE state IN ('pending', 'retrying');

CREATE INDEX idx_pg_background_tasks_state 
    ON pg_background_tasks(state, closed_at DESC);

CREATE INDEX idx_pg_background_tasks_topic 
    ON pg_background_tasks(topic);

-- Drop old functions
DROP FUNCTION IF EXISTS pg_background_result(pg_catalog.int4);
DROP FUNCTION IF EXISTS pg_background_detach(pg_catalog.int4);
DROP FUNCTION IF EXISTS pg_background_launch(pg_catalog.text, pg_catalog.int4);
DROP FUNCTION IF EXISTS pg_background_launch(pg_catalog.text, pg_catalog.text[]);
DROP FUNCTION IF EXISTS pg_background_enqueue(pg_catalog.text, pg_catalog.text[]);
DROP FUNCTION IF EXISTS grant_pg_background_privileges(pg_catalog.text, boolean);
DROP FUNCTION IF EXISTS revoke_pg_background_privileges(pg_catalog.text, boolean);

-- Create new simplified API
CREATE FUNCTION pg_background_enqueue(sql pg_catalog.text,
					   topic pg_catalog.text DEFAULT NULL)
    RETURNS pg_catalog.int8
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;
