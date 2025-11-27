
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_background" to load this file. \quit

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
    priority INTEGER NOT NULL DEFAULT 0,
    state pg_background_task_state NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    retry_delay_in_sec INTEGER NOT NULL DEFAULT 0,
    errors TEXT
);

CREATE INDEX idx_pg_background_tasks_queue
    ON pg_background_tasks(joined_at, id)
    WHERE state IN ('pending', 'retrying');

CREATE INDEX idx_pg_background_tasks_state
    ON pg_background_tasks(state, closed_at DESC);

CREATE INDEX idx_pg_background_tasks_topic
    ON pg_background_tasks(topic);

-- Enqueue a background task (single overloaded function)
CREATE FUNCTION pg_background_enqueue(sql pg_catalog.text,
					   topic pg_catalog.text DEFAULT NULL)
    RETURNS pg_catalog.int8
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

-- Function to ensure workers are running (called by pg_cron)
CREATE FUNCTION pg_background_ensure_workers()
    RETURNS void
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

-- Get current active workers count
CREATE FUNCTION pg_background_active_workers_count()
    RETURNS integer
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

-- Calibrate active workers count by querying actual processes
CREATE FUNCTION pg_background_calibrate_workers_count()
    RETURNS integer
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

-- Schedule using pg_cron (requires pg_cron extension)
-- SELECT cron.schedule('pg_background_ensure_workers', '* * * * *', 'SELECT pg_background_ensure_workers();');
-- Optional: Schedule periodic calibration using pg_cron
-- SELECT cron.schedule('pg_background_calibrate', '*/5 * * * *', 'SELECT pg_background_calibrate_workers_count();');
