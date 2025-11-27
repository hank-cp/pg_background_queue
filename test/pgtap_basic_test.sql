-- Clean Up
DROP EXTENSION IF EXISTS pg_background CASCADE;
DROP TABLE IF EXISTS t;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background;
CREATE EXTENSION IF NOT EXISTS pgtap;
SET log_min_messages = NOTICE;
-- ALTER SYSTEM SET log_min_messages = 'info';
-- ALTER SYSTEM RESET logging_collector;
-- ALTER SYSTEM RESET log_filename;
-- ALTER SYSTEM RESET log_min_messages;
-- ALTER SYSTEM RESET pg_background.max_parallel_running_tasks_count;
-- ALTER SYSTEM SET pg_background.max_parallel_running_tasks_count = 20;
-- SELECT pg_reload_conf();

-- Test
SELECT plan(15);

SELECT has_table('pg_background_tasks', 'pg_background_tasks table should exist');
SELECT has_column('pg_background_tasks', 'id', 'id column should exist');
SELECT has_column('pg_background_tasks', 'sql_statement', 'sql_statement column should exist');
SELECT has_column('pg_background_tasks', 'state', 'state column should exist');
SELECT has_column('pg_background_tasks', 'topic', 'topic column should exist');
SELECT has_column('pg_background_tasks', 'retry_count', 'retry_count column should exist');
SELECT has_function('pg_background_enqueue', ARRAY['text', 'text'], 'pg_background_enqueue function should exist');
SELECT has_function('pg_background_ensure_workers', 'pg_background_ensure_workers function should exist');
SELECT has_function('pg_background_active_workers_count', 'pg_background_active_workers_count function should exist');
SELECT has_function('pg_background_calibrate_workers_count', 'pg_background_calibrate_workers_count function should exist');

CREATE TABLE t(id integer);

SELECT lives_ok(
         $$SELECT pg_background_enqueue('INSERT INTO t SELECT 1'::text)$$,
         'enqueue task should succeed'
       );

-- DEBUG QUERIES
-- SELECT pg_background_enqueue('SELECT pg_sleep(60)');
-- SELECT * FROM pg_background_tasks ORDER BY id;
-- TRUNCATE pg_background_tasks;
-- SELECT * FROM pg_stat_activity WHERE backend_type = 'pg_background';
-- SELECT COUNT(*) FROM pg_stat_activity WHERE backend_type = 'pg_background';
-- SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'running';
-- SELECT * FROM pg_stat_activity WHERE client_port IS NULL AND usesysid IS NOT NULL;

SELECT is(
  (SELECT COUNT(*) FROM pg_background_tasks)::text,
  '1',
  'should have 1 tasks in queue'
);

SELECT pg_sleep(2);

SELECT is(
  (SELECT COUNT(*) FROM pg_background_tasks WHERE closed_at IS NOT NULL AND state = 'finished'),
  '1',
  'task record closed timestamp should be record');

SELECT is(
  (SELECT id FROM t WHERE id = 1),
  '1',
  'test data should be insert by background worker.');

SELECT is(
  (SELECT pg_background_calibrate_workers_count()),
  '0',
  'should have no active background worker now.'
);

SELECT is(
  (SELECT pg_background_active_workers_count()),
  '0',
  'should have no active background worker now.'
);

SELECT finish();