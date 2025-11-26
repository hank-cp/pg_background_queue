-- Clean Up
DROP EXTENSION IF EXISTS pg_background CASCADE;
DROP TABLE IF EXISTS t;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background;
CREATE EXTENSION IF NOT EXISTS pgtap;
SET log_min_messages = NOTICE;
-- ALTER SYSTEM SET log_min_messages = 'info';

-- Test
SELECT plan(11);

SELECT has_table('pg_background_tasks', 'pg_background_tasks table should exist');
SELECT has_column('pg_background_tasks', 'id', 'id column should exist');
SELECT has_column('pg_background_tasks', 'sql_statement', 'sql_statement column should exist');
SELECT has_column('pg_background_tasks', 'state', 'state column should exist');
SELECT has_column('pg_background_tasks', 'topic', 'topic column should exist');
SELECT has_column('pg_background_tasks', 'retry_count', 'retry_count column should exist');
SELECT has_function('pg_background_enqueue', ARRAY['text', 'text[]'], 'pg_background_enqueue function should exist');

CREATE TABLE t(id integer);

SELECT lives_ok(
         $$SELECT pg_background_enqueue('INSERT INTO t SELECT 1'::text)$$,
         'enqueue task should succeed'
       );
-- SELECT * FROM pg_background_tasks;

SELECT is(
  (SELECT COUNT(*) FROM pg_background_tasks)::text,
  '2',
  'should have 2 tasks in queue'
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

SELECT finish();