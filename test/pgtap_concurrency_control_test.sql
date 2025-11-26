-- Clean Up
DROP EXTENSION IF EXISTS pg_background CASCADE;
DROP TABLE IF EXISTS test_results CASCADE;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background VERSION '1.5';
CREATE EXTENSION IF NOT EXISTS pgtap;
SET log_min_messages = NOTICE;

-- Test
SELECT plan(11);

SELECT has_table('pg_background_tasks', 'pg_background_tasks table should exist');

SELECT has_column('pg_background_tasks', 'id', 'id column should exist');
SELECT has_column('pg_background_tasks', 'sql_statement', 'sql_statement column should exist');
SELECT has_column('pg_background_tasks', 'state', 'state column should exist');
SELECT has_column('pg_background_tasks', 'topics', 'topics column should exist');
SELECT has_column('pg_background_tasks', 'retry_count', 'retry_count column should exist');

SELECT has_function('pg_background_launch', ARRAY['text', 'text[]'], 'pg_background_launch with topics should exist');

CREATE TABLE test_results(value int);

SELECT lives_ok(
    $$SELECT pg_background_launch('INSERT INTO test_results VALUES (42)'::text, NULL::text[])$$,
    'launch task without topics should succeed'
);

SELECT lives_ok(
    $$SELECT pg_background_launch('INSERT INTO test_results VALUES (100)'::text, ARRAY['test_topic'])$$,
    'launch task with topics should succeed'
);

SELECT is(
    (SELECT COUNT(*) FROM pg_background_tasks)::text,
    '2',
    'should have 2 tasks in queue'
);

SELECT pg_sleep(3);

SELECT is(
    (SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'finished')::text,
    '2',
    'both tasks should be finished after waiting'
);

SELECT finish();
