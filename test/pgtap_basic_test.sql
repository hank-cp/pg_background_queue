-- Clean Up
DROP EXTENSION IF EXISTS pg_background CASCADE;
DROP TABLE IF EXISTS t;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background;
CREATE EXTENSION IF NOT EXISTS pgtap;
SET log_min_messages = NOTICE;
ALTER SYSTEM SET log_min_messages = 'info';

-- Test
SELECT plan(4);

SELECT has_function('pg_background_enqueue', ARRAY['text', 'text[]'], 'pg_background_enqueue function should exist');

CREATE TABLE t(id integer);
SELECT pg_background_enqueue('INSERT INTO t SELECT 1');
SELECT * FROM pg_background_tasks;

SELECT is(
  (SELECT COUNT(*) FROM pg_background_tasks WHERE closed_at IS NOT NULL),
  '1',
  'task record closed timestamp should be record');

SELECT is(
  (SELECT state FROM pg_background_tasks LIMIT 1),
  'finished',
  'background worker should be finished.');

SELECT is(
  (SELECT id FROM t WHERE id = 1), '1',
  'test data should be insert by background worker.');

SELECT finish();