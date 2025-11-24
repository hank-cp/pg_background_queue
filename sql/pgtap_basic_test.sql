-- Clean Up
DROP EXTENSION IF EXISTS pg_background CASCADE;
DROP TABLE IF EXISTS t;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background;
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Test
SELECT plan(8);

SELECT has_function('revoke_pg_background_privileges', ARRAY['text', 'boolean'], 'revoke_pg_background_privileges function should exist');
SELECT has_function('grant_pg_background_privileges', ARRAY['text', 'boolean'], 'grant_pg_background_privileges function should exist');
SELECT has_function('pg_background_launch', ARRAY['text', 'int4', 'text[]'], 'pg_background_launch function should exist');
SELECT has_function('pg_background_result', ARRAY['int4'], 'pg_background_result function should exist');
SELECT has_function('pg_background_detach', ARRAY['int4'], 'pg_background_detach function should exist');

CREATE TABLE t(id integer);
SELECT pg_background_launch('INSERT INTO t SELECT 1');

SELECT col_not_null('t', 'closed_at', 'closed_at column should be not null');

SELECT is(
   (SELECT state FROM pg_background_tasks LIMIT 1), 'finished',
   'background worker should be finished.');

SELECT is(
  (SELECT id FROM t WHERE id = 1), '1',
  'test data should be insert by background worker.');

SELECT finish();