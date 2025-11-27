-- Clean Up
DROP EXTENSION IF EXISTS pg_background CASCADE;
DROP TABLE IF EXISTS t;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background VERSION '1.5';
CREATE EXTENSION IF NOT EXISTS pgtap;
SET log_min_messages = NOTICE;

-- Test
SELECT plan(0);

-- ALTER SYSTEM SET pg_background.retry_count.test = 5;
-- SELECT pg_reload_conf();
--
-- SELECT lives_ok(
--          $$SELECT pg_background_enqueue('INSERT INTO t SELECT 1', 'test')$$,
--          'enqueue task should succeed'
--        );

SELECT finish();
