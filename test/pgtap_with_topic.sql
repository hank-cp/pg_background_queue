-- Clean Up
DROP EXTENSION IF EXISTS pg_background CASCADE;
DROP TABLE IF EXISTS test_results CASCADE;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background VERSION '1.5';
CREATE EXTENSION IF NOT EXISTS pgtap;
SET log_min_messages = NOTICE;

-- Test
SELECT plan(0);

SELECT finish();
