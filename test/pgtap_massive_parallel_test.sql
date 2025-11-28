-- Clean Up
DROP EXTENSION IF EXISTS pg_background_queue CASCADE;
DROP TABLE IF EXISTS pg_background_tasks CASCADE;
DROP TRIGGER IF EXISTS t_trigger ON t;
DROP TABLE IF EXISTS t CASCADE;
DROP FUNCTION IF EXISTS test_trigger CASCADE;
DROP FUNCTION IF EXISTS test_trigger_func CASCADE;
--TRUNCATE t;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background_queue;
CREATE EXTENSION IF NOT EXISTS pgtap;

CREATE OR REPLACE FUNCTION test_trigger()
  RETURNS TRIGGER AS $$
BEGIN
  IF NEW.touch IS NULL THEN
    RAISE NOTICE 'Enter tigger %', NEW.id;
    PERFORM pg_background_queue_enqueue(format('SELECT test_trigger_func(%s)', NEW.id));
    RAISE NOTICE 'Exit tigger %', NEW.id;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_trigger_func(t_id integer)
  RETURNS VOID AS $$
BEGIN
  RAISE NOTICE 'Worker % started.', t_id;
  PERFORM pg_sleep(2);
  UPDATE t SET touch = true WHERE id = t_id;
  RAISE NOTICE 'Worker % finished.', t_id;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE t(id integer, touch boolean);

CREATE TRIGGER t_trigger
  AFTER UPDATE ON t
  FOR EACH ROW
EXECUTE FUNCTION test_trigger();

-- Test
SELECT plan(5);

INSERT INTO t VALUES (generate_series(1, 30));

SELECT lives_ok(
  $$UPDATE t SET touch = NULL$$,
  'update trigger should fire background tasks'
);

-- DEBUG QUERIES
-- SELECT * FROM t;
-- SELECT * FROM pg_background_tasks ORDER BY id;
-- TRUNCATE t;
-- TRUNCATE pg_background_tasks;
-- SELECT test_trigger_func(1);
-- SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'running';
-- SELECT * FROM pg_stat_activity WHERE backend_type = 'pg_background_queue';
-- SELECT * FROM pg_stat_activity WHERE client_port IS NULL AND usesysid IS NOT NULL;

SELECT pg_sleep(0.2);

SELECT is(
  (SELECT COUNT(*) FROM pg_background_tasks),
  '30',
  'background tasks should be planned');

SELECT is(
  (SELECT pg_background_queue_active_workers_count()),
  '4',
  '(4 = 8/2) active background worker.'
);

SELECT pg_sleep(3);

SELECT is(
    (SELECT COUNT(*) FROM t WHERE touch = TRUE),
    '4',
    '4 test table record should be touched');

SELECT is(
   (SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'finished'),
    '4',
    '4 background tasks should be finished');

SELECT finish();