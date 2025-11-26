-- Clean Up
DROP EXTENSION IF EXISTS pg_background CASCADE;
DROP TRIGGER IF EXISTS t_trigger ON t;
DROP TABLE IF EXISTS t CASCADE;
DROP FUNCTION IF EXISTS test_trigger CASCADE;
DROP FUNCTION IF EXISTS test_trigger_func CASCADE;
--TRUNCATE t;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background;
CREATE EXTENSION IF NOT EXISTS pgtap;
SET log_min_messages = NOTICE;

CREATE OR REPLACE FUNCTION test_trigger()
  RETURNS TRIGGER AS $$
BEGIN
  IF NEW.touch IS NULL THEN
    RAISE NOTICE 'Enter tigger %', NEW.id;
    SELECT pg_background_enqueue(format('SELECT test_trigger_func(%s)', NEW.id));
    RAISE NOTICE 'Exit tigger %', NEW.id;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_trigger_func(t_id integer)
  RETURNS integer AS $$
BEGIN
  RAISE NOTICE 'Worker % started.', t_id;
  SELECT pg_sleep(2);
  UPDATE t SET touch = true WHERE id = t_id;
  RAISE NOTICE 'Worker % finished.', t_id;
  RETURN t_id;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER t_trigger
  AFTER UPDATE ON t
  FOR EACH ROW
EXECUTE FUNCTION test_trigger();

-- Test
SELECT plan(2);

INSERT INTO t VALUES (generate_series(1, 100));

UPDATE t SET touch = NULL;

SELECT is(
  (SELECT COUNT(*) FROM pg_background_tasks),
  '100',
  'background tasks should be planned');

SELECT is(
    (SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'running'),
    '8',
    '8 background tasks should be executed parallel');

SELECT pg_sleep(3);

SELECT is(
    (SELECT COUNT(*) FROM t WHERE touch = TRUE),
    '8',
    '8 test table record should be touched');

SELECT is(
   (SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'finished'),
    '8',
    '8 background tasks should be finished');

SELECT finish();