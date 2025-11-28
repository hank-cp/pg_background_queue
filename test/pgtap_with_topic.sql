-- Clean Up
DROP EXTENSION IF EXISTS pg_background_queue CASCADE;
DROP TABLE IF EXISTS t;

-- Setup
CREATE EXTENSION IF NOT EXISTS pg_background_queue;
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Clean up test data
-- TRUNCATE pg_background_tasks;

-- Setup configurations for test
ALTER SYSTEM RESET pg_background_queue.topic_config;
ALTER SYSTEM RESET pg_background_queue.retry_count;
ALTER SYSTEM RESET pg_background_queue.delay_in_sec;
ALTER SYSTEM SET pg_background_queue.retry_count = 1;
ALTER SYSTEM SET pg_background_queue.topic_config = '{"high_priority": {"retry_count": 2, "priority": 10}, "partial": {"priority": 50}}';
SELECT pg_reload_conf();

CREATE TABLE t(id integer);

SELECT plan(10);

-- ============================================================================
-- Test Group 2: Configuration Priority (8 tests)
-- ============================================================================

-- Test 2: JSON config priority overrides global config
SELECT pg_background_enqueue('INSERT INTO t SELECT 1', 'high_priority');
SELECT is(
    (SELECT priority FROM pg_background_tasks WHERE topic = 'high_priority' ORDER BY id DESC LIMIT 1),
    10,
    'Topic with JSON config should use JSON priority (10), not global (100)'
);

-- Test 3: Partial config uses JSON for priority
SELECT pg_background_enqueue('INSERT INTO t SELECT 2', 'partial');
SELECT is(
    (SELECT priority FROM pg_background_tasks WHERE topic = 'partial' ORDER BY id DESC LIMIT 1),
    50,
    'Topic with partial JSON config should use JSON priority (50)'
);

-- Test 4: Unconfigured topic uses global config
SELECT pg_background_enqueue('INSERT INTO t SELECT 3', 'unconfigured');
SELECT is(
    (SELECT priority FROM pg_background_tasks WHERE topic = 'unconfigured' ORDER BY id DESC LIMIT 1),
    100,
    'Unconfigured topic should use global priority (100)'
);

-- Test 5: NULL topic uses global config
SELECT pg_background_enqueue('INSERT INTO t SELECT 4');
SELECT is(
    (SELECT priority FROM pg_background_tasks WHERE topic IS NULL ORDER BY id DESC LIMIT 1),
    100,
    'NULL topic should use global priority (100)'
);

-- ============================================================================
-- Test Group 3: Test retry
-- ============================================================================

-- Test 6: JSON config topic
SELECT pg_background_enqueue('INSERT INTO tt SELECT 1', 'high_priority');
SELECT pg_background_enqueue('INSERT INTO ttt SELECT 2', 'unconfigured');
SELECT pg_background_enqueue('INSERT INTO ttt SELECT 2');
SELECT pg_sleep(60);

SELECT is(
    (SELECT retry_count FROM pg_background_tasks WHERE topic = 'high_priority' ORDER BY id DESC LIMIT 1),
    2,
    'Topic with JSON config should use JSON retry_count (3), not retry_count (2)'
);
SELECT is(
    (SELECT state FROM pg_background_tasks WHERE topic = 'high_priority' ORDER BY id DESC LIMIT 1),
    'failed',
    'Topic with JSON config should use JSON retry_count (3), not retry_count (2)'
);

-- Test 7: Unconfigured topic uses global config
SELECT is(
    (SELECT retry_count FROM pg_background_tasks WHERE topic = 'unconfigured' ORDER BY id DESC LIMIT 1),
    1,
    'Unconfigured topic should use global retry_count (2)'
);
SELECT is(
    (SELECT state FROM pg_background_tasks WHERE topic = 'unconfigured' ORDER BY id DESC LIMIT 1),
    'failed',
    'Topic with JSON config should use JSON retry_count (3), not retry_count (2)'
);

-- Test 8: Null topic uses global config
SELECT is(
    (SELECT retry_count FROM pg_background_tasks WHERE topic = 'unconfigured' ORDER BY id DESC LIMIT 1),
    1,
    'Unconfigured topic should use global retry_count (1)'
);
SELECT is(
    (SELECT state FROM pg_background_tasks WHERE topic IS NULL ORDER BY id DESC LIMIT 1),
    'failed',
    'Topic without JSON config should use JSON retry_count (1), not retry_count (2)'
);
SELECT * FROM pg_background_tasks ORDER BY id DESC;

-- ============================================================================
-- Cleanup
-- ============================================================================

ALTER SYSTEM RESET pg_background_queue.topic_config;
ALTER SYSTEM RESET pg_background_queue.retry_count;
ALTER SYSTEM RESET pg_background_queue.delay_in_sec;
SELECT pg_reload_conf();

SELECT * FROM finish();