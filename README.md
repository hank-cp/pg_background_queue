![GitHub Release](https://img.shields.io/github/v/release/hank-cp/pg_background_queue)
[![Tests](https://github.com/hank-cp/pg_background_queue/actions/workflows/test.yml/badge.svg)](https://github.com/hank-cp/pg_background_queue/actions/workflows/test.yml)
![GitHub](https://img.shields.io/github/license/hank-cp/pg_background_queue.svg)
![GitHub last commit](https://img.shields.io/github/last-commit/hank-cp/pg_background_queue.svg)

# pg_background_queue

A PostgreSQL extension that provides a robust background task queue system with advanced concurrency control, automatic retries, and topic-based configuration.

## Introduction

`pg_background_queue` is a PostgreSQL extension designed to execute SQL commands asynchronously using background workers. Unlike the original `pg_background` extension, this version addresses the fundamental limitation of `max_worker_processes` by introducing a persistent task queue system. When background workers are unavailable, tasks are automatically queued and processed when resources become available.

This extension is ideal for:
- **Asynchronous processing**: Execute long-running queries without blocking your application
- **Job scheduling**: Queue maintenance tasks, data processing, or ETL operations
- **Rate limiting**: Control task execution rate with configurable delays
- **Fault tolerance**: Automatic retry with exponential backoff for failed tasks
- **Multi-tenancy**: Topic-based configuration for different workload types

## Key Features

- **✅ Unlimited Task Queuing**: Submit unlimited tasks even when worker processes are fully utilized
- **✅ Automatic Worker Management**: Dynamic background worker allocation with built-in concurrency limits
- **✅ Priority Support**: Execute high-priority tasks before lower-priority ones
- **✅ Retry Mechanism**: Configurable automatic retry with exponential backoff for failed tasks
- **✅ Topic-based Configuration**: Per-topic settings for concurrency, delays, and retry policies
- **✅ Rate Limiting**: Configure delays between task execution to prevent system overload
- **✅ Health Monitoring**: Track task states (pending, running, retrying, failed, finished)
- **✅ pg_cron Integration**: Optional scheduling for enhanced stability and automatic worker recovery

## Installation

### Prerequisites

- PostgreSQL 13 or higher
- Build tools (gcc, make)
- PostgreSQL development headers

### Build and Install

```bash
# Clone the repository
cd /path/to/pg_background_queue

# Build the extension
make clean && make

# Install to PostgreSQL
make install

# Restart PostgreSQL to load the extension
pg_ctl restart
```

### Enable the Extension

```sql
-- Create the extension in your database
CREATE EXTENSION pg_background_queue;

-- Verify installation
SELECT * FROM pg_extension WHERE extname = 'pg_background_queue';
```

## Getting Started

### Basic Usage

```sql
-- Enqueue a simple task
SELECT pg_background_queue_enqueue('SELECT pg_sleep(5)');

-- Enqueue a task with a topic
SELECT pg_background_queue_enqueue(
    'INSERT INTO logs (message) VALUES (''Background task completed'')',
    'logging'
);

-- Check task status
SELECT id, state, sql_statement, joined_at, started_at, closed_at, errors
FROM pg_background_tasks
ORDER BY id DESC
LIMIT 10;

-- View pending tasks
SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'pending';

-- View running tasks
SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'running';
```

### Priority-based Execution

```sql
-- High priority task (lower number = higher priority)
SELECT pg_background_queue_enqueue('SELECT critical_operation()', 'critical');

-- Low priority task
SELECT pg_background_queue_enqueue('SELECT cleanup_old_data()', 'maintenance');
```

Tasks with lower priority values are executed first.

### Worker Management

```sql
-- Check active worker count
SELECT pg_background_queue_active_workers_count();

-- Ensure workers are running (manually trigger)
SELECT pg_background_queue_ensure_workers();

-- Calibrate worker count (sync with actual processes)
SELECT pg_background_queue_calibrate_workers_count();
```

## Configuration

### Global Configuration (postgresql.conf)

```bash
# Maximum parallel tasks (default: max_worker_processes / 2)
pg_background_queue.max_parallel_running_tasks_count = 4

# Default retry count for failed tasks (default: 0)
pg_background_queue.retry_count = 3

# Default delay between tasks in seconds (default: 0)
pg_background_queue.delay_in_sec = 1
```

### Topic-based Configuration

Override global settings for specific topics using JSON configuration:

```bash
# In postgresql.conf
pg_background_queue.topic_config = '{
  "critical": {
    "max_parallel_running_tasks_count": 10,
    "retry_count": 5,
    "delay_in_sec": 0,
    "priority": 1
  },
  "maintenance": {
    "max_parallel_running_tasks_count": 2,
    "retry_count": 1,
    "delay_in_sec": 5,
    "priority": 100
  },
  "reporting": {
    "max_parallel_running_tasks_count": 3,
    "retry_count": 2,
    "delay_in_sec": 2,
    "priority": 50
  }
}'
```

**Configuration Parameters:**

- `max_parallel_running_tasks_count`: Maximum concurrent tasks for this topic (default: global setting)
- `retry_count`: Number of retry attempts for failed tasks (default: global setting)
- `delay_in_sec`: Delay in seconds before executing each task (default: global setting)
- `priority`: Default priority for tasks in this topic (lower = higher priority, default: 100)

**Apply Configuration:**

```sql
-- Reload PostgreSQL configuration
SELECT pg_reload_conf();
```

### Retry Behavior

When a task fails:
1. If `retry_count` > 0, the task state changes to `retrying`
2. Retry delay follows exponential backoff: `delay = 2^retry_attempt` seconds
3. After exhausting retries, the task state becomes `failed`
4. Error messages are stored in the `errors` column

## Using pg_cron for Stability

For production deployments, it's highly recommended to use `pg_cron` to periodically ensure workers are running. This provides automatic recovery from crashes or unexpected worker terminations.

### Install pg_cron

```sql
CREATE EXTENSION pg_cron;
```

### Schedule Worker Management

```sql
-- Ensure workers are checked every minute
SELECT cron.schedule(
    'pg_background_queue_ensure_workers',
    '* * * * *',
    'SELECT pg_background_queue_ensure_workers();'
);

-- Optional: Calibrate worker count every 5 minutes
-- This syncs the internal counter with actual running processes
SELECT cron.schedule(
    'pg_background_queue_calibrate',
    '*/5 * * * *',
    'SELECT pg_background_queue_calibrate_workers_count();'
);
```

### View Scheduled Jobs

```sql
SELECT * FROM cron.job WHERE jobname LIKE 'pg_background_queue%';
```

### Remove Scheduled Jobs

```sql
SELECT cron.unschedule('pg_background_queue_ensure_workers');
SELECT cron.unschedule('pg_background_queue_calibrate');
```

## API Reference

### Functions

#### `pg_background_queue_enqueue(sql text, topic text DEFAULT NULL)`

Enqueue a task for background execution.

**Parameters:**
- `sql`: SQL statement to execute
- `topic`: Optional topic for topic-specific configuration

**Returns:** Task ID (bigint)

**Example:**
```sql
SELECT pg_background_queue_enqueue('VACUUM ANALYZE users', 'maintenance');
```

#### `pg_background_queue_ensure_workers()`

Ensure background workers are running and launch new workers if needed.

**Returns:** void

**Example:**
```sql
SELECT pg_background_queue_ensure_workers();
```

#### `pg_background_queue_active_workers_count()`

Get the current count of active background workers.

**Returns:** integer

**Example:**
```sql
SELECT pg_background_queue_active_workers_count();
```

#### `pg_background_queue_calibrate_workers_count()`

Calibrate the internal worker counter by querying actual running processes. Use this to recover from inconsistent state.

**Returns:** integer (actual worker count)

**Example:**
```sql
SELECT pg_background_queue_calibrate_workers_count();
```

### Tables

#### `pg_background_tasks`

Main task queue table.

**Columns:**
- `id` (bigserial): Unique task identifier
- `sql_statement` (text): SQL command to execute
- `topic` (text): Optional topic for configuration
- `joined_at` (timestamp): When task was enqueued
- `started_at` (timestamp): When task execution started
- `closed_at` (timestamp): When task finished (success or failure)
- `priority` (integer): Task priority (lower = higher priority)
- `state` (enum): Current task state (pending, running, retrying, failed, finished)
- `retry_count` (integer): Number of retry attempts made
- `retry_delay_in_sec` (integer): Current retry delay
- `errors` (text): Error messages from failed executions

### Task States

- **pending**: Task is queued and waiting for execution
- **running**: Task is currently being executed by a worker
- **retrying**: Task failed and is scheduled for retry
- **failed**: Task failed after exhausting all retry attempts
- **finished**: Task completed successfully

## Monitoring and Troubleshooting

### View Task Statistics

```sql
-- Task count by state
SELECT state, COUNT(*) as count
FROM pg_background_tasks
GROUP BY state;

-- Recent failures
SELECT id, sql_statement, errors, closed_at
FROM pg_background_tasks
WHERE state = 'failed'
ORDER BY closed_at DESC
LIMIT 10;

-- Average task duration by topic
SELECT 
    topic,
    COUNT(*) as total_tasks,
    AVG(EXTRACT(EPOCH FROM (closed_at - started_at))) as avg_duration_seconds
FROM pg_background_tasks
WHERE state = 'finished' AND started_at IS NOT NULL
GROUP BY topic;
```

## Performance Considerations

1. **Worker Count**: Set `max_parallel_running_tasks_count` based on your system resources
2. **Priority**: Use priority to ensure critical tasks execute first
3. **Delays**: Configure `delay_in_sec` to prevent overwhelming your database
4. **Indexing**: The extension creates indexes on `state`, `topic`, and `joined_at` for optimal performance
5. **Cleanup**: Regularly archive or delete old tasks to maintain performance

## Upgrading from pg_background

This extension is a complete rewrite and is not backward compatible with the original `pg_background` extension. To migrate:

1. Export any pending tasks from the old system
2. Uninstall the old extension: `DROP EXTENSION pg_background CASCADE;`
3. Install the new extension: `CREATE EXTENSION pg_background_queue;`
4. Re-enqueue tasks using the new API

## License

Copyright (C) 2014-2025, PostgreSQL Global Development Group

This extension is based on the original `pg_background` extension and is distributed under the same license terms.

## Contributing

Contributions are welcome! Please submit issues and pull requests to the project repository.

## Support

For bug reports and feature requests, please use the GitHub issue tracker.
