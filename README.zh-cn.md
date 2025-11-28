![GitHub Release](https://img.shields.io/github/v/release/hank-cp/pg_background_queue)
[![Tests](https://github.com/hank-cp/pg_background_queue/actions/workflows/test.yml/badge.svg)](https://github.com/hank-cp/pg_background_queue/actions/workflows/test.yml)
![GitHub](https://img.shields.io/github/license/hank-cp/pg_background_queue.svg)
![GitHub last commit](https://img.shields.io/github/last-commit/hank-cp/pg_background_queue.svg)

# pg_background_queue

一个 PostgreSQL 扩展,提供具有高级并发控制、自动重试和基于主题配置的强大后台任务队列系统。

## 简介

`pg_background_queue` 是一个 PostgreSQL 扩展,旨在使用后台工作进程异步执行 SQL 命令。与原始的 `pg_background` 扩展不同,该版本通过引入持久化任务队列系统解决了 `max_worker_processes` 的根本限制。当后台工作进程不可用时,任务会自动排队,并在资源可用时进行处理。

该扩展适用于:
- **异步处理**:执行长时间运行的查询而不阻塞应用程序
- **作业调度**:队列维护任务、数据处理或 ETL 操作
- **速率限制**:通过可配置的延迟控制任务执行速率
- **容错能力**:对失败的任务进行指数退避的自动重试
- **多租户**:针对不同工作负载类型的基于主题的配置

## 核心特性

- **✅ 无限任务队列**:即使工作进程已满,也可以提交无限任务
- **✅ 自动工作进程管理**:具有内置并发限制的动态后台工作进程分配
- **✅ 优先级支持**:优先执行高优先级任务
- **✅ 重试机制**:可配置的自动重试,支持指数退避
- **✅ 基于主题的配置**:针对并发、延迟和重试策略的每个主题设置
- **✅ 速率限制**:配置任务执行之间的延迟以防止系统过载
- **✅ 健康监控**:跟踪任务状态(待处理、运行中、重试中、失败、已完成)
- **✅ pg_cron 集成**:可选调度以增强稳定性和自动工作进程恢复

## 安装

### 前置要求

- PostgreSQL 13 或更高版本
- 构建工具(gcc, make)
- PostgreSQL 开发头文件

### 构建和安装

```bash
# 克隆仓库
cd /path/to/pg_background_queue

# 构建扩展
make clean && make

# 安装到 PostgreSQL
make install
```

### 配置共享预加载库

**重要提示**:此扩展需要共享内存,必须在服务器启动时加载。

编辑您的 `postgresql.conf` 文件:

```bash
# 将 pg_background_queue 添加到 shared_preload_libraries
shared_preload_libraries = 'pg_background_queue'  # 如果已有其他扩展,添加到列表中

# 可选:配置工作进程限制
max_worker_processes = 16  # 确保有足够的工作进程满足您的工作负载
```

查找 `postgresql.conf` 位置:

```sql
SHOW config_file;
```

修改配置后,重启 PostgreSQL:

```bash
pg_ctl restart
```

### 启用扩展

```sql
-- 在数据库中创建扩展
CREATE EXTENSION pg_background_queue;

-- 验证安装
SELECT * FROM pg_extension WHERE extname = 'pg_background_queue';
```

## 快速开始

### 基本用法

```sql
-- 入队一个简单任务
SELECT pg_background_enqueue('SELECT pg_sleep(5)');

-- 入队一个带主题的任务
SELECT pg_background_enqueue(
    'INSERT INTO logs (message) VALUES (''Background task completed'')',
    'logging'
);

-- 检查任务状态
SELECT id, state, sql_statement, joined_at, started_at, closed_at, errors
FROM pg_background_tasks
ORDER BY id DESC
LIMIT 10;

-- 查看待处理任务
SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'pending';

-- 查看运行中任务
SELECT COUNT(*) FROM pg_background_tasks WHERE state = 'running';
```

### 基于优先级的执行

```sql
-- 高优先级任务(数字越小优先级越高)
SELECT pg_background_enqueue('SELECT critical_operation()', 'critical');

-- 低优先级任务
SELECT pg_background_enqueue('SELECT cleanup_old_data()', 'maintenance');
```

优先级值较低的任务会优先执行。

### 工作进程管理

```sql
-- 检查活跃工作进程数
SELECT pg_background_queue_active_workers_count();

-- 确保工作进程正在运行(手动触发)
SELECT pg_background_queue_ensure_workers();

-- 校准工作进程数(与实际进程同步)
SELECT pg_background_queue_calibrate_workers_count();
```

## 配置

### 全局配置 (postgresql.conf)

```bash
# 最大并行任务数(默认: max_worker_processes / 2)
pg_background_queue.max_parallel_running_tasks_count = 4

# 失败任务的默认重试次数(默认: 0)
pg_background_queue.retry_count = 3

# 任务之间的默认延迟秒数(默认: 0)
pg_background_queue.delay_in_sec = 1
```

### 基于主题的配置

使用 JSON 配置覆盖特定主题的全局设置:

```bash
# 在 postgresql.conf 中
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

**配置参数:**

- `max_parallel_running_tasks_count`: 该主题的最大并发任务数(默认:全局设置)
- `retry_count`: 失败任务的重试次数(默认:全局设置)
- `delay_in_sec`: 执行每个任务前的延迟秒数(默认:全局设置)
- `priority`: 该主题任务的默认优先级(越小优先级越高,默认: 100)

**应用配置:**

```sql
-- 重新加载 PostgreSQL 配置
SELECT pg_reload_conf();
```

### 重试行为

当任务失败时:
1. 如果 `retry_count` > 0,任务状态变为 `retrying`
2. 重试延迟遵循指数退避: `delay = 2^retry_attempt` 秒
3. 耗尽重试次数后,任务状态变为 `failed`
4. 错误消息存储在 `errors` 列中

## 使用 pg_cron 提升稳定性

对于生产环境部署,强烈建议使用 `pg_cron` 定期确保工作进程正在运行。这提供了从崩溃或意外工作进程终止中自动恢复的能力。

### 安装 pg_cron

```sql
CREATE EXTENSION pg_cron;
```

### 调度工作进程管理

```sql
-- 每分钟检查一次工作进程
SELECT cron.schedule(
    'pg_background_queue_ensure_workers',
    '* * * * *',
    'SELECT pg_background_queue_ensure_workers();'
);

-- 可选:每 5 分钟校准一次工作进程数
-- 这将内部计数器与实际运行的进程同步
SELECT cron.schedule(
    'pg_background_queue_calibrate',
    '*/5 * * * *',
    'SELECT pg_background_queue_calibrate_workers_count();'
);
```

### 查看调度作业

```sql
SELECT * FROM cron.job WHERE jobname LIKE 'pg_background_queue%';
```

### 移除调度作业

```sql
SELECT cron.unschedule('pg_background_queue_ensure_workers');
SELECT cron.unschedule('pg_background_queue_calibrate');
```

## API 参考

### 函数

#### `pg_background_enqueue(sql text, topic text DEFAULT NULL)`

入队一个后台执行任务。

**参数:**
- `sql`: 要执行的 SQL 语句
- `topic`: 可选的主题,用于主题特定配置

**返回值:** 任务 ID (bigint)

**示例:**
```sql
SELECT pg_background_enqueue('VACUUM ANALYZE users', 'maintenance');
```

#### `pg_background_queue_ensure_workers()`

确保后台工作进程正在运行,并在需要时启动新的工作进程。

**返回值:** void

**示例:**
```sql
SELECT pg_background_queue_ensure_workers();
```

#### `pg_background_queue_active_workers_count()`

获取当前活跃后台工作进程的数量。

**返回值:** integer

**示例:**
```sql
SELECT pg_background_queue_active_workers_count();
```

#### `pg_background_queue_calibrate_workers_count()`

通过查询实际运行的进程来校准内部工作进程计数器。使用此函数从不一致状态中恢复。

**返回值:** integer (实际工作进程数)

**示例:**
```sql
SELECT pg_background_queue_calibrate_workers_count();
```

### 表

#### `pg_background_tasks`

主任务队列表。

**列:**
- `id` (bigserial): 唯一任务标识符
- `sql_statement` (text): 要执行的 SQL 命令
- `topic` (text): 可选的主题用于配置
- `joined_at` (timestamp): 任务入队时间
- `started_at` (timestamp): 任务执行开始时间
- `closed_at` (timestamp): 任务完成时间(成功或失败)
- `priority` (integer): 任务优先级(越小优先级越高)
- `state` (enum): 当前任务状态(pending, running, retrying, failed, finished)
- `retry_count` (integer): 已进行的重试次数
- `retry_delay_in_sec` (integer): 当前重试延迟
- `errors` (text): 失败执行的错误消息

### 任务状态

- **pending**: 任务已排队并等待执行
- **running**: 任务当前正在被工作进程执行
- **retrying**: 任务失败并计划重试
- **failed**: 任务在耗尽所有重试次数后失败
- **finished**: 任务成功完成

## 监控和故障排查

### 查看任务统计

```sql
-- 按状态统计任务数
SELECT state, COUNT(*) as count
FROM pg_background_tasks
GROUP BY state;

-- 最近的失败任务
SELECT id, sql_statement, errors, closed_at
FROM pg_background_tasks
WHERE state = 'failed'
ORDER BY closed_at DESC
LIMIT 10;

-- 按主题统计平均任务持续时间
SELECT 
    topic,
    COUNT(*) as total_tasks,
    AVG(EXTRACT(EPOCH FROM (closed_at - started_at))) as avg_duration_seconds
FROM pg_background_tasks
WHERE state = 'finished' AND started_at IS NOT NULL
GROUP BY topic;
```

## 性能考虑

1. **工作进程数**: 根据系统资源设置 `max_parallel_running_tasks_count`
2. **优先级**: 使用优先级确保关键任务优先执行
3. **延迟**: 配置 `delay_in_sec` 以防止数据库过载
4. **索引**: 扩展在 `state`、`topic` 和 `joined_at` 上创建索引以获得最佳性能
5. **清理**: 定期归档或删除旧任务以保持性能

## 从 pg_background 升级

该扩展是完全重写的,与原始 `pg_background` 扩展不向后兼容。要迁移:

1. 从旧系统导出任何待处理任务
2. 卸载旧扩展: `DROP EXTENSION pg_background CASCADE;`
3. 安装新扩展: `CREATE EXTENSION pg_background_queue;`
4. 使用新 API 重新入队任务

## 许可证

Copyright (C) 2014-2025, PostgreSQL Global Development Group

该扩展基于原始 `pg_background` 扩展,并在相同的许可条款下分发。

## 贡献

欢迎贡献!请向项目仓库提交问题和拉取请求。

## 支持

对于错误报告和功能请求,请使用 GitHub issue 跟踪器。