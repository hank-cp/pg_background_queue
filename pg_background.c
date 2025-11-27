/*--------------------------------------------------------------------------
 *
 * pg_background.c
 *		Run SQL commands using background workers with queue management.
 *
 * Copyright (C) 2014, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pg_authid.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"
#include "postmaster/bgworker.h"

#include "pg_background.h"

PG_MODULE_MAGIC;

typedef struct PgBackgroundShmemStruct
{
	pg_atomic_uint32 active_workers_count;
} PgBackgroundShmemStruct;

static PgBackgroundShmemStruct *pg_background_shmem = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/* GUC variables */
static int	pg_background_max_parallel_running_tasks_count = 0;
static int	pg_background_retry_count = 0;
static int	pg_background_delay_in_sec = 0;
static int	pg_background_topic_priority = 100;

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;

/* Function declarations */
PG_FUNCTION_INFO_V1(pg_background_enqueue);
PGDLLEXPORT void pg_background_worker_loop(Datum);

static void handle_sigterm(SIGNAL_ARGS);
static void execute_sql_string(const char *sql);
static void launch_background_worker(void);
static uint32 get_active_workers_count(void);
static int get_config_for_topic(const char *param_name, const char *topic, int default_value);
static void update_task_state_running(int64 task_id);
static void update_task_state_finished(int64 task_id);
static void update_task_state_retrying(int64 task_id, int new_retry_count, int retry_delay, const char *error_msg);
static void update_task_state_failed(int64 task_id, const char *error_msg);
static void pg_background_shmem_startup(void);
static void pg_background_shmem_request(void);
static void pg_background_worker_cleanup(int code, Datum arg);

/*
 * Module initialization function
 */
void
_PG_init(void)
{
	const char *guc_value;
	int			max_worker_processes_value;
	int			default_max_parallel;

	guc_value = GetConfigOption("max_worker_processes", false, false);
	max_worker_processes_value = atoi(guc_value);
	default_max_parallel = max_worker_processes_value / 2;

	DefineCustomIntVariable("pg_background.max_parallel_running_tasks_count",
							"Maximum parallel running tasks (0 = auto).",
							NULL,
							&pg_background_max_parallel_running_tasks_count,
							default_max_parallel, 0, INT_MAX,
							PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_background.retry_count",
							"Number of retries for failed tasks.",
							NULL,
							&pg_background_retry_count,
							0, 0, INT_MAX,
							PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_background.delay_in_sec",
							"Delay in seconds before executing each task.",
							NULL,
							&pg_background_delay_in_sec,
							0, 0, INT_MAX,
							PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_background.topic_priority",
							"Default priority for topics (lower number = higher priority).",
							NULL,
							&pg_background_topic_priority,
							100, 0, INT_MAX,
							PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_background_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_background_shmem_startup;
}

/*
 * Enqueue a background task.
 * Inserts task into queue and triggers worker loop.
 * Following AGENTS.md flow: Insert task → launch_background_worker() → End
 */
Datum
pg_background_enqueue(PG_FUNCTION_ARGS)
{
	text	   *sql;
	text	   *topic_text = NULL;
	char	   *sql_cstr;
	char	   *topic = NULL;
	int64		task_id;
	int			ret;
	StringInfoData query;
	Oid			argtypes[3];
	Datum		values[3];
	char		nulls[3];
	bool		isnull;
	int     priority = 100;

	if (!PG_ARGISNULL(0))
		sql = PG_GETARG_TEXT_PP(0);
	else
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sql argument cannot be NULL")));

	sql_cstr = text_to_cstring(sql);

	if (PG_NARGS() >= 2 && !PG_ARGISNULL(1))
	{
		topic_text = PG_GETARG_TEXT_PP(1);
		topic = text_to_cstring(topic_text);
	}
	priority = get_config_for_topic("priority", topic, pg_background_topic_priority);

	/* Step 1: Insert task to pg_background_tasks */
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not connect to SPI manager")));

	initStringInfo(&query);
	appendStringInfoString(&query,
						   "INSERT INTO pg_background_tasks "
						   "(sql_statement, state, retry_count, retry_delay_in_sec, topic, priority) "
						   "VALUES ($1, 'pending', 0, 0, $2, $3) "
						   "RETURNING id");

	argtypes[0] = TEXTOID;
	argtypes[1] = TEXTOID;
	argtypes[2] = INT4OID;
	values[0] = CStringGetTextDatum(sql_cstr);
	values[1] = (topic != NULL) ? CStringGetTextDatum(topic) : (Datum) 0;
	values[2] = Int32GetDatum(priority);
	nulls[0] = ' ';
	nulls[1] = (topic == NULL) ? 'n' : ' ';
	nulls[2] = ' ';

	ret = SPI_execute_with_args(query.data, 3, argtypes, values, nulls, false, 0);

	if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to insert task into queue")));
	}

	task_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc,
										  1, &isnull));

	SPI_finish();

	elog(INFO, "PG_BACKGROUND_QUEUE MAIN: task %ld enqueued", (long) task_id);

	/* Step 2: launch_background_worker() */
	launch_background_worker();

	PG_RETURN_INT64(task_id);
}

/*
 * Launch background worker loop.
 * Following AGENTS.md flow:
 *   Check max_parallel_running_tasks_count for topic →
 *   If allowed, register worker for pg_background_worker_loop → End
 */
static void
launch_background_worker(void)
{
	uint32		running_count;
	BackgroundWorker worker;
	BackgroundWorkerHandle *worker_handle;
	Oid			db_oid;
	Oid			user_oid;

	if (SPI_connect() != SPI_OK_CONNECT)
		return;

	running_count = get_active_workers_count();

	if (running_count >= pg_background_max_parallel_running_tasks_count)
	{
		elog(LOG, "QUEUE_CONTROL: max parallel limit reached (%u/%d)",
			 running_count, pg_background_max_parallel_running_tasks_count);
		SPI_finish();
		return;
	}
	else
	{
		elog(LOG, "QUEUE_CONTROL: concurrency queue status: (%u/%d)",
			 running_count, pg_background_max_parallel_running_tasks_count);
	}

	SPI_finish();

	db_oid = MyDatabaseId;
	user_oid = GetUserId();

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_background");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "pg_background_worker_loop");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_background worker loop");
#if PG_VERSION_NUM >= 110000
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_background");
#endif
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = MyProcPid;

	memcpy(&worker.bgw_extra[0], &db_oid, sizeof(Oid));
	memcpy(&worker.bgw_extra[sizeof(Oid)], &user_oid, sizeof(Oid));

	if (!RegisterDynamicBackgroundWorker(&worker, &worker_handle))
	{
		elog(WARNING, "QUEUE_CONTROL: Bad happened!! cannot register background process. "
		  "`active_workers_count` is not counted correctly, "
		  "or maybe `pg_background.max_parallel_running_tasks_count` is larger than `max_worker_processes`.");
	}
	else
	{
		BgwHandleStatus status;
		pid_t pid;

		elog(INFO, "QUEUE_CONTROL: worker registered successfully");

		status = WaitForBackgroundWorkerStartup(worker_handle, &pid);

		if (status == BGWH_STARTED)
		{
			uint32 count = pg_atomic_fetch_add_u32(&pg_background_shmem->active_workers_count, 1);
			elog(LOG, "QUEUE_CONTROL: worker started with PID %d, active count: %u",
				 (int)pid, count + 1);
		}
		else if (status == BGWH_STOPPED)
		{
			elog(WARNING, "QUEUE_CONTROL: worker stopped (completed tasks or no tasks available)");
		}
		else if (status == BGWH_POSTMASTER_DIED)
		{
			elog(ERROR, "launch_background_worker: postmaster died, cannot start worker");
		}

		pfree(worker_handle);
	}
}

/*
 * Background worker loop function.
 * Following AGENTS.md flow:
 *   Loop: Pop task → Begin transaction → Mark running → Check delay →
 *   pg_sleep → Execute SQL → Mark finished/failed/retrying → Commit →
 *   Repeat until no tasks
 */
void
pg_background_worker_loop(Datum main_arg)
{
	Oid			database_id;
	Oid			user_id;

	// after background worker launched, wait awhile in case massive tasks are enqueued,
	// worker closed too quick before new tasks batch inserting finished.
	pg_usleep(1000000L);

	before_shmem_exit(pg_background_worker_cleanup, (Datum) 0);

	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Extract database_id and user_id from bgw_extra BEFORE any setup */
	memcpy(&database_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));

	/* Connect to the database - this sets up memory contexts and resource owners */
	BackgroundWorkerInitializeConnectionByOid(database_id, user_id, 0);

	elog(INFO, "PG_BACKGROUND_WORKER: started for database %u, user %u",
		 database_id, user_id);

	/* Main loop: process tasks until queue is empty */
	while (!got_sigterm)
	{
		int			ret;
		int64		task_id = 0;
		char	   *sql = NULL;
		char	   *topic = NULL;
		int			retry_count = 0;
		int			delay_in_sec = 0;
		int			max_retries;
		int			retry_delay;
		bool		isnull;
		StringInfoData query;
		ErrorData  *edata;
		char		error_msg[1024];
		Datum		topic_datum;

		/* Step 1: Begin transaction & Pop top 1 task from pending queue */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_connect() != SPI_OK_CONNECT)
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
			break;
		}

		initStringInfo(&query);
		appendStringInfoString(&query,
							   "SELECT id, sql_statement, topic, retry_count "
							   "FROM pg_background_tasks "
							   "WHERE state IN ('pending', 'retrying') "
							   "AND (joined_at + (COALESCE(retry_delay_in_sec, 0) || ' seconds')::INTERVAL) <= NOW() "
							   "ORDER BY priority, joined_at, id LIMIT 1 FOR UPDATE SKIP LOCKED");

		ret = SPI_execute(query.data, false, 0);

		if (ret != SPI_OK_SELECT || SPI_processed == 0)
		{
			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
      elog(INFO, "PG_BACKGROUND_WORKER: no pending tasks, exiting...");
			break;
		}

		/* Extract task details */
		task_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc,
											  1, &isnull));

		elog(INFO, "PG_BACKGROUND_WORKER: processing task %ld", (long) task_id);

		sql = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc,
												2, &isnull));

		elog(DEBUG1, "PG_BACKGROUND_WORKER: sql=%s", sql);

		topic_datum = SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc,
										  3, &isnull);
		if (!isnull)
			topic = TextDatumGetCString(topic_datum);
		else
			topic = NULL;

		elog(DEBUG1, "PG_BACKGROUND_WORKER: topic=%s", topic ? topic : "NULL");

		retry_count = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc,
												  4, &isnull));

		elog(DEBUG1, "PG_BACKGROUND_WORKER: retry_count=%d", retry_count);

		/* Step 2: Mark task state as running */
		update_task_state_running(task_id);

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		/* Step 3: Check delay config by topic & pg_sleep */
		delay_in_sec = get_config_for_topic("delay_in_sec", topic, pg_background_delay_in_sec);

		if (delay_in_sec > 0)
		{
			elog(DEBUG1, "PG_BACKGROUND_WORKER: sleeping for %d seconds", delay_in_sec);
			pg_usleep(delay_in_sec * 1000000L);
		}

		/* Step 4: Execute sql_statement */
		SetCurrentStatementStartTimestamp();
		debug_query_string = sql;
		pgstat_report_activity(STATE_RUNNING, sql);

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

#if PG_VERSION_NUM >= 130000
		if (StatementTimeout > 0)
			enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout);
		else
			disable_timeout(STATEMENT_TIMEOUT, false);
#endif

		PG_TRY();
		{
			execute_sql_string(sql);

#if PG_VERSION_NUM >= 130000
			disable_timeout(STATEMENT_TIMEOUT, false);
#endif

			PopActiveSnapshot();
			CommitTransactionCommand();

			/* Step 5: Mark task state as finished */
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			if (SPI_connect() == SPI_OK_CONNECT)
			{
				update_task_state_finished(task_id);
				SPI_finish();
			}
			PopActiveSnapshot();
			CommitTransactionCommand();

			elog(DEBUG1, "PG_BACKGROUND_WORKER: task %ld completed successfully", (long) task_id);
		}
		PG_CATCH();
		{
#if PG_VERSION_NUM >= 130000
			disable_timeout(STATEMENT_TIMEOUT, false);
#endif

			AbortCurrentTransaction();

			edata = CopyErrorData();
			FlushErrorState();

			snprintf(error_msg, sizeof(error_msg), "%s", edata->message);
			FreeErrorData(edata);

			elog(NOTICE, "PG_BACKGROUND_WORKER: task %ld failed: %s", (long) task_id, error_msg);

			/* Check retry config & mark retrying/failed */
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			if (SPI_connect() == SPI_OK_CONNECT)
			{
				max_retries = get_config_for_topic("retry_count", topic, pg_background_retry_count);

				if (retry_count < max_retries)
				{
					retry_delay = 1 << (retry_count + 1);
					update_task_state_retrying(task_id, retry_count + 1, retry_delay, error_msg);
					elog(INFO, "PG_BACKGROUND_WORKER: task %ld will retry (attempt %d/%d, delay %ds)",
						 (long) task_id, retry_count + 1, max_retries, retry_delay);
				}
				else
				{
					update_task_state_failed(task_id, error_msg);
					elog(WARNING, "PG_BACKGROUND_WORKER: task %ld marked as failed", (long) task_id);
				}

				SPI_finish();
			}
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
		PG_END_TRY();

		pgstat_report_activity(STATE_IDLE, NULL);

		if (topic != NULL)
			pfree(topic);
	}

	elog(INFO, "PG_BACKGROUND_WORKER: exiting...");
}

static void
pg_background_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(sizeof(PgBackgroundShmemStruct));
}

static void
pg_background_shmem_startup(void)
{
	bool found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pg_background_shmem = ShmemInitStruct("pg_background",
										  sizeof(PgBackgroundShmemStruct),
										  &found);

	if (!found)
	{
		pg_atomic_init_u32(&pg_background_shmem->active_workers_count, 0);
	}

	LWLockRelease(AddinShmemInitLock);
}

static void
pg_background_worker_cleanup(int code, Datum arg)
{
	if (pg_background_shmem != NULL)
	{
		uint32 count = pg_atomic_fetch_sub_u32(&pg_background_shmem->active_workers_count, 1);
		elog(LOG, "QUEUE_CONTROL: active workers count decremented to %u", count - 1);
	}
}

static uint32
get_active_workers_count(void)
{
	return pg_atomic_read_u32(&pg_background_shmem->active_workers_count);
}

/*
 * Get configuration value for a specific topic.
 */
static int
get_config_for_topic(const char *param_name, const char *topic, int default_value)
{
	char		guc_name[256];
	const char *guc_value;

	if (topic != NULL && topic[0] != '\0')
	{
		snprintf(guc_name, sizeof(guc_name), "pg_background.%s.%s", param_name, topic);
		guc_value = GetConfigOption(guc_name, true, false);
		if (guc_value != NULL)
			return atoi(guc_value);
	}

	snprintf(guc_name, sizeof(guc_name), "pg_background.%s", param_name);
	guc_value = GetConfigOption(guc_name, true, false);
	if (guc_value != NULL)
		return atoi(guc_value);

	return default_value;
}

/*
 * Update task state to 'running'.
 */
static void
update_task_state_running(int64 task_id)
{
	StringInfoData query;
	Oid			argtypes[1];
	Datum		values[1];
	char		nulls[1];

	initStringInfo(&query);
	appendStringInfoString(&query,
						   "UPDATE pg_background_tasks "
						   "SET state = 'running', started_at = NOW() "
						   "WHERE id = $1");

	argtypes[0] = INT8OID;
	values[0] = Int64GetDatum(task_id);
	nulls[0] = ' ';

	SPI_execute_with_args(query.data, 1, argtypes, values, nulls, false, 0);
}

/*
 * Update task state to 'finished'.
 */
static void
update_task_state_finished(int64 task_id)
{
	StringInfoData query;
	Oid			argtypes[1];
	Datum		values[1];
	char		nulls[1];

	initStringInfo(&query);
	appendStringInfoString(&query,
						   "UPDATE pg_background_tasks "
						   "SET state = 'finished', closed_at = NOW() "
						   "WHERE id = $1");

	argtypes[0] = INT8OID;
	values[0] = Int64GetDatum(task_id);
	nulls[0] = ' ';

	SPI_execute_with_args(query.data, 1, argtypes, values, nulls, false, 0);
}

/*
 * Update task state to 'retrying'.
 */
static void
update_task_state_retrying(int64 task_id, int new_retry_count, int retry_delay, const char *error_msg)
{
	StringInfoData query;
	Oid			argtypes[4];
	Datum		values[4];
	char		nulls[4];

	initStringInfo(&query);
	appendStringInfoString(&query,
						   "UPDATE pg_background_tasks "
						   "SET state = 'retrying', retry_count = $2, "
						   "retry_delay_in_sec = $3, errors = $4 "
						   "WHERE id = $1");

	argtypes[0] = INT8OID;
	argtypes[1] = INT4OID;
	argtypes[2] = INT4OID;
	argtypes[3] = TEXTOID;
	values[0] = Int64GetDatum(task_id);
	values[1] = Int32GetDatum(new_retry_count);
	values[2] = Int32GetDatum(retry_delay);
	values[3] = CStringGetTextDatum(error_msg);
	nulls[0] = ' ';
	nulls[1] = ' ';
	nulls[2] = ' ';
	nulls[3] = ' ';

	SPI_execute_with_args(query.data, 4, argtypes, values, nulls, false, 0);
}

/*
 * Update task state to 'failed'.
 */
static void
update_task_state_failed(int64 task_id, const char *error_msg)
{
	StringInfoData query;
	Oid			argtypes[2];
	Datum		values[2];
	char		nulls[2];

	initStringInfo(&query);
	appendStringInfoString(&query,
						   "UPDATE pg_background_tasks "
						   "SET state = 'failed', closed_at = NOW(), errors = $2 "
						   "WHERE id = $1");

	argtypes[0] = INT8OID;
	argtypes[1] = TEXTOID;
	values[0] = Int64GetDatum(task_id);
	values[1] = CStringGetTextDatum(error_msg);
	nulls[0] = ' ';
	nulls[1] = ' ';

	SPI_execute_with_args(query.data, 2, argtypes, values, nulls, false, 0);
}

/*
 * Signal handler for SIGTERM.
 */
static void
handle_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Execute SQL string using PostgreSQL's parser and executor.
 */
static void
execute_sql_string(const char *sql)
{
	List	   *raw_parsetree_list;
	ListCell   *lc1;
	bool		isTopLevel;
	int			commands_remaining;
	MemoryContext parsecontext;
	MemoryContext oldcontext;

	parsecontext = AllocSetContextCreate(TopMemoryContext,
										 "pg_background parse/plan",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(parsecontext);
	raw_parsetree_list = pg_parse_query(sql);
	commands_remaining = list_length(raw_parsetree_list);
	isTopLevel = commands_remaining == 1;
	MemoryContextSwitchTo(oldcontext);

	foreach(lc1, raw_parsetree_list)
	{
#if PG_VERSION_NUM < 100000
		Node	   *parsetree = (Node *) lfirst(lc1);
#else
		RawStmt    *parsetree = (RawStmt *) lfirst(lc1);
#endif
#if PG_VERSION_NUM >= 130000
		CommandTag	commandTag;
#else
		const char *commandTag;
#endif
#if PG_VERSION_NUM < 130000
		char		completionTag[COMPLETION_TAG_BUFSIZE];
#else
		QueryCompletion qc;
#endif
		List	   *querytree_list,
				   *plantree_list;
		bool		snapshot_set = false;
		Portal		portal;
		DestReceiver *receiver;

		if (IsA(parsetree, TransactionStmt))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("transaction control statements are not allowed in pg_background")));

		commandTag = CreateCommandTag_compat(parsetree);
		set_ps_display_compat(GetCommandTagName(commandTag));

		BeginCommand(commandTag, DestNone);

		if (analyze_requires_snapshot(parsetree))
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshot_set = true;
		}

		querytree_list = pg_analyze_and_rewrite_compat(parsetree, sql, NULL, 0, NULL);

		plantree_list = pg_plan_queries(querytree_list, sql,
#if PG_VERSION_NUM >= 130000
										CURSOR_OPT_PARALLEL_OK,
#else
										0,
#endif
										NULL);

		portal = CreatePortal("pg_background", true, true);
		PortalDefineQuery(portal, NULL, sql, commandTag, plantree_list, NULL);
		PortalStart(portal, NULL, 0, InvalidSnapshot);

		receiver = CreateDestReceiver(DestNone);

		(void) PortalRun(portal, FETCH_ALL, isTopLevel, true, receiver, receiver,
#if PG_VERSION_NUM < 130000
						 completionTag
#else
						 &qc
#endif
		);

		receiver->rDestroy(receiver);

#if PG_VERSION_NUM >= 130000
		EndCommand_compat(&qc, DestNone, false);
#else
		EndCommand(completionTag, DestNone);
#endif

		PortalDrop(portal, false);

		if (snapshot_set)
			PopActiveSnapshot();

		commands_remaining--;
	}

	MemoryContextDelete(parsecontext);
}
