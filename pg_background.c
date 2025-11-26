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
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
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

/* GUC variables */
static int	pg_background_reserved_workers = 2;
static int	pg_background_max_parallel_running_tasks_count = 0;
static int	pg_background_retry_count = 0;
static int	pg_background_delay_in_sec = 0;

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;

/* Function declarations */
PG_FUNCTION_INFO_V1(pg_background_launch);
PGDLLEXPORT void pg_background_worker_main(Datum);

static void handle_sigterm(SIGNAL_ARGS);
static void execute_sql_string(const char *sql);
static void process_pending_tasks(void);
static char* extract_first_topic(ArrayType *topics_array);
static int count_running_tasks_for_topic(const char *topic);
static int get_config_for_topic(const char *param_name, const char *topic, int default_value);
static void update_task_state_running(int64 task_id);
static void update_task_state_finished(int64 task_id);
static void update_task_state_retrying(int64 task_id, int new_retry_count, int retry_delay, const char *error_msg);
static void update_task_state_failed(int64 task_id, const char *error_msg);

/*
 * Module initialization function
 */
void
_PG_init(void)
{
	DefineCustomIntVariable("pg_background.reserved_workers",
							"Number of workers to reserve for PostgreSQL.",
							NULL,
							&pg_background_reserved_workers,
							2, 0, INT_MAX,
							PGC_SIGHUP, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_background.max_parallel_running_tasks_count",
							"Maximum parallel running tasks (0 = auto).",
							NULL,
							&pg_background_max_parallel_running_tasks_count,
							0, 0, INT_MAX,
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
}

/*
 * Launch a background task.
 * Inserts task into queue and triggers processing.
 * Following AGENTS.md flow: Save task → Process pending tasks → End
 */
Datum
pg_background_launch(PG_FUNCTION_ARGS)
{
	text	   *sql;
	ArrayType  *topics_array = NULL;
	char	   *sql_cstr;
	int64		task_id;
	int			ret;
	StringInfoData query;
	Oid			argtypes[2];
	Datum		values[2];
	char		nulls[2];
	bool		isnull;

	if (!PG_ARGISNULL(0))
		sql = PG_GETARG_TEXT_PP(0);
	else
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sql argument cannot be NULL")));

	sql_cstr = text_to_cstring(sql);

	elog(NOTICE, "=== pg_background_launch ENTRY ===");

	if (PG_NARGS() >= 2 && !PG_ARGISNULL(1))
		topics_array = PG_GETARG_ARRAYTYPE_P(1);

	/* Step 1: Save task to pg_background_tasks */
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not connect to SPI manager")));

	initStringInfo(&query);
	appendStringInfoString(&query,
						   "INSERT INTO pg_background_tasks "
						   "(sql_statement, state, retry_count, retry_delay_in_sec, topics) "
						   "VALUES ($1, 'pending', 0, 0, $2) "
						   "RETURNING id");

	argtypes[0] = TEXTOID;
	argtypes[1] = TEXTARRAYOID;
	values[0] = CStringGetTextDatum(sql_cstr);
	values[1] = PointerGetDatum(topics_array);
	nulls[0] = ' ';
	nulls[1] = (topics_array == NULL) ? 'n' : ' ';

	ret = SPI_execute_with_args(query.data, 2, argtypes, values, nulls, false, 0);

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

	/* Step 2: Process pending tasks */
	elog(NOTICE, "Before process_pending_tasks");
	process_pending_tasks();
	elog(NOTICE, "=== pg_background_launch EXIT ===");

	PG_RETURN_INT64(task_id);
}

/*
 * Process pending tasks from the queue.
 * Following AGENTS.md flow:
 *   Pop top 1 task → Check max_parallel_running_tasks_count →
 *   Register worker if allowed → End
 */
static void
process_pending_tasks(void)
{
	int			ret;
	int64		task_id = 0;
	ArrayType  *topics_array = NULL;
	char	   *topic = NULL;
	int			max_parallel;
	int			running_count;
	bool		isnull;
	const char *query;
	BackgroundWorker worker;
	BackgroundWorkerHandle *worker_handle;

	elog(NOTICE, "process_pending_tasks: START");

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(NOTICE, "process_pending_tasks: SPI_connect failed");
		return;
	}

	elog(NOTICE, "process_pending_tasks: SPI connected");

	/* Step 1: Pop top 1 task from pending queue */
	query = "SELECT id, topics FROM pg_background_tasks "
			"WHERE state IN ('pending', 'retrying') "
			"AND (joined_at + (COALESCE(retry_delay_in_sec, 0) || ' seconds')::INTERVAL) <= NOW() "
			"ORDER BY joined_at, id LIMIT 1 FOR UPDATE SKIP LOCKED";

	elog(NOTICE, "process_pending_tasks: About to execute query");
	ret = SPI_execute(query, false, 0);
	elog(NOTICE, "process_pending_tasks: Query executed, ret=%d, processed=%lu", ret, SPI_processed);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		elog(NOTICE, "process_pending_tasks: No tasks to process");
		SPI_finish();
		return;
	}

	elog(NOTICE, "process_pending_tasks: Getting task details");

	task_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc,
										  1, &isnull));

	elog(NOTICE, "process_pending_tasks: task_id=%ld", (long)task_id);

	if (!isnull)
	{
		Datum topics_datum = SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc,
										   2, &isnull);
		if (!isnull)
		{
			topics_array = DatumGetArrayTypePCopy(topics_datum);
		}
	}

	elog(NOTICE, "process_pending_tasks: About to extract topic");

	if (topics_array != NULL)
		topic = extract_first_topic(topics_array);

	elog(NOTICE, "process_pending_tasks: topic=%s", topic ? topic : "NULL");

	/* Step 2: Check max_parallel_running_tasks_count for topic */
	max_parallel = (pg_background_max_parallel_running_tasks_count > 0)
		? get_config_for_topic("max_parallel_running_tasks_count", topic,
							   pg_background_max_parallel_running_tasks_count)
		: (max_worker_processes - pg_background_reserved_workers);

	running_count = count_running_tasks_for_topic(topic);

	if (running_count >= max_parallel)
	{
		if (topic != NULL)
			pfree(topic);
		SPI_finish();
		return;
	}

	SPI_finish();

	/* Step 3: Try RegisterDynamicBackgroundWorker */
	/* Get database and user OIDs */
	Oid db_oid = MyDatabaseId;
	Oid user_oid = GetUserId();

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_background");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "pg_background_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_background task %ld", (long) task_id);
#if PG_VERSION_NUM >= 110000
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_background");
#endif
	worker.bgw_main_arg = Int64GetDatum(task_id);
	worker.bgw_notify_pid = 0;

	/* Store only database_id and user_id as OIDs in bgw_extra (8 bytes total) */
	memcpy(&worker.bgw_extra[0], &db_oid, sizeof(Oid));
	memcpy(&worker.bgw_extra[sizeof(Oid)], &user_oid, sizeof(Oid));

	elog(NOTICE, "pg_background: attempting to register worker for task %ld", (long) task_id);

	if (!RegisterDynamicBackgroundWorker(&worker, &worker_handle))
	{
		elog(WARNING, "pg_background: failed to register worker for task %ld", (long) task_id);
	}
	else
	{
		BgwHandleStatus status;
		pid_t		pid;

		elog(NOTICE, "pg_background: successfully registered worker for task %ld", (long) task_id);

		/* Wait for the worker to start */
		status = WaitForBackgroundWorkerStartup(worker_handle, &pid);

		if (status == BGWH_STARTED)
		{
			elog(NOTICE, "pg_background: worker %ld started with PID %d", (long) task_id, (int) pid);
		}
		else if (status == BGWH_STOPPED)
		{
			elog(WARNING, "pg_background: worker %ld stopped before starting", (long) task_id);
		}
		else
		{
			elog(WARNING, "pg_background: worker %ld failed to start, status=%d", (long) task_id, status);
		}

		pfree(worker_handle);
	}

	if (topic != NULL)
		pfree(topic);
}

/*
 * Background worker main function.
 * Following AGENTS.md flow:
 *   Begin transaction → Mark running → Check delay → pg_sleep →
 *   Execute SQL → Mark finished/failed/retrying → Commit →
 *   Process pending tasks (recursive)
 */
void
pg_background_worker_main(Datum main_arg)
{
	int64		task_id;
	int			ret;
	char	   *sql = NULL;
	ArrayType  *topics_array = NULL;
	char	   *topic = NULL;
	int			retry_count = 0;
	int			delay_in_sec = 0;
	int			max_retries;
	int			retry_delay;
	bool		isnull;
	StringInfoData query;
	ErrorData  *edata;
	char		error_msg[1024];
	Oid			database_id;
	Oid			user_id;

	task_id = DatumGetInt64(main_arg);

	/* Establish signal handlers */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	elog(NOTICE, "pg_background_worker_main: started for task %ld", (long) task_id);

	/* Set up a memory context and resource owner BEFORE accessing MyBgworkerEntry */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_background");
	CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
												 "pg_background session",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

	/* Extract database_id and user_id from bgw_extra */
	memcpy(&database_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));

	elog(NOTICE, "pg_background_worker_main: database_id=%u, user_id=%u",
		 database_id, user_id);

	/* Connect to the database using OIDs directly */
	BackgroundWorkerInitializeConnectionByOid(database_id, user_id
#if PG_VERSION_NUM >= 110000
										 , 0
#endif
	);

	/* Step 1: Begin transaction & fetch task details */
	StartTransactionCommand();

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		CommitTransactionCommand();
		return;
	}

  elog(NOTICE, "pg_background_worker_main: database(%s) is connected", get_database_name(database_id));

	initStringInfo(&query);
	appendStringInfoString(&query,
						   "SELECT sql_statement, topics, retry_count "
						   "FROM pg_background_tasks WHERE id = $1");

	{
		Oid			argtypes[1];
		Datum		values[1];
		char		nulls[1];

		argtypes[0] = INT8OID;
		values[0] = Int64GetDatum(task_id);
		nulls[0] = ' ';

		ret = SPI_execute_with_args(query.data, 1, argtypes, values, nulls, false, 0);
	}

	if (ret != SPI_OK_SELECT || SPI_processed != 1)
	{
		SPI_finish();
		CommitTransactionCommand();
		return;
	}

	sql = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc,
											1, &isnull));

	topics_array = DatumGetArrayTypeP(SPI_getbinval(SPI_tuptable->vals[0],
													SPI_tuptable->tupdesc,
													2, &isnull));
	if (isnull)
		topics_array = NULL;

	retry_count = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc,
											  3, &isnull));

	if (topics_array != NULL)
		topic = extract_first_topic(topics_array);

	/* Step 2: Mark task state as running */
	update_task_state_running(task_id);

	SPI_finish();
	CommitTransactionCommand();

	/* Step 3: Check delay config by topic & pg_sleep */
	delay_in_sec = get_config_for_topic("delay_in_sec", topic, pg_background_delay_in_sec);

	if (delay_in_sec > 0)
		pg_usleep(delay_in_sec * 1000000L);

	/* Step 4: Execute sql_statement */
	SetCurrentStatementStartTimestamp();
	debug_query_string = sql;
	pgstat_report_activity(STATE_RUNNING, sql);

	StartTransactionCommand();

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

		CommitTransactionCommand();

		/* Step 5: Mark task state as finished */
		StartTransactionCommand();
		if (SPI_connect() == SPI_OK_CONNECT)
		{
			update_task_state_finished(task_id);
			SPI_finish();
		}
		CommitTransactionCommand();
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

		/* Step 6: Check retry config & mark retrying/failed */
		StartTransactionCommand();
		if (SPI_connect() == SPI_OK_CONNECT)
		{
			max_retries = get_config_for_topic("retry_count", topic, pg_background_retry_count);

			if (retry_count < max_retries)
			{
				retry_delay = 1 << (retry_count + 1);
				update_task_state_retrying(task_id, retry_count + 1, retry_delay, error_msg);
			}
			else
			{
				update_task_state_failed(task_id, error_msg);
			}

			SPI_finish();
		}
		CommitTransactionCommand();
	}
	PG_END_TRY();

	pgstat_report_activity(STATE_IDLE, NULL);

	if (topic != NULL)
		pfree(topic);

	/* Step 7: Process pending tasks (recursive trigger) */
	StartTransactionCommand();
	process_pending_tasks();
	elog(NOTICE, "=== pg_background_launch EXIT ===");
	CommitTransactionCommand();
}

/*
 * Count running tasks for a specific topic (or all tasks if topic is NULL).
 */
static int
count_running_tasks_for_topic(const char *topic)
{
	int			ret;
	int			count = 0;
	bool		count_isnull;
	StringInfoData query;

	initStringInfo(&query);

	if (topic == NULL)
	{
		appendStringInfoString(&query,
							   "SELECT COUNT(*) FROM pg_background_tasks "
							   "WHERE state = 'running'");
		ret = SPI_execute(query.data, true, 0);
	}
	else
	{
		Oid			argtypes[1];
		Datum		values[1];
		char		nulls[1];

		appendStringInfoString(&query,
							   "SELECT COUNT(*) FROM pg_background_tasks "
							   "WHERE state = 'running' AND $1 = ANY(topics)");

		argtypes[0] = TEXTOID;
		values[0] = CStringGetTextDatum(topic);
		nulls[0] = ' ';

		ret = SPI_execute_with_args(query.data, 1, argtypes, values, nulls, true, 0);
	}

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
		count = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc,
											1, &count_isnull));

	return count;
}

/*
 * Extract the first topic from a PostgreSQL TEXT[] array.
 */
static char*
extract_first_topic(ArrayType *topics_array)
{
	Datum		topic_datum;
	bool		isnull;
	text	   *topic_text;

	if (topics_array == NULL || ARR_NDIM(topics_array) == 0)
		return NULL;

	if (ARR_DIMS(topics_array)[0] == 0)
		return NULL;

	/* Use deconstruct_array to safely extract elements */
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;

	deconstruct_array(topics_array, TEXTOID, -1, false, TYPALIGN_INT,
					  &elems, &nulls, &nelems);

	if (nelems == 0 || nulls[0])
	{
		pfree(elems);
		pfree(nulls);
		return NULL;
	}

	topic_text = DatumGetTextPP(elems[0]);
	char *result = text_to_cstring(topic_text);

	pfree(elems);
	pfree(nulls);

	return result;
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
