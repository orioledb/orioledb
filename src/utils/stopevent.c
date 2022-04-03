/*-------------------------------------------------------------------------
 *
 * stopevent.c
 *     Auxiliary infrastructure for automated testing of concurrency issues.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *   contrib/orioledb/src/utils/stopevent.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "utils/stopevent.h"

#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/condition_variable.h"
#include "storage/proclist.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonpath.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#define QUERY_BUFFER_SIZE 1024

typedef struct
{
	char		condition[QUERY_BUFFER_SIZE];
	bool		enabled;
	slock_t		lock;
	ConditionVariable cv;
} StopEvent;

static StopEvent *stopevents = NULL;

bool		enable_stopevents = false;
bool		trace_stopevents = false;
MemoryContext stopevents_cxt = NULL;

PG_FUNCTION_INFO_V1(pg_stopevent_set);
PG_FUNCTION_INFO_V1(pg_stopevent_reset);
PG_FUNCTION_INFO_V1(pg_stopevents);

static const char *const stopeventnames[] = {
#include "utils/stopevents_data.h"
};

Size
StopEventShmemSize(void)
{
	Size		size;

	size = mul_size(STOPEVENTS_COUNT, sizeof(StopEvent));
	return size;
}

void
StopEventShmemInit(Pointer ptr, bool found)
{
	stopevents = (StopEvent *) ptr;

	if (!found)
	{
		int			i;

		for (i = 0; i < STOPEVENTS_COUNT; i++)
		{
			SpinLockInit(&stopevents[i].lock);
			stopevents[i].enabled = false;
			ConditionVariableInit(&stopevents[i].cv);
		}
	}
}

static StopEvent *
find_stop_event(text *name)
{
	int			i;
	char	   *name_data = VARDATA_ANY(name);
	int			len = VARSIZE_ANY_EXHDR(name);

	for (i = 0; i < STOPEVENTS_COUNT; i++)
	{
		if (strlen(stopeventnames[i]) == len &&
			memcmp(name_data, stopeventnames[i], len) == 0)
			return &stopevents[i];
	}

	elog(ERROR, "unknown stop event: \"%s\"", text_to_cstring(name));
	return NULL;
}

Datum
pg_stopevent_set(PG_FUNCTION_ARGS)
{
	text	   *event_name = PG_GETARG_TEXT_PP(0);
	JsonPath   *condition = PG_GETARG_JSONPATH_P(1);
	StopEvent  *event;

	event = find_stop_event(event_name);

	if (VARSIZE_ANY(condition) > QUERY_BUFFER_SIZE)
		elog(ERROR, "jsonpath condition is too long");

	SpinLockAcquire(&event->lock);
	event->enabled = true;
	memcpy(&event->condition, condition, VARSIZE_ANY(condition));
	SpinLockRelease(&event->lock);

	ConditionVariableBroadcast(&event->cv);

	PG_FREE_IF_COPY(condition, 1);
	PG_RETURN_VOID();
}

Datum
pg_stopevent_reset(PG_FUNCTION_ARGS)
{
	text	   *event_name = PG_GETARG_TEXT_PP(0);
	StopEvent  *event;
	proclist_mutable_iter iter;
	bool		result = false;

	event = find_stop_event(event_name);

	SpinLockAcquire(&event->lock);

	SpinLockAcquire(&event->cv.mutex);
	proclist_foreach_modify(iter, &event->cv.wakeup, cvWaitLink)
	{
		result = true;
	}
	SpinLockRelease(&event->cv.mutex);

	event->enabled = false;
	SpinLockRelease(&event->lock);

	ConditionVariableBroadcast(&event->cv);

	PG_RETURN_BOOL(result);
}

Datum
pg_stopevents(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	bool		randomAccess;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext oldcontext;
	AttrNumber	attnum;
	int			i;

	/* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupdesc = CreateTemplateTupleDesc(3);
	attnum = (AttrNumber) 1;
	TupleDescInitEntry(tupdesc, attnum, "stopevent", TEXTOID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "condition", JSONPATHOID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "waiters", INT4ARRAYOID, -1, 0);

	randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
	tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < STOPEVENTS_COUNT; i++)
	{
		Datum		values[3];
		bool		nulls[3] = {false, false, false};
		StopEvent  *event = &stopevents[i];
		proclist_mutable_iter iter;
		List	   *waiters = NIL;
		Datum	   *elems;
		ListCell   *lc;
		int			j;

		SpinLockAcquire(&event->lock);
		if (!event->enabled)
		{
			SpinLockRelease(&event->lock);
			continue;
		}
		values[0] = PointerGetDatum(cstring_to_text(stopeventnames[i]));
		values[1] = PointerGetDatum(&event->condition);

		SpinLockAcquire(&event->cv.mutex);
		proclist_foreach_modify(iter, &event->cv.wakeup, cvWaitLink)
		{
			PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

			waiters = lappend_int(waiters, waiter->pid);
		}
		SpinLockRelease(&event->cv.mutex);

		elems = (Datum *) palloc(sizeof(Datum) * list_length(waiters));
		j = 0;
		foreach(lc, waiters)
		{
			elems[j] = Int32GetDatum(lfirst_int(lc));
			j++;
		}
		values[2] = PointerGetDatum(construct_array(elems, list_length(waiters), INT4OID, 4, true, 'i'));

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		SpinLockRelease(&event->lock);
	}
	PG_RETURN_VOID();
}

bool
pid_is_waiting_for_stopevent(int pid)
{
	int			i;

	for (i = 0; i < STOPEVENTS_COUNT; i++)
	{
		StopEvent  *event = &stopevents[i];
		proclist_mutable_iter iter;

		SpinLockAcquire(&event->lock);
		if (!event->enabled)
		{
			SpinLockRelease(&event->lock);
			continue;
		}

		SpinLockAcquire(&event->cv.mutex);
		proclist_foreach_modify(iter, &event->cv.wakeup, cvWaitLink)
		{
			PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

			if (waiter->pid == pid)
			{
				SpinLockRelease(&event->cv.mutex);
				SpinLockRelease(&event->lock);
				return true;
			}
		}
		SpinLockRelease(&event->cv.mutex);
		SpinLockRelease(&event->lock);
	}
	return false;
}

static Jsonb *
make_process_params(void)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	const char *beType;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

#if PG_VERSION_NUM >= 140000
	if (MyBEEntry->st_backendType == B_BG_WORKER)
		beType = GetBackgroundWorkerTypeByPid(MyBEEntry->st_procpid);
	else
		beType = GetBackendTypeDesc(MyBEEntry->st_backendType);
#else
	if (MyBackendType == B_BG_WORKER)
		beType = GetBackgroundWorkerTypeByPid(MyProcPid);
	else
		beType = GetBackendTypeDesc(MyBackendType);
#endif

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(&state, "pid", MyProcPid);
	if (beType)
		jsonb_push_string_key(&state, "backendType", beType);
	else
		jsonb_push_null_key(&state, "backendType");
	jsonb_push_string_key(&state, "applicationName", application_name);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

static bool
check_stopevent_condition(StopEvent *event, Jsonb *params)
{
	Datum		res;

	SpinLockAcquire(&event->lock);
	if (!event->enabled)
	{
		SpinLockRelease(&event->lock);
		return false;
	}

	res = DirectFunctionCall4(jsonb_path_match,
							  PointerGetDatum(params),
							  PointerGetDatum(&event->condition),
							  PointerGetDatum(make_process_params()),
							  BoolGetDatum(false));

	SpinLockRelease(&event->lock);

	return DatumGetBool(res);
}

static Jsonb *
make_empty_params(void)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

void
handle_stopevent(int event_id, Jsonb *params)
{
	StopEvent  *event = &stopevents[event_id];

	Assert(event_id >= 0 && event_id < STOPEVENTS_COUNT);

	if (!params)
		params = make_empty_params();

	if (event->enabled && check_stopevent_condition(event, params))
	{
		ConditionVariablePrepareToSleep(&event->cv);
		for (;;)
		{
			if (!check_stopevent_condition(event, params))
				break;
			ConditionVariableSleep(&event->cv, PG_WAIT_EXTENSION);
		}
		ConditionVariableCancelSleep();
	}

	if (trace_stopevents)
	{
		char	   *params_string;

		params_string = DatumGetCString(DirectFunctionCall1(jsonb_out, PointerGetDatum(params)));
		elog(LOG, "stop event \"%s\", params \"%s\"",
			 stopeventnames[event_id],
			 params_string);
		pfree(params_string);
	}

	MemoryContextReset(stopevents_cxt);
}

bool
check_stopevent(int event_id, Jsonb *params)
{
	StopEvent  *event = &stopevents[event_id];

	Assert(event_id >= 0 && event_id < STOPEVENTS_COUNT);

	if (event->enabled && check_stopevent_condition(event, params))
		return true;

	return false;
}

void
wait_for_stopevent_enabled(int event_id)
{
	StopEvent  *event = &stopevents[event_id];

	Assert(event_id >= 0 && event_id < STOPEVENTS_COUNT);

	if (event->enabled)
		return;

	ConditionVariablePrepareToSleep(&event->cv);
	for (;;)
	{
		if (event->enabled)
			break;
		ConditionVariableSleep(&event->cv, PG_WAIT_EXTENSION);
	}
	ConditionVariableCancelSleep();
}

void
stopevents_make_cxt(void)
{
	if (!stopevents_cxt)
		stopevents_cxt = AllocSetContextCreate(TopMemoryContext,
											   "StopEventsMemoryContext",
											   ALLOCSET_DEFAULT_SIZES);
}
