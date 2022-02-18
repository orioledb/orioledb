/*-------------------------------------------------------------------------
 *
 * stopevent.h
 *		Decalarations forstop events.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/stopevent.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __STOPEVENT_H__
#define __STOPEVENT_H__

#include "utils/jsonb.h"
#include "utils/stopevents_defs.h"

extern bool enable_stopevents;
extern bool trace_stopevents;
extern MemoryContext stopevents_cxt;

#define STOPEVENTS_ENABLED() \
	(enable_stopevents || trace_stopevents)

#define STOPEVENT(event_id, params) \
	do { \
		if (STOPEVENTS_ENABLED()) \
			handle_stopevent((event_id), (params)); \
	} while(0)

#define STOPEVENT_CONDITION(event_id, params) \
	(STOPEVENTS_ENABLED() && check_stopevent((event_id), (params)))

extern Size StopEventShmemSize(void);
extern void StopEventShmemInit(Pointer ptr, bool found);
extern Datum pg_stopevent_set(PG_FUNCTION_ARGS);
extern Datum pg_stopevent_reset(PG_FUNCTION_ARGS);
extern Datum pg_stopevents(PG_FUNCTION_ARGS);
extern bool pid_is_waiting_for_stopevent(int pid);
extern void handle_stopevent(int event_id, Jsonb *params);
extern bool check_stopevent(int event_id, Jsonb *params);
extern void wait_for_stopevent_enabled(int event_id);
extern void stopevents_make_cxt(void);

#endif							/* __STOPEVENT_H__ */
