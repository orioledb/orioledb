/*-------------------------------------------------------------------------
 *
 * interrupt.c
 *		Routines for background workers interrupt handling.
 *
 * Copyright (c) 2024-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/worker/interrupt.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "workers/interrupt.h"

#include "postmaster/interrupt.h"

/*
 * Exit from an orioledb worker
 */
void
o_worker_shutdown(int elevel)
{
	Assert(MyBackendType == B_BG_WORKER);
	ereport(elevel,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating orioledb worker due to administrator command")));
}

void
o_worker_handle_interrupts(void)
{
	/*
	 * In case of a pending shutdown request we just raise an ERROR message
	 * currently.
	 */
	if (ShutdownRequestPending)
		o_worker_shutdown(ERROR);
}
