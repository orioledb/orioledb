/*-------------------------------------------------------------------------
 *
 * interrupt.c
 *		Routines for background workers interrupt handling.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/workers/interrupt.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "workers/interrupt.h"

#include "postmaster/interrupt.h"
#include "postmaster/startup.h"

#if PG_VERSION_NUM >= 180000
#define O_STARTUP_PROC_INTERRUPTS() ProcessStartupProcInterrupts()
#else
#define O_STARTUP_PROC_INTERRUPTS() HandleStartupProcInterrupts()
#endif

static void o_worker_shutdown(int elevel);

/*
 * Exit from an orioledb worker
 */
static void
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
	if (AmStartupProcess())
	{
		O_STARTUP_PROC_INTERRUPTS();
	}
	else if (MyBackendType == B_BG_WORKER)
	{
		/*
		 * In case of a pending shutdown request we just raise an ERROR
		 * message currently.
		 */
		if (ShutdownRequestPending)
			o_worker_shutdown(ERROR);
	}
	else
	{
		CHECK_FOR_INTERRUPTS();
	}
}
