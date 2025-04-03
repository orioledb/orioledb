/*-------------------------------------------------------------------------
 *
 * rewind.c
 *		Routines for rewind worker process.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/workers/rewind.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/undo.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/ucm.h"
#include "utils/stopevent.h"
#include "workers/rewind.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/sinvaladt.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#include "pgstat.h"

static volatile sig_atomic_t shutdown_requested = false;
int 	RewindHorizonCheckDelay = 1000; /* Time between checking in ms */
										   
static void
handle_sigterm(SIGNAL_ARGS)
{
	shutdown_requested = true;
	SetLatch(MyLatch);
}

void
register_rewind_worker(void)
{
	BackgroundWorker worker;

	/* Set up background worker parameters */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 0;
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "rewind_worker_main");
	strcpy(worker.bgw_name, "orioledb rewind worker");
	strcpy(worker.bgw_type, "orioledb rewind worker");
	RegisterBackgroundWorker(&worker);
}

void
rewind_worker_main(Datum main_arg)
{
	int			rc,
				wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT;

	/* enable timeout for relation lock */
	RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);

	/* enable relation cache invalidation (remove old OTableDescr) */
	RelationCacheInitialize();
	InitCatalogCache();
	SharedInvalBackendInit(false);

	/* show the in pg_stat_activity, used for tests */
	InitializeSessionUserIdStandalone();
	pgstat_beinit();
	pgstat_bestart();

	SetProcessingMode(NormalProcessing);

	/* catch SIGTERM signal for reason to not interupt background writing */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "orioledb rewind worker started");

	CurTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb rewind worker current transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb rewind worker top transaction context",
												  ALLOCSET_DEFAULT_SIZES);

	ResetLatch(MyLatch);

	PG_TRY();
	{
		MemoryContextSwitchTo(CurTransactionContext);
		while (true)
		{

			if (shutdown_requested)
				break;

			/*
			 * Sleep until we are signaled or time delay for moving rewind horizon.
			 */
			rc = WaitLatch(MyLatch, wake_events,
						   RewindHorizonCheckDelay,
						   WAIT_EVENT_REWIND_WORKER_MAIN);

			if (rc & WL_POSTMASTER_DEATH)
				shutdown_requested = true;

			elog(LOG, "Rewind worker came to check");
		//			MemoryContextReset(CurTransactionContext);
		//			MemoryContextReset(TopTransactionContext);

			ResetLatch(MyLatch);
		}
		elog(LOG, "orioledb rewind worker is shut down");
	}
	PG_CATCH();
	{
		LockReleaseSession(DEFAULT_LOCKMETHOD);
		PG_RE_THROW();
	}
	PG_END_TRY();
}
