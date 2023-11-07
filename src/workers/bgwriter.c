/*-------------------------------------------------------------------------
 *
 * bgwriter.c
 *		Routines for background writer process.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/workers/bgwriter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/undo.h"
#include "s3/headers.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/ucm.h"
#include "utils/stopevent.h"
#include "workers/bgwriter.h"

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
bool		IsBGWriter = false;

static void
handle_sigterm(SIGNAL_ARGS)
{
	shutdown_requested = true;
	SetLatch(MyLatch);
}

void
register_bgwriter(void)
{
	BackgroundWorker worker;

	/* Set up background worker parameters */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 0;
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "bgwriter_main");
	strcpy(worker.bgw_name, "orioledb background writer");
	strcpy(worker.bgw_type, "orioledb background writer");
	RegisterBackgroundWorker(&worker);
}

void
bgwriter_main(Datum main_arg)
{
	OPagePool  *pool;
	int			rc,
				wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT;
	bool		need_eviction,
				need_write;

	/* enable timeout for relation lock */
	RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);

	/* enable relation cache invalidation (remove old OTableDescr) */
	RelationCacheInitialize();
	InitCatalogCache();
	SharedInvalBackendInit(false);

	/* show the bgwriter in pg_stat_activity, used for tests */
	InitializeSessionUserIdStandalone();
#if PG_VERSION_NUM >= 140000
	pgstat_beinit();
#else
	pgstat_initialize();
#endif
	pgstat_bestart();

	SetProcessingMode(NormalProcessing);

	/* catch SIGTERM signal for reason to not interupt background writing */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "orioledb background writer started");
	IsBGWriter = true;

	if (debug_disable_bgwriter)
	{
		elog(LOG, "orioledb background writer stopped: orioledb.debug_disable_bgwriter = True");
		return;
	}

	CurTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb bgwriter current transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb bgwriter top transaction context",
												  ALLOCSET_DEFAULT_SIZES);

	ResetLatch(MyLatch);

	PG_TRY();
	{
		MemoryContextSwitchTo(CurTransactionContext);
		while (true)
		{
			OPagePoolType poolType;
			UndoLocation lastUsedLocation;
			UndoLocation writeInProgressLocation;

			if (shutdown_requested)
				break;

			/*
			 * Sleep until we are signaled or it's time for another
			 * checkpoint.
			 */
			rc = WaitLatch(MyLatch, wake_events,
						   BgWriterDelay,
						   WAIT_EVENT_BGWRITER_MAIN);

			if (rc & WL_POSTMASTER_DEATH)
				shutdown_requested = true;

			for (poolType = 0; poolType < OPagePoolTypesCount && !shutdown_requested; poolType++)
			{
				pool = get_ppool(poolType);
				need_eviction = ppool_free_pages_count(pool) < pool->size / 20;
				need_write = ppool_dirty_pages_count(pool) > pool->size / 2;

				if (need_eviction || need_write)
				{
					int			i = 0;

					while (need_eviction || need_write)
					{
						ppool_run_clock(pool, need_eviction, &shutdown_requested);
						i++;

						if (i >= bgwriter_lru_maxpages * (BLCKSZ / ORIOLEDB_BLCKSZ))
							break;

						if (shutdown_requested)
							break;

						need_eviction = ppool_free_pages_count(pool) < pool->size / 20;
						need_write = ppool_dirty_pages_count(pool) > pool->size / 2;
					}

					MemoryContextReset(CurTransactionContext);
					MemoryContextReset(TopTransactionContext);
				}

				if (!shutdown_requested && ucm_epoch_needs_shift(&pool->ucm))
				{
					if (ucm_epoch_needs_shift(&pool->ucm))
						ucm_epoch_shift(&pool->ucm);
				}
			}

			writeInProgressLocation = pg_atomic_read_u64(&undo_meta->writeInProgressLocation);
			lastUsedLocation = pg_atomic_read_u64(&undo_meta->lastUsedLocation);
			if (writeInProgressLocation + undo_circular_buffer_size <
				lastUsedLocation + undo_circular_buffer_size / 20)
			{
				UndoLocation minProcReservedLocation = pg_atomic_read_u64(&undo_meta->minProcReservedLocation);
				UndoLocation targetLocation = lastUsedLocation - (19 * undo_circular_buffer_size) / 20;

				if (targetLocation < minProcReservedLocation)
					write_undo(targetLocation, minProcReservedLocation, true);
			}

			check_pending_truncates();

			if (orioledb_s3_mode)
				s3_headers_try_eviction_cycle();

			ResetLatch(MyLatch);
		}
		elog(LOG, "orioledb bgwriter is shut down");
	}
	PG_CATCH();
	{
		LockReleaseSession(DEFAULT_LOCKMETHOD);
		PG_RE_THROW();
	}
	PG_END_TRY();
}
