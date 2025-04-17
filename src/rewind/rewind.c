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
#include "rewind/rewind.h"

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
static	RewindItem *rewindBuffer = NULL;

#define REWIND_FILE_SIZE (0x1000000)
#define REWIND_BUFFERS_TAG (0)

RewindMeta    *rewindMeta;

static OBuffersDesc buffersDesc = {
	.singleFileSize = REWIND_FILE_SIZE,
	.filenameTemplate = {ORIOLEDB_DATA_DIR "/%02X%08X.rewindmap"},
	.groupCtlTrancheName = "rewindBuffersGroupCtlTranche",
	.bufferCtlTrancheName = "rewindBuffersCtlTranche"
};

Size
rewind_shmem_needs(void)
{
	Size	size;

	if (!enable_rewind)
		return 0;

	buffersDesc.buffersCount = rewind_buffers_count;

	size = CACHELINEALIGN(sizeof(RewindMeta));
	size = add_size(size, mul_size(rewind_circular_buffer_size, sizeof(RewindItem)));
	size = add_size(size, o_buffers_shmem_needs(&buffersDesc));

	return size;
}

void
rewind_init_shmem(Pointer ptr, bool found)
{
	if (!enable_rewind)
		return;

	rewind_meta = (XidMeta *) ptr;
	ptr += MAXALIGN(sizeof(RewindMeta));
	rewindBuffer = (RewindItem *) ptr;
	ptr += rewind_circular_buffer_size * sizeof(RewindItem);
	o_buffers_shmem_init(&buffersDesc, ptr, found);
	ptr += o_buffers_shmem_needs(&buffersDesc);

	if (!found)
	{
		int64       i;

		for (i = 0; i < rewind_circular_buffer_size; i++)
		{
			rewindBuffer[i].oxid = InvalidOXid;
			rewindBuffer[i].undoStackLocations.location = InvalidUndoLocation;
			rewindBuffer[i].undoStackLocations.branchLocation = InvalidUndoLocation;
			rewindBuffer[i].undoStackLocations.subxactLocation = InvalidUndoLocation;
			rewindBuffer[i].undoStackLocations.onCommitLocation = InvalidUndoLocation;
			rewindBuffer[i].timestamp = 0;
		}

		rewindMeta->rewindMapTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&rewindMeta->xidMapWriteLock, rewindMeta->rewindMapTrancheId);

		rewindMeta.readPos = 0;
		rewindMeta.writtenPos = 0;
	}
}

/*
 * Code for rewind worker
 */

static void
handle_sigterm(SIGNAL_ARGS)
{
	shutdown_requested = true;
	SetLatch(MyLatch);
}

void
register_rewindworker(void)
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
				UndoStackSharedLocations *sharedLocations;
				RewindItem  *rewindItem;
				bufferLength = ORIOLEDB_BLCKSZ / sizeof(RewindMapItem);

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

			while (true)
			{
				UndoStackSharedLocations *sharedLocations;

				if (rewindMeta.cleanedPos == rewindMeta.addPos)
				{
					/* All are already fixed, do nothing */
					break;
				}

				rewindItem = &rewindBuffer[rewindMeta.cleanedPos % xid_circular_buffer_size];
				Assert (rewindItem->oxid != InvalidOXid);

				if (!TimestampDifferenceExceeds(rewindItem->timestamp,
												 GetCurrentTimestamp(),
												 rewind_max_period))
				{
					/* Too early, do nothing */
					break;
				}

				/* Fix current transaction clear the item in the circular buffer and move to the next */
				sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
				pg_atomic_write_u64(&sharedLocations->onCommitLocation, newOnCommitLocation);

				Assert(COMMITSEQNO_IS_RETAINED_FOR_REWIND(rewindItem.csn));

				csn = rewindItem->oxid.csn & (~COMMITSEQNO_RETAINED_FOR_REWIND);
				///
				///
				///

				rewindItem->oxid = InvalidOXid;

				rewindMeta.cleanedPos++;

				if (rewindMeta.cleanedPos - rewindMeta.readPos > bufferLength)
				{
					/* Read buffer from disk */
					int start = rewindMeta.readPos % rewind_circular_buffer_size;
					int length_to_end = rewind_circular_buffer_size - start;

#ifdef USE_ASSERT_CHECKING
					for(int pos = rewindMeta.readPos; pos <= rewindMeta.readPos + bufferLength; pos++)
						Assert(rewindBuffer[pos % rewind_circular_buffer_size].oxid == InvalidOXid);
#endif

					if (length_to_end < bufferLength)
					{
						o_buffers_read(&buffersDesc,
								(Pointer) &rewindBuffer[start], REWWIND_BUFFERS_TAG,
								rewindMeta->readPos * sizeof(RewindMapItem),
								length_to_end * sizeof(RewindMapItem));
						o_buffers_read(&buffersDesc,
								(Pointer) &rewindBuffer[0], REWWIND_BUFFERS_TAG,
								(rewindMeta->readPos + length_to_end) * sizeof(RewindMapItem),
								(bufferLength - length_to_end) * sizeof(RewindMapItem));
					}
					else
					{
						o_buffers_read(&buffersDesc,
								(Pointer) &rewindBuffer[start], REWWIND_BUFFERS_TAG,
								rewindMeta->readPos * sizeof(RewindMapItem),
								bufferLength * sizeof(RewindMapItem));
					}

#ifdef USE_ASSERT_CHECKING
					for(int pos = rewindMeta.readPos; pos <= rewindMeta.readPos + bufferLength; pos++)
						Assert(rewindBuffer[pos % rewind_circular_buffer_size].oxid != InvalidOXid);
#endif
					rewindMeta->readPos += bufferLength;
				}

			}

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
