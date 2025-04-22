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

Size
rewind_shmem_needs(void)
{
	Size	size;

	if (!enable_rewind)
		return 0;

	rewindBuffersDesc.buffersCount = rewind_buffers_count;

	size = CACHELINEALIGN(sizeof(RewindMeta));
	size = add_size(size, mul_size(rewind_circular_buffer_size, sizeof(RewindItem)));
	size = add_size(size, o_buffers_shmem_needs(&rewindBuffersDesc));

	return size;
}

static void
cleanup_rewind_files(OBuffersDesc *desc, uint32 tag)
{
	char        curFileName[MAXPGPATH];
	File        curFile;
	uint64 		fileNum = 0;

	Assert(OBuffersMaxTagIsValid(tag));

	while (true)
	{
		pg_snprintf(curFileName, MAXPGPATH,
					desc->filenameTemplate[tag],
					(uint32) (fileNum >> 32),
					(uint32) fileNum);
		curFile = PathNameOpenFile(curFileName,
				O_RDWR | O_CREAT | PG_BINARY);
		if (curFile < 0)
			break;

		FileClose(curFile);

		(void) unlink(curFileName);
		fileNum++;
	}
}

void
rewind_init_shmem(Pointer ptr, bool found)
{
	if (!enable_rewind)
		return;

	rewindBuffersDesc.singleFileSize = REWIND_FILE_SIZE;
	rewindBuffersDesc.filenameTemplate = ORIOLEDB_DATA_DIR "/%02X%08X.rewindmap";
	rewindBuffersDesc.groupCtlTrancheName = "rewindBuffersGroupCtlTranche";
	rewindBuffersDesc.bufferCtlTrancheName = "rewindBuffersCtlTranche";

	rewindMeta = (RewindMeta *) ptr;
	ptr += MAXALIGN(sizeof(RewindMeta));
	rewindBuffer = (RewindItem *) ptr;
	ptr += rewind_circular_buffer_size * sizeof(RewindItem);
	o_buffers_shmem_init(&rewindBuffersDesc, ptr, found);
	ptr += o_buffers_shmem_needs(&rewindBuffersDesc);

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

	//	rewindMeta->rewindMapTrancheId = LWLockNewTrancheId();
	//	LWLockInitialize(&rewindMeta->xidMapWriteLock, rewindMeta->rewindMapTrancheId);

		rewindMeta->readPos = 0;
		rewindMeta->writtenPos = 0;
		rewindMeta->addedPos = 0;
		rewindMeta->completedPos = 0;
		rewindMeta->oldCleanedFileNum = 0;

		/* Rewind buffers are not persistent */
		cleanup_rewind_files(&rewindBuffersDesc, REWIND_BUFFERS_TAG);
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
	UndoStackSharedLocations *sharedLocations;
	RewindItem  *rewindItem;
	int			bufferLength = ORIOLEDB_BLCKSZ / sizeof(RewindItem);

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

//			elog(LOG, "Rewind worker came to check");

			while (true)
			{
				UndoStackSharedLocations   *sharedLocations;
				XLogRecPtr				 	xlogPtr;
				CommitSeqNo					csn;

				if (rewindMeta->completedPos == rewindMeta->addedPos)
				{
					/* All are already fixed, do nothing */
					break;
				}

				rewindItem = &rewindBuffer[rewindMeta->completedPos % xid_circular_buffer_size];
				Assert (rewindItem->oxid != InvalidOXid);

				if (!TimestampDifferenceExceeds(rewindItem->timestamp,
												 GetCurrentTimestamp(),
												 rewind_max_period))
				{
					/* Too early, do nothing */
					break;
				}

				/* Fix current transaction clear the item in the circular buffer and move to the next */

				map_oxid(rewindItem->oxid, &csn, &xlogPtr);
				Assert(COMMITSEQNO_IS_RETAINED_FOR_REWIND(csn));
				csn = csn & (~COMMITSEQNO_RETAINED_FOR_REWIND);
				///
				///

				// XXXX Do we need only regular ?
				sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(UndoLogRegular);
				pg_atomic_write_u64(&sharedLocations->onCommitLocation, rewindItem->undoStackLocations.onCommitLocation);
				rewindItem->oxid = InvalidOXid;

				rewindMeta->completedPos++;

				if (rewindMeta->completedPos - rewindMeta->readPos > bufferLength)
				{
					int		 start = rewindMeta->readPos % rewind_circular_buffer_size;
					int		 length_to_end = rewind_circular_buffer_size - start;
					uint64	 currentCleanFileNum;
					int 	 itemsPerFile;

					/* Read buffer from disk */
#ifdef USE_ASSERT_CHECKING
					for(int pos = rewindMeta->readPos; pos <= rewindMeta->readPos + bufferLength; pos++)
						Assert(rewindBuffer[pos % rewind_circular_buffer_size].oxid == InvalidOXid);
#endif

					if (length_to_end < bufferLength)
					{
						o_buffers_read(&rewindBuffersDesc,
								(Pointer) &rewindBuffer[start], REWIND_BUFFERS_TAG,
								rewindMeta->readPos * sizeof(RewindItem),
								length_to_end * sizeof(RewindItem));
						o_buffers_read(&rewindBuffersDesc,
								(Pointer) &rewindBuffer[0], REWIND_BUFFERS_TAG,
								(rewindMeta->readPos + length_to_end) * sizeof(RewindItem),
								(bufferLength - length_to_end) * sizeof(RewindItem));
					}
					else
					{
						o_buffers_read(&rewindBuffersDesc,
								(Pointer) &rewindBuffer[start], REWIND_BUFFERS_TAG,
								rewindMeta->readPos * sizeof(RewindItem),
								bufferLength * sizeof(RewindItem));
					}

#ifdef USE_ASSERT_CHECKING
					for(int pos = rewindMeta->readPos; pos <= rewindMeta->readPos + bufferLength; pos++)
						Assert(rewindBuffer[pos % rewind_circular_buffer_size].oxid != InvalidOXid);
#endif
					rewindMeta->readPos += bufferLength;

					/* Clean old buffer files if needed */
					itemsPerFile = REWIND_FILE_SIZE / sizeof(RewindItem);
					currentCleanFileNum = rewindMeta->completedPos / itemsPerFile;

					if (currentCleanFileNum > rewindMeta->oldCleanedFileNum)
					{
						o_buffers_unlink_files_range(&rewindBuffersDesc,
													 REWIND_BUFFERS_TAG,
													 rewindMeta->oldCleanedFileNum,
													 currentCleanFileNum);
						rewindMeta->oldCleanedFileNum = currentCleanFileNum;
					}
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
