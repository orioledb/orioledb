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
#include "checkpoint/checkpoint.h"

#include "pgstat.h"

static volatile sig_atomic_t shutdown_requested = false;
int			RewindHorizonCheckDelay = 1000; /* Time between checking in ms */
static RewindItem *rewindBuffer = NULL;
static RewindMeta *rewindMeta = NULL;
static bool rewindWorker = false;

static OBuffersDesc rewindBuffersDesc = {
	.singleFileSize = REWIND_FILE_SIZE,
	.filenameTemplate = {ORIOLEDB_DATA_DIR "/%02X%08X.rewindmap"},
	.groupCtlTrancheName = "rewindBuffersGroupCtlTranche",
	.bufferCtlTrancheName = "rewindBuffersCtlTranche"
};

Size
rewind_shmem_needs(void)
{
	Size		size;

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
	char		curFileName[MAXPGPATH];
	File		curFile;
	uint64		fileNum = 0;

	Assert(OBuffersMaxTagIsValid(tag));

	while (true)
	{
		pg_snprintf(curFileName, MAXPGPATH,
					desc->filenameTemplate[tag],
					(uint32) (fileNum >> 32),
					(uint32) fileNum);
		curFile = BasicOpenFile(curFileName, O_RDONLY | PG_BINARY);

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

	rewindMeta = (RewindMeta *) ptr;
	ptr += MAXALIGN(sizeof(RewindMeta));
	rewindBuffer = (RewindItem *) ptr;
	ptr += rewind_circular_buffer_size * sizeof(RewindItem);
	o_buffers_shmem_init(&rewindBuffersDesc, ptr, found);
	ptr += o_buffers_shmem_needs(&rewindBuffersDesc);

	if (!found)
	{
		int64		i;
		int			j;

		for (i = 0; i < rewind_circular_buffer_size; i++)
		{
			rewindBuffer[i].oxid = InvalidOXid;
			rewindBuffer[i].timestamp = 0;
			for (j = 0; j < (int) UndoLogsCount; j++)
			{
				rewindBuffer[i].onCommitUndoLocation[j] = InvalidUndoLocation;
				rewindBuffer[i].minRetainLocation[j] = 0;
			}
		}

		for (j = 0; j < (int) UndoLogsCount; j++)
		{
			UndoMeta   *undoMeta = get_undo_meta_by_type((UndoLogType) j);

			pg_atomic_write_u64(&undoMeta->minRewindRetainLocation, pg_atomic_read_u64(&undoMeta->minProcRetainLocation));
		}

		/* rewindMeta->rewindMapTrancheId = LWLockNewTrancheId(); */

		/*
		 * LWLockInitialize(&rewindMeta->xidMapWriteLock,
		 * rewindMeta->rewindMapTrancheId);
		 */

		rewindMeta->readPos = 0;
		rewindMeta->writePos = 0;
		rewindMeta->addPos = 0;
		rewindMeta->evictPos = InvalidRewindPos;
		rewindMeta->completePos = 0;
		rewindMeta->oldCleanedFileNum = 0;
		rewindMeta->freeSpace = rewind_circular_buffer_size;


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

bool
is_rewind_worker(void)
{
	return rewindWorker;
}

void
rewind_worker_main(Datum main_arg)
{
	int			rc,
				wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT;
	RewindItem *rewindItem;

	rewindWorker = true;

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
			 * Sleep until we are signaled or time delay for moving rewind
			 * horizon.
			 */
			rc = WaitLatch(MyLatch, wake_events,
						   RewindHorizonCheckDelay,
						   WAIT_EVENT_REWIND_WORKER_MAIN);

			if (rc & WL_POSTMASTER_DEATH)
				shutdown_requested = true;

/* 			elog(LOG, "Rewind worker came to check"); */

			while (true)
			{
				uint64		location PG_USED_FOR_ASSERTS_ONLY;
				int			i;

				if (rewindMeta->completePos == rewindMeta->addPos)
				{
					/* All are already fixed, do nothing */
					break;
				}

				rewindItem = &rewindBuffer[rewindMeta->completePos % rewind_circular_buffer_size];
				Assert(rewindItem->oxid != InvalidOXid);

				if (!TimestampDifferenceExceeds(rewindItem->timestamp,
												GetCurrentTimestamp(),
												rewind_max_period))
				{
					/* Too early, do nothing */
					break;
				}

				/* Fix current rewind item */

				clear_rewind_oxid(rewindItem->oxid);
				for (i = 0; i < (int) UndoLogsCount; i++)
				{
					UndoMeta   *undoMeta = get_undo_meta_by_type((UndoLogType) i);

					location = walk_undo_range_with_buf((UndoLogType) i, rewindItem->onCommitUndoLocation[i], InvalidUndoLocation,
											   rewindItem->oxid, false, NULL,
											   true);
					Assert(!UndoLocationIsValid(location));
#ifdef USE_ASSERT_CHECKING
					Assert(pg_atomic_read_u64(&undoMeta->minRewindRetainLocation) <= rewindItem->minRetainLocation[i]);
#endif
					pg_atomic_write_u64(&undoMeta->minRewindRetainLocation, rewindItem->minRetainLocation[i]);
				}

				/* Clear the item from the circular buffer */
				rewindItem->oxid = InvalidOXid;
				rewindMeta->completePos++;
				rewindMeta->freeSpace++;

				/*
				 * Restore extent from disk (Operation 5 described in
				 * rewind/rewind.h)
				 */
				if (rewindMeta->readPos < rewindMeta->writePos)
				{
					Assert(rewindMeta->freeSpace <= REWIND_DISK_BUFFER_LENGTH);

					if (rewindMeta->freeSpace == REWIND_DISK_BUFFER_LENGTH)
					{
						int			start;
						int			length_to_end;
						uint64		currentCleanFileNum;
						int			itemsPerFile;

						/*
						 * Stage 1. Shift last page part in a ring buffer have
						 * a page-sized space in a circular buffer before it
						 */

						for (uint64 pos = rewindMeta->evictPos; pos < rewindMeta->addPos; pos++)
						{
							int			src = pos % rewind_circular_buffer_size;
							int			dst = (pos + REWIND_DISK_BUFFER_LENGTH) % rewind_circular_buffer_size;

							Assert(rewindBuffer[dst].oxid == InvalidOXid);
							memmove(&rewindBuffer[dst], &rewindBuffer[src], sizeof(RewindItem));
						}

						/* Shift addPos according to moving last page */
						rewindMeta->addPos += REWIND_DISK_BUFFER_LENGTH;
						rewindMeta->freeSpace -= REWIND_DISK_BUFFER_LENGTH;

						/*
						 * Stage 2. Restore oldest written buffer page to a
						 * clean space before the last page in a circular
						 * buffer
						 */
						start = rewindMeta->evictPos % rewind_circular_buffer_size;
						length_to_end = rewind_circular_buffer_size - start;

						if (length_to_end <= REWIND_DISK_BUFFER_LENGTH)
						{
							o_buffers_read(&rewindBuffersDesc,
										   (Pointer) &rewindBuffer[start], REWIND_BUFFERS_TAG,
										   rewindMeta->readPos * sizeof(RewindItem),
										   length_to_end * sizeof(RewindItem));
							o_buffers_read(&rewindBuffersDesc,
										   (Pointer) &rewindBuffer[0], REWIND_BUFFERS_TAG,
										   (rewindMeta->readPos + length_to_end) * sizeof(RewindItem),
										   (REWIND_DISK_BUFFER_LENGTH - length_to_end) * sizeof(RewindItem));
						}
						else
						{
							o_buffers_read(&rewindBuffersDesc,
										   (Pointer) &rewindBuffer[start], REWIND_BUFFERS_TAG,
										   rewindMeta->readPos * sizeof(RewindItem),
										   REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));
						}

#ifdef USE_ASSERT_CHECKING
						for (uint64 pos = start; pos < start + REWIND_DISK_BUFFER_LENGTH; pos++)
							Assert(rewindBuffer[pos % rewind_circular_buffer_size].oxid != InvalidOXid);
#endif
						rewindMeta->evictPos += REWIND_DISK_BUFFER_LENGTH;
						rewindMeta->readPos += REWIND_DISK_BUFFER_LENGTH;

						/* Clean old buffer files if needed */
						itemsPerFile = REWIND_FILE_SIZE / sizeof(RewindItem);
						currentCleanFileNum = rewindMeta->readPos / itemsPerFile;

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

void
add_to_rewind_buffer(OXid oxid)
{
	RewindItem *rewindItem;
	int			i;

	rewindMeta->freeSpace--;
	Assert(rewindMeta->addPos <= rewindMeta->completePos + rewind_circular_buffer_size -
		   (rewindMeta->writePos - rewindMeta->readPos));

	/*
	 * Evict page from a ring buffer to disk. (Operation 8 described in
	 * rewind/rewind.h)
	 */
	if (rewindMeta->freeSpace == 0)
	{
		uint64		pos;
		int			start;
		int			length_to_end;
		int			oldEvictPos;

		if (rewindMeta->evictPos == InvalidRewindPos)
			rewindMeta->evictPos = rewindMeta->addPos;

		/*
		 * Stage 1. Shift evictPos and write REWIND_DISK_BUFFER_LENGTH number
		 * of records to disk
		 */
		oldEvictPos = rewindMeta->evictPos;
		rewindMeta->evictPos -= REWIND_DISK_BUFFER_LENGTH;

		start = (rewindMeta->evictPos % rewind_circular_buffer_size);
		length_to_end = rewind_circular_buffer_size - start;

		if (length_to_end < REWIND_DISK_BUFFER_LENGTH)
		{
			o_buffers_write(&rewindBuffersDesc,
							(Pointer) &rewindBuffer[start],
							REWIND_BUFFERS_TAG,
							rewindMeta->writePos * sizeof(RewindItem),
							length_to_end * sizeof(RewindItem));

			o_buffers_write(&rewindBuffersDesc,
							(Pointer) &rewindBuffer[0],
							REWIND_BUFFERS_TAG,
							(rewindMeta->writePos + length_to_end) * sizeof(RewindItem),
							(REWIND_DISK_BUFFER_LENGTH - length_to_end) * sizeof(RewindItem));
		}
		else
		{
			o_buffers_write(&rewindBuffersDesc,
							(Pointer) &rewindBuffer[start],
							REWIND_BUFFERS_TAG,
							rewindMeta->writePos * sizeof(RewindItem),
							REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));
		}
		rewindMeta->writePos += REWIND_DISK_BUFFER_LENGTH;

		/* Clean written items from ring buffer */
		for (pos = rewindMeta->evictPos; pos < rewindMeta->evictPos + REWIND_DISK_BUFFER_LENGTH; pos++)
		{
			rewindBuffer[(pos % rewind_circular_buffer_size)].oxid = InvalidOXid;
		}

		/* Stage 2. Shift last page part by one page left */
		for (pos = oldEvictPos; pos < rewindMeta->addPos; pos++)
		{
			int			src = pos % rewind_circular_buffer_size;
			int			dst = (pos - REWIND_DISK_BUFFER_LENGTH) % rewind_circular_buffer_size;

			memmove(&rewindBuffer[dst], &rewindBuffer[src], sizeof(RewindItem));
			rewindBuffer[src].oxid = InvalidOXid;
		}

		/*
		 * Continue to add from the beginning of last clean page in ring
		 * buffer
		 */
		rewindMeta->addPos -= REWIND_DISK_BUFFER_LENGTH;
		rewindMeta->freeSpace += REWIND_DISK_BUFFER_LENGTH;
	}

	/* Fill entry in a circular buffer. */
	rewindItem = &rewindBuffer[rewindMeta->addPos % rewind_circular_buffer_size];

	Assert(rewindItem->oxid == InvalidOXid);
	Assert(csn_is_retained_for_rewind(oxid_get_csn(oxid)));

	rewindItem->timestamp = GetCurrentTimestamp();
	rewindItem->oxid = oxid;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		UndoMeta   *undoMeta = get_undo_meta_by_type((UndoLogType) i);

		UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS((UndoLogType) i);

		rewindItem->onCommitUndoLocation[i] = pg_atomic_read_u64(&sharedLocations->onCommitLocation);
		elog(LOG, "%lu %d %lu", oxid, i, rewindItem->onCommitUndoLocation[i]);
		rewindItem->minRetainLocation[i] = pg_atomic_read_u64(&undoMeta->minProcRetainLocation);
	}

	rewindMeta->addPos++;
}

/*
 * Write rewind records from circular in-memory rewind buffer and on-disk rewind buffer
 * to xid buffer at checkpoint.
 */
void
checkpoint_write_rewind_xids(void)
{
	int			i;
	uint64		pos;
	uint64		start;
	uint64		finish1;

	/*
	 * Start from the last non-completed position not written to checkpoint
	 * yet
	 */
	if (rewindMeta->checkpointPos == InvalidRewindPos)
		rewindMeta->checkpointPos = rewindMeta->completePos;

	start = Max(rewindMeta->completePos, rewindMeta->checkpointPos);
	finish1 = (rewindMeta->evictPos == InvalidRewindPos) ? rewindMeta->addPos : rewindMeta->evictPos;

	/*
	 * Write rewind records from in-memory rewind buffer (before evicted
	 * records)
	 */
	for (pos = start; pos < finish1; pos++)
		checkpoint_write_rewind_item(&rewindBuffer[pos % rewind_circular_buffer_size]);

	/* Write rewind records from on-disk buffer if they exist */
	for (pos = rewindMeta->readPos; pos < rewindMeta->writePos; pos += REWIND_DISK_BUFFER_LENGTH)
	{
		RewindItem	buffer[REWIND_DISK_BUFFER_LENGTH];

		o_buffers_read(&rewindBuffersDesc,
					   (Pointer) &buffer, REWIND_BUFFERS_TAG,
					   rewindMeta->readPos * sizeof(RewindItem),
					   REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));

		for (i = 0; i < REWIND_DISK_BUFFER_LENGTH; i++)
		{
			Assert(pos + i < rewindMeta->writePos);
			checkpoint_write_rewind_item(&buffer[pos + i]);
		}
	}

	/*
	 * Write rewind records from in-memory rewind buffer (after evicted
	 * records if they exist)
	 */
	for (pos = finish1; pos < rewindMeta->addPos; pos++)
		checkpoint_write_rewind_item(&rewindBuffer[pos % rewind_circular_buffer_size]);

	rewindMeta->checkpointPos = pos;
}
