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

#include "access/transam.h"
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

PG_FUNCTION_INFO_V1(orioledb_rewind_sync);
PG_FUNCTION_INFO_V1(orioledb_rewind);

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

Datum
orioledb_rewind_sync(PG_FUNCTION_ARGS)
{
	if (!enable_rewind)
		PG_RETURN_VOID();

	PG_TRY();
	{
		rewindMeta->skipCheck = true;
		while (rewindMeta->completePos != pg_atomic_read_u64(&rewindMeta->addPos))
			pg_usleep(100000);
	}
	PG_FINALLY();
	{
		rewindMeta->skipCheck = false;
	}
	PG_END_TRY();
	PG_RETURN_VOID();
}

Datum
orioledb_rewind(PG_FUNCTION_ARGS)
{
	TimestampTz	currentTime;
	RewindItem 	tmpbuf[REWIND_DISK_BUFFER_LENGTH];
	int		i = 0;
	int		retry;
	int 		nbackends;
	int		nprepared;
	int		rewind_time = PG_GETARG_INT32(0);
	uint64		pos;
	RewindItem     *rewindItem;

	elog(WARNING, "Rewind requested, for %d s", rewind_time);

	if (!enable_rewind)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("orioledb rewind mode is turned off")),
				errdetail("to use rewind set orioledb.enable_rewind = on in PostgreSQL config file."));
		PG_RETURN_VOID();
	}
	else if (rewind_time > rewind_max_period)
	{

		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Requested rewind %d s exceeds rewind_max_period %d s", rewind_time, rewind_max_period)),
				 errdetail("increase orioledb.rewind_max_period in PostgreSQL config file or request rewind less than %d s back", rewind_max_period));
		PG_RETURN_VOID();
	}
	else if (rewind_time == 0)
	{
		elog(WARNING, "Zero rewind requested, do nothing");
		PG_RETURN_VOID();
	}

	CountOtherDBBackends(InvalidOid, &nbackends, &nprepared);
	if (nprepared > 0)
		ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                 errmsg("Can't rewind due to prepared transactions in the cluster")));

	/* XXX disable new transactions committing ? */

	/* Disable adding new transactions to rewind buffer by rewind worker */
	rewindMeta->rewindInProgressRequested = true;
	retry = 0;
	while (!rewindMeta->rewindInProgressStarted)
	{
		pg_usleep(10000L);
		retry++;
		if (retry >= 1000)
			/* We don't know the state of rewind worker so we can't rewind or continue. */
			ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Rewind worker couldn't stop in 100s, aborting rewind")));
	}
	/* Terminate all other backends */
	retry = 0;
	TerminateOtherDBBackends(InvalidOid);
	while(true)
	{
		(void) CountOtherDBBackends(InvalidOid, &nbackends, &nprepared);
		if (nbackends <= 2) /* Autovaccum launcher and worker are not terminated by TerminateOtherDBBackends */
			break;
		pg_usleep(1000000L);
		retry++;
		if (retry >= 100)
			/*
			 * Rewind worker already stopped and come transactions could be not in the buffer
			 * so we can't continue.
			 */
			ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Backends couldn't stop in 100s, aborting rewind")));
	}

	/* Do actual rewind */
	currentTime = GetCurrentTimestamp();

	while (true)
	{
		if (rewindMeta->readPos < rewindMeta->writePos)
		{
			pos = pg_atomic_read_u64(&rewindMeta->addPos);

			if (pos > rewindMeta->evictPos)
			{
				/* Read from circular buffer backwards */
				pos = pg_atomic_fetch_sub_u64(&rewindMeta->addPos, 1);
				rewindItem = &rewindBuffer[pos % rewind_circular_buffer_size];
			}
			else
			{
				/* Read from disk buffer backwards */
				if (rewindMeta->writePos % REWIND_DISK_BUFFER_LENGTH == 0)
				{
					o_buffers_read(&rewindBuffersDesc,
						   (Pointer) &tmpbuf, REWIND_BUFFERS_TAG,
						   rewindMeta->writePos - REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem),
						   REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));
				}
				rewindItem = &tmpbuf[(rewindMeta->writePos % REWIND_DISK_BUFFER_LENGTH) - 1];
				rewindMeta->writePos--;
			}
		}
		else
		{
			/* Read  from circular buffer backwards*/
			pos = pg_atomic_fetch_sub_u64(&rewindMeta->addPos, 1);
			rewindItem = &rewindBuffer[pos % rewind_circular_buffer_size];
		}

		if (TimestampDifferenceExceeds(rewindItem->timestamp, currentTime, rewind_time * 1000))
		{
			/* Rewind complete */
			break;
		}

		Assert(rewindItem->oxid != InvalidOXid);

		/* Rewind current rewind item */
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			UndoLocation location PG_USED_FOR_ASSERTS_ONLY;

			location = walk_undo_range_with_buf((UndoLogType) i,
				rewindItem->undoLocation[i],
				InvalidUndoLocation,
				rewindItem->oxid, true, &rewindItem->onCommitUndoLocation[i],
				true);
			Assert(!UndoLocationIsValid(location));
		}

		Assert(csn_is_retained_for_rewind(oxid_get_csn(rewindItem->oxid, true)));
		set_oxid_csn(rewindItem->oxid, COMMITSEQNO_ABORTED);
		
		/* Abort transactions for non-orioledb tables */ 
		TransactionIdAbortTree(XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), 0, NULL);

		/* Clear the item from the circular buffer */
		rewindItem->oxid = InvalidOXid;	

		/* Buffer finished. Unlikely case when rewind_time equals rewind_max_period */
		if (pos == rewindMeta->completePos)
			break;
	}

	elog(WARNING, "Rewind complete, for %d s", rewind_time);
	/* Restart Postgres */
	(void) kill(PostmasterPid, SIGTERM);
	PG_RETURN_VOID();
}

TransactionId
orioledb_vacuum_horizon_hook(void)
{
	return XidFromFullTransactionId(rewindMeta->oldestConsideredRunningXid);
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

/*
 * Convert a 32 bit transaction id into 64 bit transaction id, by assuming it
 * is within MaxTransactionId / 2 of XidFromFullTransactionId(rel).
 *
 * Be very careful about when to use this function. It can only safely be used
 * when there is a guarantee that xid is within MaxTransactionId / 2 xids of
 * rel. That e.g. can be guaranteed if the caller assures a snapshot is
 * held by the backend and xid is from a table (where vacuum/freezing ensures
 * the xid has to be within that range), or if xid is from the procarray and
 * prevents xid wraparound that way.
 */
static inline FullTransactionId
FullXidRelativeTo(FullTransactionId rel, TransactionId xid)
{
	TransactionId rel_xid = XidFromFullTransactionId(rel);

	Assert(TransactionIdIsValid(xid));
	Assert(TransactionIdIsValid(rel_xid));

	/* not guaranteed to find issues, but likely to catch mistakes */
	AssertTransactionIdInAllowableRange(xid);

	return FullTransactionIdFromU64(U64FromFullTransactionId(rel)
									+ (int32) (xid - rel_xid));
}

static FullTransactionId
GetOldestFullTransactionIdConsideredRunning(void)
{
	TransactionId oldestConsideredRunningXid;
	FullTransactionId latestCompletedXid;

	oldestConsideredRunningXid = GetOldestTransactionIdConsideredRunning();
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	latestCompletedXid = TRANSAM_VARIABLES->latestCompletedXid;
	LWLockRelease(ProcArrayLock);
	return FullXidRelativeTo(latestCompletedXid, oldestConsideredRunningXid);
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
		pg_atomic_init_u64(&rewindMeta->addPos, 0);
		rewindMeta->evictPos = InvalidRewindPos;
		rewindMeta->completePos = 0;
		rewindMeta->oldCleanedFileNum = 0;
		pg_atomic_write_u64(&rewindMeta->oldestConsideredRunningXid,
							InvalidTransactionId);

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

/*
 * Restore page from disk to an empty space in circular in-memory buffer.
 * Takes an exclusive lock to avoid cocurrent page eviction.
 * Modifies externally visible rewindMeta->addPos before doing changes to
 * in-memory buffer so that concurrently inserted items don't affect a
 * region restoration region.
 */
static void
restore_evicted_rewind_page(void)
{
	int			start;
	int			length_to_end;
	uint64		currentCleanFileNum;
	int			itemsPerFile;
	uint64		curAddPos;

	LWLockAcquire(&rewindMeta->evictLock, LW_EXCLUSIVE);

	/*
	 * Stage 1. Shift last page part in a ring buffer have a page-sized space
	 * in a circular buffer before it
	 */
	curAddPos = pg_atomic_fetch_add_u64(&rewindMeta->addPos, REWIND_DISK_BUFFER_LENGTH);

	for (uint64 pos = rewindMeta->evictPos; pos < curAddPos; pos++)
	{
		int			src = pos % rewind_circular_buffer_size;
		int			dst = (pos + REWIND_DISK_BUFFER_LENGTH) % rewind_circular_buffer_size;

		Assert(rewindBuffer[dst].oxid == InvalidOXid);
		memmove(&rewindBuffer[dst], &rewindBuffer[src], sizeof(RewindItem));
	}

	/*
	 * Stage 2. Restore oldest written buffer page to a clean space before the
	 * last page in a circular buffer
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

	LWLockRelease(&rewindMeta->evictLock);

	/* Clean old buffer files if needed. No lock. */
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

				if (rewindMeta->rewindInProgressRequested)
				{
					/* Rewind has started. Quit rewind worker */
					shutdown_requested = true;
					rewindMeta->rewindInProgressStarted = true;
					break;
				}

				if (rewindMeta->completePos == pg_atomic_read_u64(&rewindMeta->addPos))
				{
					/* All are already fixed, do nothing */
					break;
				}

				rewindItem = &rewindBuffer[rewindMeta->completePos % rewind_circular_buffer_size];
				Assert(rewindItem->oxid != InvalidOXid);

				if (!rewindMeta->skipCheck &&
					!TimestampDifferenceExceeds(rewindItem->timestamp,
												GetCurrentTimestamp(),
												rewind_max_period * 1000))
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
				pg_atomic_write_u64(&rewindMeta->oldestConsideredRunningXid,
									rewindItem->oldestConsideredRunningXid.value);

				/* Clear the item from the circular buffer */
				rewindItem->oxid = InvalidOXid;
				rewindMeta->completePos++;

				/*
				 * Restore extent from disk (Operation 5 described in
				 * rewind/rewind.h)
				 */
				if (rewindMeta->readPos < rewindMeta->writePos)
				{
					int			freeSpace;

					freeSpace = rewind_circular_buffer_size -
						((pg_atomic_read_u64(&rewindMeta->addPos) - rewindMeta->completePos) - (rewindMeta->writePos - rewindMeta->readPos));

					Assert(freeSpace <= REWIND_DISK_BUFFER_LENGTH);

					if (freeSpace == REWIND_DISK_BUFFER_LENGTH)
						restore_evicted_rewind_page();
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

/*
 * Evict page from a ring buffer to disk. (Operation 8 described in
 * rewind/rewind.h). Needs exclusive lock against concurrent eviction.
 */
static void
evict_rewind_page(void)
{
	uint64		pos;
	uint64		curAddPos;
	int			start;
	int			length_to_end;
	int			oldEvictPos;

	if (LWLockAcquireOrWait(&rewindMeta->evictLock, LW_EXCLUSIVE))
	{
		curAddPos = pg_atomic_read_u64(&rewindMeta->addPos);

		if (rewindMeta->evictPos == InvalidRewindPos)
			rewindMeta->evictPos = curAddPos;

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
		for (pos = oldEvictPos; pos < curAddPos; pos++)
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
		pg_atomic_fetch_sub_u64(&rewindMeta->addPos, REWIND_DISK_BUFFER_LENGTH);

		LWLockRelease(&rewindMeta->evictLock);
	}
}

void
add_to_rewind_buffer(OXid oxid)
{
	RewindItem *rewindItem;
	int			i;
	uint64		curAddPos;
	int			freeSpace;

	freeSpace = rewind_circular_buffer_size -
		((pg_atomic_read_u64(&rewindMeta->addPos) - rewindMeta->completePos) - (rewindMeta->writePos - rewindMeta->readPos));
	Assert(freeSpace >= 0);

	while (freeSpace == 0)
	{
		evict_rewind_page();
		freeSpace = rewind_circular_buffer_size -
			((pg_atomic_read_u64(&rewindMeta->addPos) - rewindMeta->completePos) - (rewindMeta->writePos - rewindMeta->readPos));
	}

	/* Fill entry in a circular buffer. */
	curAddPos = pg_atomic_fetch_add_u64(&rewindMeta->addPos, 1);
	Assert(curAddPos <= rewindMeta->completePos + rewind_circular_buffer_size -
		   (rewindMeta->writePos - rewindMeta->readPos));

	rewindItem = &rewindBuffer[curAddPos % rewind_circular_buffer_size];

	Assert(rewindItem->oxid == InvalidOXid);
	Assert(csn_is_retained_for_rewind(oxid_get_csn(oxid, true)));

	rewindItem->timestamp = GetCurrentTimestamp();
	rewindItem->oxid = oxid;
	rewindItem->xid = GetCurrentTransactionIdIfAny();
	rewindItem->oldestConsideredRunningXid = GetOldestFullTransactionIdConsideredRunning();

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		UndoMeta   *undoMeta = get_undo_meta_by_type((UndoLogType) i);
		UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS((UndoLogType) i);

		rewindItem->onCommitUndoLocation[i] = pg_atomic_read_u64(&sharedLocations->onCommitLocation);
		elog(LOG, "%lu %d %lu", oxid, i, rewindItem->onCommitUndoLocation[i]);
		rewindItem->undoLocation[i] = pg_atomic_read_u64(&sharedLocations->location);
		rewindItem->minRetainLocation[i] = pg_atomic_read_u64(&undoMeta->minProcRetainLocation);
	}
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
	uint64		curAddPos;

	curAddPos = pg_atomic_read_u64(&rewindMeta->addPos);

	/*
	 * Start from the last non-completed position not written to checkpoint
	 * yet
	 */
	if (rewindMeta->checkpointPos == InvalidRewindPos)
		rewindMeta->checkpointPos = rewindMeta->completePos;

	start = Max(rewindMeta->completePos, rewindMeta->checkpointPos);
	finish1 = (rewindMeta->evictPos == InvalidRewindPos) ? curAddPos : rewindMeta->evictPos;

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
	for (pos = finish1; pos < curAddPos; pos++)
		checkpoint_write_rewind_item(&rewindBuffer[pos % rewind_circular_buffer_size]);

	rewindMeta->checkpointPos = pos;
}
