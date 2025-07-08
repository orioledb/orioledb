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
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/dsa.h"
#include "utils/page_pool.h"
#include "utils/ucm.h"
#include "utils/stopevent.h"
#include "rewind/rewind.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/autovacuum.h"
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
static RewindItem *rewindAddBuffer = NULL;
static RewindItem *rewindCompleteBuffer = NULL;
static RewindMeta *rewindMeta = NULL;
static bool rewindWorker = false;

static OBuffersDesc rewindBuffersDesc = {
	.singleFileSize = REWIND_FILE_SIZE,
	.filenameTemplate = {ORIOLEDB_DATA_DIR "/%02X%08X.rewindmap"},
	.groupCtlTrancheName = "rewindBuffersGroupCtlTranche",
	.bufferCtlTrancheName = "rewindBuffersCtlTranche"
};
static FullTransactionId GetOldestFullTransactionIdConsideredRunning(void);

/* Temporary storage for heap info between pre-commit and commit in a backend */
static TransactionId precommit_xid;
static int	precommit_nsubxids;
static TransactionId *precommit_subxids;

PG_FUNCTION_INFO_V1(orioledb_rewind_by_time);
PG_FUNCTION_INFO_V1(orioledb_rewind_to_timestamp);
PG_FUNCTION_INFO_V1(orioledb_rewind_to_transaction);
PG_FUNCTION_INFO_V1(orioledb_get_current_oxid);
PG_FUNCTION_INFO_V1(orioledb_get_rewind_queue_length);
PG_FUNCTION_INFO_V1(orioledb_get_rewind_evicted_length);
PG_FUNCTION_INFO_V1(orioledb_get_complete_oxid);
PG_FUNCTION_INFO_V1(orioledb_get_complete_xid);
/* Functions only under IS_DEV: */
PG_FUNCTION_INFO_V1(orioledb_rewind_sync);
PG_FUNCTION_INFO_V1(orioledb_rewind_set_complete);

#define REWIND_MODE_UNKNOWN (0)
#define	REWIND_MODE_TIME (1)
#define REWIND_MODE_TIMESTAMP (2)
#define	REWIND_MODE_XID	(3)

static void orioledb_rewind_internal(int rewind_mode, int rewind_time, OXid rewind_oxid, TransactionId rewind_xid, TimestampTz rewind_timestamp);

/* Interface functions */

/* OrioleDB analog of pg_current_xact_id() */
Datum
orioledb_get_current_oxid(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(get_current_oxid());
}

Datum
orioledb_rewind_by_time(PG_FUNCTION_ARGS)
{
	int			rewind_time = PG_GETARG_INT32(0);

	orioledb_rewind_internal(REWIND_MODE_TIME, rewind_time, InvalidOXid, InvalidTransactionId, (TimestampTz) 0);
	PG_RETURN_VOID();
}

Datum
orioledb_rewind_to_transaction(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_INT32(0);
	OXid		oxid = PG_GETARG_INT64(1);

	orioledb_rewind_internal(REWIND_MODE_XID, 0, oxid, xid, (TimestampTz) 0);
	PG_RETURN_VOID();
}

Datum
orioledb_rewind_to_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz rewind_timestamp = PG_GETARG_TIMESTAMPTZ(0);

	orioledb_rewind_internal(REWIND_MODE_TIMESTAMP, 0, InvalidOXid, InvalidTransactionId, rewind_timestamp);
	PG_RETURN_VOID();
}

/* Testing/convenience functions avaliable for the user */

/*
 * Access to a rewindMeta without a lock. This is ok when we check if it's time to fix oldest items in the queue.
 * If precise value is needed getting rewindMeta lock is a responsibility of a caller.
 */
Datum
orioledb_get_rewind_queue_length(PG_FUNCTION_ARGS)
{
	if (!enable_rewind)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("orioledb rewind mode is turned off")),
				errdetail("to use rewind set orioledb.enable_rewind = on in PostgreSQL config file."));
	}

	PG_RETURN_UINT64(pg_atomic_read_u64(&rewindMeta->addPosReserved) - rewindMeta->completePos);
}

Datum
orioledb_get_rewind_evicted_length(PG_FUNCTION_ARGS)
{
	if (!enable_rewind)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("orioledb rewind mode is turned off")),
				errdetail("to use rewind set orioledb.enable_rewind = on in PostgreSQL config file."));
	}

	PG_RETURN_UINT64(rewindMeta->evictPos - rewindMeta->restorePos);
}

/* NB: Disabled because of too complicated logic for convenience function.
Datum
orioledb_rewind_queue_age(PG_FUNCTION_ARGS)
{
	RewindItem *rewindItem;
	long	secs;
	int	usecs;

	if (!enable_rewind)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("orioledb rewind mode is turned off")),
				errdetail("to use rewind set orioledb.enable_rewind = on in PostgreSQL config file."));
		PG_RETURN_VOID();
	}

	pos = rewindMeta->completePos
	while(rewindItem->tag == REWIND_ITEM_EMPTY)
	{
		rewindItem = &rewindBuffer[rewindMeta->completePos % rewind_circular_buffer_size];
	}

	TimestampDifference(rewindItem->timestamp, GetCurrentTimestamp(), &secs, &usecs);
	PG_RETURN_UINT64(secs);
}
*/

/* Not safe against concurrent operations */
Datum
orioledb_get_complete_oxid(PG_FUNCTION_ARGS)
{
	RewindItem *rewindItem;
	OXid		oxid;
	TransactionId xid;
	uint64		pos;

	if (!enable_rewind)
		PG_RETURN_VOID();

	LWLockAcquire(&rewindMeta->rewindEvictLock, LW_SHARED);
	pos = rewindMeta->completePos;

	while (true)
	{
		rewindItem = &rewindCompleteBuffer[pos % rewind_circular_buffer_size];
		Assert(rewindItem->tag != EMPTY_ITEM_TAG);
		xid = rewindItem->xid;
		oxid = rewindItem->oxid;

		if (OXidIsValid(oxid))
			break;

		pos++;
		if (pos >= rewindMeta->restorePos)
			elog(ERROR, "Couldn't get oxid from completeBuffer");
	}
	LWLockRelease(&rewindMeta->rewindEvictLock);
	elog(DEBUG3, "COMPLETE OXID %lu XID %u", oxid, xid);
	PG_RETURN_DATUM(rewindItem->oxid);
}

/* Not safe against concurrent operations */
Datum
orioledb_get_complete_xid(PG_FUNCTION_ARGS)
{
	RewindItem *rewindItem;
	OXid		oxid;
	TransactionId xid;
	uint64		pos;

	if (!enable_rewind)
		PG_RETURN_VOID();

	LWLockAcquire(&rewindMeta->rewindEvictLock, LW_SHARED);
	pos = rewindMeta->completePos;

	while (true)
	{
		rewindItem = &rewindCompleteBuffer[pos % rewind_circular_buffer_size];
		Assert(rewindItem->tag != EMPTY_ITEM_TAG);
		xid = rewindItem->xid;
		oxid = rewindItem->oxid;

		if (TransactionIdIsValid(xid))
			break;

		pos++;
		if (pos >= rewindMeta->restorePos)
			elog(ERROR, "Couldn't get xid from completeBuffer");
	}
	LWLockRelease(&rewindMeta->rewindEvictLock);
	elog(DEBUG3, "COMPLETE XID %lu XID %u", oxid, xid);
	PG_RETURN_DATUM(rewindItem->xid);
}

/* Testing functions included under IS_DEV */

/* Set complete xid/oxid for testing queue complete operation */
Datum
orioledb_rewind_set_complete(PG_FUNCTION_ARGS)
{
	if (!enable_rewind)
		PG_RETURN_VOID();

	rewindMeta->force_complete_xid = PG_GETARG_INT32(0);
	rewindMeta->force_complete_oxid = PG_GETARG_INT64(1);

	elog(WARNING, "force_complete_xid %u force_complete_oxid %lu", rewindMeta->force_complete_xid, rewindMeta->force_complete_oxid);

	PG_RETURN_VOID();
}

/* Complete full rewind queue. For calling from regression tests */
Datum
orioledb_rewind_sync(PG_FUNCTION_ARGS)
{
	if (!enable_rewind)
		PG_RETURN_VOID();

	PG_TRY();
	{
		rewindMeta->skipCheck = true;
		while (rewindMeta->completePos != pg_atomic_read_u64(&rewindMeta->addPosReserved))
			pg_usleep(100000);
	}
	PG_FINALLY();
	{
		rewindMeta->skipCheck = false;
	}
	PG_END_TRY();
	PG_RETURN_VOID();
}

/* Testing functions end */

static inline
void
print_rewind_item(RewindItem *rewindItem, uint64 pos, int source_buffer)
{
	/* To shorten output invalid values are printed as -1 */
	int64		oxid = OXidIsValid(rewindItem->oxid) ? rewindItem->oxid : -1;
	int64		undoLocation[3];
	int64		onCommitUndoLocation[3];
	int64		minRetainLocation[3];

	for (int i = 0; i < 3; i++)
	{
		undoLocation[i] = (rewindItem->undoLocation[i] == InvalidUndoLocation) ? -1 : rewindItem->undoLocation[i];
		onCommitUndoLocation[i] = (rewindItem->onCommitUndoLocation[i] == InvalidUndoLocation) ? -1 : rewindItem->onCommitUndoLocation[i];
		minRetainLocation[i] = (rewindItem->minRetainLocation[i] == InvalidUndoLocation) ? -1 : rewindItem->minRetainLocation[i];
	}

	elog(LOG, "P %lu:%u T %u OX %ld X %u L0 %ld OCL0 %ld, MRL0 %ld, L1 %ld OCL1 %ld, MRL1 %ld L2 %ld OCL2 %ld, MRL2 %ld ORX %u NS %u", pos, source_buffer, rewindItem->tag, oxid, rewindItem->xid, undoLocation[0], onCommitUndoLocation[0], minRetainLocation[0], undoLocation[1], onCommitUndoLocation[1], minRetainLocation[1], undoLocation[2], onCommitUndoLocation[2], minRetainLocation[2], XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);
}

/*
 * Debug print all rewind records from circular in-memory rewind buffer and on-disk rewind buffer
 * to log. Doesn't take any locks, so results are not warrantied with any concurrent operation
 * on the queue.
 */
void
log_print_rewind_queue(void)
{
	int			i;
	uint64		pos;
	uint64		curAddPosFilled;
	int			freeAddSpace;

	if (!enable_rewind)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("orioledb rewind mode is turned off")),
				errdetail("to use rewind set orioledb.enable_rewind = on in PostgreSQL config file."));
		return;
	}

	curAddPosFilled = pg_atomic_read_u64(&rewindMeta->addPosFilledUpto);
	freeAddSpace = rewind_circular_buffer_size - (curAddPosFilled - rewindMeta->evictPos);

	elog(LOG, "Print rewind queue: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%u", curAddPosFilled, pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, freeAddSpace);

	/* From completeBuffer if exists */
	for (pos = rewindMeta->completePos; pos < rewindMeta->restorePos; pos++)
		print_rewind_item(&rewindCompleteBuffer[pos % rewind_circular_buffer_size], pos, 1);

	/* From on-disk buffer if they exist */
	for (; pos < rewindMeta->evictPos; pos += REWIND_DISK_BUFFER_LENGTH)
	{
		RewindItem	buffer[REWIND_DISK_BUFFER_LENGTH];

		o_buffers_read(&rewindBuffersDesc,
					   (Pointer) &buffer, REWIND_BUFFERS_TAG,
					   pos * sizeof(RewindItem),
					   REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));

		for (i = 0; i < REWIND_DISK_BUFFER_LENGTH; i++)
		{
			Assert(pos + i < rewindMeta->evictPos);
			print_rewind_item(&buffer[i], pos + i, 2);
		}
	}

	/*
	 * From in-memory rewind addBuffer (after evicted records if they exist)
	 */
	for (; pos < curAddPosFilled; pos++)
		print_rewind_item(&rewindAddBuffer[pos % rewind_circular_buffer_size], pos, 3);
}

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

/* Perform actual rewind within one backend left */
static void
do_rewind(int rewind_mode, int rewind_time, TimestampTz rewindStartTimeStamp, OXid rewind_oxid, TransactionId rewind_xid, TimestampTz rewind_timestamp)
{
	int			i = 0;
	int			k = 0;
	RewindItem *rewindItem;
	RewindItem	tmpbuf[REWIND_DISK_BUFFER_LENGTH];
	uint64		pos;
	uint64		cur_read;
	uint64		cur_write;
	uint64		reserved;
	uint64		filled;
	int			nsubxids = 0;
	int			subxids_count = 0;
	bool		got_all_subxids = false;
#ifdef USE_ASSERT_CHECKING
	bool		started_subxids = false;
#endif
	TransactionId *subxids = NULL;
	TransactionId xid = InvalidTransactionId;
	OXid		oxid = InvalidOXid;
	CommitSeqNo csn PG_USED_FOR_ASSERTS_ONLY;
	long		secs;
	int			usecs;
	int			source_buffer;

#ifdef REWIND_DEBUG_MODE
	log_print_rewind_queue();
#endif
	/*
	 * Bump rewindMeta->addPosFilledUpto compacting all filled entries between
	 * addPosFilledUpto and addPosReserved and dropping non-filled ones
	 */
	pg_read_barrier();
	reserved = pg_atomic_read_u64(&rewindMeta->addPosReserved);
	filled = pg_atomic_read_u64(&rewindMeta->addPosFilledUpto);
	cur_read = filled;
	cur_write = filled;

	while (cur_read < reserved)
	{
		if ((&rewindAddBuffer[cur_read % rewind_circular_buffer_size])->tag != EMPTY_ITEM_TAG)
		{
			if (cur_read != cur_write)
			{
				Assert(cur_write < cur_read);
				memcpy(&rewindAddBuffer[cur_write % rewind_circular_buffer_size],
				       &rewindAddBuffer[cur_read % rewind_circular_buffer_size],
				       sizeof(RewindItem));
				(&rewindAddBuffer[cur_read % rewind_circular_buffer_size])->tag = EMPTY_ITEM_TAG;
			}

			/*
			 * Bump addPosFilledUpto.
			 */
			cur_read++;
			cur_write++;

			if (!pg_atomic_compare_exchange_u64(&rewindMeta->addPosFilledUpto, &filled, cur_write))
				elog(ERROR, "Could not bump addPosFilledUpto");
		}
		else
		{
			/* Current item is not written yet */
			cur_read++;
		}
	}

	/* Decrement before, as addPosFilledUpto was a next element to add. */
	pos = pg_atomic_sub_fetch_u64(&rewindMeta->addPosFilledUpto, 1);

	while (true)
	{
		if (pos >= rewindMeta->evictPos)	/* At evictPos is the next element
											 * to be evicted. It's actually
											 * still in rewindAddBuffer */
		{
			rewindItem = &rewindAddBuffer[pos % rewind_circular_buffer_size];
			source_buffer = 3;
		}
		else if (pos >= rewindMeta->restorePos) /* At restorePos is the next
												 * element to be restored.
												 * It's actually in disk
												 * buffer */
		{
			if (k == 0)
			{
				o_buffers_read(&rewindBuffersDesc,
							   (Pointer) &tmpbuf, REWIND_BUFFERS_TAG,
							   (pos - REWIND_DISK_BUFFER_LENGTH + 1) * sizeof(RewindItem),
							   REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));
				k = REWIND_DISK_BUFFER_LENGTH;
			}
			k--;
			rewindItem = &tmpbuf[k];
			source_buffer = 2;
		}
		else
		{
			rewindItem = &rewindCompleteBuffer[pos % rewind_circular_buffer_size];
			source_buffer = 1;
		}

		elog(DEBUG3, "Rewind read elem: pos %lu:%u oxid %lu xid %u oldestRunXid %u, nsubxids %u", pos, source_buffer, rewindItem->oxid, rewindItem->xid, XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);

		Assert(rewindItem->tag != EMPTY_ITEM_TAG);

		/*
		 * Gather subxids from subxids items (may be multiple). As we read
		 * backwards, we don't yet have filled respective rewindItem.
		 */
		if (rewindItem->tag == SUBXIDS_ITEM_TAG)
		{
			SubxidsItem *subxidsItem = (SubxidsItem *) rewindItem;

			Assert(!got_all_subxids);
			Assert(subxidsItem->nsubxids);

			if (nsubxids == 0)
			{
				/*
				 * First subxids item (As we read backwards so it's last
				 * written one)
				 */
				Assert(!started_subxids);
#ifdef USE_ASSERT_CHECKING
				started_subxids = true;
#endif
				nsubxids = subxidsItem->nsubxids;
				oxid = subxidsItem->oxid;
				subxids_count = nsubxids;
				subxids = palloc0(nsubxids * sizeof(TransactionId));
			}
			else
			{
				Assert(started_subxids);

				/*
				 * subxidsItem->nsubxids for every subxidsItem contains the
				 * same sum of of subxids for a rewindItem (not just the
				 * number of subxids in a current subxidsItem.
				 */
				Assert(subxidsItem->nsubxids == nsubxids);
				Assert(subxidsItem->oxid == oxid);
			}

			while (true)
			{
				subxids_count--;
				subxids[subxids_count] = subxidsItem->subxids[subxids_count % SUBXIDS_PER_ITEM];

				elog(DEBUG3, "Rewinding SubXid: oxid %lu cur xid %u subxid [%u/%u] %u", oxid, xid, subxids_count + 1, nsubxids, subxids[subxids_count]);

				if (subxids_count == 0)
				{
					got_all_subxids = true;
					break;
				}
				else if ((subxids_count % SUBXIDS_PER_ITEM) == 0)
					break;		/* Read next subxids item (As we read
								 * backwards so it's previous written one) */
			}
		}
		else
		{
			Assert(rewindItem->tag == REWIND_ITEM_TAG);

			Assert(OXidIsValid(rewindItem->oxid) || TransactionIdIsValid(rewindItem->xid));

			if (rewind_mode == REWIND_MODE_TIME)
			{
				if (TimestampDifferenceExceeds(rewindItem->timestamp, rewindStartTimeStamp, rewind_time * 1000))
					goto rewind_complete;
			}
			else if (rewind_mode == REWIND_MODE_XID)
			{
				if (TransactionIdIsValid(rewind_xid) && TransactionIdIsValid(rewindItem->xid) && TransactionIdPrecedes(rewindItem->xid, rewind_xid))
					goto rewind_complete;
				else if (OXidIsValid(rewind_oxid) && OXidIsValid(rewindItem->oxid) && rewindItem->oxid < rewind_oxid)
					goto rewind_complete;
			}
			else if (rewind_mode == REWIND_MODE_TIMESTAMP)
			{
				if (rewindItem->timestamp < rewind_timestamp)
					goto rewind_complete;
			}
			else
			{
				elog(ERROR, "Unknown rewind mode");
			}

			/* Rewind current rewind item */

			/* Undo orioledb transaction */
			if (OXidIsValid(rewindItem->oxid))
			{
				for (i = 0; i < (int) UndoLogsCount; i++)
				{
					UndoLocation location PG_USED_FOR_ASSERTS_ONLY;

					elog(DEBUG3, "Rewinding: oxid %lu xid %u logtype %d undoLoc %lu onCommitLoc %lu, minRetainLoc %lu, oldestRunXid %u, nsubxids %u", rewindItem->oxid, rewindItem->xid, i, rewindItem->undoLocation[i], rewindItem->onCommitUndoLocation[i], rewindItem->minRetainLocation[i], XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);

					location = walk_undo_range_with_buf((UndoLogType) i,
														rewindItem->undoLocation[i],
														InvalidUndoLocation,
														rewindItem->oxid, true, &rewindItem->onCommitUndoLocation[i],
														true);
					Assert(!UndoLocationIsValid(location));
				}

#ifdef USE_ASSERT_CHECKING
				csn = oxid_get_csn(rewindItem->oxid, true);
				Assert(csn_is_retained_for_rewind(csn));
#endif
				set_oxid_csn(rewindItem->oxid, COMMITSEQNO_ABORTED);
			}

			/* Abort transaction for non-orioledb tables */
			if (TransactionIdIsValid(rewindItem->xid))
			{
				Assert((started_subxids && got_all_subxids) || (!started_subxids && !got_all_subxids));
				elog(DEBUG3, "Rewinding: oxid %lu xid %u oldestRunXid %u, nsubxids %u", rewindItem->oxid, rewindItem->xid, XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);

				if (got_all_subxids)
				{
					Assert(nsubxids);
					Assert(rewindItem->nsubxids == nsubxids);
					Assert(rewindItem->oxid == oxid);

					TransactionIdAbortTree(rewindItem->xid, nsubxids, subxids);

					got_all_subxids = false;

#ifdef USE_ASSERT_CHECKING
					started_subxids = false;
#endif
					pfree(subxids);
					nsubxids = 0;
				}
				else
					TransactionIdAbortTree(rewindItem->xid, 0, NULL);
			}
			else
				Assert(!started_subxids && !got_all_subxids && nsubxids == 0);
		}

		/* Clear the item from the circular buffer */
		rewindItem->tag = EMPTY_ITEM_TAG;

		/*
		 * Buffer finished (rewind was requested for all rewind buffer)
		 */
		if (pos == rewindMeta->completePos)
		{
			TimestampDifference(rewindItem->timestamp, rewindStartTimeStamp, &secs, &usecs);
			ereport(LOG, errmsg("orioledb rewind completed on full rewind capacity. Last rewound transaction xid %u, oxid %lu is %ld.%d seconds back", rewindItem->xid, rewindItem->oxid, secs, usecs / 1000));
			return;
		}
		pos--;
	}

rewind_complete:

	TimestampDifference(rewindItem->timestamp, rewindStartTimeStamp, &secs, &usecs);
	ereport(LOG, errmsg("orioledb rewind completed. Last remaining transaction xid %u, oxid %lu is %ld.%d seconds back", rewindItem->xid, rewindItem->oxid, secs, usecs / 1000));
	return;
}

/*
 *  Check rewind conditions for sanity, disable adding new transactions to a rewind buffer,
 *  terminate all other backends, do actual rewind, then shutdown postgres.
 *  NB: starting up Postgres after rewind is not possible from backend, it needs to be done externally.
 */
static void
orioledb_rewind_internal(int rewind_mode, int rewind_time, OXid rewind_oxid, TransactionId rewind_xid, TimestampTz rewind_timestamp)
{
	TimestampTz rewindStartTimeStamp;
	int			retry;
	int			nbackends;
	int			nprepared;

	if (!enable_rewind)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("orioledb rewind mode is turned off")),
				errdetail("to use rewind set orioledb.enable_rewind = on in PostgreSQL config file."));
		return;
	}

	if (rewind_mode == REWIND_MODE_TIME)
	{
		elog(LOG, "Rewind requested, for %d s", rewind_time);

		if (rewind_time > rewind_max_time)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Requested rewind %d s exceeds rewind_max_time %d s", rewind_time, rewind_max_time)),
					errdetail("increase orioledb.rewind_max_time in PostgreSQL config file or request rewind less than %d s back", rewind_max_time));
			return;
		}
		else if (rewind_time == 0)
		{
			elog(WARNING, "Zero rewind requested, do nothing");
			return;
		}
	}
	else if (rewind_mode == REWIND_MODE_XID)
	{
		TransactionId oldest = XidFromFullTransactionId(rewindMeta->oldestConsideredRunningXid);

		elog(LOG, "Rewind requested, to Xid %u, OXid %lu", rewind_xid, rewind_oxid);

		if (!TransactionIdIsValid(rewind_xid) && !OXidIsValid(rewind_oxid))
		{
			elog(ERROR, "Neither rewind XID nor OXid are valid");
			return;
		}

		if (TransactionIdIsValid(rewind_xid))
		{
			if (TransactionIdFollowsOrEquals(rewind_xid, (TransactionId) GetTopTransactionId()))
				elog(WARNING, "Rewind XID is not in the past. Rewind will be based only on rewind OXid");
			else if (TransactionIdPrecedes(rewind_xid, oldest))
			{
				elog(WARNING, "Rewind XID is older than saved for rewind. Rewind to oldest possible XID %u", oldest);
				rewind_xid = oldest;
			}
		}

		if (OXidIsValid(rewind_oxid))
		{
			if (!xid_is_finished(rewind_oxid))
			{
				elog(WARNING, "Rewind OXid is not in the past. Rewind will be based only on rewind XID");
			}
		}
	}
	else if (rewind_mode == REWIND_MODE_TIMESTAMP)
	{
		elog(LOG, "Rewind requested, to timestamp %ld", rewind_timestamp);

		/*
		 * NB: these timestamp checks are from current time so they are little
		 * bit stricter than needed as actual rewindStartTimeStamp is recorded
		 * little bit later, after stopping rewind worker.
		 */
		if (rewind_timestamp >= GetCurrentTimestamp())
		{
			elog(WARNING, "Zero rewind requested, do nothing");
			return;
		}
		else if (TimestampDifferenceExceeds(rewind_timestamp, GetCurrentTimestamp(), rewind_max_time * 1000))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Requested rewind to timestamp %ld exceeds rewind_max_time %d s", rewind_timestamp, rewind_max_time)),
					errdetail("increase orioledb.rewind_max_time in PostgreSQL config file or request rewind less than %d s back", rewind_max_time));
			return;
		}
	}
	else
	{
		elog(ERROR, "Unknown rewind mode");
		return;
	}

	CountOtherDBBackends(InvalidOid, &nbackends, &nprepared);
	if (nprepared > 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Can't rewind due to prepared transactions in the cluster")));

	LWLockAcquire(&rewindMeta->rewindEvictLock, LW_EXCLUSIVE);

	/* Disable adding new transactions to rewind buffer by rewind worker */
	rewindMeta->rewindWorkerStopRequested = true;
	retry = 0;
	while (!rewindMeta->rewindWorkerStopped)
	{
		pg_usleep(10000L);
		retry++;
		if (retry >= 1000)

			/*
			 * We don't know the state of rewind worker so we can't rewind or
			 * continue.
			 */
			ereport(FATAL,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Rewind worker couldn't stop in 100s, aborting rewind")));
	}

	rewindStartTimeStamp = GetCurrentTimestamp();
	elog(LOG, "Rewind started");

	/* Terminate all other backends */
	retry = 0;
	TerminateOtherDBBackends(InvalidOid);
	while (CountOtherDBBackends(InvalidOid, &nbackends, &nprepared))
	{
		if (AutoVacuumingActive() && nbackends <= 1)
			break;

		elog(WARNING, "%u backends left", nbackends);
		pg_usleep(1000000L);
		retry++;
		if (retry >= 100)

			/*
			 * Rewind worker already stopped and come transactions could be
			 * not in the buffer so we can't continue.
			 */
			ereport(FATAL,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Backends couldn't stop in 100s, aborting rewind")));
	}

	rewindMeta->addToRewindQueueDisabled = true;

	/* All good. Do actual rewind */
	do_rewind(rewind_mode, rewind_time, rewindStartTimeStamp, rewind_oxid, rewind_xid, rewind_timestamp);

	LWLockRelease(&rewindMeta->rewindEvictLock);
	elog(LOG, "Rewind complete");
	/* Restart Postgres */
	(void) kill(PostmasterPid, SIGTERM);
	return;
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
	int			curFile;
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

		close(curFile);

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

	Assert(sizeof(struct RewindItem) == sizeof(struct SubxidsItem));
	rewindMeta = (RewindMeta *) ptr;
	ptr += MAXALIGN(sizeof(RewindMeta));
	rewindAddBuffer = (RewindItem *) ptr;
	ptr += rewind_circular_buffer_size * sizeof(RewindItem);
	rewindCompleteBuffer = (RewindItem *) ptr;
	ptr += rewind_circular_buffer_size * sizeof(RewindItem);
	o_buffers_shmem_init(&rewindBuffersDesc, ptr, found);
	ptr += o_buffers_shmem_needs(&rewindBuffersDesc);

	if (!found)
	{
		int64		i;
		int			j;

		for (i = 0; i < rewind_circular_buffer_size; i++)
		{
			rewindAddBuffer[i].oxid = InvalidOXid;
			rewindAddBuffer[i].tag = EMPTY_ITEM_TAG;
			rewindAddBuffer[i].timestamp = 0;
			rewindCompleteBuffer[i].oxid = InvalidOXid;
			rewindCompleteBuffer[i].tag = EMPTY_ITEM_TAG;
			rewindCompleteBuffer[i].timestamp = 0;

			for (j = 0; j < (int) UndoLogsCount; j++)
			{
				rewindAddBuffer[i].onCommitUndoLocation[j] = InvalidUndoLocation;
				rewindAddBuffer[i].minRetainLocation[j] = 0;
				rewindCompleteBuffer[i].onCommitUndoLocation[j] = InvalidUndoLocation;
				rewindCompleteBuffer[i].minRetainLocation[j] = 0;
			}
		}

		for (j = 0; j < (int) UndoLogsCount; j++)
		{
			UndoMeta   *undoMeta = get_undo_meta_by_type((UndoLogType) j);

			pg_atomic_write_u64(&undoMeta->minRewindRetainLocation, pg_atomic_read_u64(&undoMeta->minProcRetainLocation));
		}

		rewindMeta->rewindEvictTrancheId = LWLockNewTrancheId();
		rewindMeta->rewindCheckpointTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&rewindMeta->rewindEvictLock, rewindMeta->rewindEvictTrancheId);
		LWLockInitialize(&rewindMeta->rewindCheckpointLock, rewindMeta->rewindCheckpointTrancheId);
		pg_atomic_init_u64(&rewindMeta->addPosReserved, 0);
		pg_atomic_init_u64(&rewindMeta->addPosFilledUpto, 0);
		rewindMeta->completePos = 0;
		rewindMeta->evictPos = 0;
		rewindMeta->restorePos = 0;
		rewindMeta->checkpointPos = 0;
		rewindMeta->oldCleanedFileNum = 0;
		pg_atomic_write_u64(&rewindMeta->oldestConsideredRunningXid,
							InvalidTransactionId);
		pg_atomic_write_u64(&rewindMeta->runXmin, InvalidOXid);

		/* Rewind buffers are not persistent */
		cleanup_rewind_files(&rewindBuffersDesc, REWIND_BUFFERS_TAG);
		rewindMeta->force_complete_xid = InvalidTransactionId;
		rewindMeta->force_complete_oxid = InvalidOXid;
	}
	LWLockRegisterTranche(rewindMeta->rewindEvictTrancheId, "RewindEvictTranche");
	LWLockRegisterTranche(rewindMeta->rewindCheckpointTrancheId, "RewindCheckpointTranche");
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
 * Restore page from disk to an empty space in completeBuffer.
 * Takes an exclusive lock to avoid cocurrent page eviction.
 */
static void
try_restore_evicted_rewind_page(void)
{
	int			start;
	int			length_to_end;
	uint64		currentCleanFileNum;
	int			itemsPerFile;

	LWLockAcquire(&rewindMeta->rewindEvictLock, LW_EXCLUSIVE);

	if (rewindMeta->restorePos < rewindMeta->evictPos && (rewind_circular_buffer_size - (rewindMeta->restorePos - rewindMeta->completePos) >= REWIND_DISK_BUFFER_LENGTH))
	{
		/* Restore from disk by full pages */
		start = rewindMeta->restorePos % rewind_circular_buffer_size;
		length_to_end = rewind_circular_buffer_size - start;

#ifdef USE_ASSERT_CHECKING
		for (uint64 pos = start; pos < start + REWIND_DISK_BUFFER_LENGTH; pos++)
		{
			Assert(rewindCompleteBuffer[pos % rewind_circular_buffer_size].tag == EMPTY_ITEM_TAG);
		}
#endif

		if (length_to_end < REWIND_DISK_BUFFER_LENGTH)
		{
			o_buffers_read(&rewindBuffersDesc,
						   (Pointer) &rewindCompleteBuffer[start], REWIND_BUFFERS_TAG,
						   rewindMeta->restorePos * sizeof(RewindItem),
						   length_to_end * sizeof(RewindItem));
			o_buffers_read(&rewindBuffersDesc,
						   (Pointer) &rewindCompleteBuffer[0], REWIND_BUFFERS_TAG,
						   (rewindMeta->restorePos + length_to_end) * sizeof(RewindItem),
						   (REWIND_DISK_BUFFER_LENGTH - length_to_end) * sizeof(RewindItem));
		}
		else
		{
			o_buffers_read(&rewindBuffersDesc,
						   (Pointer) &rewindCompleteBuffer[start], REWIND_BUFFERS_TAG,
						   rewindMeta->restorePos * sizeof(RewindItem),
						   REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));
		}

#ifdef USE_ASSERT_CHECKING
		for (uint64 pos = start; pos < start + REWIND_DISK_BUFFER_LENGTH; pos++)
		{
			Assert(rewindCompleteBuffer[pos % rewind_circular_buffer_size].tag != EMPTY_ITEM_TAG);
		}
#endif

		rewindMeta->restorePos += REWIND_DISK_BUFFER_LENGTH;
		Assert(rewindMeta->restorePos <= rewindMeta->evictPos);
	}
	else
	{
		/* Try to restore from rewindAddBuffer */
		while (rewindMeta->restorePos == rewindMeta->evictPos &&
			   rewindMeta->restorePos >= rewindMeta->completePos &&
			   rewindMeta->restorePos - rewindMeta->completePos < rewind_circular_buffer_size &&
			   rewindMeta->evictPos < pg_atomic_read_u64(&rewindMeta->addPosFilledUpto))
		{
			elog(DEBUG3, "FASTPATH_restore: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%lu", pg_atomic_read_u64(&rewindMeta->addPosFilledUpto), pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, rewind_circular_buffer_size - (pg_atomic_read_u64(&rewindMeta->addPosReserved) - rewindMeta->evictPos));
			Assert(rewindAddBuffer[(rewindMeta->evictPos % rewind_circular_buffer_size)].tag != EMPTY_ITEM_TAG);
			Assert(rewindCompleteBuffer[(rewindMeta->restorePos % rewind_circular_buffer_size)].tag == EMPTY_ITEM_TAG);

			memcpy(&rewindCompleteBuffer[rewindMeta->restorePos % rewind_circular_buffer_size], &rewindAddBuffer[rewindMeta->evictPos % rewind_circular_buffer_size], sizeof(RewindItem));
			rewindAddBuffer[(rewindMeta->evictPos % rewind_circular_buffer_size)].tag = EMPTY_ITEM_TAG;
			rewindMeta->evictPos++;
			rewindMeta->restorePos++;
		}
	}
	LWLockRelease(&rewindMeta->rewindEvictLock);

	/* Clean old buffer files if needed. No lock. */
	itemsPerFile = REWIND_FILE_SIZE / sizeof(RewindItem);
	currentCleanFileNum = rewindMeta->restorePos / itemsPerFile;

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

			elog(DEBUG3, "Rewind worker came to check");

			while (rewindMeta->completePos < pg_atomic_read_u64(&rewindMeta->addPosFilledUpto))
			{
				uint64		location PG_USED_FOR_ASSERTS_ONLY;
				int			i;

				if (rewindMeta->rewindWorkerStopRequested)
				{
					/* Rewind has started. Quit rewind worker */
					shutdown_requested = true;
					rewindMeta->rewindWorkerStopped = true;
					break;
				}

				if (rewindMeta->completePos >= rewindMeta->restorePos)
				{
					/*
					 * rewindCompleteBuffer is empty. Read from
					 * rewindAddBuffer
					 */
					try_restore_evicted_rewind_page();
				}

				rewindItem = &rewindCompleteBuffer[rewindMeta->completePos % rewind_circular_buffer_size];
				Assert(rewindItem->tag != EMPTY_ITEM_TAG);

				elog(DEBUG3, "force_complete_check 1: AF=%lu, AR=%lu, E=%lu, R=%lu, C=%lu tag: %u oxid %lu xid %u nsubxids %u force_complete_oxid %lu force_complete_xid %u", pg_atomic_read_u64(&rewindMeta->addPosFilledUpto), pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->restorePos, rewindMeta->completePos, rewindItem->tag, rewindItem->oxid, rewindItem->xid, rewindItem->nsubxids, rewindMeta->force_complete_oxid, rewindMeta->force_complete_xid);

				if (rewindItem->tag == REWIND_ITEM_TAG)
				{
					bool		queue_exceeds_age = TimestampDifferenceExceeds(rewindItem->timestamp, GetCurrentTimestamp(), rewind_max_time * 1000);
					bool		queue_exceeds_length = pg_atomic_read_u64(&rewindMeta->addPosFilledUpto) - rewindMeta->completePos > rewind_max_transactions;
					bool		force_complete;

					if (OXidIsValid(rewindMeta->force_complete_oxid) || TransactionIdIsValid(rewindMeta->force_complete_xid))
					{
						elog(DEBUG3, "force_complete_check 2: AF=%lu, AR=%lu, E=%lu, R=%lu, C=%lu", pg_atomic_read_u64(&rewindMeta->addPosFilledUpto), pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->restorePos, rewindMeta->completePos);

						if (OXidIsValid(rewindMeta->force_complete_oxid) && OXidIsValid(rewindItem->oxid) && rewindItem->oxid < rewindMeta->force_complete_oxid)
							force_complete = true;
						else if (TransactionIdIsValid(rewindMeta->force_complete_xid) && TransactionIdIsValid(rewindItem->xid) && TransactionIdPrecedes(rewindItem->xid, rewindMeta->force_complete_xid))
							force_complete = true;
						else
							force_complete = false;
					}
					else
						force_complete = false;

					if (!rewindMeta->skipCheck && !queue_exceeds_age && !queue_exceeds_length && !force_complete)
					{
						/*
						 * Too early to fix the oldest item in the queue. Wait
						 * and check again.
						 */
						break;
					}

					Assert(OXidIsValid(rewindItem->oxid) || TransactionIdIsValid(rewindItem->xid));
					if (OXidIsValid(rewindItem->oxid))
					{
						/* Fix current oriole rewind item */

						clear_rewind_oxid(rewindItem->oxid);
						for (i = 0; i < (int) UndoLogsCount; i++)
						{
							UndoMeta   *undoMeta = get_undo_meta_by_type((UndoLogType) i);

							elog(DEBUG3, "Completing: oxid %lu xid %u logtype %d undoLoc %lu onCommitLoc %lu, minRetainLoc %lu, oldestRunXid %u, nsubxids %u", rewindItem->oxid, rewindItem->xid, i, rewindItem->undoLocation[i], rewindItem->onCommitUndoLocation[i], rewindItem->minRetainLocation[i], XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);

							location = walk_undo_range_with_buf((UndoLogType) i, rewindItem->onCommitUndoLocation[i], InvalidUndoLocation, rewindItem->oxid, false, NULL, true);
							Assert(!UndoLocationIsValid(location));
#ifdef USE_ASSERT_CHECKING
							Assert(pg_atomic_read_u64(&undoMeta->minRewindRetainLocation) <= rewindItem->minRetainLocation[i]);
#endif
							pg_atomic_write_u64(&undoMeta->minRewindRetainLocation, rewindItem->minRetainLocation[i]);
						}
					}
					else
					{
						elog(DEBUG3, "Completing: oxid %lu xid %u oldestRunXid %u, nsubxids %u", rewindItem->oxid, rewindItem->xid, XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);
					}

					pg_atomic_write_u64(&rewindMeta->oldestConsideredRunningXid,
										rewindItem->oldestConsideredRunningXid.value);
					pg_atomic_write_u64(&rewindMeta->runXmin,
										rewindItem->runXmin);
				}

				/*
				 * Clear the REWIND_ITEM or SUBXIDS_ITEM from the circular
				 * buffer
				 */
				rewindItem->tag = EMPTY_ITEM_TAG;
				rewindMeta->completePos++;
				Assert(rewindMeta->completePos <= rewindMeta->restorePos);
				try_restore_evicted_rewind_page();
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
 * Evict page from a ring buffer to disk. Takes exclusive lock against concurrent eviction.
 */
static void
evict_rewind_items(uint64 curAddPosFilled)
{
	int			start;
	int			length_to_end;

	LWLockAcquire(&rewindMeta->rewindCheckpointLock, LW_SHARED);
	if (LWLockAcquireOrWait(&rewindMeta->rewindEvictLock, LW_EXCLUSIVE))
	{
		Assert(rewindMeta->restorePos <= rewindMeta->evictPos);

		if (rewindMeta->restorePos == rewindMeta->evictPos && rewindMeta->restorePos - rewindMeta->completePos < rewind_circular_buffer_size)
		{
			/* Fast path: move to rewindCompleteBuffer */
			while (rewindMeta->restorePos - rewindMeta->completePos < rewind_circular_buffer_size && rewindMeta->evictPos < curAddPosFilled)
			{
				elog(DEBUG3, "FASTPATH_evict: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%lu", curAddPosFilled, pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, rewind_circular_buffer_size - (pg_atomic_read_u64(&rewindMeta->addPosReserved) - rewindMeta->evictPos));
				Assert(rewindAddBuffer[(rewindMeta->evictPos % rewind_circular_buffer_size)].tag != EMPTY_ITEM_TAG);
				Assert(rewindCompleteBuffer[(rewindMeta->restorePos % rewind_circular_buffer_size)].tag == EMPTY_ITEM_TAG);

				memcpy(&rewindCompleteBuffer[rewindMeta->restorePos % rewind_circular_buffer_size], &rewindAddBuffer[rewindMeta->evictPos % rewind_circular_buffer_size], sizeof(RewindItem));
				rewindAddBuffer[(rewindMeta->evictPos % rewind_circular_buffer_size)].tag = EMPTY_ITEM_TAG;
				rewindMeta->evictPos++;
				rewindMeta->restorePos++;
			}
		}
		else if (rewindMeta->evictPos + REWIND_DISK_BUFFER_LENGTH < curAddPosFilled)
		{
			/* Evict to disk buffers */
			start = (rewindMeta->evictPos % rewind_circular_buffer_size);
			length_to_end = rewind_circular_buffer_size - start;

			elog(DEBUG3, "DISK_evict: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%lu", curAddPosFilled, pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, rewind_circular_buffer_size - (pg_atomic_read_u64(&rewindMeta->addPosReserved) - rewindMeta->evictPos));

			if (length_to_end < REWIND_DISK_BUFFER_LENGTH)
			{
				o_buffers_write(&rewindBuffersDesc,
								(Pointer) &rewindAddBuffer[start],
								REWIND_BUFFERS_TAG,
								rewindMeta->evictPos * sizeof(RewindItem),
								length_to_end * sizeof(RewindItem));

				o_buffers_write(&rewindBuffersDesc,
								(Pointer) &rewindAddBuffer[0],
								REWIND_BUFFERS_TAG,
								(rewindMeta->evictPos + length_to_end) * sizeof(RewindItem),
								(REWIND_DISK_BUFFER_LENGTH - length_to_end) * sizeof(RewindItem));
			}
			else
			{
				o_buffers_write(&rewindBuffersDesc,
								(Pointer) &rewindAddBuffer[start],
								REWIND_BUFFERS_TAG,
								rewindMeta->evictPos * sizeof(RewindItem),
								REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));
			}

			/* Clean written items from ring buffer */
			for (int i = 0; i < REWIND_DISK_BUFFER_LENGTH; i++)
			{
				rewindAddBuffer[(rewindMeta->evictPos % rewind_circular_buffer_size)].tag = EMPTY_ITEM_TAG;
				rewindMeta->evictPos++;
			}
		}

		Assert(rewindMeta->evictPos <= curAddPosFilled);
		LWLockRelease(&rewindMeta->rewindEvictLock);
	}
	else
	{
		elog(DEBUG3, "evict_CONCURRENT_SKIPPED: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%lu", curAddPosFilled, pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, rewind_circular_buffer_size - (pg_atomic_read_u64(&rewindMeta->addPosReserved) - rewindMeta->evictPos));
	}

	LWLockRelease(&rewindMeta->rewindCheckpointLock);
}

void
reset_precommit_xid_subxids(void)
{
	precommit_xid = InvalidTransactionId;
	precommit_nsubxids = 0;
	precommit_subxids = NULL;
}

void
save_precommit_xid_subxids(void)
{
	TransactionId xid1 = InvalidTransactionId;
	TransactionId xid2 = InvalidTransactionId;

	xid1 = GetTopTransactionIdIfAny();
	xid2 = GetCurrentTransactionIdIfAny();
	elog(DEBUG3, "PRE-COMMIT top xid %u, cur xid %u", xid1, xid2);

	/* Don't overwrite existing precommit_xid with zero. */
	if (TransactionIdIsValid(xid1))
	{
		Assert(!TransactionIdIsValid(precommit_xid));

		precommit_xid = xid1;
		precommit_nsubxids = xactGetCommittedChildren(&precommit_subxids);
	}
}

TransactionId
get_precommit_xid_subxids(int *nsubxids, TransactionId **subxids)
{
	if (TransactionIdIsValid(precommit_xid))
	{
		*nsubxids = precommit_nsubxids;
		*subxids = precommit_subxids;
	}

	return precommit_xid;
}


void
add_to_rewind_buffer(OXid oxid, TransactionId xid, int nsubxids, TransactionId *subxids)
{
	RewindItem *rewindItem;
	int			i;
	uint64		curAddPos;
	uint64		startAddPos;
	uint64		reserved;
	uint64		filled;
	uint64		cur;
	int			freeAddSpace;
	bool		subxid_only = false;
	int			subxids_count = 0;
	int			nitems;

	if (rewindMeta->addToRewindQueueDisabled)
	{
		elog(WARNING, "Adding to rewind queue is blocked by rewind");
		goto add_items_finished;
	}

	nitems = nsubxids ? 2 + (nsubxids / SUBXIDS_PER_ITEM) : 1;
	startAddPos = pg_atomic_fetch_add_u64(&rewindMeta->addPosReserved, nitems);

	while (true)
	{
		/*
		 * freeAddSpace is the free space in rewindAddBuffer presumind space
		 * needed the current items for the current process nitems as already
		 * occupied. Space reserved by concurrent processes later is not
		 * counted as occupied here. Concurrent processes will see theit
		 * freeAddSpace under the same presumption for them and will use it
		 * for eviction in the same way. So double eviction is possible and OK
		 * but very unlikely.
		 */
		freeAddSpace = rewind_circular_buffer_size - (startAddPos + nitems - rewindMeta->evictPos);
		elog(DEBUG3, "add_to_rewind_buffer: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%u", pg_atomic_read_u64(&rewindMeta->addPosFilledUpto), pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, freeAddSpace);

		if (freeAddSpace <= REWIND_DISK_BUFFER_LENGTH * 4 || (rewind_circular_buffer_size < REWIND_DISK_BUFFER_LENGTH * 8 && freeAddSpace <= REWIND_DISK_BUFFER_LENGTH))
		{
			elog(DEBUG3, "evict_rewind_items START: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%u", pg_atomic_read_u64(&rewindMeta->addPosFilledUpto), pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, freeAddSpace);
			evict_rewind_items(pg_atomic_read_u64(&rewindMeta->addPosFilledUpto));
			elog(DEBUG3, "evict_rewind_items STOP: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%lu", pg_atomic_read_u64(&rewindMeta->addPosFilledUpto), pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, rewind_circular_buffer_size - (pg_atomic_read_u64(&rewindMeta->addPosReserved) - rewindMeta->evictPos));
		}

		if (freeAddSpace >= 0)
			break;
	}

	curAddPos = startAddPos;

next_subxids_item:
	Assert(curAddPos < startAddPos + nitems);
	rewindItem = &rewindAddBuffer[curAddPos % rewind_circular_buffer_size];
	Assert(rewindItem->tag == EMPTY_ITEM_TAG);

	if (subxid_only)
	{
		/* Fill subxids entry in a circular buffer. */
		SubxidsItem *subxidsItem = (SubxidsItem *) rewindItem;

		Assert(TransactionIdIsValid(xid));
		Assert(nsubxids > 0);
		subxidsItem->oxid = oxid;
		subxidsItem->nsubxids = nsubxids;
		while (true)
		{
			elog(DEBUG3, "Add SubXid to buffer: oxid %lu cur xid %u subxid [%u/%u] %u", oxid, xid, subxids_count + 1, nsubxids, subxids[subxids_count]);
			subxidsItem->subxids[subxids_count % SUBXIDS_PER_ITEM] = subxids[subxids_count];
			subxids_count++;
			if (subxids_count == nsubxids)
			{
				subxid_only = false;
				nsubxids = 0;
				pg_write_barrier();
				subxidsItem->tag = SUBXIDS_ITEM_TAG;

				goto add_items_finished;
			}

			if (subxids_count % SUBXIDS_PER_ITEM == 0)
			{
				curAddPos++;
				pg_write_barrier();
				subxidsItem->tag = SUBXIDS_ITEM_TAG;

				goto next_subxids_item; /* Write next rewindItem only with
										 * subxids */
			}
		}
	}
	else
	{
		/* Fill rewind entry in a circular buffer. */
		rewindItem->timestamp = GetCurrentTimestamp();
		rewindItem->oxid = oxid;
		rewindItem->xid = xid;
		rewindItem->nsubxids = nsubxids;
		rewindItem->oldestConsideredRunningXid = GetOldestFullTransactionIdConsideredRunning();
		rewindItem->runXmin = pg_atomic_read_u64(&xid_meta->runXmin);

		if (!OXidIsValid(pg_atomic_read_u64(&rewindMeta->runXmin)))
		{
			uint64		curValue = InvalidOXid;

			(void) pg_atomic_compare_exchange_u64(&rewindMeta->runXmin, &curValue, rewindItem->runXmin);
		}

		if (pg_atomic_read_u64(&rewindMeta->oldestConsideredRunningXid) == InvalidTransactionId)
		{
			uint64		curValue = InvalidTransactionId;

			(void) pg_atomic_compare_exchange_u64(&rewindMeta->oldestConsideredRunningXid, &curValue, rewindItem->oldestConsideredRunningXid.value);
		}

		Assert(OXidIsValid(oxid) || TransactionIdIsValid(xid));

		if (OXidIsValid(oxid))
		{
			Assert(csn_is_retained_for_rewind(oxid_get_csn(oxid, true)));

			for (i = 0; i < (int) UndoLogsCount; i++)
			{
				UndoMeta   *undoMeta = get_undo_meta_by_type((UndoLogType) i);
				UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS((UndoLogType) i);

				rewindItem->onCommitUndoLocation[i] = pg_atomic_read_u64(&sharedLocations->onCommitLocation);
				rewindItem->undoLocation[i] = pg_atomic_read_u64(&sharedLocations->location);
				rewindItem->minRetainLocation[i] = pg_atomic_read_u64(&undoMeta->minProcRetainLocation);
				elog(DEBUG3, "Add to buffer: oxid %lu xid %u logtype %d undoLoc %lu onCommitLoc %lu, minRetainLoc %lu, oldestRunningXid %u nsubxids %u", oxid, xid, i, rewindItem->undoLocation[i], rewindItem->onCommitUndoLocation[i], rewindItem->minRetainLocation[i], XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), nsubxids);
			}

		}
		else
		{
			elog(DEBUG3, "Add to buffer: oxid %lu xid %u oldestRunningXid %u nsubxids %u", oxid, xid, XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), nsubxids);
		}

		pg_write_barrier();
		rewindItem->tag = REWIND_ITEM_TAG;

		if (nsubxids)
		{
			Assert(TransactionIdIsValid(xid));
			subxid_only = true;
			subxids_count = 0;
			curAddPos++;
			goto next_subxids_item; /* Write first rewindItem only with
									 * subxids */
		}
	}
add_items_finished:
repeat_bump_filled_pos:

	/*
	 * Bump rewindMeta->addPosFilledUpto until the first item that was
	 * reserved by some concurrent process but not filled yet. If unlikely
	 * in-progress queue becomes too big, wait until blocking in-progress
	 * inserter completes insertion and eventually until the transient part of
	 * a queue decreases under allowed threshold..
	 */
	pg_read_barrier();
	reserved = pg_atomic_read_u64(&rewindMeta->addPosReserved);
	filled = pg_atomic_read_u64(&rewindMeta->addPosFilledUpto);
	cur = filled;

	while (cur < reserved)
	{
		if ((&rewindAddBuffer[cur % rewind_circular_buffer_size])->tag != EMPTY_ITEM_TAG)
		{
			/*
			 * Try to bump addPosFilledUpto. Skip to next if already bumped by
			 * concurrent process
			 */
			cur++;
			pg_atomic_compare_exchange_u64(&rewindMeta->addPosFilledUpto, &filled, cur);
		}
		else
		{
			/* Current item is not written yet */
			break;
		}
	}

	/*
	 * Still too many transient items in the queue, try to repeat bumping
	 * addPosFilledUpto
	 */
	if (reserved - filled > 20)
		goto repeat_bump_filled_pos;
}

/*
 * Write rewind records from circular in-memory rewind buffer and on-disk rewind buffer
 * to xid buffer at checkpoint.
 */
void
checkpoint_write_rewind_xids(void)
{
	bool		buffer_loaded = false;
	RewindItem	buffer[REWIND_DISK_BUFFER_LENGTH];

	if (!enable_rewind)
		return;

	if (rewindMeta->addToRewindQueueDisabled)
	{
		elog(WARNING, "Adding to rewind queue to checkpoint is blocked by rewind");
		return;
	}

	LWLockAcquire(&rewindMeta->rewindCheckpointLock, LW_EXCLUSIVE);

	/*
	 * Start from the last non-completed position not written to checkpoint
	 * yet
	 */
	rewindMeta->checkpointPos = Max(rewindMeta->completePos, rewindMeta->checkpointPos);

	/*
	 * Write rewind records from in-memory rewind buffer (before evicted
	 * records)
	 */
	for (; rewindMeta->checkpointPos < rewindMeta->restorePos; rewindMeta->checkpointPos++)
		checkpoint_write_rewind_item(&rewindCompleteBuffer[rewindMeta->checkpointPos % rewind_circular_buffer_size]);

	/* Write rewind records from on-disk buffer if they exist */
	while (rewindMeta->checkpointPos < rewindMeta->evictPos)
	{
		if (!buffer_loaded)
		{
			int			startbuf = rewindMeta->checkpointPos - (rewindMeta->checkpointPos % REWIND_DISK_BUFFER_LENGTH);

			o_buffers_read(&rewindBuffersDesc,
						   (Pointer) &buffer, REWIND_BUFFERS_TAG,
						   startbuf * sizeof(RewindItem),
						   REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));
			buffer_loaded = true;
		}

		elog(DEBUG3, "CHECKPOINT FROM DISK: AF=%lu AR=%lu E=%lu C=%lu R=%lu freeAdd=%lu", pg_atomic_read_u64(&rewindMeta->addPosFilledUpto), pg_atomic_read_u64(&rewindMeta->addPosReserved), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, rewind_circular_buffer_size - (pg_atomic_read_u64(&rewindMeta->addPosReserved) - rewindMeta->evictPos));

		checkpoint_write_rewind_item(&buffer[rewindMeta->checkpointPos % REWIND_DISK_BUFFER_LENGTH]);
		rewindMeta->checkpointPos++;
		if (rewindMeta->checkpointPos % REWIND_DISK_BUFFER_LENGTH == 0)
			buffer_loaded = false;
	}

	/*
	 * Write rewind records from in-memory rewind buffer (after evicted
	 * records if they exist)
	 */
	for (; rewindMeta->checkpointPos < pg_atomic_read_u64(&rewindMeta->addPosFilledUpto); rewindMeta->checkpointPos++)
		checkpoint_write_rewind_item(&rewindAddBuffer[rewindMeta->checkpointPos % rewind_circular_buffer_size]);

	LWLockRelease(&rewindMeta->rewindCheckpointLock);
}

OXid
get_rewind_run_xmin(void)
{
	return pg_atomic_read_u64(&rewindMeta->runXmin);
}
