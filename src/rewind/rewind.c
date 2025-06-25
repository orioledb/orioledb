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
static TransactionId   precommit_xid;
static int             precommit_nsubxids;
static TransactionId  *precommit_subxids;

PG_FUNCTION_INFO_V1(orioledb_rewind_sync);
PG_FUNCTION_INFO_V1(orioledb_rewind_by_time);
PG_FUNCTION_INFO_V1(orioledb_rewind_to_timestamp);
PG_FUNCTION_INFO_V1(orioledb_rewind_to_transaction);
PG_FUNCTION_INFO_V1(orioledb_current_oxid);
PG_FUNCTION_INFO_V1(orioledb_rewind_queue_length);
PG_FUNCTION_INFO_V1(orioledb_rewind_evicted_length);

/* User-available */
#define REWIND_MODE_UNKNOWN (0)
#define	REWIND_MODE_TIME (1)
#define REWIND_MODE_TIMESTAMP (2)
/* Modes below allowed only in debug functions under IS_DEV */
#define	REWIND_MODE_XID	(3)

static inline
void print_rewind_item(RewindItem *rewindItem, uint64 pos)
{
	/* To shorten output invalid values are printed as -1 */
	int64 oxid = OXidIsValid (rewindItem->oxid) ? rewindItem->oxid : -1;
	int64 undoLocation[3];
	int64 onCommitUndoLocation[3];
	int64 minRetainLocation[3];

	for (int i = 0; i < 3; i++)
	{
		undoLocation[i] = (rewindItem->undoLocation[i] == InvalidUndoLocation) ? -1 : rewindItem->undoLocation[i];
		onCommitUndoLocation[i] = (rewindItem->onCommitUndoLocation[i] == InvalidUndoLocation) ? -1 : rewindItem->onCommitUndoLocation[i];
		minRetainLocation[i] = (rewindItem->minRetainLocation[i] == InvalidUndoLocation) ? -1 : rewindItem->minRetainLocation[i];
	}

	elog(LOG, "P %lu T %u OX %ld X %u L0 %ld OCL0 %ld, MRL0 %ld, L1 %ld OCL1 %ld, MRL1 %ld L2 %ld OCL2 %ld, MRL2 %ld ORX %u NS %u", pos, rewindItem->tag, oxid, rewindItem->xid, undoLocation[0], onCommitUndoLocation[0], minRetainLocation[0], undoLocation[1], onCommitUndoLocation[1], minRetainLocation[1], undoLocation[2], onCommitUndoLocation[2], minRetainLocation[2], XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);
}

/*
 * Write rewind records from circular in-memory rewind buffer and on-disk rewind buffer
 * to xid buffer at checkpoint.
 */
static
void log_print_rewind_queue(void)
{
	int			i;
	uint64			pos;
	uint64			curAddPos;
	int 			freeAddSpace;

	if (!enable_rewind)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("orioledb rewind mode is turned off")),
				errdetail("to use rewind set orioledb.enable_rewind = on in PostgreSQL config file."));
		return;
	}

	/* NB: addPos could be increased concurrently */
	curAddPos = pg_atomic_read_u64(&rewindMeta->addPos);
	freeAddSpace = rewind_circular_buffer_size - (curAddPos - rewindMeta->evictPos);

	elog(LOG, "Print rewind queue: A=%lu E=%lu C=%lu R=%lu freeAdd=%u", pg_atomic_read_u64(&rewindMeta->addPos), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, freeAddSpace);

	/* From completeBuffer if exists*/
	for (pos = rewindMeta->completePos; pos < rewindMeta->restorePos; pos++)
		print_rewind_item(&rewindCompleteBuffer[pos % rewind_circular_buffer_size], pos);

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
			print_rewind_item(&buffer[i], pos + i);
		}
	}

	/*
	 * From in-memory rewind addBuffer (after evicted
	 * records if they exist)
	 */
	for (; pos < curAddPos; pos++)
		print_rewind_item(&rewindAddBuffer[pos % rewind_circular_buffer_size], pos);
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
orioledb_current_oxid(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(get_current_oxid());
}

static void
do_rewind(int rewind_mode, int rewind_time, TimestampTz rewindStartTimeStamp, OXid rewind_oxid, TransactionId rewind_xid, TimestampTz rewind_timestamp)
{
	int	i = 0;
	RewindItem     *rewindItem;
	RewindItem 	tmpbuf[REWIND_DISK_BUFFER_LENGTH];
	uint64		pos;
	int		nsubxids = 0;
	int 		subxids_count = 0;
	bool		got_all_subxids = false;
#ifdef USE_ASSERT_CHECKING
	bool 		started_subxids = false;
#endif
	TransactionId   *subxids = NULL;
	TransactionId	xid = InvalidTransactionId;
	OXid		oxid = InvalidOXid;
	CommitSeqNo	csn PG_USED_FOR_ASSERTS_ONLY;
	long		secs;
	int		usecs;

	log_print_rewind_queue();

	/* Decrement before, as addPos was a next element to add. */
	pos = pg_atomic_sub_fetch_u64(&rewindMeta->addPos, 1);

	while (true)
	{
		if (pos >= rewindMeta->evictPos) /* At evictPos is the next element to be evicted. It's actually still in rewindAddBuffer */
		{
			rewindItem = &rewindAddBuffer[pos % rewind_circular_buffer_size];
		}
		else if (pos >= rewindMeta->restorePos) /* At restorePos is the next element to be restored. It's actually in disk buffer */
		{
			if (i == 0)
			{
				o_buffers_read(&rewindBuffersDesc,
						(Pointer) &tmpbuf, REWIND_BUFFERS_TAG,
						(pos - REWIND_DISK_BUFFER_LENGTH) * sizeof(RewindItem),
						REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));
				i = REWIND_DISK_BUFFER_LENGTH;
			}
			i--;
			rewindItem = &tmpbuf[i];
		}
		else
		{
			rewindItem = &rewindCompleteBuffer[pos % rewind_circular_buffer_size];
		}

		Assert(rewindItem->tag != EMPTY_ITEM_TAG);

		/*
		 * Gather subxids from subxids items (may be multiple). As we read backwards, we don't yet
		 * have filled respective rewindItem.
		 */
		if (rewindItem->tag == SUBXIDS_ITEM_TAG)
		{
			SubxidsItem *subxidsItem = (SubxidsItem *) rewindItem;

			Assert(!got_all_subxids);
			Assert(subxidsItem->nsubxids);

			if (nsubxids == 0)
			{
				/* First subxids item (As we read backwards so it's last written one) */
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
				 * subxidsItem->nsubxids for every subxidsItem contains the same sum of
				 * of subxids for a rewindItem (not just the number of subxids in a current subxidsItem.
				 */
				Assert(subxidsItem->nsubxids == nsubxids);
				Assert(subxidsItem->oxid == oxid);
			}

			while (true)
			{
				subxids_count--;
				subxids[subxids_count] = subxidsItem->subxids[subxids_count % SUBXIDS_PER_ITEM];

				elog(LOG, "Rewinding SubXid: oxid %lu cur xid %u subxid [%u/%u] %u", oxid, xid, subxids_count + 1, nsubxids, subxids[subxids_count]);

				if (subxids_count == 0)
				{
					got_all_subxids = true;
					break;
				}
				else if ((subxids_count % SUBXIDS_PER_ITEM) == 0)
					break; /* Read next subxids item (As we read backwards so it's previous written one) */
			}
		}
		else
		{
			Assert(rewindItem->tag == REWIND_ITEM_TAG);

			Assert(OXidIsValid(rewindItem->oxid) || TransactionIdIsValid(rewindItem->xid));

			if (rewind_mode == REWIND_MODE_TIME)
			{
				if(TimestampDifferenceExceeds(rewindItem->timestamp, rewindStartTimeStamp, rewind_time * 1000))
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

					elog(LOG, "Rewinding: oxid %lu xid %u logtype %d undoLoc %lu onCommitLoc %lu, minRetainLoc %lu, oldestRunXid %u, nsubxids %u", rewindItem->oxid, rewindItem->xid, i, rewindItem->undoLocation[i], rewindItem->onCommitUndoLocation[i], rewindItem->minRetainLocation[i], XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);

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
				elog(LOG, "Rewinding: oxid %lu xid %u oldestRunXid %u, nsubxids %u", rewindItem->oxid, rewindItem->xid, XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);

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
				Assert (!started_subxids && !got_all_subxids && nsubxids == 0);
		}

		/* Clear the item from the circular buffer */
		rewindItem->tag = EMPTY_ITEM_TAG;

		/*
		 * Buffer finished (rewind was requested for all rewind buffer)
		 */
		if (pos == rewindMeta->completePos)
		{
			TimestampDifference(rewindItem->timestamp, rewindStartTimeStamp, &secs, &usecs);
			ereport(LOG, errmsg("orioledb rewind completed on full rewind capacity. Last rewound transaction xid %u, oxid %lu is %ld.%d seconds back", rewindItem->xid, rewindItem->oxid, secs, usecs/1000));
			return;
		}
		pos--;
	}

rewind_complete:

	TimestampDifference(rewindItem->timestamp, rewindStartTimeStamp, &secs, &usecs);
	ereport(LOG, errmsg("orioledb rewind completed. Last remaining transaction xid %u, oxid %lu is %ld.%d seconds back", rewindItem->xid, rewindItem->oxid, secs, usecs/1000));
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
	TimestampTz	rewindStartTimeStamp;
	int		retry;
	int 		nbackends;
	int		nprepared;

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
		 * NB: these timestamp checks are from current time so they are little bit stricter than needed as
		 * actual rewindStartTimeStamp is recorded little bit later, after stopping rewind worker.
		 */
		if(rewind_timestamp >= GetCurrentTimestamp())
		{
			elog(WARNING, "Zero rewind requested, do nothing");
			return;
		}
		else if(TimestampDifferenceExceeds(rewind_timestamp, GetCurrentTimestamp(), rewind_max_time * 1000))
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

	LWLockAcquire(&rewindMeta->evictLock, LW_EXCLUSIVE);

	/* Disable adding new transactions to rewind buffer by rewind worker */
	rewindMeta->rewindWorkerStopRequested = true;
	retry = 0;
	while (!rewindMeta->rewindWorkerStopped)
	{
		pg_usleep(10000L);
		retry++;
		if (retry >= 1000)
			/* We don't know the state of rewind worker so we can't rewind or continue. */
			ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Rewind worker couldn't stop in 100s, aborting rewind")));
	}

	rewindStartTimeStamp = GetCurrentTimestamp();
	elog(LOG, "Rewind started, for %d s", rewind_time);

	/* Terminate all other backends */
	retry = 0;
	TerminateOtherDBBackends(InvalidOid);
	while(CountOtherDBBackends(InvalidOid, &nbackends, &nprepared))
	{
		if (AutoVacuumingActive() && nbackends <= 1)
			break;

		elog(WARNING, "%u backends left", nbackends);
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

	rewindMeta->addToRewindQueueDisabled = true;

	/* All good. Do actual rewind */
	do_rewind(rewind_mode, rewind_time, rewindStartTimeStamp, rewind_oxid, rewind_xid, rewind_timestamp);

	elog(LOG, "Rewind complete, for %d s", rewind_time);
	/* Restart Postgres */
	(void) kill(PostmasterPid, SIGTERM);
	return;
}

/* Interface functions */
Datum
orioledb_rewind_by_time(PG_FUNCTION_ARGS)
{
	int	rewind_time = PG_GETARG_INT32(0);

	orioledb_rewind_internal(REWIND_MODE_TIME, rewind_time, InvalidOXid, InvalidTransactionId, (TimestampTz) 0);
	PG_RETURN_VOID();
}

Datum
orioledb_rewind_to_transaction(PG_FUNCTION_ARGS)
{
	TransactionId 	xid = PG_GETARG_INT32(0);
	OXid 		oxid = PG_GETARG_INT64(1);

	orioledb_rewind_internal(REWIND_MODE_XID, 0, oxid, xid, (TimestampTz) 0);
	PG_RETURN_VOID();
}

Datum
orioledb_rewind_to_timestamp(PG_FUNCTION_ARGS)
{
        TimestampTz	rewind_timestamp = PG_GETARG_TIMESTAMPTZ(0);

        orioledb_rewind_internal(REWIND_MODE_TIMESTAMP, 0, InvalidOXid, InvalidTransactionId, rewind_timestamp);
        PG_RETURN_VOID();
}

/*
 * Access to a rewindMeta without a lock. This is ok when we check if it's time to fix oldest items in the queue.
 * If precise value is needed getting rewindMeta lock is a responsibility of a caller.
 */
Datum
orioledb_rewind_queue_length(PG_FUNCTION_ARGS)
{
	if (!enable_rewind)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("orioledb rewind mode is turned off")),
				errdetail("to use rewind set orioledb.enable_rewind = on in PostgreSQL config file."));
	}

	PG_RETURN_UINT64(pg_atomic_read_u64(&rewindMeta->addPos) - rewindMeta->completePos);
}

Datum
orioledb_rewind_evicted_length(PG_FUNCTION_ARGS)
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
		LWLockInitialize(&rewindMeta->evictLock, rewindMeta->rewindEvictTrancheId);

		pg_atomic_init_u64(&rewindMeta->addPos, 0);
		rewindMeta->completePos = 0;
		rewindMeta->evictPos = 0;
		rewindMeta->restorePos = 0;
		rewindMeta->checkpointPos = 0;
		rewindMeta->oldCleanedFileNum = 0;
		pg_atomic_write_u64(&rewindMeta->oldestConsideredRunningXid,
							InvalidTransactionId);
		pg_atomic_write_u64(&rewindMeta->runXmin,
							InvalidOXid);

		/* Rewind buffers are not persistent */
		cleanup_rewind_files(&rewindBuffersDesc, REWIND_BUFFERS_TAG);
	}
	LWLockRegisterTranche(rewindMeta->rewindEvictTrancheId, "RewindEvictTranche");
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

	LWLockAcquire(&rewindMeta->evictLock, LW_EXCLUSIVE);

	start = rewindMeta->restorePos % rewind_circular_buffer_size;
	length_to_end = rewind_circular_buffer_size - start;

	if (length_to_end <= REWIND_DISK_BUFFER_LENGTH)
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

	LWLockRelease(&rewindMeta->evictLock);

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

/* 			elog(LOG, "Rewind worker came to check"); */

			while (rewindMeta->completePos < pg_atomic_read_u64(&rewindMeta->addPos))
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

				if (rewindMeta->completePos < rewindMeta->restorePos)
					/* Read from rewindCompleteBuffer */
					rewindItem = &rewindCompleteBuffer[rewindMeta->completePos % rewind_circular_buffer_size];
				else
				{
					/* rewindCompleteBuffer is empty. Read from rewindAddBuffer */
					Assert(rewindMeta->restorePos == rewindMeta->evictPos);
					rewindItem = &rewindAddBuffer[rewindMeta->completePos % rewind_circular_buffer_size];
				}
				Assert(rewindItem->tag != EMPTY_ITEM_TAG);

				if (rewindItem->tag == REWIND_ITEM_TAG)
				{
					bool queue_exceeds_age = TimestampDifferenceExceeds(rewindItem->timestamp, GetCurrentTimestamp(), rewind_max_time * 1000);
					bool queue_exceeds_length = pg_atomic_read_u64(&rewindMeta->addPos) - rewindMeta->completePos > rewind_max_transactions;

					if (!rewindMeta->skipCheck && !queue_exceeds_age && !queue_exceeds_length)
					{
						/* Too early to fix the oldest item in the queue. Wait and check again. */
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

							elog(LOG, "Completing: oxid %lu xid %u logtype %d undoLoc %lu onCommitLoc %lu, minRetainLoc %lu, oldestRunXid %u, nsubxids %u", rewindItem->oxid, rewindItem->xid, i, rewindItem->undoLocation[i], rewindItem->onCommitUndoLocation[i], rewindItem->minRetainLocation[i], XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);

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
						elog(LOG, "Completing: oxid %lu xid %u oldestRunXid %u, nsubxids %u", rewindItem->oxid, rewindItem->xid, XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), rewindItem->nsubxids);
					}

					pg_atomic_write_u64(&rewindMeta->oldestConsideredRunningXid,
										rewindItem->oldestConsideredRunningXid.value);
					pg_atomic_write_u64(&rewindMeta->runXmin,
										rewindItem->runXmin);
				}

				/* Clear the REWIND_ITEM or SUBXIDS_ITEM from the circular buffer */
				rewindItem->tag = EMPTY_ITEM_TAG;
				rewindMeta->completePos++;

				if (rewindMeta->restorePos < rewindMeta->evictPos && rewind_circular_buffer_size - (rewindMeta->restorePos - rewindMeta->completePos) >= REWIND_DISK_BUFFER_LENGTH)
					restore_evicted_rewind_page();
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
evict_rewind_items(uint64 curAddPos)
{
	int			start;
	int			length_to_end;

//	if (LWLockAcquireOrWait(&rewindMeta->evictLock, LW_EXCLUSIVE))

	LWLockAcquire(&rewindMeta->evictLock, LW_EXCLUSIVE);

	{
		Assert(rewindMeta->restorePos <= rewindMeta->evictPos);

		if (rewindMeta->restorePos == rewindMeta->evictPos && rewindMeta->restorePos - rewindMeta->completePos < rewind_circular_buffer_size)
		{
			/* Fast path: move to rewindCompleteBuffer */
			while (rewindMeta->restorePos - rewindMeta->completePos < rewind_circular_buffer_size && rewindMeta->evictPos < curAddPos)
			{
//				elog(WARNING, "FASTPATH_evict: A=%lu E=%lu C=%lu R=%lu freeAdd=%u", pg_atomic_read_u64(&rewindMeta->addPos), rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, rewind_circular_buffer_size - (pg_atomic_read_u64(&rewindMeta->addPos) - rewindMeta->evictPos));

				Assert(rewindAddBuffer[(rewindMeta->evictPos % rewind_circular_buffer_size)].tag != EMPTY_ITEM_TAG);
				Assert(rewindCompleteBuffer[(rewindMeta->restorePos % rewind_circular_buffer_size)].tag == EMPTY_ITEM_TAG);

				memcpy(&rewindCompleteBuffer[rewindMeta->restorePos % rewind_circular_buffer_size], &rewindAddBuffer[rewindMeta->evictPos % rewind_circular_buffer_size], sizeof(RewindItem));
				rewindAddBuffer[(rewindMeta->evictPos % rewind_circular_buffer_size)].tag = EMPTY_ITEM_TAG;
				rewindMeta->evictPos++;
				rewindMeta->restorePos++;
			}
		}
		else
		{
			/* Evict to disk buffers */
			start = (rewindMeta->evictPos % rewind_circular_buffer_size);
			length_to_end = rewind_circular_buffer_size - start;

			elog(WARNING, "DISK_evict: A=%lu E=%lu C=%lu R=%lu freeAdd=%lu", curAddPos, rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, rewind_circular_buffer_size - curAddPos - rewindMeta->evictPos);

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

		Assert(rewindMeta->evictPos <= curAddPos);
	}
	LWLockRelease(&rewindMeta->evictLock);
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
	TransactionId	xid1 = InvalidTransactionId;
        TransactionId	xid2 = InvalidTransactionId;

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
	int			freeAddSpace;
	bool 		subxid_only = false;
	int		subxids_count = 0;

	if (rewindMeta->addToRewindQueueDisabled)
	{
		elog(WARNING, "Adding to rewind queue is blocked by rewind");
		return;
	}

next_subxids_item:

//	LWLockAcquire(&rewindMeta->evictLock, LW_SHARED);//
	curAddPos = pg_atomic_fetch_add_u64(&rewindMeta->addPos, 1);
//	LWLockRelease(&rewindMeta->evictLock);//

	freeAddSpace = rewind_circular_buffer_size - (curAddPos - rewindMeta->evictPos);
	Assert(freeAddSpace >= 0);
	elog(LOG, "add_to_rewind_buffer: A=%lu E=%lu C=%lu R=%lu freeAdd=%u", curAddPos, rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, freeAddSpace);

	if (freeAddSpace < REWIND_DISK_BUFFER_LENGTH * 4)
	{
		elog(LOG, "evict_rewind_items START: A=%lu E=%lu C=%lu R=%lu freeAdd=%u", curAddPos, rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, freeAddSpace);
		evict_rewind_items(curAddPos);
		elog(LOG, "evict_rewind_items STOP: A=%lu E=%lu C=%lu R=%lu freeAdd=%u", curAddPos, rewindMeta->evictPos, rewindMeta->completePos, rewindMeta->restorePos, freeAddSpace);
	}

	rewindItem = &rewindAddBuffer[curAddPos % rewind_circular_buffer_size];
	Assert(rewindItem->tag == EMPTY_ITEM_TAG);

	if (subxid_only)
	{
		/* Fill subxids entry in a circular buffer. */
		SubxidsItem *subxidsItem = (SubxidsItem *) rewindItem;

		Assert (TransactionIdIsValid(xid));
		Assert (nsubxids > 0);
		subxidsItem->tag = SUBXIDS_ITEM_TAG;
		subxidsItem->oxid = oxid;
		subxidsItem->nsubxids = nsubxids;
		while (true)
		{
			elog(LOG, "Add SubXid to buffer: oxid %lu cur xid %u subxid [%u/%u] %u", oxid, xid, subxids_count + 1, nsubxids, subxids[subxids_count]);
			subxidsItem->subxids[subxids_count % SUBXIDS_PER_ITEM] = subxids[subxids_count];
			subxids_count++;
			if (subxids_count == nsubxids)
			{
				subxid_only = false;
				nsubxids = 0;
				return;
			}

			if (subxids_count % SUBXIDS_PER_ITEM == 0)
				goto next_subxids_item; /* Write next rewindItem only with subxids */
		}
	}
	else
	{
		/* Fill rewind entry in a circular buffer. */
		rewindItem->timestamp = GetCurrentTimestamp();
		rewindItem->tag = REWIND_ITEM_TAG;
		rewindItem->oxid = oxid;
		rewindItem->xid = xid;
		rewindItem->nsubxids = nsubxids;
		rewindItem->oldestConsideredRunningXid = GetOldestFullTransactionIdConsideredRunning();
		rewindItem->runXmin = pg_atomic_read_u64(&xid_meta->runXmin);
		Assert (OXidIsValid(oxid) || TransactionIdIsValid(xid));

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
				elog(LOG, "Add to buffer: oxid %lu xid %u logtype %d undoLoc %lu onCommitLoc %lu, minRetainLoc %lu, oldestRunningXid %u nsubxids %u", oxid, xid, i, rewindItem->undoLocation[i], rewindItem->onCommitUndoLocation[i], rewindItem->minRetainLocation[i], XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), nsubxids);
			}
		}
		else
		{
		elog(LOG, "Add to buffer: oxid %lu xid %u oldestRunningXid %u nsubxids %u", oxid, xid, XidFromFullTransactionId(rewindItem->oldestConsideredRunningXid), nsubxids);
		}

		if (nsubxids)
		{
			Assert(TransactionIdIsValid(xid));
			subxid_only = true;
			subxids_count = 0;
			goto next_subxids_item; /* Write first rewindItem only with subxids */
		}
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

	if (!enable_rewind)
		return;

	if (rewindMeta->addToRewindQueueDisabled)
	{
		elog(WARNING, "Adding to rewind queue to checkpoint is blocked by rewind");
		return;
	}

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
	for (; rewindMeta->checkpointPos < rewindMeta->evictPos; rewindMeta->checkpointPos += REWIND_DISK_BUFFER_LENGTH)
	{
		RewindItem	buffer[REWIND_DISK_BUFFER_LENGTH];

		o_buffers_read(&rewindBuffersDesc,
					   (Pointer) &buffer, REWIND_BUFFERS_TAG,
					   rewindMeta->checkpointPos * sizeof(RewindItem),
					   REWIND_DISK_BUFFER_LENGTH * sizeof(RewindItem));

		for (i = 0; i < REWIND_DISK_BUFFER_LENGTH; i++)
		{
			Assert(rewindMeta->checkpointPos + i < rewindMeta->evictPos);
			checkpoint_write_rewind_item(&buffer[i]);
		}
	}

	/*
	 * Write rewind records from in-memory rewind buffer (after evicted
	 * records if they exist)
	 */
	for (; rewindMeta->checkpointPos < pg_atomic_read_u64(&rewindMeta->addPos); rewindMeta->checkpointPos++)
		checkpoint_write_rewind_item(&rewindAddBuffer[rewindMeta->checkpointPos % rewind_circular_buffer_size]);
}

OXid
get_rewind_run_xmin(void)
{
	return pg_atomic_read_u64(&rewindMeta->runXmin);
}
