/*-------------------------------------------------------------------------
 *
 * undo.c
 *		Implementation of OrioleDB undo log.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/transam/undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "orioledb.h"

#include "btree/scan.h"
#include "btree/undo.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/o_buffers.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "utils/memutils.h"

#define GET_UNDO_REC(loc) (o_undo_buffers + (loc) % undo_circular_buffer_size)
#define UNDO_FILE_SIZE (0x4000000)

static int	undoLocCmp(const pairingheap_node *a, const pairingheap_node *b, void *arg);

static pairingheap retainUndoLocHeap = {&undoLocCmp, NULL, NULL};

typedef void (*UndoCallback) (UndoLocation location, UndoStackItem *item,
							  OXid oxid, bool abort,
							  bool changeCountsValid);

static void o_stub_item_callback(UndoLocation location, UndoStackItem *baseItem,
								 OXid oxid, bool abort,
								 bool changeCountsValid);

/*
 * Descriptor of undo item type.
 */
typedef struct
{
	UndoItemType type;
	bool		callOnCommit;	/* call the callback on commit */
	UndoCallback callback;		/* callback to be called on transaction finish */
} UndoItemTypeDescr;

UndoItemTypeDescr undoItemTypeDescrs[] = {
	{
		.type = ModifyUndoItemType,
		.callback = modify_undo_callback,
		.callOnCommit = false
	},
	{
		.type = RowLockUndoItemType,
		.callback = lock_undo_callback,
		.callOnCommit = false
	},
	{
		.type = RelnodeUndoItemType,
		.callback = btree_relnode_undo_callback,
		.callOnCommit = true
	},
	{
		.type = SysTreesLockUndoItemType,
		.callback = systrees_lock_callback,
		.callOnCommit = false
	},
	{
		.type = InvalidateUndoItemType,
		.callback = o_invalidate_undo_item_callback,
		.callOnCommit = true
	},
	{
		.type = BranchUndoItemType,
		.callback = o_stub_item_callback,
		.callOnCommit = false
	},
	{
		.type = SubXactUndoItemType,
		.callback = o_stub_item_callback,
		.callOnCommit = false
	}
};


PG_FUNCTION_INFO_V1(orioledb_has_retained_undo);

UndoMeta   *undo_meta = NULL;

UndoLocation curRetainUndoLocation = InvalidUndoLocation;
bool		oxid_needs_wal_flush = false;

static Size reserved_undo_size = 0;

static OBuffersDesc buffersDesc = {
	.singleFileSize = UNDO_FILE_SIZE,
	.filenameTemplate = ORIOLEDB_UNDO_DIR "/%02X%08X",
	.groupCtlTrancheName = "undoBuffersGroupCtlTranche",
	.bufferCtlTrancheName = "undoBuffersCtlTranche"
};

static bool wait_for_reserved_location(UndoLocation undoLocationToWait);

Size
undo_shmem_needs(void)
{
	Size		size;

	buffersDesc.buffersCount = undo_buffers_count;

	size = CACHELINEALIGN(sizeof(UndoMeta));
	size = add_size(size, undo_circular_buffer_size);
	size = add_size(size, o_buffers_shmem_needs(&buffersDesc));

	return size;
}

void
undo_shmem_init(Pointer buf, bool found)
{
	Pointer		ptr = buf;

	undo_meta = (UndoMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(UndoMeta));

	o_undo_buffers = ptr;
	ptr += undo_circular_buffer_size;

	o_buffers_shmem_init(&buffersDesc, ptr, found);

	if (!found)
	{
		SpinLockInit(&undo_meta->minUndoLocationsMutex);
		undo_meta->minUndoLocationsChangeCount = 0;
		undo_meta->undoWriteTrancheId = LWLockNewTrancheId();
		undo_meta->pendingTruncatesTrancheId = LWLockNewTrancheId();
		undo_meta->undoStackLocationsFlushLockTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&undo_meta->undoWriteLock,
						 undo_meta->undoWriteTrancheId);
		LWLockInitialize(&undo_meta->pendingTruncatesLock,
						 undo_meta->pendingTruncatesTrancheId);

		/* Undo locations are initialized in checkpoint_shmem_init() */
	}
	LWLockRegisterTranche(undo_meta->undoWriteTrancheId,
						  "OUndoWriteTranche");
	LWLockRegisterTranche(undo_meta->pendingTruncatesTrancheId,
						  "OPendingTruncatesTranche");
	LWLockRegisterTranche(undo_meta->undoStackLocationsFlushLockTrancheId,
						  "UndoStackPosFlushTranche");
}

static UndoItemTypeDescr *
item_type_get_descr(UndoItemType type)
{
	UndoItemTypeDescr *result;

	Assert((int) type >= 1 && (int) type <= sizeof(undoItemTypeDescrs) / sizeof(undoItemTypeDescrs[0]));

	result = &undoItemTypeDescrs[(int) type - 1];
	Assert(result->type == type);
	return result;
}

void
update_min_undo_locations(bool have_lock, bool do_cleanup)
{
	UndoLocation minReservedLocation,
				minRetainLocation,
				minTransactionRetainLocation,
				lastUsedLocation;
	UndoLocation oldCleanedLocation,
				oldCheckpointStartLocation,
				oldCheckpointEndLocation,
				newCheckpointStartLocation,
				newCheckpointEndLocation;
	int			i;

	Assert(!have_lock || !do_cleanup);

	if (!have_lock)
		SpinLockAcquire(&undo_meta->minUndoLocationsMutex);
	START_CRIT_SECTION();

	Assert((undo_meta->minUndoLocationsChangeCount & 1) == 0);

	undo_meta->minUndoLocationsChangeCount++;

	pg_write_barrier();

	lastUsedLocation = pg_atomic_read_u64(&undo_meta->lastUsedLocation);
	minTransactionRetainLocation = minRetainLocation = minReservedLocation = lastUsedLocation;

	for (i = 0; i < max_procs; i++)
	{
		UndoLocation tmp;

		tmp = pg_atomic_read_u64(&oProcData[i].reservedUndoLocation);
		minReservedLocation = Min(minReservedLocation, tmp);

		tmp = pg_atomic_read_u64(&oProcData[i].transactionUndoRetainLocation);
		minRetainLocation = Min(minRetainLocation, tmp);
		minTransactionRetainLocation = Min(minTransactionRetainLocation, tmp);
		tmp = pg_atomic_read_u64(&oProcData[i].snapshotRetainUndoLocation);
		minRetainLocation = Min(minRetainLocation, tmp);
	}

	/*
	 * Make sure none of calculated variables goes backwards.
	 */
	minReservedLocation = Max(pg_atomic_read_u64(&undo_meta->minProcReservedLocation),
							  minReservedLocation);
	minRetainLocation = Max(pg_atomic_read_u64(&undo_meta->minProcRetainLocation),
							minRetainLocation);
	minTransactionRetainLocation = Max(pg_atomic_read_u64(&undo_meta->minProcTransactionRetainLocation),
									   minTransactionRetainLocation);

	pg_atomic_write_u64(&undo_meta->minProcReservedLocation, minReservedLocation);
	pg_atomic_write_u64(&undo_meta->minProcRetainLocation, minRetainLocation);
	pg_atomic_write_u64(&undo_meta->minProcTransactionRetainLocation, minTransactionRetainLocation);
	pg_atomic_write_u64(&undo_meta->lastUsedUndoLocationWhenUpdatedMinLocation, lastUsedLocation);

	pg_write_barrier();

	undo_meta->minUndoLocationsChangeCount++;

	Assert((undo_meta->minUndoLocationsChangeCount & 1) == 0);

	if (!have_lock)
	{
		UndoLocation writeInProgressLocation,
					writtenLocation;

		writeInProgressLocation = pg_atomic_read_u64(&undo_meta->writeInProgressLocation);
		writtenLocation = pg_atomic_read_u64(&undo_meta->writtenLocation);
		if (writtenLocation == writeInProgressLocation && writtenLocation < minRetainLocation &&
			LWLockConditionalAcquire(&undo_meta->undoWriteLock, LW_EXCLUSIVE))
		{
			Assert(minRetainLocation >= pg_atomic_read_u64(&undo_meta->writeInProgressLocation));
			Assert(minRetainLocation >= pg_atomic_read_u64(&undo_meta->writtenLocation));
			pg_atomic_write_u64(&undo_meta->writeInProgressLocation, minRetainLocation);
			pg_atomic_write_u64(&undo_meta->writtenLocation, minRetainLocation);
			LWLockRelease(&undo_meta->undoWriteLock);
		}
	}

	if (do_cleanup)
	{
		oldCleanedLocation = pg_atomic_read_u64(&undo_meta->cleanedLocation);
		oldCheckpointStartLocation = pg_atomic_read_u64(&undo_meta->cleanedCheckpointStartLocation);
		oldCheckpointEndLocation = pg_atomic_read_u64(&undo_meta->cleanedCheckpointEndLocation);
		newCheckpointStartLocation = pg_atomic_read_u64(&undo_meta->checkpointRetainStartLocation);
		newCheckpointEndLocation = pg_atomic_read_u64(&undo_meta->checkpointRetainEndLocation);

		if (oldCleanedLocation != minRetainLocation ||
			oldCheckpointStartLocation != newCheckpointStartLocation ||
			oldCheckpointEndLocation != newCheckpointEndLocation)
		{
			pg_atomic_write_u64(&undo_meta->cleanedCheckpointStartLocation, newCheckpointStartLocation);
			pg_atomic_write_u64(&undo_meta->cleanedCheckpointEndLocation, newCheckpointEndLocation);
			pg_atomic_write_u64(&undo_meta->cleanedLocation, minRetainLocation);
		}
		else
		{
			do_cleanup = false;
		}
	}

	END_CRIT_SECTION();
	if (!have_lock)
		SpinLockRelease(&undo_meta->minUndoLocationsMutex);

	if (do_cleanup)
	{
		int64		oldCleanedNum = oldCleanedLocation / UNDO_FILE_SIZE,
					newCleanedNum = minRetainLocation / UNDO_FILE_SIZE,
					oldCheckpointStartNum = oldCheckpointStartLocation / UNDO_FILE_SIZE,
					oldCheckpointEndNum = oldCheckpointEndLocation / UNDO_FILE_SIZE,
					newCheckpointStartNum = newCheckpointStartLocation / UNDO_FILE_SIZE,
					newCheckpointEndNum = newCheckpointEndLocation / UNDO_FILE_SIZE;

		if (oldCheckpointEndLocation % UNDO_FILE_SIZE == 0)
			oldCheckpointEndNum--;
		if (newCheckpointEndLocation % UNDO_FILE_SIZE == 0)
			newCheckpointEndNum--;

		o_buffers_unlink_files_range(&buffersDesc,
									 oldCheckpointStartNum,
									 Min(oldCheckpointEndNum,
										 Min(newCheckpointStartNum - 1,
											 newCleanedNum - 1)));

		o_buffers_unlink_files_range(&buffersDesc,
									 Max(oldCheckpointStartNum,
										 newCheckpointEndNum + 1),
									 Min(oldCheckpointEndNum,
										 newCleanedNum - 1));

		o_buffers_unlink_files_range(&buffersDesc,
									 oldCleanedNum,
									 Min(newCheckpointStartNum - 1,
										 newCleanedNum - 1));

		o_buffers_unlink_files_range(&buffersDesc,
									 Max(oldCleanedNum,
										 newCheckpointEndNum + 1),
									 newCleanedNum - 1);
	}
}

/*
 * Guarantees that concurrent update_min_undo_locations() finishes.
 */
static void
wait_for_even_changecount(void)
{
	SpinDelayStatus status;

	init_local_spin_delay(&status);
	while (undo_meta->minUndoLocationsChangeCount & 1)
	{
		perform_spin_delay(&status);
		pg_read_barrier();
	}
	finish_spin_delay(&status);
}

static void
set_my_reserved_location(void)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoLocation lastUsedLocation;

	while (true)
	{
		lastUsedLocation = pg_atomic_read_u64(&undo_meta->lastUsedLocation);
		if (!UndoLocationIsValid(pg_atomic_read_u64(&curProcData->reservedUndoLocation)))
			pg_atomic_write_u64(&curProcData->reservedUndoLocation, lastUsedLocation);
		if (!UndoLocationIsValid(pg_atomic_read_u64(&curProcData->transactionUndoRetainLocation)))
			pg_atomic_write_u64(&curProcData->transactionUndoRetainLocation, lastUsedLocation);

		wait_for_even_changecount();

		/*
		 * Retry if minimal positions run higher due to concurrent
		 * update_min_undo_locations().
		 */
		if (pg_atomic_read_u64(&undo_meta->minProcReservedLocation) > lastUsedLocation)
			continue;
		if (pg_atomic_read_u64(&undo_meta->minProcTransactionRetainLocation) > lastUsedLocation)
			continue;

		break;
	}
	if (!UndoLocationIsValid(curRetainUndoLocation))
		curRetainUndoLocation = lastUsedLocation;
}

static UndoLocation
set_my_retain_location(void)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoLocation curSnapshotRetainUndoLocation,
				retainUndoLocation;

	while (true)
	{
		retainUndoLocation = pg_atomic_read_u64(&undo_meta->minProcTransactionRetainLocation);
		curSnapshotRetainUndoLocation = pg_atomic_read_u64(&curProcData->snapshotRetainUndoLocation);

		if (!UndoLocationIsValid(curSnapshotRetainUndoLocation) ||
			retainUndoLocation < curSnapshotRetainUndoLocation)
			pg_atomic_write_u64(&curProcData->snapshotRetainUndoLocation, retainUndoLocation);

		pg_memory_barrier();

		wait_for_even_changecount();

		/*
		 * Retry if minimal positions run higher due to concurrent
		 * update_min_undo_locations().
		 */
		if (pg_atomic_read_u64(&undo_meta->minProcRetainLocation) > retainUndoLocation)
			continue;

		break;
	}
	return retainUndoLocation;
}

static bool
wait_for_reserved_location(UndoLocation undoLocationToWait)
{
	SpinDelayStatus delay;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	bool		delay_inited;
	int			i;

	if (undoLocationToWait > pg_atomic_read_u64(&curProcData->reservedUndoLocation) + undo_circular_buffer_size)
	{
		return false;
	}

	for (i = 0; i < max_procs; i++)
	{
		delay_inited = false;
		while (undoLocationToWait > pg_atomic_read_u64(&oProcData[i].reservedUndoLocation) + undo_circular_buffer_size)
		{
			if (!delay_inited)
			{
				init_local_spin_delay(&delay);
				delay_inited = true;
			}
			else
			{
				perform_spin_delay(&delay);
			}
		}

		if (delay_inited)
			finish_spin_delay(&delay);
	}

	return true;
}

#define UNDO_ITEM_BUF_SIZE	2048

typedef struct
{
	char		staticData[UNDO_ITEM_BUF_SIZE];
	Pointer		data;
	Size		length;
} UndoItemBuf;

static void
init_undo_item_buf(UndoItemBuf *buf)
{
	buf->data = buf->staticData;
	buf->length = UNDO_ITEM_BUF_SIZE;
}

static UndoStackItem *
undo_item_buf_read_item(UndoItemBuf *buf, UndoLocation location)
{
	LocationIndex itemSize;

	undo_read(location, sizeof(UndoStackItem), buf->data);

	itemSize = ((UndoStackItem *) buf->data)->itemSize;
	if (itemSize > buf->length)
	{
		buf->length *= 2;
		if (buf->data == buf->staticData)
		{
			buf->data = palloc(buf->length);
			memcpy(buf->data, buf->staticData, sizeof(UndoStackItem));
		}
		else
		{
			buf->data = repalloc(buf->data, buf->length);
		}
	}

	Assert(itemSize >= sizeof(UndoStackItem));
	undo_read(location + sizeof(UndoStackItem),
			  itemSize - sizeof(UndoStackItem),
			  buf->data + sizeof(UndoStackItem));

	return (UndoStackItem *) buf->data;
}

static void
free_undo_item_buf(UndoItemBuf *buf)
{
	if (buf->data != buf->staticData)
		pfree(buf->data);
}

static UndoLocation
o_add_branch_undo_item(UndoLocation newLocation)
{
	UndoLocation location;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();
	BranchUndoStackItem *item;
	LocationIndex size;

	size = sizeof(BranchUndoStackItem);
	item = (BranchUndoStackItem *) get_undo_record_unreserved(UndoReserveTxn,
															  &location,
															  MAXALIGN(size));

	item->longPathLocation = pg_atomic_read_u64(&sharedLocations->location);
	item->prevBranchLocation = pg_atomic_read_u64(&sharedLocations->branchLocation);
	item->header.type = BranchUndoItemType;
	item->header.indexType = oIndexPrimary;
	item->header.itemSize = size;
	item->header.prev = newLocation;

	release_undo_size(UndoReserveTxn);

	return location;
}

/*
 * Walk through the undo stack calling the callbacks for each item.
 */
static UndoLocation
walk_undo_range(UndoLocation location, UndoLocation toLoc, UndoItemBuf *buf,
				OXid oxid, bool abort_val, UndoLocation *onCommitLocation,
				bool changeCountsValid)
{
	UndoStackItem *item;
	UndoItemTypeDescr *descr;

	while (UndoLocationIsValid(location) && (location > toLoc || !UndoLocationIsValid(toLoc)))
	{
		item = undo_item_buf_read_item(buf, location);
		descr = item_type_get_descr(item->type);
		descr->callback(location, item, oxid, abort_val, changeCountsValid);

		/*
		 * Update location of the last item, which needs an action on commit,
		 * if needed.
		 */
		if (onCommitLocation && *onCommitLocation == location)
		{
			OnCommitUndoStackItem *fItem = (OnCommitUndoStackItem *) item;

			*onCommitLocation = fItem->onCommitLocation;
		}

		/*
		 * On commit, we only walk through the specific items. On abort, we
		 * walk through all the items.
		 */
		if (!abort_val)
		{
			OnCommitUndoStackItem *fItem = (OnCommitUndoStackItem *) item;

			location = fItem->onCommitLocation;
		}
		else
		{
			location = item->prev;
		}
	}

	return location;
}

/*
 * Apply undo branches: parts of transaction undo chain, which should be already
 * aborted.  This is used during recovery: despite some parts of chain are
 * already aborted, checkpointed items could still reference them.
 */
void
apply_undo_branches(OXid oxid)
{
	UndoItemBuf buf;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();
	BranchUndoStackItem *item;
	UndoLocation location;

	init_undo_item_buf(&buf);

	location = pg_atomic_read_u64(&sharedLocations->branchLocation);
	while (UndoLocationIsValid(location))
	{
		item = (BranchUndoStackItem *) undo_item_buf_read_item(&buf, location);
		location = item->prevBranchLocation;
		walk_undo_range(item->longPathLocation, item->header.prev, &buf,
						oxid, true, NULL, false);
	}
	free_undo_item_buf(&buf);
}

/*
 * Walk transaction undo stack chain during (sub)transaction abort or
 * transaction commit.
 */
static void
walk_undo_stack(OXid oxid, UndoStackLocations *toLocation, bool abortTrx,
				bool changeCountsValid)
{
	UndoItemBuf buf;
	UndoLocation location,
				newOnCommitLocation;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();

	if (STOPEVENTS_ENABLED())
	{
		Jsonb	   *params;
		JsonbParseState *state = NULL;

		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		jsonb_push_bool_key(&state, "commit", !abortTrx);
		params = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));

		STOPEVENT(STOPEVENT_BEFORE_APPLY_UNDO, params);
	}

	init_undo_item_buf(&buf);

	if (!abortTrx)
	{
		/*
		 * One could only do the "on commit" action for the whole transaction
		 * chain.
		 */
		Assert(!toLocation);
		location = pg_atomic_read_u64(&sharedLocations->onCommitLocation);
		location = walk_undo_range(location, InvalidUndoLocation, &buf,
								   oxid, false, NULL, changeCountsValid);
		Assert(!UndoLocationIsValid(location));
		newOnCommitLocation = InvalidUndoLocation;
	}
	else
	{
		/*
		 * Abort can relate to part of transactio chain.  "On commit" location
		 * needs to be updated accordingly.
		 */
		location = pg_atomic_read_u64(&sharedLocations->location);
		newOnCommitLocation = pg_atomic_read_u64(&sharedLocations->onCommitLocation);
		location = walk_undo_range(location,
								   toLocation ? toLocation->location : InvalidUndoLocation,
								   &buf, oxid, true, &newOnCommitLocation,
								   changeCountsValid);
	}

	/*
	 * Create special branch item, which allows finding aborted items.
	 */
	if (toLocation)
		location = o_add_branch_undo_item(location);

	LWLockAcquire(&curProcData->undoStackLocationsFlushLock, LW_EXCLUSIVE);
	if (toLocation)
		pg_atomic_write_u64(&sharedLocations->branchLocation, location);

	/*
	 * Flush undo location to checkpoint if concurrent checkpointing requires
	 * that.
	 */
	if (!toLocation && curProcData->flushUndoLocations)
	{
		XidFileRec	rec;

		rec.oxid = oxid;
		read_shared_undo_locations(&rec.undoLocation, sharedLocations);
		write_to_xids_queue(&rec);
	}

	pg_atomic_write_u64(&sharedLocations->location, location);
	pg_atomic_write_u64(&sharedLocations->onCommitLocation, newOnCommitLocation);
	LWLockRelease(&curProcData->undoStackLocationsFlushLock);

	free_undo_item_buf(&buf);
}

void
apply_undo_stack(OXid oxid, UndoStackLocations *toLocation,
				 bool changeCountsValid)
{
	walk_undo_stack(oxid, toLocation, true, changeCountsValid);
}

void
on_commit_undo_stack(OXid oxid, bool changeCountsValid)
{
	walk_undo_stack(oxid, NULL, false, changeCountsValid);
}

bool
have_retained_undo_location(void)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	return UndoLocationIsValid(pg_atomic_read_u64(&curProcData->transactionUndoRetainLocation));
}

UndoLocation
get_snapshot_retained_undo_location(void)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	return pg_atomic_read_u64(&curProcData->snapshotRetainUndoLocation);
}

void
free_retained_undo_location(void)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	Assert(reserved_undo_size == 0);
	Assert(pg_atomic_read_u64(&curProcData->reservedUndoLocation) == InvalidUndoLocation);
	pg_atomic_write_u64(&curProcData->transactionUndoRetainLocation, InvalidUndoLocation);
	curRetainUndoLocation = InvalidUndoLocation;

}
static bool
check_reserved_undo_location(UndoLocation location,
							 uint64 *minProcReservedLocation,
							 bool waitForUndoLocation)
{
	*minProcReservedLocation = pg_atomic_read_u64(&undo_meta->minProcReservedLocation);
	while (location > *minProcReservedLocation + undo_circular_buffer_size)
	{
		bool		failed = true;

		if (waitForUndoLocation)
			failed = !wait_for_reserved_location(location);

		if (failed)
			return false;

		*minProcReservedLocation = pg_atomic_read_u64(&undo_meta->minProcReservedLocation);
		if (location <= *minProcReservedLocation + undo_circular_buffer_size)
			return true;

		update_min_undo_locations(false, waitForUndoLocation);
		*minProcReservedLocation = pg_atomic_read_u64(&undo_meta->minProcReservedLocation);
	}

	return true;
}

static void
write_undo_range(Pointer buf, UndoLocation minLoc, UndoLocation maxLoc)
{
	if (maxLoc > minLoc)
		o_buffers_write(&buffersDesc, buf, minLoc, maxLoc - minLoc);
}

static void
read_undo_range(Pointer buf, UndoLocation minLoc, UndoLocation maxLoc)
{
	Assert(maxLoc > minLoc);
	o_buffers_read(&buffersDesc, buf, minLoc, maxLoc - minLoc);
}

void
write_undo(UndoLocation targetUndoLocation,
		   UndoLocation minProcReservedLocation,
		   bool attempt)
{
	UndoLocation retainUndoLocation,
				writtenLocation;

	Assert(targetUndoLocation <= minProcReservedLocation);

	if (attempt)
	{
		if (!LWLockConditionalAcquire(&undo_meta->undoWriteLock, LW_EXCLUSIVE))
			return;
	}
	else
	{
		LWLockAcquire(&undo_meta->undoWriteLock, LW_EXCLUSIVE);
	}

	SpinLockAcquire(&undo_meta->minUndoLocationsMutex);

	update_min_undo_locations(true, false);

	retainUndoLocation = pg_atomic_read_u64(&undo_meta->minProcRetainLocation);

	if (targetUndoLocation <= retainUndoLocation ||
		targetUndoLocation <= pg_atomic_read_u64(&undo_meta->writtenLocation))
	{
		/* We don't have to really write undo. */
		if (pg_atomic_read_u64(&undo_meta->writeInProgressLocation) < retainUndoLocation)
		{
			pg_atomic_write_u64(&undo_meta->writeInProgressLocation, retainUndoLocation);
			pg_atomic_write_u64(&undo_meta->writtenLocation, retainUndoLocation);
		}
		SpinLockRelease(&undo_meta->minUndoLocationsMutex);
		LWLockRelease(&undo_meta->undoWriteLock);
		return;
	}

	/* Try to write 5% of the whole undo size if possible */
	writtenLocation = pg_atomic_read_u64(&undo_meta->writtenLocation);
	retainUndoLocation = Max(retainUndoLocation, writtenLocation);
	targetUndoLocation = Max(targetUndoLocation, writtenLocation + undo_circular_buffer_size / 20);
	targetUndoLocation = Min(targetUndoLocation, minProcReservedLocation);

	Assert(targetUndoLocation >= pg_atomic_read_u64(&undo_meta->writeInProgressLocation));
	pg_atomic_write_u64(&undo_meta->writeInProgressLocation, targetUndoLocation);

	SpinLockRelease(&undo_meta->minUndoLocationsMutex);

	minProcReservedLocation = pg_atomic_read_u64(&undo_meta->minProcReservedLocation);
	if (minProcReservedLocation < targetUndoLocation)
		(void) wait_for_reserved_location(targetUndoLocation + undo_circular_buffer_size);


	if (retainUndoLocation % undo_circular_buffer_size <
		targetUndoLocation % undo_circular_buffer_size)
	{
		write_undo_range(o_undo_buffers + retainUndoLocation % undo_circular_buffer_size,
						 retainUndoLocation, targetUndoLocation);
	}
	else
	{
		UndoLocation breakUndoLocation;

		breakUndoLocation = retainUndoLocation + (undo_circular_buffer_size -
												  (retainUndoLocation % undo_circular_buffer_size));
		write_undo_range(o_undo_buffers + retainUndoLocation % undo_circular_buffer_size,
						 retainUndoLocation, breakUndoLocation);
		write_undo_range(o_undo_buffers, breakUndoLocation, targetUndoLocation);
	}

	SpinLockAcquire(&undo_meta->minUndoLocationsMutex);
	Assert(targetUndoLocation >= pg_atomic_read_u64(&undo_meta->writtenLocation));
	pg_atomic_write_u64(&undo_meta->writtenLocation, targetUndoLocation);
	SpinLockRelease(&undo_meta->minUndoLocationsMutex);

	LWLockRelease(&undo_meta->undoWriteLock);
}

bool
reserve_undo_size_extended(UndoReserveType type, Size size,
						   bool waitForUndoLocation, bool reportError)
{
	UndoLocation location;
	uint64		minProcReservedLocation;

	Assert(!waitForUndoLocation || !have_locked_pages());
	Assert(type == UndoReserveTxn);
	Assert(size > 0);

	if (reserved_undo_size >= size)
		return true;

	size -= reserved_undo_size;

	location = pg_atomic_fetch_add_u64(&undo_meta->advanceReservedLocation, size);
	reserved_undo_size += size;

	if (location + size <=
		pg_atomic_read_u64(&undo_meta->writtenLocation) + undo_circular_buffer_size)
		return true;

	update_min_undo_locations(false, waitForUndoLocation);

	if (!check_reserved_undo_location(location + size, &minProcReservedLocation,
									  waitForUndoLocation))
	{
		/*
		 * we add size to reserver_undo_size and
		 * undo_meta->advanceReservedLocation and must revert this action
		 */
		pg_atomic_fetch_sub_u64(&undo_meta->advanceReservedLocation, size);
		reserved_undo_size -= size;
		if (reportError)
			report_undo_overflow();
		else
			return false;
	}

	/* Recheck if the required location was already written */
	if (location + size <=
		pg_atomic_read_u64(&undo_meta->writtenLocation) + undo_circular_buffer_size)
		return true;

	if (!waitForUndoLocation)
	{
		/*
		 * No more chances to succeed without waiting.
		 */
		pg_atomic_fetch_sub_u64(&undo_meta->advanceReservedLocation, size);
		reserved_undo_size -= size;
		if (reportError)
			report_undo_overflow();
		else
			return false;
	}

	if (location + size <=
		pg_atomic_read_u64(&undo_meta->writeInProgressLocation) + undo_circular_buffer_size)
	{
		/*
		 * Current in-progress undo write should cover our required location.
		 * It should be enough to just wait for current in-progress write to
		 * be finished.
		 */
		LWLockAcquire(&undo_meta->undoWriteLock, LW_SHARED);
		LWLockRelease(&undo_meta->undoWriteLock);

		SpinLockAcquire(&undo_meta->minUndoLocationsMutex);
		Assert(location + size <= pg_atomic_read_u64(&undo_meta->writtenLocation) + undo_circular_buffer_size);
		SpinLockRelease(&undo_meta->minUndoLocationsMutex);
		return true;
	}

	write_undo(location + size - undo_circular_buffer_size, minProcReservedLocation, false);
	Assert(location + size <= pg_atomic_read_u64(&undo_meta->writtenLocation) + undo_circular_buffer_size);

	return true;
}

void
fsync_undo_range(UndoLocation fromLoc, UndoLocation toLoc, uint32 wait_event_info)
{
	UndoLocation minProcReservedLocation;

	(void) check_reserved_undo_location(toLoc + undo_circular_buffer_size,
										&minProcReservedLocation,
										true);

	if (toLoc <= pg_atomic_read_u64(&undo_meta->writeInProgressLocation))
	{
		/*
		 * Current in-progress undo write should cover our required location.
		 * It should be enough to just wait for current in-progress write to
		 * be finished.
		 */
		LWLockAcquire(&undo_meta->undoWriteLock, LW_SHARED);
		LWLockRelease(&undo_meta->undoWriteLock);

		SpinLockAcquire(&undo_meta->minUndoLocationsMutex);
		Assert(toLoc <= pg_atomic_read_u64(&undo_meta->writtenLocation));
		SpinLockRelease(&undo_meta->minUndoLocationsMutex);
	}
	else
	{
		write_undo(toLoc, minProcReservedLocation, false);
	}

	o_buffers_sync(&buffersDesc, fromLoc, toLoc, wait_event_info);
}

Pointer
get_undo_record(UndoReserveType type, UndoLocation *undoLocation, Size size)
{
	Assert(size == MAXALIGN(size));
	Assert(type == UndoReserveTxn);

	set_my_reserved_location();

	pg_write_barrier();

	while (true)
	{
		UndoLocation location;

		Assert(reserved_undo_size >= size);

		location = pg_atomic_fetch_add_u64(&undo_meta->lastUsedLocation, size);
		reserved_undo_size -= size;

		/*
		 * We might hit the boundary of circular buffer.  If so then just
		 * retry. Thankfully we've reserved twice more space than required.
		 *
		 * This situation shouldn't happen twice, since we've reserved undo
		 * location.
		 */
		if ((location + size) % undo_circular_buffer_size >
			location % undo_circular_buffer_size)
		{
			*undoLocation = location;
			return GET_UNDO_REC(location);
		}
	}
}

Pointer
get_undo_record_unreserved(UndoReserveType type, UndoLocation *undoLocation, Size size)
{
	Assert(size == MAXALIGN(size));
	Assert(reserved_undo_size == 0);

	reserve_undo_size(type, 2 * size);
	return get_undo_record(type, undoLocation, size);
}

void
release_undo_size(UndoReserveType type)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	Assert(type == UndoReserveTxn);

	if (reserved_undo_size != 0)
	{
		pg_atomic_fetch_sub_u64(&undo_meta->advanceReservedLocation, reserved_undo_size);
		reserved_undo_size = 0;
	}
	pg_atomic_write_u64(&curProcData->reservedUndoLocation, InvalidUndoLocation);
}

Size
get_reserved_undo_size(UndoReserveType type)
{
	Assert(type == UndoReserveTxn);

	return reserved_undo_size;
}

void
add_new_undo_stack_item(UndoLocation location)
{
	UndoStackItem *item = (UndoStackItem *) GET_UNDO_REC(location);
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();
	UndoItemTypeDescr *descr = item_type_get_descr(item->type);

	item->prev = pg_atomic_read_u64(&sharedLocations->location);
	pg_atomic_write_u64(&sharedLocations->location, location);

	if (descr->callOnCommit)
	{
		OnCommitUndoStackItem *fItem = (OnCommitUndoStackItem *) item;

		fItem->onCommitLocation = pg_atomic_read_u64(&sharedLocations->onCommitLocation);
		pg_atomic_write_u64(&sharedLocations->onCommitLocation, location);
	}
}

UndoLocation
get_subxact_undo_location(void)
{
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();

	return pg_atomic_read_u64(&sharedLocations->subxactLocation);
}

void
read_shared_undo_locations(UndoStackLocations *to, UndoStackSharedLocations *from)
{
	to->location = pg_atomic_read_u64(&from->location);
	to->branchLocation = pg_atomic_read_u64(&from->branchLocation);
	to->subxactLocation = pg_atomic_read_u64(&from->subxactLocation);
	to->onCommitLocation = pg_atomic_read_u64(&from->onCommitLocation);
}

void
write_shared_undo_locations(UndoStackSharedLocations *to, UndoStackLocations *from)
{
	pg_atomic_write_u64(&to->location, from->location);
	pg_atomic_write_u64(&to->branchLocation, from->branchLocation);
	pg_atomic_write_u64(&to->subxactLocation, from->subxactLocation);
	pg_atomic_write_u64(&to->onCommitLocation, from->onCommitLocation);
}

UndoStackLocations
get_cur_undo_locations(void)
{
	UndoStackLocations location;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();

	read_shared_undo_locations(&location, sharedLocations);

	return location;
}

void
set_cur_undo_locations(UndoStackLocations location)
{
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();

	write_shared_undo_locations(sharedLocations, &location);
}

void
reset_cur_undo_locations(void)
{
	UndoStackLocations location = {InvalidUndoLocation, InvalidUndoLocation, InvalidUndoLocation, InvalidUndoLocation};

	set_cur_undo_locations(location);
}

void
orioledb_reset_xmin_hook(void)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	RetainUndoLocationPHNode *location;

	if (ActiveSnapshotSet())
		return;

	if (pairingheap_is_empty(&retainUndoLocHeap))
	{
		pg_atomic_write_u64(&curProcData->snapshotRetainUndoLocation, InvalidUndoLocation);
		pg_atomic_write_u64(&curProcData->xmin, InvalidOXid);
	}
	else
	{
		location = pairingheap_container(RetainUndoLocationPHNode, ph_node,
										 pairingheap_first(&retainUndoLocHeap));
		if (location->undoLocation > pg_atomic_read_u64(&curProcData->snapshotRetainUndoLocation))
		{
			pg_atomic_write_u64(&curProcData->snapshotRetainUndoLocation, location->undoLocation);
			pg_atomic_write_u64(&curProcData->xmin, location->xmin);
		}
	}
}

void
undo_xact_callback(XactEvent event, void *arg)
{
	OXid		oxid = get_current_oxid_if_any();
	CommitSeqNo csn;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	bool		isParallelWorker;

	isParallelWorker = (MyProc->lockGroupLeader != NULL &&
						MyProc->lockGroupLeader != MyProc) ||
		IsInParallelMode();

	/*
	 * Cleanup EXPLAY ANALYZE counters pointer to handle case when execution
	 * of node was interrupted by exception.
	 */
	ea_counters = NULL;

	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
		seq_scans_cleanup();

	if (!OXidIsValid(oxid) || isParallelWorker)
	{
		if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
		{
			reset_cur_undo_locations();
			orioledb_reset_xmin_hook();
			oxid_needs_wal_flush = false;
		}
	}
	else
	{
		switch (event)
		{
			case XACT_EVENT_PRE_COMMIT:
				if (!RecoveryInProgress())
				{
					TransactionId xid = GetTopTransactionIdIfAny();

					if (TransactionIdIsValid(xid))
						wal_joint_commit(oxid, xid);
				}
				break;
			case XACT_EVENT_COMMIT:
				if (!RecoveryInProgress())
				{
					TransactionId xid = GetTopTransactionIdIfAny();

					if (!TransactionIdIsValid(xid))
						wal_commit(oxid);
				}

				current_oxid_precommit();
				csn = GetCurrentCSN();
				if (csn == COMMITSEQNO_INPROGRESS)
					csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
				current_oxid_commit(csn);

				on_commit_undo_stack(oxid, true);
				wal_after_commit();
				reset_cur_undo_locations();
				oxid_needs_wal_flush = false;
				break;
			case XACT_EVENT_ABORT:
				if (!RecoveryInProgress())
					wal_rollback(oxid);
				apply_undo_stack(oxid, NULL, true);
				reset_cur_undo_locations();
				current_oxid_abort();
				oxid_needs_wal_flush = false;

				/*
				 * Remove registered snapshot one-by-one, so that we can evade
				 * double removing in undo_snapshot_deregister_hook().
				 */
				while (!pairingheap_is_empty(&retainUndoLocHeap))
					pairingheap_remove_first(&retainUndoLocHeap);

				pg_atomic_write_u64(&curProcData->snapshotRetainUndoLocation, InvalidUndoLocation);
				break;
			default:
				break;
		}
	}

	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
	{
		int			i;

		release_undo_size(UndoReserveTxn);

		for (i = 0; i < OPagePoolTypesCount; i++)
		{
			OPagePool  *pool = get_ppool((OPagePoolType) i);

			ppool_release_reserved(pool, PPOOL_RESERVE_MASK_ALL);
		}

		free_retained_undo_location();
		saved_undo_location = InvalidUndoLocation;
	}

	if (event == XACT_EVENT_COMMIT && isParallelWorker)
		parallel_worker_set_oxid();
}

void
add_subxact_undo_item(SubTransactionId parentSubid)
{
	SubXactUndoStackItem *item;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();
	UndoLocation location;
	Size		size;

	size = sizeof(SubXactUndoStackItem);

	item = (SubXactUndoStackItem *) get_undo_record_unreserved(UndoReserveTxn,
															   &location,
															   MAXALIGN(size));
	item->prevSubLocation = pg_atomic_read_u64(&sharedLocations->subxactLocation);
	item->parentSubid = parentSubid;
	item->header.type = SubXactUndoItemType;
	item->header.indexType = oIndexPrimary;
	item->header.itemSize = size;
	add_new_undo_stack_item(location);
	release_undo_size(UndoReserveTxn);
	pg_atomic_write_u64(&sharedLocations->subxactLocation, location);
}

static bool
search_for_undo_sub_location(UndoStackKind kind, UndoLocation location,
							 UndoItemBuf *buf, SubTransactionId parentSubid,
							 UndoLocation *toLoc, UndoLocation *toSubLoc)
{
	SubXactUndoStackItem *item;

	if (!UndoLocationIsValid(location))
	{
		if (kind == UndoStackFull)
		{
			elog(FATAL, "subxact goes out of order");
		}
		else if (kind == UndoStackTail)
		{
			*toLoc = InvalidUndoLocation;
			*toSubLoc = InvalidUndoLocation;
			return true;
		}
		else
		{
			return false;
		}
	}

	while (true)
	{
		item = (SubXactUndoStackItem *) undo_item_buf_read_item(buf, location);
		if (item->parentSubid != parentSubid)
		{
			if (kind == UndoStackFull)
			{
				elog(FATAL, "subxact goes out of order");
			}
			else if (kind == UndoStackTail)
			{
				*toLoc = InvalidUndoLocation;
				*toSubLoc = InvalidUndoLocation;
				return true;
			}
			else if (kind == UndoStackHead)
			{
				if (item->parentSubid > parentSubid)
				{
					location = item->prevSubLocation;
					continue;
				}
				else
				{
					return false;
				}
			}
		}
		*toLoc = location;
		*toSubLoc = item->prevSubLocation;
		return true;
	}
}

static void
update_subxact_undo_location(UndoLocation subxactLocation)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();

	LWLockAcquire(&curProcData->undoStackLocationsFlushLock, LW_EXCLUSIVE);

	pg_atomic_write_u64(&sharedLocations->subxactLocation, subxactLocation);

	LWLockRelease(&curProcData->undoStackLocationsFlushLock);
}

void
rollback_to_savepoint(UndoStackKind kind, SubTransactionId parentSubid,
					  bool changeCountsValid)
{
	UndoStackLocations toLoc;
	UndoLocation location;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();
	UndoItemBuf buf;
	OXid		oxid;
	bool		applyResult;

	init_undo_item_buf(&buf);
	location = pg_atomic_read_u64(&sharedLocations->subxactLocation);
	applyResult = search_for_undo_sub_location(kind, location, &buf, parentSubid,
											   &toLoc.location, &toLoc.subxactLocation);
	free_undo_item_buf(&buf);

	if (!applyResult)
		return;

	oxid = get_current_oxid_if_any();
	if (OXidIsValid(oxid))
		walk_undo_stack(oxid, &toLoc, true, changeCountsValid);
	update_subxact_undo_location(toLoc.subxactLocation);
}

static void
update_subxact_undo_location_on_commit(SubTransactionId parentSubid)
{
	UndoStackLocations toLoc;
	UndoLocation location;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS();
	UndoItemBuf buf;

	init_undo_item_buf(&buf);
	location = pg_atomic_read_u64(&sharedLocations->subxactLocation);
	search_for_undo_sub_location(UndoStackFull, location, &buf, parentSubid,
								 &toLoc.location, &toLoc.subxactLocation);
	free_undo_item_buf(&buf);
	update_subxact_undo_location(toLoc.subxactLocation);
}

void
undo_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
					  SubTransactionId parentSubid, void *arg)
{
	/*
	 * Cleanup EXPLAY ANALYZE counters pointer to handle case when execution
	 * of node was interrupted by exception.
	 */
	ea_counters = NULL;

	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:
			(void) get_current_oxid();
			add_subxact_undo_item(parentSubid);
			add_savepoint_wal_record(parentSubid);
			break;
		case SUBXACT_EVENT_COMMIT_SUB:
			update_subxact_undo_location_on_commit(parentSubid);
			saved_undo_location = InvalidUndoLocation;
			break;
		case SUBXACT_EVENT_ABORT_SUB:
			rollback_to_savepoint(UndoStackFull, parentSubid, true);
			add_rollback_to_savepoint_wal_record(parentSubid);
			saved_undo_location = InvalidUndoLocation;

			/*
			 * It might happen that we've released some row-level locks.  Some
			 * waiters must be woken up.  We currently can't distinguish them
			 * and just wake up everybody.
			 */
			oxid_notify_all();
			break;
		default:
			break;
	}
}

bool
have_current_undo(void)
{
	UndoStackLocations location = get_cur_undo_locations();

	return (!UndoLocationIsValid(location.location));
}

void
report_undo_overflow(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_SNAPSHOT_TOO_OLD),
			 errmsg("failed to add an undo record: undo size is exceeded")));
}

Datum
orioledb_has_retained_undo(PG_FUNCTION_ARGS)
{
	UndoLocation location;
	bool		result = false;
	int			i;

	for (i = 0; i < max_procs; i++)
	{
		location = pg_atomic_read_u64(&oProcData[i].transactionUndoRetainLocation);
		if (UndoLocationIsValid(location))
		{
			result = true;
			break;
		}
	}

	PG_RETURN_BOOL(result);
}

void
start_autonomous_transaction(OAutonomousTxState *state)
{
	Assert(!is_recovery_process());
	state->needs_wal_flush = oxid_needs_wal_flush;
	state->oxid = get_current_oxid();
	state->has_retained_undo_location = have_retained_undo_location();

	if (!local_wal_is_empty())
		flush_local_wal(false);

	oxid_needs_wal_flush = false;
	reset_current_oxid();
	GET_CUR_PROCDATA()->autonomousNestingLevel++;
}

void
abort_autonomous_transaction(OAutonomousTxState *state)
{
	OXid		oxid = get_current_oxid_if_any();

	if (OXidIsValid(oxid))
	{
		wal_rollback(oxid);
		current_oxid_abort();
		apply_undo_stack(oxid, NULL, true);

		release_undo_size(UndoReserveTxn);
		if (!state->has_retained_undo_location)
			free_retained_undo_location();
	}

	oxid_needs_wal_flush = state->needs_wal_flush;
	GET_CUR_PROCDATA()->autonomousNestingLevel--;
	set_current_oxid(state->oxid);
}

void
finish_autonomous_transaction(OAutonomousTxState *state)
{
	OXid		oxid = get_current_oxid_if_any();

	if (OXidIsValid(oxid))
	{
		CommitSeqNo csn;

		wal_commit(oxid);

		current_oxid_precommit();
		csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
		current_oxid_commit(csn);

		on_commit_undo_stack(oxid, true);
		wal_after_commit();

		release_undo_size(UndoReserveTxn);
		if (!state->has_retained_undo_location)
			free_retained_undo_location();
	}

	oxid_needs_wal_flush = state->needs_wal_flush;
	GET_CUR_PROCDATA()->autonomousNestingLevel--;
	set_current_oxid(state->oxid);
}

void
undo_read(UndoLocation location, Size size, Pointer buf)
{
	UndoLocation writtenLocation;

	writtenLocation = pg_atomic_read_u64(&undo_meta->writtenLocation);

	if (location + size > writtenLocation)
	{
		UndoLocation maxLoc,
					minLoc;

		pg_read_barrier();

		maxLoc = location + size;
		minLoc = Max(writtenLocation, location);
		memcpy(buf + (minLoc - location), GET_UNDO_REC(minLoc), maxLoc - minLoc);

		pg_read_barrier();

		writtenLocation = pg_atomic_read_u64(&undo_meta->writtenLocation);
		if (writtenLocation > location)
			read_undo_range(buf, location, Min(location + size, writtenLocation));
	}
	else
	{
		read_undo_range(buf, location, location + size);
	}
}

/*
 * Write buffer to the given undo location.
 */
void
undo_write(UndoLocation location, Size size, Pointer buf)
{
	UndoLocation writeInProgressLocation,
				prevReservedUndoLocation,
				memoryUndoLocation;
	bool		undoLocationIsReserved = false;
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	while (true)
	{
		writeInProgressLocation = pg_atomic_read_u64(&undo_meta->writeInProgressLocation);
		if (writeInProgressLocation >= location + size)
		{
			/* Nothing we can write to the memory */
			memoryUndoLocation = location + size;
			break;
		}

		/* Reserve the location we're going to write into */
		memoryUndoLocation = Max(location, writeInProgressLocation);
		prevReservedUndoLocation = pg_atomic_read_u64(&curProcData->reservedUndoLocation);
		if (!UndoLocationIsValid(prevReservedUndoLocation) || prevReservedUndoLocation > memoryUndoLocation)
		{
			pg_atomic_write_u64(&curProcData->reservedUndoLocation, memoryUndoLocation);
			undoLocationIsReserved = true;
		}

		pg_memory_barrier();

		/* Recheck is writeInProgressLocation was advanced concurrently */
		writeInProgressLocation = pg_atomic_read_u64(&undo_meta->writeInProgressLocation);
		if (writeInProgressLocation > memoryUndoLocation)
		{
			if (undoLocationIsReserved)
			{
				pg_atomic_write_u64(&curProcData->reservedUndoLocation, prevReservedUndoLocation);
				undoLocationIsReserved = false;
			}
			continue;
		}

		/*
		 * At this point we should either detect concurrent writing of undo
		 * log. Or concurrent writing of undo log should wait for our reserved
		 * location. So, it should be safe to write to the memory.
		 */

		memcpy(GET_UNDO_REC(memoryUndoLocation),
			   buf + (memoryUndoLocation - location),
			   size - (memoryUndoLocation - location));
		break;
	}

	if (undoLocationIsReserved)
	{
		pg_atomic_write_u64(&curProcData->reservedUndoLocation, prevReservedUndoLocation);
		undoLocationIsReserved = false;
	}

	if (memoryUndoLocation == location)
	{
		/* Everything is written to the in-memory buffer */
		return;
	}

	/* Wait for in-progress write if needed */
	if (pg_atomic_read_u64(&undo_meta->writtenLocation) < memoryUndoLocation)
	{
		LWLockAcquire(&undo_meta->undoWriteLock, LW_SHARED);
		LWLockRelease(&undo_meta->undoWriteLock);
		Assert(pg_atomic_read_u64(&undo_meta->writtenLocation) >= memoryUndoLocation);
	}

	/* Finally perform writing to the file */
	write_undo_range(buf, location, memoryUndoLocation);
}

/*
 * Comparison function for retainUndoLocHeap.  Smallest undo location at the
 * top.
 */
static int
undoLocCmp(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	const RetainUndoLocationPHNode *aloc = pairingheap_const_container(RetainUndoLocationPHNode, ph_node, a);
	const RetainUndoLocationPHNode *bloc = pairingheap_const_container(RetainUndoLocationPHNode, ph_node, b);

	if (aloc->undoLocation < bloc->undoLocation)
		return 1;
	else if (aloc->undoLocation > bloc->undoLocation)
		return -1;
	else
		return 0;
}

void
undo_snapshot_register_hook(Snapshot snapshot)
{
	pairingheap_add(&retainUndoLocHeap, &snapshot->undoLocationPhNode.ph_node);
}

void
undo_snapshot_deregister_hook(Snapshot snapshot)
{
	/*
	 * Skip if it was already removed during transaction abort.
	 */
	if (snapshot->undoLocationPhNode.ph_node.prev_or_parent == NULL &&
		&snapshot->undoLocationPhNode.ph_node != retainUndoLocHeap.ph_root)
		return;

	pairingheap_remove(&retainUndoLocHeap, &snapshot->undoLocationPhNode.ph_node);
}

void
orioledb_snapshot_hook(Snapshot snapshot)
{
	UndoLocation lastUsedLocation,
				lastUsedUndoLocationWhenUpdatedMinLocation;
	OXid		curXmin;
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	lastUsedLocation = pg_atomic_read_u64(&undo_meta->lastUsedLocation);
	lastUsedUndoLocationWhenUpdatedMinLocation = pg_atomic_read_u64(&undo_meta->lastUsedUndoLocationWhenUpdatedMinLocation);
	if (lastUsedLocation - lastUsedUndoLocationWhenUpdatedMinLocation > undo_circular_buffer_size / 10)
		update_min_undo_locations(false, true);


	snapshot->undoLocationPhNode.undoLocation = set_my_retain_location();
	snapshot->undoLocationPhNode.xmin = pg_atomic_read_u64(&xid_meta->runXmin);
	curXmin = pg_atomic_read_u64(&curProcData->xmin);
	if (!OXidIsValid(curXmin))
		pg_atomic_write_u64(&curProcData->xmin, snapshot->undoLocationPhNode.xmin);

	/*
	 * Snapshot CSN could be newer than retained location, not older.  Enforce
	 * this with barrier.
	 */
	pg_read_barrier();

	snapshot->snapshotcsn = pg_atomic_read_u64(&ShmemVariableCache->nextCommitSeqNo);
}

static void
o_stub_item_callback(UndoLocation location, UndoStackItem *baseItem, OXid oxid,
					 bool abort, bool changeCountsValid)
{
	Assert(abort);
	return;
}
