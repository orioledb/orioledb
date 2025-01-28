/*-------------------------------------------------------------------------
 *
 * undo.c
 *		Implementation of OrioleDB undo log.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
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

#define GET_UNDO_REC(undoType, loc) (o_undo_buffers[(int) (undoType)] + \
	(loc) % o_undo_circular_sizes[(int) (undoType)])

static int	undoLocCmp(const pairingheap_node *a, const pairingheap_node *b, void *arg);

static pairingheap retainUndoLocRegularHeap = {&undoLocCmp, NULL, NULL};
static pairingheap retainUndoLocSystemHeap = {&undoLocCmp, NULL, NULL};

typedef void (*UndoCallback) (UndoLogType undoType, UndoLocation location,
							  UndoStackItem *item, OXid oxid, bool abort,
							  bool changeCountsValid);

static void init_undo_meta(UndoMeta *meta, bool found);
static void o_stub_item_callback(UndoLogType undoType, UndoLocation location,
								 UndoStackItem *baseItem,
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

static UndoMeta *undo_metas = NULL;
static Pointer o_undo_buffers[(int) UndoLogsCount] =
{
	NULL
};
static Size o_undo_circular_sizes[(int) UndoLogsCount] =
{
	0
};
PendingTruncatesMeta *pending_truncates_meta;

UndoLocation curRetainUndoLocations[(int) UndoLogsCount] =
{
	InvalidUndoLocation
};
bool		oxid_needs_wal_flush = false;

static Size reserved_undo_size = 0;

static OBuffersDesc undoBuffersDescs[(int) UndoLogsCount] =
{
	{
		.singleFileSize = UNDO_FILE_SIZE,
			.filenameTemplate = ORIOLEDB_UNDO_DATA_FILENAME_TEMPLATE,
			.groupCtlTrancheName = "undoRegularBuffersGroupCtlTranche",
			.bufferCtlTrancheName = "undoRegularBuffersCtlTranche"
	},
	{
		.singleFileSize = UNDO_FILE_SIZE,
			.filenameTemplate = ORIOLEDB_UNDO_SYSTEM_FILENAME_TEMPLATE,
			.groupCtlTrancheName = "undoSystemBuffersGroupCtlTranche",
			.bufferCtlTrancheName = "undoSystemBuffersCtlTranche"
	}
};

static bool wait_for_reserved_location(UndoLogType undoType,
									   UndoLocation undoLocationToWait);

Size
undo_shmem_needs(void)
{
	Size		size;
	int			i;

	size = CACHELINEALIGN(sizeof(UndoMeta) * (int) UndoLogsCount);
	size = add_size(size, undo_circular_buffer_size);
	size = add_size(size, undo_system_circular_buffer_size);
	undoBuffersDescs[UndoLogRegular].buffersCount = undo_buffers_count;
	undoBuffersDescs[UndoLogSystem].buffersCount = undo_system_buffers_count;
	for (i = 0; i < (int) UndoLogsCount; i++)
		size = add_size(size, o_buffers_shmem_needs(&undoBuffersDescs[i]));

	return size;
}

void
undo_shmem_init(Pointer buf, bool found)
{
	Pointer		ptr = buf;
	int			i;

	undo_metas = (UndoMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(UndoMeta) * (int) UndoLogsCount);

	pending_truncates_meta = (PendingTruncatesMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(PendingTruncatesMeta));

	o_undo_buffers[UndoLogRegular] = ptr;
	o_undo_circular_sizes[UndoLogRegular] = undo_circular_buffer_size;
	ptr += undo_circular_buffer_size;
	o_undo_buffers[UndoLogSystem] = ptr;
	o_undo_circular_sizes[UndoLogSystem] = undo_system_circular_buffer_size;
	ptr += undo_system_circular_buffer_size;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		init_undo_meta(&undo_metas[i], found);

		o_buffers_shmem_init(&undoBuffersDescs[i], ptr, found);
		ptr += o_buffers_shmem_needs(&undoBuffersDescs[i]);
	}

	if (!found)
	{
		pending_truncates_meta->pendingTruncatesTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&pending_truncates_meta->pendingTruncatesLock,
						 pending_truncates_meta->pendingTruncatesTrancheId);
	}
	LWLockRegisterTranche(pending_truncates_meta->pendingTruncatesTrancheId,
						  "OPendingTruncatesTranche");
}

static void
init_undo_meta(UndoMeta *meta, bool found)
{
	if (!found)
	{
		SpinLockInit(&meta->minUndoLocationsMutex);
		meta->minUndoLocationsChangeCount = 0;
		meta->undoWriteTrancheId = LWLockNewTrancheId();
		meta->undoStackLocationsFlushLockTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&meta->undoWriteLock,
						 meta->undoWriteTrancheId);

		/* Undo locations are initialized in checkpoint_shmem_init() */
	}
	LWLockRegisterTranche(meta->undoWriteTrancheId,
						  "OUndoWriteTranche");
	LWLockRegisterTranche(meta->undoStackLocationsFlushLockTrancheId,
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

UndoMeta *
get_undo_meta_by_type(UndoLogType undoType)
{
	int			index = (int) undoType;

	Assert(index >= 0 && index < (int) UndoLogsCount);

	return &undo_metas[index];
}

static OBuffersDesc *
get_undo_buffers_by_type(UndoLogType undoType)
{
	int			index = (int) undoType;

	Assert(index >= 0 && index < (int) UndoLogsCount);

	return &undoBuffersDescs[index];
}

void
update_min_undo_locations(UndoLogType undoType,
						  bool have_lock, bool do_cleanup)
{
	UndoLocation minReservedLocation,
				minRetainLocation,
				minTransactionRetainLocation,
				lastUsedLocation;
	UndoLocation oldCleanedLocation = InvalidUndoLocation,
				oldCheckpointStartLocation = InvalidUndoLocation,
				oldCheckpointEndLocation = InvalidUndoLocation,
				newCheckpointStartLocation = InvalidUndoLocation,
				newCheckpointEndLocation = InvalidUndoLocation;
	int			i;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	Assert(!have_lock || !do_cleanup);

	if (!have_lock)
		SpinLockAcquire(&meta->minUndoLocationsMutex);
	START_CRIT_SECTION();

	Assert((meta->minUndoLocationsChangeCount & 1) == 0);

	meta->minUndoLocationsChangeCount++;

	pg_write_barrier();

	lastUsedLocation = pg_atomic_read_u64(&meta->lastUsedLocation);
	minTransactionRetainLocation = minRetainLocation = minReservedLocation = lastUsedLocation;

	for (i = 0; i < max_procs; i++)
	{
		UndoLocation tmp;

		tmp = pg_atomic_read_u64(&oProcData[i].undoRetainLocations[undoType].reservedUndoLocation);
		minReservedLocation = Min(minReservedLocation, tmp);

		tmp = pg_atomic_read_u64(&oProcData[i].undoRetainLocations[undoType].transactionUndoRetainLocation);
		minRetainLocation = Min(minRetainLocation, tmp);
		minTransactionRetainLocation = Min(minTransactionRetainLocation, tmp);
		tmp = pg_atomic_read_u64(&oProcData[i].undoRetainLocations[undoType].snapshotRetainUndoLocation);
		minRetainLocation = Min(minRetainLocation, tmp);
	}

	/*
	 * Make sure none of calculated variables goes backwards.
	 */
	minReservedLocation = Max(pg_atomic_read_u64(&meta->minProcReservedLocation),
							  minReservedLocation);
	minRetainLocation = Max(pg_atomic_read_u64(&meta->minProcRetainLocation),
							minRetainLocation);
	minTransactionRetainLocation = Max(pg_atomic_read_u64(&meta->minProcTransactionRetainLocation),
									   minTransactionRetainLocation);

	pg_atomic_write_u64(&meta->minProcReservedLocation, minReservedLocation);
	pg_atomic_write_u64(&meta->minProcRetainLocation, minRetainLocation);
	pg_atomic_write_u64(&meta->minProcTransactionRetainLocation, minTransactionRetainLocation);
	pg_atomic_write_u64(&meta->lastUsedUndoLocationWhenUpdatedMinLocation, lastUsedLocation);

	pg_write_barrier();

	meta->minUndoLocationsChangeCount++;

	Assert((meta->minUndoLocationsChangeCount & 1) == 0);

	if (!have_lock)
	{
		UndoLocation writeInProgressLocation,
					writtenLocation;

		writeInProgressLocation = pg_atomic_read_u64(&meta->writeInProgressLocation);
		writtenLocation = pg_atomic_read_u64(&meta->writtenLocation);
		if (writtenLocation == writeInProgressLocation && writtenLocation < minRetainLocation &&
			LWLockConditionalAcquire(&meta->undoWriteLock, LW_EXCLUSIVE))
		{
			Assert(minRetainLocation >= pg_atomic_read_u64(&meta->writeInProgressLocation));
			Assert(minRetainLocation >= pg_atomic_read_u64(&meta->writtenLocation));
			pg_atomic_write_u64(&meta->writeInProgressLocation, minRetainLocation);
			pg_atomic_write_u64(&meta->writtenLocation, minRetainLocation);
			LWLockRelease(&meta->undoWriteLock);
		}
	}

	if (do_cleanup)
	{
		oldCleanedLocation = pg_atomic_read_u64(&meta->cleanedLocation);
		oldCheckpointStartLocation = pg_atomic_read_u64(&meta->cleanedCheckpointStartLocation);
		oldCheckpointEndLocation = pg_atomic_read_u64(&meta->cleanedCheckpointEndLocation);
		newCheckpointStartLocation = pg_atomic_read_u64(&meta->checkpointRetainStartLocation);
		newCheckpointEndLocation = pg_atomic_read_u64(&meta->checkpointRetainEndLocation);

		if (oldCleanedLocation != minRetainLocation ||
			oldCheckpointStartLocation != newCheckpointStartLocation ||
			oldCheckpointEndLocation != newCheckpointEndLocation)
		{
			pg_atomic_write_u64(&meta->cleanedCheckpointStartLocation, newCheckpointStartLocation);
			pg_atomic_write_u64(&meta->cleanedCheckpointEndLocation, newCheckpointEndLocation);
			pg_atomic_write_u64(&meta->cleanedLocation, minRetainLocation);
		}
		else
		{
			do_cleanup = false;
		}
	}

	END_CRIT_SECTION();
	if (!have_lock)
		SpinLockRelease(&meta->minUndoLocationsMutex);

	if (do_cleanup)
	{
		int64		oldCleanedNum = oldCleanedLocation / UNDO_FILE_SIZE,
					newCleanedNum = minRetainLocation / UNDO_FILE_SIZE,
					oldCheckpointStartNum = oldCheckpointStartLocation / UNDO_FILE_SIZE,
					oldCheckpointEndNum = oldCheckpointEndLocation / UNDO_FILE_SIZE,
					newCheckpointStartNum = newCheckpointStartLocation / UNDO_FILE_SIZE,
					newCheckpointEndNum = newCheckpointEndLocation / UNDO_FILE_SIZE;
		OBuffersDesc *buffersDesc = get_undo_buffers_by_type(undoType);

		if (oldCheckpointEndLocation % UNDO_FILE_SIZE == 0)
			oldCheckpointEndNum--;
		if (newCheckpointEndLocation % UNDO_FILE_SIZE == 0)
			newCheckpointEndNum--;

		o_buffers_unlink_files_range(buffersDesc,
									 oldCheckpointStartNum,
									 Min(oldCheckpointEndNum,
										 Min(newCheckpointStartNum - 1,
											 newCleanedNum - 1)));

		o_buffers_unlink_files_range(buffersDesc,
									 Max(oldCheckpointStartNum,
										 newCheckpointEndNum + 1),
									 Min(oldCheckpointEndNum,
										 newCleanedNum - 1));

		o_buffers_unlink_files_range(buffersDesc,
									 oldCleanedNum,
									 Min(newCheckpointStartNum - 1,
										 newCleanedNum - 1));

		o_buffers_unlink_files_range(buffersDesc,
									 Max(oldCleanedNum,
										 newCheckpointEndNum + 1),
									 newCleanedNum - 1);
	}
}

/*
 * Guarantees that concurrent update_min_undo_locations() finishes.
 */
static void
wait_for_even_changecount(UndoMeta *meta)
{
	SpinDelayStatus status;

	init_local_spin_delay(&status);
	while (meta->minUndoLocationsChangeCount & 1)
	{
		perform_spin_delay(&status);
		pg_read_barrier();
	}
	finish_spin_delay(&status);
}

static void
set_my_reserved_location(UndoLogType undoType)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoLocation lastUsedLocation;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	while (true)
	{
		lastUsedLocation = pg_atomic_read_u64(&meta->lastUsedLocation);
		if (!UndoLocationIsValid(pg_atomic_read_u64(&curProcData->undoRetainLocations[undoType].reservedUndoLocation)))
			pg_atomic_write_u64(&curProcData->undoRetainLocations[undoType].reservedUndoLocation, lastUsedLocation);
		if (!UndoLocationIsValid(pg_atomic_read_u64(&curProcData->undoRetainLocations[undoType].transactionUndoRetainLocation)))
			pg_atomic_write_u64(&curProcData->undoRetainLocations[undoType].transactionUndoRetainLocation, lastUsedLocation);

		wait_for_even_changecount(meta);

		/*
		 * Retry if minimal positions run higher due to concurrent
		 * update_min_undo_locations().
		 */
		if (pg_atomic_read_u64(&meta->minProcReservedLocation) > lastUsedLocation)
			continue;
		if (pg_atomic_read_u64(&meta->minProcTransactionRetainLocation) > lastUsedLocation)
			continue;

		break;
	}
	if (!UndoLocationIsValid(curRetainUndoLocations[undoType]))
		curRetainUndoLocations[undoType] = lastUsedLocation;
}

static UndoLocation
set_my_retain_location(UndoLogType undoType)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoLocation curSnapshotRetainUndoLocation,
				retainUndoLocation;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	while (true)
	{
		retainUndoLocation = pg_atomic_read_u64(&meta->minProcTransactionRetainLocation);
		curSnapshotRetainUndoLocation = pg_atomic_read_u64(&curProcData->undoRetainLocations[undoType].snapshotRetainUndoLocation);

		if (!UndoLocationIsValid(curSnapshotRetainUndoLocation) ||
			retainUndoLocation < curSnapshotRetainUndoLocation)
			pg_atomic_write_u64(&curProcData->undoRetainLocations[undoType].snapshotRetainUndoLocation, retainUndoLocation);

		pg_memory_barrier();

		wait_for_even_changecount(meta);

		/*
		 * Retry if minimal positions run higher due to concurrent
		 * update_min_undo_locations().
		 */
		if (pg_atomic_read_u64(&meta->minProcRetainLocation) > retainUndoLocation)
			continue;

		break;
	}
	return retainUndoLocation;
}

static bool
wait_for_reserved_location(UndoLogType undoType,
						   UndoLocation undoLocationToWait)
{
	SpinDelayStatus delay;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	bool		delay_inited;
	int			i;

	if (undoLocationToWait > pg_atomic_read_u64(&curProcData->undoRetainLocations[undoType].reservedUndoLocation) + o_undo_circular_sizes[(int) undoType])
	{
		return false;
	}

	for (i = 0; i < max_procs; i++)
	{
		delay_inited = false;
		while (undoLocationToWait > pg_atomic_read_u64(&oProcData[i].undoRetainLocations[undoType].reservedUndoLocation) + o_undo_circular_sizes[(int) undoType])
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
undo_item_buf_read_item(UndoItemBuf *buf,
						UndoLogType undoType,
						UndoLocation location)
{
	LocationIndex itemSize;

	ASAN_UNPOISON_MEMORY_REGION(buf->data, buf->length);
	undo_read(undoType, location, sizeof(UndoStackItem), buf->data);

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
	undo_read(undoType,
			  location + sizeof(UndoStackItem),
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
o_add_branch_undo_item(UndoLogType undoType, UndoLocation newLocation)
{
	UndoLocation location;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
	BranchUndoStackItem *item;
	LocationIndex size;

	size = sizeof(BranchUndoStackItem);
	item = (BranchUndoStackItem *) get_undo_record_unreserved(undoType,
															  &location,
															  MAXALIGN(size));

	item->longPathLocation = pg_atomic_read_u64(&sharedLocations->location);
	item->prevBranchLocation = pg_atomic_read_u64(&sharedLocations->branchLocation);
	item->header.type = BranchUndoItemType;
	item->header.indexType = oIndexPrimary;
	item->header.itemSize = size;
	item->header.prev = newLocation;

	release_undo_size(undoType);

	return location;
}

/*
 * Walk through the undo stack calling the callbacks for each item.
 */
static UndoLocation
walk_undo_range(UndoLogType undoType,
				UndoLocation location, UndoLocation toLoc, UndoItemBuf *buf,
				OXid oxid, bool abort_val, UndoLocation *onCommitLocation,
				bool changeCountsValid)
{
	UndoStackItem *item;
	UndoItemTypeDescr *descr;

	while (UndoLocationIsValid(location) && (location > toLoc || !UndoLocationIsValid(toLoc)))
	{
		item = undo_item_buf_read_item(buf, undoType, location);
		descr = item_type_get_descr(item->type);
		descr->callback(undoType, location, item, oxid,
						abort_val, changeCountsValid);

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
apply_undo_branches(UndoLogType undoType, OXid oxid)
{
	UndoItemBuf buf;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
	BranchUndoStackItem *item;
	UndoLocation location;

	init_undo_item_buf(&buf);

	location = pg_atomic_read_u64(&sharedLocations->branchLocation);
	while (UndoLocationIsValid(location))
	{
		item = (BranchUndoStackItem *) undo_item_buf_read_item(&buf, undoType,
															   location);
		location = item->prevBranchLocation;
		walk_undo_range(undoType, item->longPathLocation, item->header.prev,
						&buf, oxid, true, NULL, false);
	}
	free_undo_item_buf(&buf);
}

/*
 * Walk transaction undo stack chain during (sub)transaction abort or
 * transaction commit.
 */
static void
walk_undo_stack(UndoLogType undoType, OXid oxid,
				UndoStackLocations *toLocation, bool abortTrx,
				bool changeCountsValid)
{
	UndoItemBuf buf;
	UndoLocation location,
				newOnCommitLocation;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);

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
		location = walk_undo_range(undoType, location, InvalidUndoLocation,
								   &buf, oxid, false, NULL,
								   changeCountsValid);
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
		location = walk_undo_range(undoType, location,
								   toLocation ? toLocation->location : InvalidUndoLocation,
								   &buf, oxid, true, &newOnCommitLocation,
								   changeCountsValid);
	}

	/*
	 * Create special branch item, which allows finding aborted items.
	 */
	if (toLocation)
		location = o_add_branch_undo_item(undoType, location);

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
		rec.undoType = undoType;
		read_shared_undo_locations(&rec.undoLocation, sharedLocations);
		write_to_xids_queue(&rec);
	}

	pg_atomic_write_u64(&sharedLocations->location, location);
	pg_atomic_write_u64(&sharedLocations->onCommitLocation, newOnCommitLocation);
	LWLockRelease(&curProcData->undoStackLocationsFlushLock);

	free_undo_item_buf(&buf);
}

void
apply_undo_stack(UndoLogType undoType, OXid oxid, UndoStackLocations *toLocation,
				 bool changeCountsValid)
{
	walk_undo_stack(undoType, oxid, toLocation, true, changeCountsValid);
}

void
on_commit_undo_stack(UndoLogType undoType, OXid oxid, bool changeCountsValid)
{
	walk_undo_stack(undoType, oxid, NULL, false, changeCountsValid);
}

static bool
undo_type_has_retained_location(UndoLogType undoType)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	return UndoLocationIsValid(pg_atomic_read_u64(&curProcData->undoRetainLocations[(int) undoType].transactionUndoRetainLocation));
}

bool
have_retained_undo_location(void)
{
	int			i;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		if (undo_type_has_retained_location((UndoLogType) i))
			return true;
	}

	return false;
}

UndoLocation
get_snapshot_retained_undo_location(UndoLogType undoType)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	Assert(undoType != UndoLogNone);

	return pg_atomic_read_u64(&curProcData->undoRetainLocations[(int) undoType].snapshotRetainUndoLocation);
}

void
free_retained_undo_location(UndoLogType undoType)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	Assert(reserved_undo_size == 0);
	Assert(pg_atomic_read_u64(&curProcData->undoRetainLocations[(int) undoType].reservedUndoLocation) == InvalidUndoLocation);
	pg_atomic_write_u64(&curProcData->undoRetainLocations[(int) undoType].transactionUndoRetainLocation, InvalidUndoLocation);
	curRetainUndoLocations[undoType] = InvalidUndoLocation;

}
static bool
check_reserved_undo_location(UndoLogType undoType, UndoLocation location,
							 uint64 *minProcReservedLocation,
							 bool waitForUndoLocation)
{
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	*minProcReservedLocation = pg_atomic_read_u64(&meta->minProcReservedLocation);
	while (location > *minProcReservedLocation + o_undo_circular_sizes[(int) undoType])
	{
		bool		failed = true;

		if (waitForUndoLocation)
			failed = !wait_for_reserved_location(undoType, location);

		if (failed)
			return false;

		*minProcReservedLocation = pg_atomic_read_u64(&meta->minProcReservedLocation);
		if (location <= *minProcReservedLocation + o_undo_circular_sizes[(int) undoType])
			return true;

		update_min_undo_locations(undoType, false, waitForUndoLocation);
		*minProcReservedLocation = pg_atomic_read_u64(&meta->minProcReservedLocation);
	}

	return true;
}

static void
write_undo_range(OBuffersDesc *desc, Pointer buf, UndoLocation minLoc, UndoLocation maxLoc)
{
	if (maxLoc > minLoc)
		o_buffers_write(desc, buf, minLoc, maxLoc - minLoc);
}

static void
read_undo_range(OBuffersDesc *desc, Pointer buf, UndoLocation minLoc, UndoLocation maxLoc)
{
	Assert(maxLoc > minLoc);
	o_buffers_read(desc, buf, minLoc, maxLoc - minLoc);
}

void
write_undo(UndoLogType undoType,
		   UndoLocation targetUndoLocation,
		   UndoLocation minProcReservedLocation,
		   bool attempt)
{
	UndoLocation retainUndoLocation,
				writtenLocation;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);
	Pointer		circularBuffer = o_undo_buffers[(int) undoType];
	Size		circularBufferSize = o_undo_circular_sizes[(int) undoType];

	Assert(targetUndoLocation <= minProcReservedLocation);

	if (attempt)
	{
		if (!LWLockConditionalAcquire(&meta->undoWriteLock, LW_EXCLUSIVE))
			return;
	}
	else
	{
		LWLockAcquire(&meta->undoWriteLock, LW_EXCLUSIVE);
	}

	SpinLockAcquire(&meta->minUndoLocationsMutex);

	update_min_undo_locations(undoType, true, false);

	retainUndoLocation = pg_atomic_read_u64(&meta->minProcRetainLocation);

	if (targetUndoLocation <= retainUndoLocation ||
		targetUndoLocation <= pg_atomic_read_u64(&meta->writtenLocation))
	{
		/* We don't have to really write undo. */
		if (pg_atomic_read_u64(&meta->writeInProgressLocation) < retainUndoLocation)
		{
			pg_atomic_write_u64(&meta->writeInProgressLocation, retainUndoLocation);
			pg_atomic_write_u64(&meta->writtenLocation, retainUndoLocation);
		}
		SpinLockRelease(&meta->minUndoLocationsMutex);
		LWLockRelease(&meta->undoWriteLock);
		return;
	}

	/* Try to write 5% of the whole undo size if possible */
	writtenLocation = pg_atomic_read_u64(&meta->writtenLocation);
	retainUndoLocation = Max(retainUndoLocation, writtenLocation);
	targetUndoLocation = Max(targetUndoLocation, writtenLocation + circularBufferSize / 20);
	targetUndoLocation = Min(targetUndoLocation, minProcReservedLocation);

	Assert(targetUndoLocation >= pg_atomic_read_u64(&meta->writeInProgressLocation));
	pg_atomic_write_u64(&meta->writeInProgressLocation, targetUndoLocation);

	SpinLockRelease(&meta->minUndoLocationsMutex);

	minProcReservedLocation = pg_atomic_read_u64(&meta->minProcReservedLocation);
	if (minProcReservedLocation < targetUndoLocation)
		(void) wait_for_reserved_location(undoType, targetUndoLocation + circularBufferSize);


	if (retainUndoLocation % circularBufferSize <
		targetUndoLocation % circularBufferSize)
	{
		write_undo_range(get_undo_buffers_by_type(undoType),
						 circularBuffer + retainUndoLocation % circularBufferSize,
						 retainUndoLocation, targetUndoLocation);
	}
	else
	{
		UndoLocation breakUndoLocation;

		breakUndoLocation = retainUndoLocation + (circularBufferSize -
												  (retainUndoLocation % circularBufferSize));
		write_undo_range(get_undo_buffers_by_type(undoType),
						 circularBuffer + retainUndoLocation % circularBufferSize,
						 retainUndoLocation, breakUndoLocation);
		write_undo_range(get_undo_buffers_by_type(undoType),
						 circularBuffer, breakUndoLocation, targetUndoLocation);
	}

	SpinLockAcquire(&meta->minUndoLocationsMutex);
	Assert(targetUndoLocation >= pg_atomic_read_u64(&meta->writtenLocation));
	pg_atomic_write_u64(&meta->writtenLocation, targetUndoLocation);
	SpinLockRelease(&meta->minUndoLocationsMutex);

	LWLockRelease(&meta->undoWriteLock);
}

bool
reserve_undo_size_extended(UndoLogType undoType, Size size,
						   bool waitForUndoLocation, bool reportError)
{
	UndoLocation location;
	uint64		minProcReservedLocation;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);
	Size		circularBufferSize = o_undo_circular_sizes[(int) undoType];

	Assert(!waitForUndoLocation || !have_locked_pages());
	Assert(undoType != UndoLogNone);
	Assert(size > 0);

	if (reserved_undo_size >= size)
		return true;

	size -= reserved_undo_size;

	location = pg_atomic_fetch_add_u64(&meta->advanceReservedLocation, size);
	reserved_undo_size += size;

	if (location + size <=
		pg_atomic_read_u64(&meta->writtenLocation) + circularBufferSize)
		return true;

	update_min_undo_locations(undoType, false, waitForUndoLocation);

	if (!check_reserved_undo_location(undoType, location + size,
									  &minProcReservedLocation,
									  waitForUndoLocation))
	{
		/*
		 * we add size to reserver_undo_size and meta->advanceReservedLocation
		 * and must revert this action
		 */
		pg_atomic_fetch_sub_u64(&meta->advanceReservedLocation, size);
		reserved_undo_size -= size;
		if (reportError)
			report_undo_overflow();
		else
			return false;
	}

	/* Recheck if the required location was already written */
	if (location + size <=
		pg_atomic_read_u64(&meta->writtenLocation) + circularBufferSize)
		return true;

	if (!waitForUndoLocation)
	{
		/*
		 * No more chances to succeed without waiting.
		 */
		pg_atomic_fetch_sub_u64(&meta->advanceReservedLocation, size);
		reserved_undo_size -= size;
		if (reportError)
			report_undo_overflow();
		else
			return false;
	}

	if (location + size <=
		pg_atomic_read_u64(&meta->writeInProgressLocation) + circularBufferSize)
	{
		/*
		 * Current in-progress undo write should cover our required location.
		 * It should be enough to just wait for current in-progress write to
		 * be finished.
		 */
		LWLockAcquire(&meta->undoWriteLock, LW_SHARED);
		LWLockRelease(&meta->undoWriteLock);

		SpinLockAcquire(&meta->minUndoLocationsMutex);
		Assert(location + size <= pg_atomic_read_u64(&meta->writtenLocation) + circularBufferSize);
		SpinLockRelease(&meta->minUndoLocationsMutex);
		return true;
	}

	write_undo(undoType, location + size - circularBufferSize,
			   minProcReservedLocation, false);
	Assert(location + size <= pg_atomic_read_u64(&meta->writtenLocation) + circularBufferSize);

	return true;
}

void
fsync_undo_range(UndoLogType undoType,
				 UndoLocation fromLoc, UndoLocation toLoc,
				 uint32 wait_event_info)
{
	UndoLocation minProcReservedLocation;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	(void) check_reserved_undo_location(undoType,
										toLoc + o_undo_circular_sizes[(int) undoType],
										&minProcReservedLocation,
										true);

	if (toLoc <= pg_atomic_read_u64(&meta->writeInProgressLocation))
	{
		/*
		 * Current in-progress undo write should cover our required location.
		 * It should be enough to just wait for current in-progress write to
		 * be finished.
		 */
		LWLockAcquire(&meta->undoWriteLock, LW_SHARED);
		LWLockRelease(&meta->undoWriteLock);

		SpinLockAcquire(&meta->minUndoLocationsMutex);
		Assert(toLoc <= pg_atomic_read_u64(&meta->writtenLocation));
		SpinLockRelease(&meta->minUndoLocationsMutex);
	}
	else
	{
		write_undo(undoType, toLoc, minProcReservedLocation, false);
	}

	o_buffers_sync(get_undo_buffers_by_type(undoType),
				   fromLoc, toLoc, wait_event_info);
}

Pointer
get_undo_record(UndoLogType undoType, UndoLocation *undoLocation, Size size)
{
	UndoMeta   *meta = get_undo_meta_by_type(undoType);
	Size		circularBufferSize = o_undo_circular_sizes[(int) undoType];

	Assert(size == MAXALIGN(size));
	Assert(undoType != UndoLogNone);

	set_my_reserved_location(undoType);

	pg_write_barrier();

	while (true)
	{
		UndoLocation location;

		Assert(reserved_undo_size >= size);

		location = pg_atomic_fetch_add_u64(&meta->lastUsedLocation, size);
		reserved_undo_size -= size;

		/*
		 * We might hit the boundary of circular buffer.  If so then just
		 * retry. Thankfully we've reserved twice more space than required.
		 *
		 * This situation shouldn't happen twice, since we've reserved undo
		 * location.
		 */
		if ((location + size) % circularBufferSize >
			location % circularBufferSize)
		{
			*undoLocation = location;
			return GET_UNDO_REC(undoType, location);
		}
	}
}

Pointer
get_undo_record_unreserved(UndoLogType type, UndoLocation *undoLocation, Size size)
{
	Assert(size == MAXALIGN(size));
	Assert(reserved_undo_size == 0);

	reserve_undo_size(type, 2 * size);
	return get_undo_record(type, undoLocation, size);
}

void
release_undo_size(UndoLogType undoType)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	Assert(undoType != UndoLogNone);

	if (reserved_undo_size != 0)
	{
		pg_atomic_fetch_sub_u64(&meta->advanceReservedLocation, reserved_undo_size);
		reserved_undo_size = 0;
	}
	pg_atomic_write_u64(&curProcData->undoRetainLocations[(int) undoType].reservedUndoLocation,
						InvalidUndoLocation);
}

Size
get_reserved_undo_size(UndoLogType undoType)
{
	Assert(undoType != UndoLogNone);

	return reserved_undo_size;
}

void
add_new_undo_stack_item(UndoLogType undoType, UndoLocation location)
{
	UndoStackItem *item = (UndoStackItem *) GET_UNDO_REC(undoType, location);
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
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
get_subxact_undo_location(UndoLogType undoType)
{
	if (undoType != UndoLogNone)
	{
		UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);

		return pg_atomic_read_u64(&sharedLocations->subxactLocation);
	}
	else
	{
		return InvalidUndoLocation;
	}
}

void
read_shared_undo_locations(UndoStackLocations *to, UndoStackSharedLocations *from)
{
	ASAN_UNPOISON_MEMORY_REGION(to, sizeof(*to));
	to->location = pg_atomic_read_u64(&from->location);
	to->branchLocation = pg_atomic_read_u64(&from->branchLocation);
	to->subxactLocation = pg_atomic_read_u64(&from->subxactLocation);
	to->onCommitLocation = pg_atomic_read_u64(&from->onCommitLocation);
}

void
write_shared_undo_locations(UndoStackSharedLocations *to, UndoStackLocations *from)
{
	ASAN_UNPOISON_MEMORY_REGION(from, sizeof(*from));
	pg_atomic_write_u64(&to->location, from->location);
	pg_atomic_write_u64(&to->branchLocation, from->branchLocation);
	pg_atomic_write_u64(&to->subxactLocation, from->subxactLocation);
	pg_atomic_write_u64(&to->onCommitLocation, from->onCommitLocation);
}

UndoStackLocations
get_cur_undo_locations(UndoLogType undoType)
{
	UndoStackLocations location;
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);

	read_shared_undo_locations(&location, sharedLocations);

	return location;
}

void
set_cur_undo_locations(UndoLogType undoType, UndoStackLocations location)
{
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);

	write_shared_undo_locations(sharedLocations, &location);
}

void
reset_cur_undo_locations(void)
{
	UndoStackLocations location = {InvalidUndoLocation, InvalidUndoLocation, InvalidUndoLocation, InvalidUndoLocation};
	int			i;

	for (i = 0; i < (int) UndoLogsCount; i++)
		set_cur_undo_locations((UndoLogType) i, location);
}

void
orioledb_reset_xmin_hook(void)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	RetainUndoLocationPHNode *location;
	OXid		xmin = InvalidOXid;

	if (ActiveSnapshotSet())
		return;

	if (pairingheap_is_empty(&retainUndoLocRegularHeap))
	{
		pg_atomic_write_u64(&curProcData->undoRetainLocations[UndoLogRegular].snapshotRetainUndoLocation, InvalidUndoLocation);
	}
	else
	{
		location = pairingheap_container(RetainUndoLocationPHNode, ph_node,
										 pairingheap_first(&retainUndoLocRegularHeap));
		if (location->undoLocation > pg_atomic_read_u64(&curProcData->undoRetainLocations[UndoLogRegular].snapshotRetainUndoLocation))
		{
			pg_atomic_write_u64(&curProcData->undoRetainLocations[UndoLogRegular].snapshotRetainUndoLocation, location->undoLocation);
			if (!OXidIsValid(xmin) || location->xmin < xmin)
				xmin = location->xmin;
		}
	}

	if (pairingheap_is_empty(&retainUndoLocSystemHeap))
	{
		pg_atomic_write_u64(&curProcData->undoRetainLocations[UndoLogSystem].snapshotRetainUndoLocation, InvalidUndoLocation);
	}
	else
	{
		location = pairingheap_container(RetainUndoLocationPHNode, ph_node,
										 pairingheap_first(&retainUndoLocSystemHeap));
		if (location->undoLocation > pg_atomic_read_u64(&curProcData->undoRetainLocations[UndoLogSystem].snapshotRetainUndoLocation))
		{
			pg_atomic_write_u64(&curProcData->undoRetainLocations[UndoLogSystem].snapshotRetainUndoLocation, location->undoLocation);
			if (!OXidIsValid(xmin) || location->xmin < xmin)
				xmin = location->xmin;
		}
	}
	pg_atomic_write_u64(&curProcData->xmin, xmin);
}

void
undo_xact_callback(XactEvent event, void *arg)
{
	OXid		oxid = get_current_oxid_if_any();
	CommitSeqNo csn;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	bool		isParallelWorker;
	int			i;

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
		TransactionId xid = GetTopTransactionIdIfAny();
		XLogRecPtr	flushPos = InvalidXLogRecPtr;

		Assert(!RecoveryInProgress());
		switch (event)
		{
			case XACT_EVENT_PRE_COMMIT:
				if (TransactionIdIsValid(xid))
					wal_joint_commit(oxid,
									 get_current_logical_xid(),
									 xid);
				else
					current_oxid_xlog_precommit();
				break;
			case XACT_EVENT_COMMIT:
				if (!TransactionIdIsValid(xid))
				{
					current_oxid_xlog_precommit();
					flushPos = wal_commit(oxid,
										  get_current_logical_xid());
					set_oxid_xlog_ptr(oxid, flushPos);
					if (!XLogRecPtrIsInvalid(flushPos) &&
						(synchronous_commit > SYNCHRONOUS_COMMIT_OFF ||
						 oxid_needs_wal_flush))
						XLogFlush(flushPos);
				}
				else
				{
					set_oxid_xlog_ptr(oxid, XactLastCommitEnd);
				}

				current_oxid_precommit();
				csn = GetCurrentCSN();
				if (csn == COMMITSEQNO_INPROGRESS)
					csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
				current_oxid_commit(csn);

				for (i = 0; i < (int) UndoLogsCount; i++)
					on_commit_undo_stack((UndoLogType) i, oxid, true);
				wal_after_commit();
				reset_cur_undo_locations();
				oxid_needs_wal_flush = false;
				break;
			case XACT_EVENT_ABORT:
				if (!RecoveryInProgress())
					wal_rollback(oxid,
								 get_current_logical_xid());
				for (i = 0; i < (int) UndoLogsCount; i++)
					apply_undo_stack((UndoLogType) i, oxid, NULL, true);
				reset_cur_undo_locations();
				current_oxid_abort();
				set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr);
				oxid_needs_wal_flush = false;

				/*
				 * Remove registered snapshot one-by-one, so that we can avoid
				 * double removing in undo_snapshot_deregister_hook().
				 */
				while (!pairingheap_is_empty(&retainUndoLocRegularHeap))
					pairingheap_remove_first(&retainUndoLocRegularHeap);
				while (!pairingheap_is_empty(&retainUndoLocSystemHeap))
					pairingheap_remove_first(&retainUndoLocSystemHeap);

				for (i = 0; i < (int) UndoLogsCount; i++)
					pg_atomic_write_u64(&curProcData->undoRetainLocations[i].snapshotRetainUndoLocation, InvalidUndoLocation);
				break;
			default:
				break;
		}
	}

	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
	{
		for (i = 0; i < (int) UndoLogsCount; i++)
			release_undo_size((UndoLogType) i);

		for (i = 0; i < OPagePoolTypesCount; i++)
		{
			OPagePool  *pool = get_ppool((OPagePoolType) i);

			ppool_release_reserved(pool, PPOOL_RESERVE_MASK_ALL);
		}

		for (i = 0; i < (int) UndoLogsCount; i++)
			free_retained_undo_location((UndoLogType) i);

		for (i = 0; i < (int) UndoLogsCount; i++)
			saved_undo_location[i] = InvalidUndoLocation;
	}

	if (event == XACT_EVENT_COMMIT && isParallelWorker)
		parallel_worker_set_oxid();
}

void
add_subxact_undo_item(SubTransactionId parentSubid)
{
	SubXactUndoStackItem *item;
	UndoStackSharedLocations *sharedLocations;
	UndoLocation location;
	Size		size;
	int			i;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		UndoLogType undoType = (UndoLogType) i;

		sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
		size = sizeof(SubXactUndoStackItem);

		item = (SubXactUndoStackItem *) get_undo_record_unreserved(undoType,
																   &location,
																   MAXALIGN(size));
		item->prevSubLocation = pg_atomic_read_u64(&sharedLocations->subxactLocation);
		item->parentSubid = parentSubid;
		item->header.type = SubXactUndoItemType;
		item->header.indexType = oIndexPrimary;
		item->header.itemSize = size;
		add_new_undo_stack_item(undoType, location);
		release_undo_size(undoType);
		pg_atomic_write_u64(&sharedLocations->subxactLocation, location);
	}
}

static bool
search_for_undo_sub_location(UndoLogType undoType,
							 UndoStackKind kind, UndoLocation location,
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
		item = (SubXactUndoStackItem *) undo_item_buf_read_item(buf, undoType,
																location);
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
update_subxact_undo_location(UndoLogType undoType, UndoLocation subxactLocation)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);

	LWLockAcquire(&curProcData->undoStackLocationsFlushLock, LW_EXCLUSIVE);

	pg_atomic_write_u64(&sharedLocations->subxactLocation, subxactLocation);

	LWLockRelease(&curProcData->undoStackLocationsFlushLock);
}

void
rollback_to_savepoint(UndoLogType undoType, UndoStackKind kind,
					  SubTransactionId parentSubid, bool changeCountsValid)
{
	UndoStackLocations toLoc;
	UndoLocation location;
	UndoStackSharedLocations *sharedLocations;
	UndoItemBuf buf;
	OXid		oxid;
	bool		applyResult;

	sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
	init_undo_item_buf(&buf);
	location = pg_atomic_read_u64(&sharedLocations->subxactLocation);
	applyResult = search_for_undo_sub_location(undoType, kind, location, &buf, parentSubid,
											   &toLoc.location, &toLoc.subxactLocation);
	free_undo_item_buf(&buf);

	if (!applyResult)
		return;

	oxid = get_current_oxid_if_any();
	if (OXidIsValid(oxid))
		walk_undo_stack(undoType, oxid, &toLoc, true, changeCountsValid);
	update_subxact_undo_location(undoType, toLoc.subxactLocation);
}

static void
update_subxact_undo_location_on_commit(SubTransactionId parentSubid)
{
	UndoStackLocations toLoc;
	UndoLocation location;
	UndoStackSharedLocations *sharedLocations;
	UndoItemBuf buf;
	int			i;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		UndoLogType undoType = (UndoLogType) i;

		sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
		init_undo_item_buf(&buf);
		location = pg_atomic_read_u64(&sharedLocations->subxactLocation);
		search_for_undo_sub_location(undoType, UndoStackFull, location,
									 &buf, parentSubid,
									 &toLoc.location, &toLoc.subxactLocation);
		free_undo_item_buf(&buf);
		update_subxact_undo_location(undoType, toLoc.subxactLocation);
	}
}

void
undo_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
					  SubTransactionId parentSubid, void *arg)
{
	TransactionId prentLogicalXid;
	int			i;

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
			prentLogicalXid = get_current_logical_xid();
			assign_subtransaction_logical_xid();
			add_savepoint_wal_record(parentSubid, prentLogicalXid);
			break;
		case SUBXACT_EVENT_COMMIT_SUB:
			update_subxact_undo_location_on_commit(parentSubid);
			for (i = 0; i < (int) UndoLogsCount; i++)
				saved_undo_location[i] = InvalidUndoLocation;
			break;
		case SUBXACT_EVENT_ABORT_SUB:
			for (i = 0; i < (int) UndoLogsCount; i++)
				rollback_to_savepoint((UndoLogType) i, UndoStackFull,
									  parentSubid, true);
			add_rollback_to_savepoint_wal_record(parentSubid);
			for (i = 0; i < (int) UndoLogsCount; i++)
				saved_undo_location[i] = InvalidUndoLocation;

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
have_current_undo(UndoLogType undoType)
{
	if (undoType == UndoLogNone)
	{
		return false;
	}
	else
	{
		UndoStackLocations location = get_cur_undo_locations(undoType);

		return (!UndoLocationIsValid(location.location));
	}
}

void
report_undo_overflow(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("failed to add an undo record: undo size is exceeded")));
}

Datum
orioledb_has_retained_undo(PG_FUNCTION_ARGS)
{
	UndoLocation location;
	bool		result = false;
	int			i,
				j;

	for (i = 0; i < max_procs; i++)
	{
		for (j = 0; j < (int) UndoLogsCount; j++)
		{
			location = pg_atomic_read_u64(&oProcData[i].undoRetainLocations[(UndoLogType) j].transactionUndoRetainLocation);
			if (UndoLocationIsValid(location))
			{
				result = true;
				break;
			}
		}
	}

	PG_RETURN_BOOL(result);
}

void
start_autonomous_transaction(OAutonomousTxState *state)
{
	int			i;

	Assert(!is_recovery_process());
	state->needs_wal_flush = oxid_needs_wal_flush;
	state->oxid = get_current_oxid();
	state->logicalXid = get_current_logical_xid();
	for (i = 0; i < (int) UndoLogsCount; i++)
		state->has_retained_undo_location[i] = undo_type_has_retained_location((UndoLogType) i);
	state->local_wal_has_material_changes = get_local_wal_has_material_changes();

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
		int			i;

		wal_rollback(oxid,
					 get_current_logical_xid());
		current_oxid_abort();
		for (i = 0; i < (int) UndoLogsCount; i++)
			apply_undo_stack((UndoLogType) i, oxid, NULL, true);

		for (i = 0; i < (int) UndoLogsCount; i++)
			release_undo_size((UndoLogType) i);
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			if (!state->has_retained_undo_location[i])
				free_retained_undo_location((UndoLogType) i);
		}
	}

	oxid_needs_wal_flush = state->needs_wal_flush;
	GET_CUR_PROCDATA()->autonomousNestingLevel--;
	set_current_oxid(state->oxid);
	set_current_logical_xid(state->logicalXid);
	set_local_wal_has_material_changes(state->local_wal_has_material_changes);
}

void
finish_autonomous_transaction(OAutonomousTxState *state)
{
	OXid		oxid = get_current_oxid_if_any();

	if (OXidIsValid(oxid))
	{
		CommitSeqNo csn;
		int			i;

		wal_commit(oxid,
				   get_current_logical_xid());

		current_oxid_precommit();
		csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
		current_oxid_commit(csn);

		for (i = 0; i < (int) UndoLogsCount; i++)
			on_commit_undo_stack((UndoLogType) i, oxid, true);
		wal_after_commit();

		for (i = 0; i < (int) UndoLogsCount; i++)
			release_undo_size((UndoLogType) i);
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			if (!state->has_retained_undo_location[i])
				free_retained_undo_location((UndoLogType) i);
		}
	}

	oxid_needs_wal_flush = state->needs_wal_flush;
	GET_CUR_PROCDATA()->autonomousNestingLevel--;
	set_current_oxid(state->oxid);
	set_current_logical_xid(state->logicalXid);
	set_local_wal_has_material_changes(state->local_wal_has_material_changes);
}

void
undo_read(UndoLogType undoType, UndoLocation location, Size size, Pointer buf)
{
	UndoLocation writtenLocation;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	writtenLocation = pg_atomic_read_u64(&meta->writtenLocation);

	if (location + size > writtenLocation)
	{
		UndoLocation maxLoc,
					minLoc;

		pg_read_barrier();

		maxLoc = location + size;
		minLoc = Max(writtenLocation, location);
		memcpy(buf + (minLoc - location), GET_UNDO_REC(undoType, minLoc), maxLoc - minLoc);

		pg_read_barrier();

		writtenLocation = pg_atomic_read_u64(&meta->writtenLocation);
		if (writtenLocation > location)
			read_undo_range(get_undo_buffers_by_type(undoType), buf, location,
							Min(location + size, writtenLocation));
	}
	else
	{
		read_undo_range(get_undo_buffers_by_type(undoType), buf, location,
						location + size);
	}
}

/*
 * Write buffer to the given undo location.
 */
void
undo_write(UndoLogType undoType, UndoLocation location, Size size, Pointer buf)
{
	UndoLocation writeInProgressLocation,
				prevReservedUndoLocation,
				memoryUndoLocation;
	bool		undoLocationIsReserved = false;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoRetainSharedLocations *sharedLocations = &curProcData->undoRetainLocations[(int) undoType];
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	while (true)
	{
		writeInProgressLocation = pg_atomic_read_u64(&meta->writeInProgressLocation);
		if (writeInProgressLocation >= location + size)
		{
			/* Nothing we can write to the memory */
			memoryUndoLocation = location + size;
			break;
		}

		/* Reserve the location we're going to write into */
		memoryUndoLocation = Max(location, writeInProgressLocation);
		prevReservedUndoLocation = pg_atomic_read_u64(&sharedLocations->reservedUndoLocation);
		if (!UndoLocationIsValid(prevReservedUndoLocation) || prevReservedUndoLocation > memoryUndoLocation)
		{
			pg_atomic_write_u64(&sharedLocations->reservedUndoLocation, memoryUndoLocation);
			undoLocationIsReserved = true;
		}

		pg_memory_barrier();

		/* Recheck is writeInProgressLocation was advanced concurrently */
		writeInProgressLocation = pg_atomic_read_u64(&meta->writeInProgressLocation);
		if (writeInProgressLocation > memoryUndoLocation)
		{
			if (undoLocationIsReserved)
			{
				pg_atomic_write_u64(&sharedLocations->reservedUndoLocation, prevReservedUndoLocation);
				undoLocationIsReserved = false;
			}
			continue;
		}

		/*
		 * At this point we should either detect concurrent writing of undo
		 * log. Or concurrent writing of undo log should wait for our reserved
		 * location. So, it should be safe to write to the memory.
		 */

		memcpy(GET_UNDO_REC(undoType, memoryUndoLocation),
			   buf + (memoryUndoLocation - location),
			   size - (memoryUndoLocation - location));
		break;
	}

	if (undoLocationIsReserved)
	{
		pg_atomic_write_u64(&sharedLocations->reservedUndoLocation, prevReservedUndoLocation);
		undoLocationIsReserved = false;
	}

	if (memoryUndoLocation == location)
	{
		/* Everything is written to the in-memory buffer */
		return;
	}

	/* Wait for in-progress write if needed */
	if (pg_atomic_read_u64(&meta->writtenLocation) < memoryUndoLocation)
	{
		LWLockAcquire(&meta->undoWriteLock, LW_SHARED);
		LWLockRelease(&meta->undoWriteLock);
		Assert(pg_atomic_read_u64(&meta->writtenLocation) >= memoryUndoLocation);
	}

	/* Finally perform writing to the file */
	write_undo_range(get_undo_buffers_by_type(undoType), buf, location,
					 memoryUndoLocation);
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
	pairingheap_add(&retainUndoLocRegularHeap, &snapshot->undoRegularLocationPhNode.ph_node);
	pairingheap_add(&retainUndoLocSystemHeap, &snapshot->undoSystemLocationPhNode.ph_node);
}

void
undo_snapshot_deregister_hook(Snapshot snapshot)
{
	/*
	 * Skip if it was already removed during transaction abort.
	 */
	if (snapshot->undoRegularLocationPhNode.ph_node.prev_or_parent != NULL ||
		&snapshot->undoRegularLocationPhNode.ph_node == retainUndoLocRegularHeap.ph_root)
		pairingheap_remove(&retainUndoLocRegularHeap, &snapshot->undoRegularLocationPhNode.ph_node);

	if (snapshot->undoSystemLocationPhNode.ph_node.prev_or_parent != NULL ||
		&snapshot->undoSystemLocationPhNode.ph_node == retainUndoLocSystemHeap.ph_root)
		pairingheap_remove(&retainUndoLocSystemHeap, &snapshot->undoSystemLocationPhNode.ph_node);
}

void
orioledb_snapshot_hook(Snapshot snapshot)
{
	UndoLocation lastUsedLocation,
				lastUsedUndoLocationWhenUpdatedMinLocation;
	OXid		curXmin;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	int			i;

	/*
	 * It means that there was a crash recovery and we need to cleanup. This
	 * is probably not the best place for this kind of work, but here we can
	 * do truncate of unlogged tables.
	 */
	if (*was_in_recovery &&
		!pg_atomic_exchange_u32(after_recovery_cleaned, true))
	{
		o_tables_drop_all_temporary();
	}

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		UndoLogType undoType = (UndoLogType) i;
		UndoMeta   *meta = get_undo_meta_by_type(undoType);

		lastUsedLocation = pg_atomic_read_u64(&meta->lastUsedLocation);
		lastUsedUndoLocationWhenUpdatedMinLocation = pg_atomic_read_u64(&meta->lastUsedUndoLocationWhenUpdatedMinLocation);
		if (lastUsedLocation - lastUsedUndoLocationWhenUpdatedMinLocation > o_undo_circular_sizes[(int) undoType] / 10)
			update_min_undo_locations(undoType, false, true);
	}


	snapshot->undoRegularLocationPhNode.undoLocation = set_my_retain_location(UndoLogRegular);
	snapshot->undoSystemLocationPhNode.undoLocation = set_my_retain_location(UndoLogSystem);
	snapshot->undoRegularLocationPhNode.xmin = snapshot->undoSystemLocationPhNode.xmin = pg_atomic_read_u64(&xid_meta->runXmin);
	curXmin = pg_atomic_read_u64(&curProcData->xmin);
	if (!OXidIsValid(curXmin))
		pg_atomic_write_u64(&curProcData->xmin, snapshot->undoRegularLocationPhNode.xmin);

	/*
	 * Snapshot CSN could be newer than retained location, not older.  Enforce
	 * this with barrier.
	 */
	pg_read_barrier();

	snapshot->csnSnapshotData.snapshotcsn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	snapshot->csnSnapshotData.xlogptr = InvalidXLogRecPtr;
	snapshot->csnSnapshotData.xmin = pg_atomic_read_u64(&xid_meta->runXmin);
}

static void
o_stub_item_callback(UndoLogType undoType, UndoLocation location,
					 UndoStackItem *baseItem, OXid oxid,
					 bool abort, bool changeCountsValid)
{
	Assert(abort);
	return;
}
