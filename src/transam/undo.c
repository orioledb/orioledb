/*-------------------------------------------------------------------------
 *
 * undo.c
 *		Implementation of OrioleDB undo log.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/transam/undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"

#include <unistd.h>

#include "orioledb.h"

#include "btree/scan.h"
#include "btree/undo.h"
#include "catalog/storage.h"
#include "catalog/o_sys_cache.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/o_buffers.h"
#include "utils/page_pool.h"
#include "utils/snapshot.h"
#include "utils/stopevent.h"
#include "replication/syncrep.h"
#include "rewind/rewind.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "storage/md.h"
#include "storage/proc.h"
#include "utils/memutils.h"


#define GET_UNDO_REC(undoType, loc) (o_undo_buffers[(int) (undoType)] + \
	(loc) % o_undo_circular_sizes[(int) (undoType)])

static int	undoLocCmp(const pairingheap_node *a, const pairingheap_node *b, void *arg);

static pairingheap retainUndoLocHeaps[(int) UndoLogsCount] =
{
	{
		&undoLocCmp, NULL, NULL
	},
	{
		&undoLocCmp, NULL, NULL
	},
	{
		&undoLocCmp, NULL, NULL
	}
};

/* A minimal subtransaciton id, where OrioleDB got involved */
static SubTransactionId minParentSubId = InvalidSubTransactionId;

typedef void (*UndoCallback) (UndoLogType undoType, UndoLocation location,
							  UndoStackItem *item, OXid oxid, bool abort,
							  bool changeCountsValid);

static void init_undo_meta(UndoMeta *meta, bool found);
static void o_stub_item_callback(UndoLogType undoType, UndoLocation location,
								 UndoStackItem *baseItem,
								 OXid oxid, bool abort,
								 bool changeCountsValid);
static void o_rewind_relfilenode_item_callback(UndoLogType undoType,
											   UndoLocation location,
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

typedef struct
{
	OnCommitUndoStackItem header;
	int			nCommitRels;
	int			nAbortRels;
	RelFileNode rels[FLEXIBLE_ARRAY_MEMBER];
} RewindRelFileNodeUndoStackItem;

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
	},
	{
		.type = RewindRelFileNodeUndoItemType,
		.callback = o_rewind_relfilenode_item_callback,
		.callOnCommit = true
	},
	{
		.type = SysCacheDeleteUndoItemType,
		.callback = o_sys_cache_delete_callback,
		.callOnCommit = false
	},
	{
		.type = InvalidateComparatorUndoItemType,
		.callback = o_invalidate_comparator_callback,
		.callOnCommit = true
	},
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

static Size reserved_undo_sizes[(int) UndoLogsCount] =
{
	0
};

static OBuffersDesc undoBuffersDesc =
{
	.singleFileSize = UNDO_FILE_SIZE,
	.filenameTemplate = {ORIOLEDB_UNDO_DATA_ROW_FILENAME_TEMPLATE, ORIOLEDB_UNDO_DATA_PAGE_FILENAME_TEMPLATE, ORIOLEDB_UNDO_SYSTEM_FILENAME_TEMPLATE},
	.groupCtlTrancheName = "undoBuffersGroupCtlTranche",
	.bufferCtlTrancheName = "undoBuffersCtlTranche"
};

static void wait_for_reserved_location(UndoLogType undoType,
									   UndoLocation undoLocationToWait);

/*
 * A sorted array comprising a map from CommandId to the UndoLocation of the
 * first undo record for that command.  It is used to determine visibility
 * within the same transaction and to detect "self-updated" tuples.  That is
 * a bit tricky, assuming PostgreSQL can switch execution between commands.
 * However, that could only happen for "subcommands," such as trigger
 * execution.  However, the execution of a command finishes after all of its
 * subcommands, so a comparison of undo positions should be fine for checking
 * if the given change belongs to some of the previous commands.
 */
typedef struct
{
	CommandId	cid;
	UndoLocation undoLocation;
} CommandIdInfo;

static CommandIdInfo commandInfosStatic[16];
static CommandIdInfo *commandInfos = commandInfosStatic;
static int	commandIndex = -1,
			commandInfosLength = lengthof(commandInfosStatic);
static CommandId currentCommandId;

Size
undo_shmem_needs(void)
{
	Size		size;
	double		regular_row_undo_circular_buffer_fraction;

	regular_row_undo_circular_buffer_fraction = 1.0 - regular_block_undo_circular_buffer_fraction - system_undo_circular_buffer_fraction;
	o_undo_circular_sizes[UndoLogRegular] = regular_row_undo_circular_buffer_fraction * undo_circular_buffer_size;
	o_undo_circular_sizes[UndoLogRegular] = Max(o_undo_circular_sizes[UndoLogRegular], max_procs * 2 * O_MAX_UNDO_RECORD_SIZE);
	o_undo_circular_sizes[UndoLogRegular] = CACHELINEALIGN(o_undo_circular_sizes[UndoLogRegular]);
	o_undo_circular_sizes[UndoLogRegularPageLevel] = regular_block_undo_circular_buffer_fraction * undo_circular_buffer_size;
	o_undo_circular_sizes[UndoLogRegularPageLevel] = Max(o_undo_circular_sizes[UndoLogRegularPageLevel], max_procs * 2 * O_MAX_UNDO_RECORD_SIZE);
	o_undo_circular_sizes[UndoLogRegularPageLevel] = CACHELINEALIGN(o_undo_circular_sizes[UndoLogRegularPageLevel]);
	o_undo_circular_sizes[UndoLogSystem] = system_undo_circular_buffer_fraction * undo_circular_buffer_size;
	o_undo_circular_sizes[UndoLogSystem] = Max(o_undo_circular_sizes[UndoLogSystem], max_procs * 2 * O_MAX_UNDO_RECORD_SIZE);
	o_undo_circular_sizes[UndoLogSystem] = CACHELINEALIGN(o_undo_circular_sizes[UndoLogSystem]);
	undoBuffersDesc.buffersCount = undo_buffers_count;

	size = CACHELINEALIGN(sizeof(UndoMeta) * (int) UndoLogsCount);
	size = add_size(size, CACHELINEALIGN(sizeof(PendingTruncatesMeta)));
	size = add_size(size, o_undo_circular_sizes[UndoLogRegular]);
	size = add_size(size, o_undo_circular_sizes[UndoLogRegularPageLevel]);
	size = add_size(size, o_undo_circular_sizes[UndoLogSystem]);
	size = add_size(size, o_buffers_shmem_needs(&undoBuffersDesc));

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
	ptr += o_undo_circular_sizes[UndoLogRegular];
	o_undo_buffers[UndoLogRegularPageLevel] = ptr;
	ptr += o_undo_circular_sizes[UndoLogRegularPageLevel];
	o_undo_buffers[UndoLogSystem] = ptr;
	ptr += o_undo_circular_sizes[UndoLogSystem];

	for (i = 0; i < (int) UndoLogsCount; i++)
		init_undo_meta(&undo_metas[i], found);

	o_buffers_shmem_init(&undoBuffersDesc, ptr, found);
	ptr += o_buffers_shmem_needs(&undoBuffersDesc);
	Assert(ptr - buf <= undo_shmem_needs());

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

	minRetainLocation = Min(pg_atomic_read_u64(enable_rewind ? &meta->minRewindRetainLocation : &meta->minProcRetainLocation),
							minRetainLocation);

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
		int64		persistStartNum,
					persistEndNum;
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

		/*---
		 * Ranges
		 *
		 * remove:
		 * - [oldCheckpointStartNum, oldCheckpointEndNum] - old
		 * - [oldCleanedNum, *)
		 *
		 * persist:
		 * - [newCheckpointStartNum, newCheckpointEndNum] - new
		 * - [newCleanedNum, *)
		 */

		Assert(oldCheckpointStartLocation <= newCheckpointStartLocation);
		Assert(oldCheckpointEndLocation <= newCheckpointEndLocation);
		Assert(oldCheckpointStartLocation <= oldCheckpointEndLocation);
		Assert(newCheckpointStartLocation <= newCheckpointEndLocation);
		Assert(oldCleanedNum <= newCleanedNum);

		/*
		 * Persist Ranges mutual arrangement:
		 *
		 * 1) Interleaved: start      end |---------|      |--------------->
		 * new          newCleanedNum
		 *
		 * 2) Overlapped: start          end |-------------| new
		 * |---------------> newCleanedNum
		 *
		 * 3) Overlapped: start      end |---------| new |--------------->
		 * newCleanedNum
		 */
		persistStartNum = Min(newCheckpointStartNum, newCleanedNum);

		/*
		 * Clear start segment
		 *
		 * oldCleanedNum |--------------------------------->
		 * XXXXXXXXXXXXXXX|------------------> persistStartNum
		 */
		if (oldCleanedNum < persistStartNum)
		{
			o_buffers_unlink_files_range(
										 &undoBuffersDesc, (uint32) undoType,
										 oldCleanedNum, persistStartNum - 1);
		}

		/*
		 * Clear start segment
		 *
		 * start          end |-------------| old
		 * XXXXXXXXXXXX|------------------> persistStartNum
		 */
		if (oldCheckpointStartNum < persistStartNum)
		{
			o_buffers_unlink_files_range(
										 &undoBuffersDesc, (uint32) undoType,
										 oldCheckpointStartNum, Min(persistStartNum - 1, oldCheckpointEndNum));
		}

		/*
		 * Clear interval segment
		 *
		 * start            end |---------------|XXXXXXXXXXX|--------------->
		 * new                  newCleanedNum
		 * |-------------------------------------------> oldCleanedNum
		 */
		persistEndNum = Max(oldCleanedNum, newCheckpointEndNum + 1);
		if (persistEndNum < newCleanedNum)
		{
			o_buffers_unlink_files_range(
										 &undoBuffersDesc, (uint32) undoType,
										 persistEndNum, newCleanedNum - 1);
		}

		/*
		 * Clear interval segment
		 *
		 * start            end |---------------|XXXXXXXXXXX|--------------->
		 * new                  newCleanedNum start end
		 * |------------------------------| old
		 */
		persistEndNum = Max(oldCheckpointStartNum, newCheckpointEndNum + 1);
		if (persistEndNum < newCleanedNum)
		{
			o_buffers_unlink_files_range(
										 &undoBuffersDesc, (uint32) undoType,
										 persistEndNum, Min(newCleanedNum - 1, oldCheckpointEndNum));
		}
	}
}

/*
 * Guarantees that concurrent update_min_undo_locations() finishes.
 */
static void
wait_for_even_min_undo_locations_changecount(UndoMeta *meta)
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

/*
 * Guarantees that concurrent update_min_undo_locations() finishes.
 */
static void
wait_for_even_write_in_progress_changecount(UndoMeta *meta)
{
	SpinDelayStatus status;

	init_local_spin_delay(&status);
	while (meta->writeInProgressChangeCount & 1)
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
	bool		overwriteTransactionRetainUndoLoc;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);
	UndoRetainSharedLocations *shared = &curProcData->undoRetainLocations[undoType];

	Assert(!UndoLocationIsValid(pg_atomic_read_u64(&shared->reservedUndoLocation)));

	/*
	 * If transactionUndoRetainLocation is invalid on start - overwrite it on
	 * continue, else if transactionUndoRetainLocation is valid on start - do
	 * not modify.
	 */
	overwriteTransactionRetainUndoLoc = !UndoLocationIsValid(pg_atomic_read_u64(&shared->transactionUndoRetainLocation));

	while (true)
	{
		lastUsedLocation = pg_atomic_read_u64(&meta->lastUsedLocation);

		pg_atomic_write_u64(&shared->reservedUndoLocation, lastUsedLocation);

		if (overwriteTransactionRetainUndoLoc)
			pg_atomic_write_u64(&shared->transactionUndoRetainLocation, lastUsedLocation);

		wait_for_even_min_undo_locations_changecount(meta);

		/*
		 * Retry if minimal positions run higher due to concurrent
		 * update_min_undo_locations(). Protection for write operation:
		 * prevent starting write op on busy locations.
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

		wait_for_even_min_undo_locations_changecount(meta);

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

static void
wait_for_reserved_location(UndoLogType undoType,
						   UndoLocation undoLocationToWait)
{
	SpinDelayStatus delay;
	bool		delay_inited;
	int			i;

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
		buf->length = pg_nextpower2_32(itemSize);
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

	release_reserved_undo_location(undoType);
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

UndoLocation
walk_undo_range_with_buf(UndoLogType undoType,
						 UndoLocation location, UndoLocation toLoc,
						 OXid oxid, bool abort_val, UndoLocation *onCommitLocation,
						 bool changeCountsValid)
{
	UndoItemBuf buf;

	ASAN_UNPOISON_MEMORY_REGION(&buf, sizeof(buf));

	init_undo_item_buf(&buf);
	location = walk_undo_range(undoType, location, toLoc, &buf, oxid, abort_val,
							   onCommitLocation, changeCountsValid);
	free_undo_item_buf(&buf);
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

	if (!abortTrx)
	{
		/*
		 * One could only do the "on commit" action for the whole transaction
		 * chain.
		 */
		Assert(!toLocation);
		location = pg_atomic_read_u64(&sharedLocations->onCommitLocation);

		location = walk_undo_range_with_buf(undoType, location, InvalidUndoLocation,
											oxid, false, NULL,
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
		location = walk_undo_range_with_buf(undoType, location,
											toLocation ? toLocation->location : InvalidUndoLocation,
											oxid, true, &newOnCommitLocation,
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

bool
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
		if (!waitForUndoLocation)
			return false;

		wait_for_reserved_location(undoType, location);

		*minProcReservedLocation = pg_atomic_read_u64(&meta->minProcReservedLocation);
		if (location <= *minProcReservedLocation + o_undo_circular_sizes[(int) undoType])
			return true;

		update_min_undo_locations(undoType, false, waitForUndoLocation);
		*minProcReservedLocation = pg_atomic_read_u64(&meta->minProcReservedLocation);
	}

	return true;
}

static void
write_undo_range(OBuffersDesc *desc, Pointer buf, UndoLogType undoType,
				 UndoLocation minLoc, UndoLocation maxLoc)
{
	if (maxLoc > minLoc)
		o_buffers_write(desc, buf, (uint32) undoType, minLoc, maxLoc - minLoc);
}

static void
read_undo_range(OBuffersDesc *desc, Pointer buf, UndoLogType undoType,
				UndoLocation minLoc, UndoLocation maxLoc)
{
	Assert(maxLoc > minLoc);
	o_buffers_read(desc, buf, (uint32) undoType, minLoc, maxLoc - minLoc);
}

/*
 * Evict some part of undo to the disk.
 */
void
evict_undo_to_disk(UndoLogType undoType,
				   UndoLocation targetUndoLocation,
				   UndoLocation minProcReservedLocation,
				   bool attempt)
{
	UndoLocation retainUndoLocation,
				writtenLocation,
				tmpLocation;
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

	Assert((meta->writeInProgressChangeCount & 1) == 0);
	meta->writeInProgressChangeCount++;

	pg_write_barrier();

	update_min_undo_locations(undoType, true, false);

	(void) check_reserved_undo_location(undoType, minProcReservedLocation,
										&tmpLocation, true);

	retainUndoLocation = pg_atomic_read_u64(enable_rewind ? &meta->minRewindRetainLocation : &meta->minProcRetainLocation);

	if (targetUndoLocation <= retainUndoLocation ||
		targetUndoLocation <= pg_atomic_read_u64(&meta->writtenLocation))
	{
		/* We don't have to really write undo. */
		if (pg_atomic_read_u64(&meta->writeInProgressLocation) < retainUndoLocation)
		{
			pg_atomic_write_u64(&meta->writeInProgressLocation, retainUndoLocation);
			pg_atomic_write_u64(&meta->writtenLocation, retainUndoLocation);
		}

		pg_write_barrier();

		meta->writeInProgressChangeCount++;
		Assert((meta->writeInProgressChangeCount & 1) == 0);

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

	pg_write_barrier();

	meta->writeInProgressChangeCount++;
	Assert((meta->writeInProgressChangeCount & 1) == 0);

	SpinLockRelease(&meta->minUndoLocationsMutex);

	minProcReservedLocation = pg_atomic_read_u64(&meta->minProcReservedLocation);
	if (minProcReservedLocation < targetUndoLocation)
		wait_for_reserved_location(undoType, targetUndoLocation + circularBufferSize);


	if (retainUndoLocation % circularBufferSize <
		targetUndoLocation % circularBufferSize)
	{
		write_undo_range(&undoBuffersDesc,
						 circularBuffer + retainUndoLocation % circularBufferSize,
						 undoType,
						 retainUndoLocation, targetUndoLocation);
	}
	else
	{
		UndoLocation breakUndoLocation;

		breakUndoLocation = retainUndoLocation + (circularBufferSize -
												  (retainUndoLocation % circularBufferSize));
		write_undo_range(&undoBuffersDesc,
						 circularBuffer + retainUndoLocation % circularBufferSize,
						 undoType,
						 retainUndoLocation, breakUndoLocation);
		write_undo_range(&undoBuffersDesc,
						 circularBuffer, undoType,
						 breakUndoLocation, targetUndoLocation);
	}

	SpinLockAcquire(&meta->minUndoLocationsMutex);
	Assert(targetUndoLocation >= pg_atomic_read_u64(&meta->writtenLocation));
	pg_atomic_write_u64(&meta->writtenLocation, targetUndoLocation);
	SpinLockRelease(&meta->minUndoLocationsMutex);

	LWLockRelease(&meta->undoWriteLock);
}

bool
reserve_undo_size_extended(UndoLogType undoType, Size size,
						   bool waitForUndoLocation)
{
	UndoLocation location;
	uint64		minProcReservedLocation;
	UndoMeta   *meta = get_undo_meta_by_type(undoType);
	Size		circularBufferSize = o_undo_circular_sizes[(int) undoType];
	ODBProcData *curProcData PG_USED_FOR_ASSERTS_ONLY = GET_CUR_PROCDATA();

	Assert(!waitForUndoLocation || !have_locked_pages());
	Assert(undoType != UndoLogNone);
	Assert(size > 0);
	Assert(pg_atomic_read_u64(&curProcData->undoRetainLocations[(int) undoType].reservedUndoLocation) == InvalidUndoLocation);

	if (reserved_undo_sizes[(int) undoType] >= size)
		return true;

	size -= reserved_undo_sizes[(int) undoType];

	location = pg_atomic_fetch_add_u64(&meta->advanceReservedLocation, size);
	reserved_undo_sizes[(int) undoType] += size;

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
		reserved_undo_sizes[(int) undoType] -= size;
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
		reserved_undo_sizes[(int) undoType] -= size;
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

		/* TODO: consider removing lock if assers are disabled */
		SpinLockAcquire(&meta->minUndoLocationsMutex);
		Assert(location + size <= pg_atomic_read_u64(&meta->writtenLocation) + circularBufferSize);
		SpinLockRelease(&meta->minUndoLocationsMutex);
		return true;
	}

	evict_undo_to_disk(undoType, location + size - circularBufferSize,
					   minProcReservedLocation, false);
	Assert(location + size <= pg_atomic_read_u64(&meta->writtenLocation) + circularBufferSize);

	return true;
}

/*
 * "Owns" undo size reserved by another process.  That process is intended to
 * call giveup_reserved_undo_size().
 */
void
steal_reserved_undo_size(UndoLogType undoType, Size size)
{
	Assert(undoType != UndoLogNone);

	reserved_undo_sizes[(int) undoType] += size;
}

/*
 * "Forgets" reserved by this process, because another process calls
 * steal_reserved_undo_size().
 */
void
giveup_reserved_undo_size(UndoLogType undoType)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	Assert(undoType != UndoLogNone);

	reserved_undo_sizes[(int) undoType] = 0;
	pg_atomic_write_u64(&curProcData->undoRetainLocations[(int) undoType].reservedUndoLocation,
						InvalidUndoLocation);
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

		/* TODO: consider removing lock if assers are disabled */
		SpinLockAcquire(&meta->minUndoLocationsMutex);
		Assert(toLoc <= pg_atomic_read_u64(&meta->writtenLocation));
		SpinLockRelease(&meta->minUndoLocationsMutex);
	}
	else
	{
		evict_undo_to_disk(undoType, toLoc, minProcReservedLocation, false);
	}

	o_buffers_sync(&undoBuffersDesc, (uint32) undoType,
				   fromLoc, toLoc, wait_event_info);
}

Pointer
get_undo_record(UndoLogType undoType, UndoLocation *undoLocation, Size size)
{
	UndoMeta   *meta = get_undo_meta_by_type(undoType);
	Size		circularBufferSize = o_undo_circular_sizes[(int) undoType];

	Assert(size == MAXALIGN(size) && size <= O_MAX_UNDO_RECORD_SIZE);
	Assert(undoType != UndoLogNone);

	set_my_reserved_location(undoType);

	pg_write_barrier();

	while (true)
	{
		UndoLocation location;

		if (reserved_undo_sizes[(int) undoType] < size)
			elog(PANIC, "get_undo_record(): not enough reserved undo (undoType: %d, reservedSize %u, requestedSize: %u",
				 (int) undoType,
				 (unsigned int) reserved_undo_sizes[(int) undoType],
				 (unsigned int) size);

		location = pg_atomic_fetch_add_u64(&meta->lastUsedLocation, size);
		reserved_undo_sizes[(int) undoType] -= size;

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
	Assert(reserved_undo_sizes[(int) type] == 0);

	reserve_undo_size(type, 2 * size);
	return get_undo_record(type, undoLocation, size);
}

void
release_undo_size(UndoLogType undoType)
{
	ODBProcData *curProcData PG_USED_FOR_ASSERTS_ONLY = GET_CUR_PROCDATA();
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	Assert(undoType != UndoLogNone);
	Assert(pg_atomic_read_u64(&curProcData->undoRetainLocations[(int) undoType].reservedUndoLocation) == InvalidUndoLocation);

	if (reserved_undo_sizes[(int) undoType] != 0)
	{
		pg_atomic_fetch_sub_u64(&meta->advanceReservedLocation, reserved_undo_sizes[(int) undoType]);
		reserved_undo_sizes[(int) undoType] = 0;
	}
}

Size
get_reserved_undo_size(UndoLogType undoType)
{
	Assert(undoType != UndoLogNone);

	return reserved_undo_sizes[(int) undoType];
}

void
release_reserved_undo_location(UndoLogType undoType)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();

	pg_atomic_write_u64(&curProcData->undoRetainLocations[(int) undoType].reservedUndoLocation,
						InvalidUndoLocation);
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

	release_reserved_undo_location(undoType);
}

void
add_new_undo_stack_item_to_process(UndoLogType undoType,
								   UndoLocation location,
								   int pgprocno, LocalTransactionId localXid)
{
	UndoStackItem *item = (UndoStackItem *) GET_UNDO_REC(undoType, location);
	UndoStackSharedLocations *sharedLocations;
	UndoItemTypeDescr *descr PG_USED_FOR_ASSERTS_ONLY = item_type_get_descr(item->type);

	Assert(!descr->callOnCommit);

	sharedLocations = &oProcData[pgprocno].undoStackLocations[localXid % PROC_XID_ARRAY_SIZE][undoType];
	item->prev = pg_atomic_read_u64(&sharedLocations->location);
	pg_atomic_write_u64(&sharedLocations->location, location);

	release_reserved_undo_location(undoType);
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

void
get_cur_undo_locations(UndoStackLocations *locations, UndoLogType undoType)
{
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);

	read_shared_undo_locations(locations, sharedLocations);
}

void
set_cur_undo_locations(UndoLogType undoType, UndoStackLocations locations)
{
	UndoStackSharedLocations *sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);

	write_shared_undo_locations(sharedLocations, &locations);
}

void
reset_cur_undo_locations(void)
{
	UndoStackLocations location = {InvalidUndoLocation, InvalidUndoLocation, InvalidUndoLocation, InvalidUndoLocation};
	int			i;

	for (i = 0; i < (int) UndoLogsCount; i++)
		set_cur_undo_locations((UndoLogType) i, location);
}

#define RetainUndoLocationPHNodeGetSnapshot(location, undoType) \
	(Snapshot) ((Pointer) (location) - offsetof(SnapshotData, undoRegularRowLocationPhNode) - sizeof(RetainUndoLocationPHNode) * (int) (undoType))

void
orioledb_reset_xmin_hook(void)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	RetainUndoLocationPHNode *location;
	OXid		xmin = InvalidOXid;
	int			i;

	if (ActiveSnapshotSet())
		return;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		UndoLogType undoType = (UndoLogType) i;

		if (pairingheap_is_empty(&retainUndoLocHeaps[undoType]))
		{
			pg_atomic_write_u64(&curProcData->undoRetainLocations[undoType].snapshotRetainUndoLocation, InvalidUndoLocation);
		}
		else
		{
			Snapshot	snapshot;

			location = pairingheap_container(RetainUndoLocationPHNode, ph_node,
											 pairingheap_first(&retainUndoLocHeaps[undoType]));
			snapshot = RetainUndoLocationPHNodeGetSnapshot(location, undoType);
			if (location->undoLocation > pg_atomic_read_u64(&curProcData->undoRetainLocations[undoType].snapshotRetainUndoLocation))
				pg_atomic_write_u64(&curProcData->undoRetainLocations[undoType].snapshotRetainUndoLocation, location->undoLocation);
			if (!OXidIsValid(xmin) || snapshot->csnSnapshotData.xmin < xmin)
				xmin = snapshot->csnSnapshotData.xmin;
		}
	}

	if (xmin > pg_atomic_read_u64(&curProcData->xmin))
		pg_atomic_write_u64(&curProcData->xmin, xmin);
}

static void
rewind_handle_pending_deletes(void)
{
	RelFileNode *onCommitRels,
			   *onAbortRels;
	int			nOnCommitRels,
				nOnAbortRels;

	nOnCommitRels = smgrGetPendingDeletes(true, &onCommitRels);
	nOnAbortRels = smgrGetPendingDeletes(false, &onAbortRels);

	if (nOnCommitRels + nOnAbortRels > 0)
		o_add_rewind_relfilenode_undo_item(onCommitRels,
										   onAbortRels,
										   nOnCommitRels,
										   nOnAbortRels);

	if (onCommitRels)
		pfree(onCommitRels);
	if (onAbortRels)
		pfree(onAbortRels);
	PostPrepare_smgr();
}

void
undo_xact_callback(XactEvent event, void *arg)
{
	OXid		oxid = get_current_oxid_if_any();
	CommitSeqNo csn;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	bool		isParallelWorker;
	int			i;
	TransactionId xid1 = InvalidTransactionId;
	int			nsubxids = 0;
	TransactionId *subxids = NULL;
	TransactionId heapXid;
	XLogRecPtr	flushPos;
	LogicalXidCtx logicalXidContext;

	/* elog(LOG, "UNDO XACT CALLBACK"); */
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

	if (enable_rewind && event == XACT_EVENT_PRE_COMMIT)
	{
		save_precommit_xid_subxids();
		rewind_handle_pending_deletes();
	}

	if (!OXidIsValid(oxid) || isParallelWorker)
	{
		if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
		{
			reset_cur_undo_locations();
			orioledb_reset_xmin_hook();
			reset_command_undo_locations();
			oxid_needs_wal_flush = false;
			minParentSubId = InvalidSubTransactionId;
		}

		if (enable_rewind && event == XACT_EVENT_COMMIT)
		{
			xid1 = get_precommit_xid_subxids(&nsubxids, &subxids);
			if (TransactionIdIsValid(xid1))
			{
				elog(DEBUG3, "ADD_TO_REWIND_BUFFER_HEAP");
				add_to_rewind_buffer(oxid, xid1, nsubxids, subxids);
				reset_precommit_xid_subxids();
			}
		}
	}
	else
	{
		heapXid = GetTopTransactionIdIfAny();

		flushPos = InvalidXLogRecPtr;
		get_current_logical_xid_ctx(&logicalXidContext);

		if (TransactionIdIsValid(heapXid) && TransactionIdIsValid(logicalXidContext.xid))
		{
			if (event == XACT_EVENT_PRE_COMMIT)
			{
				if (!logicalXidContext.useHeap)
				{
					elog(DEBUG4, "event %d oxid %lu SWITCH_LOGICAL_XID O2H heap xid %u -> oriole xid %u",
						 event, oxid, heapXid, logicalXidContext.xid);

					add_switch_logical_xid_wal_record(heapXid, logicalXidContext.xid);
				}
			}
		}
		else
		{
			if (TransactionIdIsValid(heapXid))
			{
				elog(DEBUG4, "event %d oxid %lu top heapXid %u independent heap transaction",
					 event, oxid, heapXid);
			}
			else if (TransactionIdIsValid(logicalXidContext.xid))
			{
				elog(DEBUG4, "event %d oxid %lu logicalXid %u independent Oriole transaction",
					 event, oxid, logicalXidContext.xid);
			}
		}

		/*
		 * Transaction cases:
		 *
		 * h - h : independent heap transaction
		 *
		 * o - o : independent Oriole transaction
		 *
		 * h - o - o - h : SWITCH_LOGICAL_XID H2O: Oriole txn acts as a
		 * sub-txn of a top heap txn
		 *
		 * o - h - o - h : SWITCH_LOGICAL_XID O2H: Oriole txn acts as a
		 * sub-txn of a top heap txn
		 */

		Assert(!RecoveryInProgress());
		switch (event)
		{
			case XACT_EVENT_PRE_COMMIT:

				/*
				 * PRE_COMMIT means that Oriole transaction is going to be
				 * commited BEFORE corresponding heap transaction. This can
				 * only happen in a case when Oriole transaction acts as
				 * sub-transaction of heap transaction. This is the case for
				 * SWITCH_LOGICAL_XID.
				 */

				elog(DEBUG4, "XACT_EVENT_PRE_COMMIT oxid %lu logicalXid %u top heapXid %u current heapXid %u useHeap %d",
					 oxid, logicalXidContext.xid, heapXid, GetCurrentTransactionIdIfAny(), logicalXidContext.useHeap);

				if (!TransactionIdIsValid(heapXid))
				{
					current_oxid_xlog_precommit();
				}

				if (TransactionIdIsValid(logicalXidContext.xid) && TransactionIdIsValid(heapXid))
				{
					Assert(logicalXidContext.xid != heapXid);

					elog(DEBUG4, "XACT_EVENT_PRE_COMMIT wal_joint_commit for SWITCH_LOGICAL_XID %s oxid %lu logicalXid %u top heapXid %u current heapXid %u",
						 logicalXidContext.useHeap ? "H2O" : "O2H", oxid, logicalXidContext.xid, heapXid, GetCurrentTransactionIdIfAny());

					wal_joint_commit(oxid,
									 get_current_logical_xid(),
									 heapXid);
				}

				break;

			case XACT_EVENT_COMMIT:

				elog(DEBUG4, "XACT_EVENT_COMMIT oxid %lu logicalXid %u top heapXid %u current heapXid %u useHeap %d",
					 oxid, logicalXidContext.xid, heapXid, GetCurrentTransactionIdIfAny(), logicalXidContext.useHeap);

				if (!TransactionIdIsValid(heapXid))
				{
					/* Commit o - o : independent Oriole transaction */

					elog(DEBUG4, "XACT_EVENT_COMMIT [independent Oriole transaction] oxid %lu logicalXid %u top heapXid %u current heapXid %u useHeap %d",
						 oxid, logicalXidContext.xid, heapXid, GetCurrentTransactionIdIfAny(), logicalXidContext.useHeap);

					current_oxid_xlog_precommit();

					flushPos = wal_commit(oxid, get_current_logical_xid(),
										  false);
					set_oxid_xlog_ptr(oxid, flushPos);

					/* Flush WAL if needed */
					if (!XLogRecPtrIsInvalid(flushPos) &&
						(synchronous_commit > SYNCHRONOUS_COMMIT_OFF ||
						 oxid_needs_wal_flush))
						XLogFlush(flushPos);

					/* Wait for synchronous replication if needed */
					if (!XLogRecPtrIsInvalid(flushPos))
						SyncRepWaitForLSN(flushPos, true);
				}
				else
				{
					Assert(TransactionIdIsValid(heapXid));

					/*
					 * Case h - h : independent heap transaction
					 */

					set_oxid_xlog_ptr(oxid, XactLastCommitEnd);
				}

				current_oxid_precommit();
				csn = GetCurrentCSN();
				if (csn == COMMITSEQNO_INPROGRESS)
					csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);

				current_oxid_commit(csn);
				Assert(enable_rewind || !csn_is_retained_for_rewind(csn));

				if (enable_rewind)
				{
					elog(DEBUG3, "ADD_TO_REWIND_BUFFER_ORIOLE");
					xid1 = get_precommit_xid_subxids(&nsubxids, &subxids);

					add_to_rewind_buffer(oxid, xid1, nsubxids, subxids);
					reset_precommit_xid_subxids();
				}

				for (i = 0; i < (int) UndoLogsCount; i++)
					on_commit_undo_stack((UndoLogType) i, oxid, true);

				wal_after_commit();
				reset_cur_undo_locations();
				reset_command_undo_locations();
				oxid_needs_wal_flush = false;
				minParentSubId = InvalidSubTransactionId;
				/* TODO: Find a better place or add a hook at the end of heap_truncate_one_rel */
				in_nontransactional_truncate = false;

				break;

			case XACT_EVENT_ABORT:

				elog(DEBUG4, "XACT_EVENT_ABORT oxid %lu logicalXid %u top heapXid %u current heapXid %u useHeap %d",
					 oxid, logicalXidContext.xid, heapXid, GetCurrentTransactionIdIfAny(), logicalXidContext.useHeap);

				if (!RecoveryInProgress())
					wal_rollback(oxid, logicalXidContext.xid, false);

				for (i = 0; i < (int) UndoLogsCount; i++)
					apply_undo_stack((UndoLogType) i, oxid, NULL, true);

				reset_cur_undo_locations();
				reset_command_undo_locations();
				current_oxid_abort();
				set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr);
				oxid_needs_wal_flush = false;
				/* TODO: Find a better place or add a hook at the end of heap_truncate_one_rel */
				in_nontransactional_truncate = false;

				/*
				 * Remove registered snapshot one-by-one, so that we can avoid
				 * double removing in undo_snapshot_deregister_hook().
				 */
				for (i = 0; i < (int) UndoLogsCount; i++)
					while (!pairingheap_is_empty(&retainUndoLocHeaps[i]))
						pairingheap_remove_first(&retainUndoLocHeaps[i]);

				for (i = 0; i < (int) UndoLogsCount; i++)
					pg_atomic_write_u64(&curProcData->undoRetainLocations[i].snapshotRetainUndoLocation, InvalidUndoLocation);

				minParentSubId = InvalidSubTransactionId;

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

		if (undoType == UndoLogRegularPageLevel)
			continue;

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

	Assert(undoType != UndoLogRegularPageLevel);

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

	if (undoType == UndoLogRegularPageLevel)
		return;

	if (parentSubid != InvalidSubTransactionId)
	{
		sharedLocations = GET_CUR_UNDO_STACK_LOCATIONS(undoType);
		init_undo_item_buf(&buf);
		location = pg_atomic_read_u64(&sharedLocations->subxactLocation);
		applyResult = search_for_undo_sub_location(undoType, kind, location, &buf, parentSubid,
												   &toLoc.location, &toLoc.subxactLocation);
		free_undo_item_buf(&buf);

		if (!applyResult)
			return;
	}
	else
	{
		toLoc.location = InvalidUndoLocation;
		toLoc.subxactLocation = InvalidUndoLocation;

	}

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

		if (undoType == UndoLogRegularPageLevel)
			continue;

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
	LogicalXidCtx logicalXidContext;

	/*
	 * Cleanup EXPLAY ANALYZE counters pointer to handle case when execution
	 * of node was interrupted by exception.
	 */
	ea_counters = NULL;

	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:

			if (have_retained_undo_location())
			{
				(void) get_current_oxid();
				add_subxact_undo_item(parentSubid);
				prentLogicalXid = GetTopTransactionId();
				assign_subtransaction_logical_xid();
				add_savepoint_wal_record(parentSubid, prentLogicalXid);
				if (minParentSubId == InvalidSubTransactionId)
					minParentSubId = parentSubid;
			}

			break;

		case SUBXACT_EVENT_COMMIT_SUB:
			if (parentSubid >= minParentSubId && minParentSubId != InvalidSubTransactionId)
				update_subxact_undo_location_on_commit(parentSubid);
			break;

		case SUBXACT_EVENT_ABORT_SUB:
			if (parentSubid < minParentSubId || minParentSubId == InvalidSubTransactionId)
				parentSubid = InvalidSubTransactionId;

			if (have_retained_undo_location())
			{
				(void) get_current_oxid();
				for (i = 0; i < (int) UndoLogsCount; i++)
					rollback_to_savepoint((UndoLogType) i, UndoStackFull,
										  parentSubid, true);

				get_current_logical_xid_ctx(&logicalXidContext);
				if (TransactionIdIsValid(logicalXidContext.xid))
				{
					if (!RecoveryInProgress())
					{
						add_rollback_to_savepoint_wal_record(parentSubid);
					}
				}

				/*
				 * It might happen that we've released some row-level locks.
				 * Some waiters must be woken up.  We currently can't
				 * distinguish them and just wake up everybody.
				 */
				oxid_notify_all();
			}

			break;

		default:
			break;
	}

	oxid_subxact_callback(event, mySubid, parentSubid, arg);
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
		UndoStackLocations locations;

		get_cur_undo_locations(&locations, undoType);

		return (!UndoLocationIsValid(locations.location));
	}
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

	state->needs_wal_flush = oxid_needs_wal_flush;
	state->oxid = get_current_oxid();
	get_current_logical_xid_ctx(&state->logicalXidContext);
	for (i = 0; i < (int) UndoLogsCount; i++)
		state->has_retained_undo_location[i] = undo_type_has_retained_location((UndoLogType) i);
	state->local_wal_has_material_changes = get_local_wal_has_material_changes();

	if (!is_recovery_process() && !local_wal_is_empty())
		flush_local_wal(false, false);

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

		if (!is_recovery_process())
			wal_rollback(oxid, get_current_logical_xid(), true);
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
	set_current_logical_xid(&state->logicalXidContext);
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

		if (!is_recovery_process())
			wal_commit(oxid, get_current_logical_xid(), true);

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
	set_current_logical_xid(&state->logicalXidContext);
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
			read_undo_range(&undoBuffersDesc, buf, undoType, location,
							Min(location + size, writtenLocation));
	}
	else
	{
		read_undo_range(&undoBuffersDesc, buf, undoType, location,
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
				memoryUndoLocation;
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	UndoRetainSharedLocations *sharedLocations = &curProcData->undoRetainLocations[(int) undoType];
	UndoMeta   *meta = get_undo_meta_by_type(undoType);

	Assert(location >= pg_atomic_read_u64(&sharedLocations->snapshotRetainUndoLocation) ||
		   location >= pg_atomic_read_u64(&sharedLocations->transactionUndoRetainLocation) ||
		   (location >= pg_atomic_read_u64(&meta->checkpointRetainStartLocation) &&
			location < pg_atomic_read_u64(&meta->checkpointRetainEndLocation)));

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
		Assert(pg_atomic_read_u64(&sharedLocations->reservedUndoLocation) == InvalidUndoLocation);
		pg_atomic_write_u64(&sharedLocations->reservedUndoLocation, memoryUndoLocation);

		pg_memory_barrier();

		/*
		 * This ensures there is no concurrent process updating
		 * writeInProgressLocation.  After this point, anybody trying to
		 * update writeInProgressLocation will notice our
		 * reservedUndoLocation.
		 */
		wait_for_even_write_in_progress_changecount(meta);

		/* Recheck if writeInProgressLocation was advanced concurrently */
		writeInProgressLocation = pg_atomic_read_u64(&meta->writeInProgressLocation);
		if (writeInProgressLocation > memoryUndoLocation)
		{
			pg_atomic_write_u64(&sharedLocations->reservedUndoLocation, InvalidUndoLocation);
			continue;
		}

		/*
		 * At this point we should either detect concurrent writing of undo
		 * log. Or concurrent writing of undo log should wait for our reserved
		 * location.  So, it should be safe to write to the memory.
		 */
		memcpy(GET_UNDO_REC(undoType, memoryUndoLocation),
			   buf + (memoryUndoLocation - location),
			   size - (memoryUndoLocation - location));
		break;
	}

	pg_atomic_write_u64(&sharedLocations->reservedUndoLocation, InvalidUndoLocation);

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
	write_undo_range(&undoBuffersDesc, buf, undoType,
					 location, memoryUndoLocation);
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
	pairingheap_add(&retainUndoLocHeaps[UndoLogRegular], &snapshot->undoRegularRowLocationPhNode.ph_node);
	pairingheap_add(&retainUndoLocHeaps[UndoLogRegularPageLevel], &snapshot->undoRegularPageLocationPhNode.ph_node);
	pairingheap_add(&retainUndoLocHeaps[UndoLogSystem], &snapshot->undoSystemLocationPhNode.ph_node);
}

void
undo_snapshot_deregister_hook(Snapshot snapshot)
{
	/*
	 * Skip if it was already removed during transaction abort.
	 */
	if (snapshot->undoRegularRowLocationPhNode.ph_node.prev_or_parent != NULL ||
		&snapshot->undoRegularRowLocationPhNode.ph_node == retainUndoLocHeaps[UndoLogRegular].ph_root)
		pairingheap_remove(&retainUndoLocHeaps[UndoLogRegular], &snapshot->undoRegularRowLocationPhNode.ph_node);

	if (snapshot->undoRegularPageLocationPhNode.ph_node.prev_or_parent != NULL ||
		&snapshot->undoRegularPageLocationPhNode.ph_node == retainUndoLocHeaps[UndoLogRegularPageLevel].ph_root)
		pairingheap_remove(&retainUndoLocHeaps[UndoLogRegularPageLevel], &snapshot->undoRegularPageLocationPhNode.ph_node);

	if (snapshot->undoSystemLocationPhNode.ph_node.prev_or_parent != NULL ||
		&snapshot->undoSystemLocationPhNode.ph_node == retainUndoLocHeaps[UndoLogSystem].ph_root)
		pairingheap_remove(&retainUndoLocHeaps[UndoLogSystem], &snapshot->undoSystemLocationPhNode.ph_node);
}

void
orioledb_snapshot_hook(Snapshot snapshot)
{
	UndoLocation lastUsedLocation,
				lastUsedUndoLocationWhenUpdatedMinLocation;
	OXid		curXmin,
				xmin;
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
		o_tables_truncate_all_unlogged();
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


	snapshot->undoRegularRowLocationPhNode.undoLocation = set_my_retain_location(UndoLogRegular);
	snapshot->undoRegularPageLocationPhNode.undoLocation = set_my_retain_location(UndoLogRegularPageLevel);
	snapshot->undoSystemLocationPhNode.undoLocation = set_my_retain_location(UndoLogSystem);
	xmin = pg_atomic_read_u64(&xid_meta->runXmin);
	curXmin = pg_atomic_read_u64(&curProcData->xmin);
	if (!OXidIsValid(curXmin))
		pg_atomic_write_u64(&curProcData->xmin, xmin);

	/*
	 * Snapshot CSN could be newer than retained location, not older.  Enforce
	 * this with barrier.
	 */
	pg_read_barrier();

	snapshot->csnSnapshotData.snapshotcsn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	snapshot->csnSnapshotData.xlogptr = InvalidXLogRecPtr;
	snapshot->csnSnapshotData.xmin = xmin;
}

static void
o_stub_item_callback(UndoLogType undoType, UndoLocation location,
					 UndoStackItem *baseItem, OXid oxid,
					 bool abort, bool changeCountsValid)
{
	Assert(abort);
	return;
}

void
reset_command_undo_locations(void)
{
	commandIndex = -1;
	if (commandInfos != commandInfosStatic)
		pfree(commandInfos);
	commandInfos = commandInfosStatic;
	commandInfosLength = lengthof(commandInfosStatic);
}

/*
 * Return the undo location for the first entry of commandInfos whose cid is
 * greater than or equal to the requested `cid`.
 *
 * If every stored `cid` is smaller than the requested one,
 * `MaxUndoLocation` is returned.
 */
CommandId
undo_location_get_command(UndoLocation location)
{
	int			lo = 0;			/* left bound (inclusive)  */
	int			hi = commandIndex + 1;	/* right bound (not inclusive) */

	/*
	 * XXX: Parallel workers don't have valid commandInfos array.  Do they
	 * need it?
	 */
	if (IsParallelWorker())
		return 0;

	Assert(commandIndex >= 0);

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (commandInfos[mid].undoLocation <= location)
			lo = mid + 1;
		else
			hi = mid;
	}
	lo--;
	Assert(lo >= 0 && lo <= commandIndex);
	return commandInfos[lo].cid;
}

UndoLocation
current_command_get_undo_location(void)
{
	CommandId	cid = o_get_current_command();

	if (commandIndex < 0 || commandInfos[commandIndex].cid != cid)
	{
		UndoLocation loc;

		(void) get_undo_record(UndoLogRegular, &loc, MAXIMUM_ALIGNOF);
		release_reserved_undo_location(UndoLogRegular);
		update_command_undo_location(cid, loc);
	}

	Assert(commandIndex >= 0 && commandInfos[commandIndex].cid == cid);
	return commandInfos[commandIndex].undoLocation;
}

void
update_command_undo_location(CommandId commandId, UndoLocation undoLocation)
{
	if (commandIndex < 0 || commandInfos[commandIndex].cid != commandId)
	{
		commandIndex++;
		if (commandIndex >= commandInfosLength)
		{
			if (commandInfos == commandInfosStatic)
			{
				commandInfosLength = 2 * lengthof(commandInfosStatic);
				commandInfos = MemoryContextAlloc(TopMemoryContext,
												  sizeof(*commandInfos) * commandInfosLength);
				memcpy(commandInfos, commandInfosStatic, sizeof(commandInfosStatic));
			}
			else
			{
				commandInfosLength *= 2;
				commandInfos = repalloc(commandInfos, sizeof(*commandInfos) * commandInfosLength);
			}
		}
		Assert(commandIndex < commandInfosLength);
		commandInfos[commandIndex].cid = commandId;
		commandInfos[commandIndex].undoLocation = undoLocation;
	}
}

void
o_set_current_command(CommandId commandId)
{
	currentCommandId = commandId;
}

CommandId
o_get_current_command(void)
{
	return currentCommandId;
}

static void
o_rewind_relfilenode_item_callback(UndoLogType undoType,
								   UndoLocation location,
								   UndoStackItem *baseItem,
								   OXid oxid, bool abort,
								   bool changeCountsValid)
{
	RewindRelFileNodeUndoStackItem *item = (RewindRelFileNodeUndoStackItem *) baseItem;

	if (enable_rewind && !is_rewind_worker())
		return;

	if (!abort)
		DropRelationFiles(item->rels, item->nCommitRels, false);
	else
		DropRelationFiles(&item->rels[item->nCommitRels], item->nAbortRels, false);
}

void
o_add_rewind_relfilenode_undo_item(RelFileNode *onCommit, RelFileNode *onAbort,
								   int nOnCommit, int nOnAbort)
{
	LocationIndex size;
	UndoLocation location;
	RewindRelFileNodeUndoStackItem *item;
	int			stepItemsCapacity = (O_MAX_UNDO_RECORD_SIZE - offsetof(RewindRelFileNodeUndoStackItem, rels)) / sizeof(RelFileNode);

	Assert(nOnCommit >= 0 && nOnAbort >= 0);

	while (nOnCommit + nOnAbort > 0)
	{
		int			stepOnCommit;
		int			stepOnAbort;

		stepOnCommit = Min(nOnCommit, stepItemsCapacity);
		stepOnAbort = Min(nOnAbort, stepItemsCapacity - stepOnCommit);

		size = offsetof(RewindRelFileNodeUndoStackItem, rels) + sizeof(RelFileNode) * (stepOnCommit + stepOnAbort);
		item = (RewindRelFileNodeUndoStackItem *) get_undo_record_unreserved(UndoLogSystem, &location, MAXALIGN(size));

		item->header.base.type = RewindRelFileNodeUndoItemType;
		item->header.base.itemSize = size;
		item->header.base.indexType = oIndexPrimary;

		item->nCommitRels = stepOnCommit;
		item->nAbortRels = stepOnAbort;

		memcpy(item->rels, onCommit, sizeof(RelFileNode) * stepOnCommit);
		memcpy(&item->rels[stepOnCommit], onAbort, sizeof(RelFileNode) * stepOnAbort);

		add_new_undo_stack_item(UndoLogSystem, location);

		release_undo_size(UndoLogSystem);

		onCommit += stepOnCommit;
		nOnCommit -= stepOnCommit;
		onAbort += stepOnAbort;
		nOnAbort -= stepOnAbort;
	}
}
