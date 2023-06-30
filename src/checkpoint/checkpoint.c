/*-------------------------------------------------------------------------
 *
 * checkpoint.c
 *		Routines for making checkpoints.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/checkpoint/checkpoint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>

#include "orioledb.h"

#include "btree/insert.h"
#include "btree/io.h"
#include "btree/merge.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "catalog/free_extents.h"
#include "catalog/o_indices.h"
#include "catalog/o_tables.h"
#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "checkpoint/checkpoint.h"
#include "recovery/internal.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/toast.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/seq_buf.h"
#include "utils/stopevent.h"
#include "utils/ucm.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#define CONTROL_FILENAME ORIOLEDB_DATA_DIR"/control"

/*
 * Single action in B-tree checkpoint loop.
 */
typedef enum WalkAction
{
	WalkUpwards,
	WalkDownwards,
	WalkContinue
} WalkAction;

typedef struct WalkMessage
{
	/* current action */
	WalkAction	action;
	union
	{
		struct
		{
			/* is we must mark upward page as dirty */
			bool		parentDirty;

			/*
			 * disk downlink to a written page, InvalidODiskDownlink if page
			 * was not written to disk
			 */
			uint64		diskDownlink;
			/* will be copied to upward level if needed */
			NextKeyType nextkeyType;
			OFixedKey	nextkey;
			/* is current internal tuple must be saved on image */
			bool		saveTuple;
		}			upwards;
		struct
		{
			/* page to process */
			OInMemoryBlkno blkno;
			uint32		pageChangeCount;
			/* lokey of downwards page */
			OFixedKey	lokey;
		}			downwards;
	}			content;
} WalkMessage;

typedef struct CheckpointWriteBack
{
	bool		isCompressed;
	int			extentsNumber;
	int			extentsAllocated;
	int			checkpointFlags;
	FileExtent *extents;
} CheckpointWriteBack;

typedef struct
{
	ORelOids	oids;
	OIndexType	type;
} IndexIdItem;

typedef struct
{
	List	   *cleanupMap;
	List	   *freeExtents;
	int			flags;
} CheckpointTablesArg;

typedef struct
{
	FileExtent *extents;
	int			size;
	int			allocated;
} FileExtentsArray;

/*
 * For a checkpoint image we can add only hikey
 * or downlink to the end of the image.
 */
typedef enum
{
	StackImageAddHikey,
	StackImageAddDownlink
} StackImageAddType;

typedef struct
{
	UndoStackItem header;
	bool		lock;			/* true for lock, false for unlock */
} SysTreesLockUndoStackItem;

CheckpointState *checkpoint_state = NULL;
MemoryContext chkp_mem_context = NULL;

static char *xidFilename = NULL;
static uint32 xidFileCheckpointnum = 0;
static File xidFile = -1;

static void init_writeback(CheckpointWriteBack *writeback, int flags, bool isCompressed);
static void writeback_put_extent(CheckpointWriteBack *writeback, FileExtent *extent);
static void perform_writeback(BTreeDescr *desc, CheckpointWriteBack *writeback);
static void free_writeback(CheckpointWriteBack *writeback);

static void write_checkpoint_control(CheckpointControl *control);
static uint64 append_file_contents(File target, char *source_filename, uint64 offset);
static uint64 finalize_chkp_map(File chkp_file, uint64 len,
								char *input_filename, uint64 input_offset,
								uint32 input_num);
static int	uint32_offsets_cmp(const void *a, const void *b);
static int	file_extents_len_off_cmp(const void *a, const void *b);
static int	file_extents_off_len_cmp(const void *a, const void *b);
static int	file_extents_writeback_cmp(const void *a, const void *b);

static void sort_checkpoint_map_file(BTreeDescr *descr, int cur_chkp_index);
static void sort_checkpoint_tmp_file(BTreeDescr *descr, int cur_chkp_index);
static inline void checkpoint_ix_init_state(CheckpointState *state, BTreeDescr *descr);
static void checkpoint_temporary_tree(int flags, BTreeDescr *descr);
static bool checkpoint_ix(int flags, BTreeDescr *descr);
static uint64 checkpoint_btree(BTreeDescr **descrPtr, CheckpointState *state,
							   CheckpointWriteBack *writeback);
static Jsonb *prepare_checkpoint_step_params(BTreeDescr *descr,
											 CheckpointState *chkpState,
											 WalkMessage *message,
											 int level);
static uint64 checkpoint_btree_loop(BTreeDescr **descrPtr, CheckpointState *state,
									CheckpointWriteBack *writeback,
									MemoryContext tmp_context);
static void checkpoint_internal_pass(BTreeDescr *descr, CheckpointState *state,
									 CheckpointWriteBack *writeback,
									 int level, WalkMessage *message);
static void prepare_leaf_page(BTreeDescr *descr, CheckpointState *state);
static void checkpoint_lock_page(BTreeDescr *descr, CheckpointState *state,
								 OInMemoryBlkno *blkno, uint32 page_chage_count,
								 int level);
static void checkpoint_tables_callback(OIndexType type, ORelOids treeOids,
									   ORelOids tableOids, bool temp_table,
									   void *arg);
static inline bool init_seq_buf_pages(BTreeDescr *desc, SeqBufDescShared *shared);
static inline void free_seq_buf_pages(BTreeDescr *desc, SeqBufDescShared *shared);
static FileExtentsArray *file_extents_array_init(void);
static void file_extents_array_free(FileExtentsArray *array);
static void file_extents_array_append(FileExtentsArray *array, FileExtent *extent);
static void foreach_extent_append(BTreeDescr *desc, FileExtent extent, void *arg);

static inline void
checkpoint_reset_stack(CheckpointState *state)
{
	OffsetNumber i;

	chkp_inc_changecount_before(state);

	for (i = 0; i < ORIOLEDB_MAX_DEPTH; i++)
	{
		state->stack[i].blkno = OInvalidInMemoryBlkno;
		state->stack[i].hikeyBlkno = OInvalidInMemoryBlkno;
		state->stack[i].leftmost = true;
		state->stack[i].offset = 0;
		state->stack[i].bound = CheckpointBoundNone;
		state->stack[i].autonomousTupleExist = false;
		Assert(state->stack[i].autonomous == false);
		Assert(BTREE_PAGE_ITEMS_COUNT(state->stack[i].image) == 0);
		Assert(state->stack[i].nextkeyType == NextKeyNone);
	}

	pg_atomic_write_u32(&checkpoint_state->autonomousLevel, ORIOLEDB_MAX_DEPTH);

	chkp_inc_changecount_after(state);
}

Size
checkpoint_shmem_size(void)
{
	Size		size;

	size = offsetof(CheckpointState, xidRecQueue);
	size = add_size(size, mul_size(sizeof(XidFileRec), XID_RECS_QUEUE_SIZE));

	return CACHELINEALIGN(size);
}

void
checkpoint_shmem_init(Pointer ptr, bool found)
{
	checkpoint_state = (CheckpointState *) ptr;

	if (!found)
	{
		int			controlFile;
		int			i;
		CheckpointControl control;
		pg_crc32c	crc;

		memset(checkpoint_state, 0, sizeof(*checkpoint_state));
		checkpoint_state->curKeyType = CurKeyFinished;
		checkpoint_state->pid = InvalidPid;
		pg_atomic_init_u64(&checkpoint_state->mmapDataLength, 0);
		pg_atomic_init_u32(&checkpoint_state->autonomousLevel, ORIOLEDB_MAX_DEPTH);
		pg_atomic_init_u64(&undo_meta->lastUsedLocation, 0);
		pg_atomic_init_u64(&undo_meta->advanceReservedLocation, 0);
		pg_atomic_init_u64(&undo_meta->writeInProgressLocation, 0);
		pg_atomic_init_u64(&undo_meta->writtenLocation, 0);
		pg_atomic_init_u64(&undo_meta->lastUsedUndoLocationWhenUpdatedMinLocation, 0);
		pg_atomic_init_u64(&undo_meta->minProcRetainLocation, 0);
		pg_atomic_init_u64(&undo_meta->minProcTransactionRetainLocation, 0);
		pg_atomic_init_u64(&undo_meta->minProcReservedLocation, 0);
		pg_atomic_init_u64(&undo_meta->cleanedLocation, 0);
		pg_atomic_init_u64(&undo_meta->checkpointRetainStartLocation, 0);
		pg_atomic_init_u64(&undo_meta->checkpointRetainEndLocation, 0);
		pg_atomic_init_u64(&undo_meta->cleanedCheckpointStartLocation, 0);
		pg_atomic_init_u64(&undo_meta->cleanedCheckpointEndLocation, 0);

		pg_atomic_init_u64(&xid_meta->nextXid, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->runXmin, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->globalXmin, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->lastXidWhenUpdatedGlobalXmin, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->writtenXmin, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->writeInProgressXmin, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->checkpointRetainXmin, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->checkpointRetainXmax, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->cleanedXmin, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->cleanedCheckpointXmin, FirstNormalTransactionId);
		pg_atomic_init_u64(&xid_meta->cleanedCheckpointXmax, FirstNormalTransactionId);

		checkpoint_reset_stack(checkpoint_state);

		checkpoint_state->oTablesMetaTrancheId = LWLockNewTrancheId();
		checkpoint_state->oSysTreesTrancheId = LWLockNewTrancheId();
		checkpoint_state->oSharedRootInfoInsertTrancheId = LWLockNewTrancheId();
		checkpoint_state->oXidQueueTrancheId = LWLockNewTrancheId();
		checkpoint_state->oXidQueueFlushTrancheId = LWLockNewTrancheId();
		checkpoint_state->copyBlknoTrancheId = LWLockNewTrancheId();
		checkpoint_state->oMetaTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&checkpoint_state->oTablesMetaLock,
						 checkpoint_state->oTablesMetaTrancheId);
		LWLockInitialize(&checkpoint_state->oSysTreesLock,
						 checkpoint_state->oSysTreesTrancheId);
		for (i = 0; i < SHARED_ROOT_INFO_INSERT_NUM_LOCKS; i++)
			LWLockInitialize(&checkpoint_state->oSharedRootInfoInsertLocks[i],
							 checkpoint_state->oSharedRootInfoInsertTrancheId);
		LWLockInitialize(&checkpoint_state->oXidQueueLock,
						 checkpoint_state->oXidQueueTrancheId);
		LWLockInitialize(&checkpoint_state->oXidQueueFlushLock,
						 checkpoint_state->oXidQueueFlushTrancheId);
		pg_atomic_init_u64(&checkpoint_state->xidRecLastPos, 0);
		pg_atomic_init_u64(&checkpoint_state->xidRecFlushPos, 0);
		memset(checkpoint_state->xidRecQueue,
			   0,
			   sizeof(XidFileRec) * XID_RECS_QUEUE_SIZE);
		for (i = 0; i < XID_RECS_QUEUE_SIZE; i++)
			checkpoint_state->xidRecQueue[i].oxid = InvalidOXid;

		controlFile = BasicOpenFile(CONTROL_FILENAME, O_RDONLY | PG_BINARY);
		if (controlFile < 0)
			return;

		if (read(controlFile, (Pointer) &control, sizeof(control)) != sizeof(control))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read data from control file %s", CONTROL_FILENAME)));

		close(controlFile);

		INIT_CRC32C(crc);
		COMP_CRC32C(crc, &control, offsetof(CheckpointControl, crc));
		FIN_CRC32C(crc);

		if (crc != control.crc)
			elog(ERROR, "Wrong CRC in control file");

		if (control.binaryVersion != ORIOLEDB_BINARY_VERSION)
			ereport(FATAL,
					(errmsg("database files are incompatible with server"),
					 errdetail("OrioleDB was initialized with binary version %d,"
							   " but the extension was compiled with binary version %d.",
							   control.binaryVersion, ORIOLEDB_BINARY_VERSION),
					 errhint("It looks like you need to initdb.")));

		checkpoint_state->lastCheckpointNumber = control.lastCheckpointNumber;
		checkpoint_state->controlToastConsistentPtr = control.toastConsistentPtr;
		checkpoint_state->controlReplayStartPtr = control.replayStartPtr;
		checkpoint_state->controlSysTreesStartPtr = control.sysTreesStartPtr;
		pg_atomic_write_u64(&checkpoint_state->mmapDataLength, control.mmapDataLength);

		pg_atomic_write_u64(&undo_meta->lastUsedLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->advanceReservedLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->writeInProgressLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->writtenLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->lastUsedUndoLocationWhenUpdatedMinLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->minProcRetainLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->minProcTransactionRetainLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->minProcReservedLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->cleanedLocation, control.lastUndoLocation);
		pg_atomic_write_u64(&undo_meta->checkpointRetainStartLocation, control.checkpointRetainStartLocation);
		pg_atomic_write_u64(&undo_meta->checkpointRetainEndLocation, control.checkpointRetainEndLocation);
		pg_atomic_write_u64(&undo_meta->cleanedCheckpointStartLocation, control.checkpointRetainStartLocation);
		pg_atomic_write_u64(&undo_meta->cleanedCheckpointEndLocation, control.checkpointRetainEndLocation);

		pg_atomic_init_u64(&xid_meta->nextXid, control.lastXid);
		pg_atomic_init_u64(&xid_meta->runXmin, control.lastXid);
		pg_atomic_init_u64(&xid_meta->globalXmin, control.lastXid);
		pg_atomic_init_u64(&xid_meta->lastXidWhenUpdatedGlobalXmin, control.lastXid);
		pg_atomic_init_u64(&xid_meta->writtenXmin, control.lastXid);
		pg_atomic_init_u64(&xid_meta->writeInProgressXmin, control.lastXid);
		pg_atomic_init_u64(&xid_meta->checkpointRetainXmin, control.checkpointRetainXmin);
		pg_atomic_init_u64(&xid_meta->checkpointRetainXmax, control.checkpointRetainXmax);
		pg_atomic_init_u64(&xid_meta->cleanedXmin, control.lastXid);
		pg_atomic_init_u64(&xid_meta->cleanedCheckpointXmin, control.checkpointRetainXmin);
		pg_atomic_init_u64(&xid_meta->cleanedCheckpointXmax, control.checkpointRetainXmax);

		startupCommitSeqNo = control.lastCSN;
	}

	LWLockRegisterTranche(checkpoint_state->oTablesMetaTrancheId,
						  "OTablesMetaTranche");
	LWLockRegisterTranche(checkpoint_state->oSysTreesTrancheId,
						  "OSysTreesTranche");
	LWLockRegisterTranche(checkpoint_state->copyBlknoTrancheId,
						  "CopyBlknoTranche");
	LWLockRegisterTranche(checkpoint_state->oMetaTrancheId,
						  "orioledb_meta");
}

static void
init_writeback(CheckpointWriteBack *writeback, int flags, bool isCompressed)
{
	writeback->isCompressed = isCompressed;
	writeback->checkpointFlags = flags;
	writeback->extentsNumber = 0;
	writeback->extentsAllocated = 16;
	writeback->extents = (FileExtent *) palloc(sizeof(FileExtent) *
											   writeback->extentsAllocated);
}

static void
writeback_put_extent(CheckpointWriteBack *writeback, FileExtent *extent)
{
	Assert(extent != NULL);

	if (writeback->extentsNumber >= writeback->extentsAllocated)
	{
		writeback->extentsAllocated *= 2;
		writeback->extents = (FileExtent *) repalloc(writeback->extents,
													 sizeof(FileExtent) * writeback->extentsAllocated);
	}
	writeback->extents[writeback->extentsNumber] = *extent;
	if (!writeback->isCompressed && use_device)
		writeback->extents[writeback->extentsNumber].len *= ORIOLEDB_BLCKSZ / ORIOLEDB_COMP_BLCKSZ;
	writeback->extentsNumber++;
}

static void
perform_writeback(BTreeDescr *desc, CheckpointWriteBack *writeback)
{
	int			i,
				len = 0;
	uint64		offset = InvalidFileExtentOff;
	double		progress = 0.0;
	uint		blcksz = (writeback->isCompressed || use_mmap) ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ;

	if (use_device && !use_mmap)
	{
		writeback->extentsNumber = 0;
		return;
	}

	pg_qsort(writeback->extents, writeback->extentsNumber,
			 sizeof(FileExtent), file_extents_writeback_cmp);

	for (i = 0; i < writeback->extentsNumber; i++)
	{
		if (i > 0 && writeback->extents[i].off == writeback->extents[i - 1].off)
		{
			/* duplicate offset */
			continue;
		}

		if (writeback->extents[i].off == offset + len && len < 1024)
		{
			len += writeback->extents[i].len;
		}
		else
		{
			if (len > 0)
				btree_smgr_writeback(desc, (off_t) offset * (off_t) blcksz,
									 (off_t) len * (off_t) blcksz);
			offset = writeback->extents[i].off;
			len = writeback->extents[i].len;
		}

		if (progress < 1.0)
		{
			progress = (double) (checkpoint_state->pagesWritten + (uint64) i)
				/ (double) checkpoint_state->dirtyPagesEstimate;
			if (progress < 1.0)
			{
				progress *= o_checkpoint_completion_ratio;
				CheckpointWriteDelay(writeback->checkpointFlags, progress);
			}
		}
	}

	if (len > 0)
		btree_smgr_writeback(desc, (off_t) offset * (off_t) blcksz,
							 (off_t) len * (off_t) blcksz);
	checkpoint_state->pagesWritten += writeback->extentsNumber;
	writeback->extentsNumber = 0;
}

static BTreeDescr *
perform_writeback_and_relock(BTreeDescr *desc,
							 CheckpointWriteBack *writeback,
							 CheckpointState *state,
							 WalkMessage *message,
							 int level)
{
	ORelOids	treeOids = desc->oids;
	OIndexType	type = desc->type;
	OIndexDescr *indexDescr;
	Jsonb	   *params;

	if (!IS_SYS_TREE_OIDS(treeOids))
	{
		/* Unlock tree: give a chance for concurrent deletion */
		o_tables_rel_unlock_extended(&treeOids, AccessShareLock, true);

		if (STOPEVENTS_ENABLED())
			params = prepare_checkpoint_step_params(desc, state,
													message, level);
		STOPEVENT(STOPEVENT_CHECKPOINT_WRITEBACK, params);

		perform_writeback(desc, writeback);

		indexDescr = o_fetch_index_descr(treeOids, type, true, NULL);
		if (!indexDescr)
			return NULL;
		desc = &indexDescr->desc;
		if (!o_btree_load_shmem_checkpoint(desc))
			return NULL;
	}
	else
	{
		perform_writeback(desc, writeback);
	}
	return desc;
}

static void
free_writeback(CheckpointWriteBack *writeback)
{
	pfree(writeback->extents);
}

static inline List *
add_index_id_item(List *list, BTreeDescr *desc)
{
	IndexIdItem *item;
	MemoryContext old_context;

	old_context = MemoryContextSwitchTo(TopMemoryContext);
	item = palloc(sizeof(IndexIdItem));
	item->oids = desc->oids;
	item->type = desc->type;
	list = lappend(list, item);
	MemoryContextSwitchTo(old_context);

	return list;
}

static inline List *
add_free_extents_item(List *free, BTreeDescr *desc)
{
	return add_index_id_item(free, desc);
}

static inline List *
add_map_cleanup_item(List *cleanup, BTreeDescr *desc)
{
	if (!remove_old_checkpoint_files)
		return cleanup;

	if (!OCompressIsValid(desc->compress) && desc->freeBuf.tag.type == 'm'
		&& desc->freeBuf.tag.num == checkpoint_state->lastCheckpointNumber)
	{
		return cleanup;
	}
	else
	{
		return add_index_id_item(cleanup, desc);
	}
}

/*
 * Wait all the committing transactions to finish completely.  Ensures all the
 * transactions finished afterwards will have greater WAL position than given
 * `redo_pos`.
 */
static inline void
wait_finish_active_commits(XLogRecPtr redo_pos)
{
	int			i;

	for (i = 0; i < max_procs; i++)
	{
		while (pg_atomic_read_u64(&oProcData[i].commitInProgressXlogLocation) <= redo_pos)
			pg_usleep(100);
	}
}

static void
unlink_xids_file(uint32 checkpointnum)
{
	char	   *xip_filename = psprintf(XID_FILENAME_FORMAT, checkpointnum);

	unlink(xip_filename);
	pfree(xip_filename);
}

/*
 * Open xids file corresponding to the current checkpoint.
 */
static void
open_xids_file(void)
{
	uint32		checkpointnum = checkpoint_state->xidQueueCheckpointNum;

	if (xidFile < 0 || xidFileCheckpointnum != checkpointnum)
	{
		MemoryContext mctx = MemoryContextSwitchTo(TopMemoryContext);

		if (xidFilename)
			pfree(xidFilename);
		if (xidFile >= 0)
			FileClose(xidFile);

		xidFilename = psprintf(XID_FILENAME_FORMAT, checkpointnum);
		MemoryContextSwitchTo(mctx);
		xidFile = PathNameOpenFile(xidFilename, O_WRONLY | O_CREAT | PG_BINARY);
		if (xidFile < 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not open xid file %s", xidFilename)));
		xidFileCheckpointnum = checkpointnum;
	}
}

static void
flush_xids_queue(void)
{
	uint64		startPos,
				location,
				endPos;

	open_xids_file();
	startPos = pg_atomic_read_u64(&checkpoint_state->xidRecFlushPos);
	endPos = Min(pg_atomic_read_u64(&checkpoint_state->xidRecLastPos), startPos + XID_RECS_QUEUE_SIZE);
	location = startPos;
	while (OXidIsValid(checkpoint_state->xidRecQueue[location % XID_RECS_QUEUE_SIZE].oxid) &&
		   location < endPos)
		location++;
	endPos = location;

	if (startPos % XID_RECS_QUEUE_SIZE < endPos % XID_RECS_QUEUE_SIZE)
	{
		if (OFileWrite(xidFile,
					   (Pointer) &checkpoint_state->xidRecQueue[startPos % XID_RECS_QUEUE_SIZE],
					   sizeof(XidFileRec) * (endPos - startPos),
					   sizeof(uint32) + sizeof(XidFileRec) * startPos,
					   WAIT_EVENT_SLRU_WRITE) != sizeof(XidFileRec) * (endPos - startPos))
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not write xid record to file %s", xidFilename)));
	}
	else
	{
		uint64		len1,
					len2;

		len1 = XID_RECS_QUEUE_SIZE - startPos % XID_RECS_QUEUE_SIZE;
		len2 = endPos % XID_RECS_QUEUE_SIZE;
		if (OFileWrite(xidFile,
					   (Pointer) &checkpoint_state->xidRecQueue[startPos % XID_RECS_QUEUE_SIZE],
					   sizeof(XidFileRec) * len1,
					   sizeof(uint32) + sizeof(XidFileRec) * startPos,
					   WAIT_EVENT_SLRU_WRITE) != sizeof(XidFileRec) * len1)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not write xid record to file %s", xidFilename)));

		if (OFileWrite(xidFile,
					   (Pointer) &checkpoint_state->xidRecQueue[0],
					   sizeof(XidFileRec) * len2,
					   sizeof(uint32) + sizeof(XidFileRec) * (startPos + len1),
					   WAIT_EVENT_SLRU_WRITE) != sizeof(XidFileRec) * len2)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not write xid record to file %s", xidFilename)));
	}

	for (location = startPos; location < endPos; location++)
		checkpoint_state->xidRecQueue[location % XID_RECS_QUEUE_SIZE].oxid = InvalidOXid;

	pg_write_barrier();

	pg_atomic_write_u64(&checkpoint_state->xidRecFlushPos, endPos);
}

static void
try_flush_xids_queue(void)
{
	if (LWLockAcquireOrWait(&checkpoint_state->oXidQueueFlushLock, LW_EXCLUSIVE))
	{
		flush_xids_queue();
		LWLockRelease(&checkpoint_state->oXidQueueFlushLock);
	}
}

/*
 * Write single xid record to queue.
 */
void
write_to_xids_queue(XidFileRec *rec)
{
	uint64		location = pg_atomic_fetch_add_u64(&checkpoint_state->xidRecLastPos, 1);
	XidFileRec *target = &checkpoint_state->xidRecQueue[location % XID_RECS_QUEUE_SIZE];

	/*
	 * Flush queue to the file till our position is available for write.
	 */
	while (location >= pg_atomic_read_u64(&checkpoint_state->xidRecFlushPos) + XID_RECS_QUEUE_SIZE)
		try_flush_xids_queue();

	target->undoLocation = rec->undoLocation;

	pg_write_barrier();

	target->oxid = rec->oxid;

	VALGRIND_CHECK_MEM_IS_DEFINED(target, sizeof(*target));
}

/*
 * Prepare xids queue for writes.
 */
void
before_writing_xids_file(int chkpnum)
{
	int			i;

	if (checkpoint_state->xidQueueCheckpointNum < chkpnum)
	{
		checkpoint_state->xidQueueCheckpointNum = chkpnum;
		pg_atomic_write_u64(&checkpoint_state->xidRecLastPos, 0);
		pg_atomic_write_u64(&checkpoint_state->xidRecFlushPos, 0);
		memset(checkpoint_state->xidRecQueue,
			   0,
			   sizeof(XidFileRec) * XID_RECS_QUEUE_SIZE);
		for (i = 0; i < XID_RECS_QUEUE_SIZE; i++)
			checkpoint_state->xidRecQueue[i].oxid = InvalidOXid;
	}
}

/*
 * Flush xids queue, fsync and close xids file.
 */
static void
close_xids_file(void)
{
	uint32		count;

	open_xids_file();

	while (pg_atomic_read_u64(&checkpoint_state->xidRecFlushPos) <
		   pg_atomic_read_u64(&checkpoint_state->xidRecLastPos))
	{
		flush_xids_queue();
	}

	count = pg_atomic_read_u64(&checkpoint_state->xidRecLastPos);

	if (OFileWrite(xidFile, (Pointer) &count,
				   sizeof(count), 0, WAIT_EVENT_SLRU_WRITE) != sizeof(count))
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not write xid record to file %s", xidFilename)));

	if (FileSync(xidFile, WAIT_EVENT_SLRU_SYNC) < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not sync xid file %s", xidFilename)));

	FileClose(xidFile);
	pfree(xidFilename);
	xidFilename = NULL;
	xidFile = -1;
}

/*
 * Start writing xid records to the xid file.  After this, information about
 * committed/aborted transactions will be written by backends to the xids
 * queue.
 */
static void
start_write_xids(uint32 chkpnum)
{
	int			i;

	for (i = 0; i < max_procs; i++)
	{
		LWLockAcquire(&oProcData[i].undoStackLocationsFlushLock, LW_EXCLUSIVE);
		oProcData[i].flushUndoLocations = true;
		LWLockRelease(&oProcData[i].undoStackLocationsFlushLock);
	}
	recovery_undo_loc_flush->finishRequestCheckpointNumber = chkpnum;
}

/*
 * Write information about undo locations of in-progress transactions.
 */
static void
finish_write_xids(uint32 chkpnum)
{
	XidFileRec	xidRec;
	int			i,
				j;

	memset(&xidRec, 0, sizeof(xidRec));

	for (i = 0; i < max_procs; i++)
	{
		LWLockAcquire(&oProcData[i].undoStackLocationsFlushLock, LW_EXCLUSIVE);
		for (j = 0; j < PROC_XID_ARRAY_SIZE; j++)
		{
			while (true)
			{
				if (OXidIsValid(oProcData[i].vxids[j].oxid))
				{
					xidRec.oxid = oProcData[i].vxids[j].oxid;
					pg_read_barrier();

					read_shared_undo_locations(&xidRec.undoLocation, &oProcData[i].undoStackLocations[j]);

					pg_read_barrier();

					if (xidRec.oxid != oProcData[i].vxids[j].oxid)
						continue;

					write_to_xids_queue(&xidRec);
				}
				break;
			}
		}
		oProcData[i].flushUndoLocations = false;
		LWLockRelease(&oProcData[i].undoStackLocationsFlushLock);
	}

	recovery_undo_loc_flush->immediateRequestCheckpointNumber = chkpnum;

	/*
	 * Wait till recovery undo position will be flushed.
	 */
	while (recovery_undo_loc_flush->completedCheckpointNumber <
		   recovery_undo_loc_flush->immediateRequestCheckpointNumber)
	{
		pg_usleep(10000L);
	}
}

void
o_perform_checkpoint(XLogRecPtr redo_pos, int flags)
{
	CheckpointTablesArg chkp_tbl_arg;
	CheckpointControl control;
	int			old_enable_stopevents;
	uint32		cur_chkp_num = checkpoint_state->lastCheckpointNumber + 1,
				prev_chkp_num = checkpoint_state->lastCheckpointNumber;
	MemoryContext prev_context;
	int			sys_tree_num;
	ODBProcData *my_proc_info = GET_CUR_PROCDATA();
	UndoLocation checkpoint_start_loc,
				checkpoint_end_loc;
	OXid		checkpoint_xmin,
				checkpoint_xmax;

	orioledb_check_shmem();

	if (IsPostmasterEnvironment)
		checkpoint_state->pid = MyProcPid;

	elog(LOG, "orioledb checkpoint %u started",
		 checkpoint_state->lastCheckpointNumber + 1);

	o_set_syscache_hooks();

	memset(&control, 0, sizeof(control));

	chkp_tbl_arg.cleanupMap = NULL;
	chkp_tbl_arg.freeExtents = NULL;
	chkp_tbl_arg.flags = flags;

	checkpoint_state->dirtyPagesEstimate = get_dirty_pages_count_sum();
	checkpoint_state->dirtyPagesEstimate *= (1.0 + CheckPointCompletionTarget
											 * o_checkpoint_completion_ratio);
	checkpoint_state->pagesWritten = 0;
	checkpoint_state->toastConsistentPtr = InvalidXLogRecPtr;

	if (chkp_mem_context == NULL)
	{
		chkp_mem_context = AllocSetContextCreate(TopMemoryContext,
												 "checkpoint temporary context",
												 ALLOCSET_DEFAULT_SIZES);
	}

	old_enable_stopevents = enable_stopevents;

	/*
	 * does not count debug events from o_tables and o_opclass checkpoint
	 */
	enable_stopevents = false;

	prev_context = MemoryContextSwitchTo(chkp_mem_context);

	checkpoint_start_loc = pg_atomic_read_u64(&undo_meta->minProcTransactionRetainLocation);
	checkpoint_xmin = pg_atomic_read_u64(&xid_meta->runXmin);
	pg_atomic_write_u64(&my_proc_info->snapshotRetainUndoLocation, checkpoint_start_loc);
	pg_atomic_write_u64(&my_proc_info->xmin, checkpoint_xmin);

	pg_write_barrier();

	checkpoint_state->replayStartPtr = GetXLogInsertRecPtr();
	wait_finish_active_commits(checkpoint_state->replayStartPtr);

	LWLockAcquire(&checkpoint_state->oXidQueueLock, LW_EXCLUSIVE);
	before_writing_xids_file(cur_chkp_num);
	start_write_xids(cur_chkp_num);

	LWLockAcquire(&checkpoint_state->oTablesMetaLock, LW_EXCLUSIVE);
	LWLockAcquire(&checkpoint_state->oSysTreesLock, LW_EXCLUSIVE);

	for (sys_tree_num = 1; sys_tree_num <= SYS_TREES_NUM; sys_tree_num++)
	{
		BTreeDescr *desc;
		bool		success PG_USED_FOR_ASSERTS_ONLY;

		if (sys_tree_get_storage_type(sys_tree_num) == BTreeStorageInMemory)
			continue;

		desc = get_sys_tree(sys_tree_num);

		if (desc->storageType == BTreeStoragePersistence)
		{
			success = checkpoint_ix(flags, desc);
			/* System trees can't be concurrently deleted */
			Assert(success);
			sort_checkpoint_map_file(desc, cur_chkp_num % 2);
			sort_checkpoint_tmp_file(desc, cur_chkp_num % 2);
			chkp_tbl_arg.cleanupMap = add_map_cleanup_item(chkp_tbl_arg.cleanupMap,
														   desc);
		}
		else
		{
			checkpoint_temporary_tree(flags, desc);
			sort_checkpoint_tmp_file(desc, cur_chkp_num % 2);
		}
	}

	/*
	 * We get start position for replay changes to system trees while holding
	 * the oTablesMetaLock.  That guarantees that we will start recovery from
	 * the state there is no partial changes to tables and indices system
	 * trees.
	 */
	checkpoint_state->sysTreesStartPtr = GetXLogInsertRecPtr();
	LWLockRelease(&checkpoint_state->oSysTreesLock);
	LWLockRelease(&checkpoint_state->oTablesMetaLock);

	MemoryContextSwitchTo(prev_context);
	MemoryContextResetOnly(chkp_mem_context);

	enable_stopevents = old_enable_stopevents;

	LWLockAcquire(&checkpoint_state->oTablesMetaLock, LW_EXCLUSIVE);
	o_indices_foreach_oids(checkpoint_tables_callback, &chkp_tbl_arg);

	chkp_inc_changecount_before(checkpoint_state);
	checkpoint_state->lastCheckpointNumber++;
	checkpoint_state->treeType = oIndexInvalid;
	checkpoint_state->datoid = InvalidOid;
	checkpoint_state->reloid = InvalidOid;
	checkpoint_state->relnode = InvalidOid;
	checkpoint_state->completed = false;
	chkp_inc_changecount_after(checkpoint_state);

	LWLockRelease(&checkpoint_state->oTablesMetaLock);

	/*
	 * It might happen there is no secondary indices, but we still need to set
	 * toastConsistentPtr.
	 */
	if (XLogRecPtrIsInvalid(checkpoint_state->toastConsistentPtr))
		checkpoint_state->toastConsistentPtr = GetXLogInsertRecPtr();

	finish_write_xids(cur_chkp_num);
	close_xids_file();
	LWLockRelease(&checkpoint_state->oXidQueueLock);

	checkpoint_end_loc = pg_atomic_read_u64(&undo_meta->lastUsedLocation);
	checkpoint_xmax = pg_atomic_read_u64(&xid_meta->nextXid);

	if (use_mmap)
		msync(mmap_data, device_length, MS_SYNC);

	fsync_undo_range(checkpoint_start_loc,
					 checkpoint_end_loc,
					 WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC);

	fsync_xidmap_range(checkpoint_xmin,
					   checkpoint_xmax,
					   WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC);

	SpinLockAcquire(&undo_meta->minUndoLocationsMutex);
	pg_atomic_write_u64(&undo_meta->checkpointRetainStartLocation, checkpoint_start_loc);
	pg_atomic_write_u64(&undo_meta->checkpointRetainEndLocation, checkpoint_end_loc);
	SpinLockRelease(&undo_meta->minUndoLocationsMutex);

	SpinLockAcquire(&xid_meta->xminMutex);
	pg_atomic_write_u64(&xid_meta->checkpointRetainXmin, checkpoint_xmin);
	pg_atomic_write_u64(&xid_meta->checkpointRetainXmax, checkpoint_xmax);
	SpinLockRelease(&xid_meta->xminMutex);

	pg_write_barrier();

	pg_atomic_write_u64(&my_proc_info->snapshotRetainUndoLocation, InvalidUndoLocation);

	control.lastCheckpointNumber = checkpoint_state->lastCheckpointNumber;
	control.lastCSN = pg_atomic_read_u64(&ShmemVariableCache->nextCommitSeqNo);
	control.lastXid = pg_atomic_read_u64(&xid_meta->nextXid);
	control.lastUndoLocation = pg_atomic_read_u64(&undo_meta->lastUsedLocation);
	control.sysTreesStartPtr = checkpoint_state->sysTreesStartPtr;
	control.replayStartPtr = checkpoint_state->replayStartPtr;
	control.toastConsistentPtr = checkpoint_state->toastConsistentPtr;
	control.mmapDataLength = pg_atomic_read_u64(&checkpoint_state->mmapDataLength);
	control.checkpointRetainStartLocation = pg_atomic_read_u64(&undo_meta->checkpointRetainStartLocation);
	control.checkpointRetainEndLocation = pg_atomic_read_u64(&undo_meta->checkpointRetainEndLocation);
	control.checkpointRetainXmin = pg_atomic_read_u64(&xid_meta->checkpointRetainXmin);
	control.checkpointRetainXmax = pg_atomic_read_u64(&xid_meta->checkpointRetainXmax);
	control.binaryVersion = ORIOLEDB_BINARY_VERSION;

	write_checkpoint_control(&control);

	/*
	 * Now we can free extents for compressed indices
	 */
	if (!(flags & CHECKPOINT_IS_SHUTDOWN) && chkp_tbl_arg.freeExtents != NIL)
	{
		IndexIdItem *item;
		ListCell   *lc;
		OIndexDescr *descr;

		foreach(lc, chkp_tbl_arg.freeExtents)
		{
			item = (IndexIdItem *) lfirst(lc);

			descr = o_fetch_index_descr(item->oids, item->type,
										true, NULL);
			if (descr == NULL)
			{
				/* table might be deleted */
				continue;
			}

			add_free_extents_from_tmp(&descr->desc,
									  remove_old_checkpoint_files);
			o_tables_rel_unlock_extended(&item->oids, AccessShareLock, true);
		}
		list_free_deep(chkp_tbl_arg.freeExtents);
	}

	/*
	 * Remove old files if needed
	 */
	if (remove_old_checkpoint_files)
	{
		IndexIdItem *item;
		SeqBufTag	cleanup_tag;
		ListCell   *lc;

		unlink_xids_file(prev_chkp_num);

		cleanup_tag.type = 'm';
		cleanup_tag.num = prev_chkp_num;

		foreach(lc, chkp_tbl_arg.cleanupMap)
		{
			item = (IndexIdItem *) lfirst(lc);
			cleanup_tag.datoid = item->oids.datoid;
			cleanup_tag.relnode = item->oids.relnode;
			seq_buf_remove_file(&cleanup_tag);
		}

		list_free_deep(chkp_tbl_arg.cleanupMap);
	}

	CheckPointProgress = o_checkpoint_completion_ratio;

	pg_atomic_write_u64(&my_proc_info->xmin, InvalidOXid);

	o_reset_syscache_hooks();

	elog(LOG, "orioledb checkpoint %u complete",
		 checkpoint_state->lastCheckpointNumber);

	if (next_CheckPoint_hook)
		next_CheckPoint_hook(redo_pos, flags);
}

/*
 * Make checkpoint of an index.
 */
static void
checkpoint_temporary_tree(int flags, BTreeDescr *descr)
{
	BTreeMetaPage *meta_page;
	SeqBufTag	next_tmp_tag = {0};
	int			cur_chkp_index,
				next_chkp_index;
	Oid			datoid = descr->oids.datoid;
	Oid			relnode = descr->oids.relnode;
	bool		success;
	CheckpointWriteBack writeback;

	Assert(!OCompressIsValid(descr->compress));

	/*
	 * TODO: can we make checkpoint on evicted or unloaded tree?
	 */
	checkpoint_ix_init_state(checkpoint_state, descr);

	cur_chkp_index = (checkpoint_state->lastCheckpointNumber + 1) % 2;
	next_chkp_index = (checkpoint_state->lastCheckpointNumber + 2) % 2;
	meta_page = BTREE_GET_META(descr);

	Assert(ORootPageIsValid(descr) && OMetaPageIsValid(descr));

	/* Initialize next tmp file */
	if (!init_seq_buf_pages(descr, &meta_page->tmpBuf[next_chkp_index]))
	{
		free_seq_buf_pages(descr, &meta_page->tmpBuf[next_chkp_index]);
		elog(FATAL, "Unable to get pages for sequence buffers.");
	}

	memset(&next_tmp_tag, 0, sizeof(next_tmp_tag));
	next_tmp_tag.datoid = datoid;
	next_tmp_tag.relnode = relnode;
	next_tmp_tag.num = checkpoint_state->lastCheckpointNumber + 2;
	next_tmp_tag.type = 't';

	success = init_seq_buf(&descr->tmpBuf[next_chkp_index],
						   &meta_page->tmpBuf[next_chkp_index],
						   &next_tmp_tag, true, true, 0, NULL);
	if (!success)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not init a new sequence buffer file %s",
						get_seq_buf_filename(&next_tmp_tag))));

	/* Make checkpoint of the tree itself */
	init_writeback(&writeback, flags, false);
	(void) checkpoint_btree(&descr, checkpoint_state, &writeback);
	(void) perform_writeback_and_relock(descr, &writeback,
										checkpoint_state, NULL, 0);
	free_writeback(&writeback);

	Assert(checkpoint_state->curKeyType == CurKeyGreatest);

	STOPEVENT(STOPEVENT_BEFORE_BLKNO_LOCK, NULL);

	/*
	 * Need a lock to be sure, that nobody is concurrently copying block
	 * number from previous checkpoint to current.  See write_page() for
	 * details.
	 */
	LWLockAcquire(&meta_page->copyBlknoLock, LW_EXCLUSIVE);
	chkp_inc_changecount_before(checkpoint_state);
	checkpoint_state->curKeyType = CurKeyFinished;
	chkp_inc_changecount_after(checkpoint_state);
	LWLockRelease(&meta_page->copyBlknoLock);

	/* finalizes *.tmp file */
	seq_buf_finalize(&descr->tmpBuf[cur_chkp_index]);
	free_seq_buf_pages(descr, descr->tmpBuf[cur_chkp_index].shared);

	chkp_inc_changecount_before(checkpoint_state);
	checkpoint_state->completed = true;
	chkp_inc_changecount_after(checkpoint_state);
}


void
o_after_checkpoint_cleanup_hook(XLogRecPtr checkPointRedo, int flags)
{
	if (!(flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY)))
	{
		o_sys_caches_delete_by_lsn(checkPointRedo);
	}
}

/*
 * Write checkpoint control file to the disk (and sync).
 */
static void
write_checkpoint_control(CheckpointControl *control)
{
	File		controlFile;

	INIT_CRC32C(control->crc);
	COMP_CRC32C(control->crc, control, offsetof(CheckpointControl, crc));
	FIN_CRC32C(control->crc);

	controlFile = PathNameOpenFile(CONTROL_FILENAME, O_RDWR | O_CREAT | PG_BINARY);
	if (controlFile < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open checkpoint control file %s", CONTROL_FILENAME)));

	if (OFileWrite(controlFile, (Pointer) control, sizeof(*control), 0,
				   WAIT_EVENT_SLRU_WRITE) != sizeof(*control) ||
		FileSync(controlFile, WAIT_EVENT_SLRU_SYNC) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not write checkpoint control to file %s", CONTROL_FILENAME)));

	FileClose(controlFile);
}

static uint64
append_file_contents(File target, char *source_filename, uint64 offset)
{
	char		buf[ORIOLEDB_BLCKSZ];
	File		source;
	uint64		len = 0;
	uint32		block_len;
	uint64		target_offset = FileSize(target);

	source = PathNameOpenFile(source_filename, O_RDONLY | PG_BINARY);
	if (source < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open file for finalize checkpoint map: %s",
							   source_filename)));

	do
	{
		block_len = OFileRead(source, buf, ORIOLEDB_BLCKSZ, offset, WAIT_EVENT_DATA_FILE_READ);
		if (OFileWrite(target, buf, block_len, target_offset, WAIT_EVENT_DATA_FILE_WRITE) != block_len)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not copy data for finalize checkpoint map: %s", source_filename)));
		target_offset += block_len;
		offset += block_len;
		len += block_len;
	}
	while (block_len == ORIOLEDB_BLCKSZ);

	FileClose(source);
	return len;
}

static uint64
finalize_chkp_map(File chkp_file, uint64 len, char *input_filename,
				  uint64 input_offset, uint32 input_num)
{
	SeqBufTag	tmp_tag;

	if (FileSize(chkp_file) != len)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not move to offset %lu for making finalize checkpoint map: %s",
							   len, FilePathName(chkp_file))));

	if (input_filename != NULL)
		len += append_file_contents(chkp_file, input_filename, input_offset);

	input_num++;
	while (input_num <= checkpoint_state->lastCheckpointNumber)
	{
		char	   *tmp_filename;

		tmp_tag.datoid = checkpoint_state->datoid;
		tmp_tag.relnode = checkpoint_state->relnode;
		tmp_tag.num = input_num;
		tmp_tag.type = 't';
		if (seq_buf_file_exist(&tmp_tag))
		{
			tmp_filename = get_seq_buf_filename(&tmp_tag);
			len += append_file_contents(chkp_file, tmp_filename, 0);
			pfree(tmp_filename);
		}
		input_num++;
	}

	return len;
}

/*
 * Comparator for sort ascending.
 */
static int
uint32_offsets_cmp(const void *a, const void *b)
{
	uint32		val1 = *(uint32 *) a;
	uint32		val2 = *(uint32 *) b;

	if (val1 != val2)
		return val1 > val2 ? 1 : -1;
	return 0;
}

/*
 * Comparator for FileExtent.len sort descending.
 */
static int
file_extents_len_off_cmp(const void *a, const void *b)
{
	FileExtent *val1 = (FileExtent *) a;
	FileExtent *val2 = (FileExtent *) b;

	if (val1->len != val2->len)
		return val1->len > val2->len ? -1 : 1;
	else if (val1->off != val2->off)
		return val1->off > val2->off ? 1 : -1;

	return 0;
}

/*
 * Comparator for FileExtent.off sort ascending.
 */
static int
file_extents_off_len_cmp(const void *a, const void *b)
{
	FileExtent *val1 = (FileExtent *) a;
	FileExtent *val2 = (FileExtent *) b;

	if (val1->off != val2->off)
		return val1->off > val2->off ? 1 : -1;
	if (val1->len != val2->len)
		return val1->len > val2->len ? 1 : -1;

	return 0;
}

/*
 * Comparator for FileExtent.off sort ascending.
 */
static int
file_extents_writeback_cmp(const void *a, const void *b)
{
	FileExtent *val1 = (FileExtent *) a;
	FileExtent *val2 = (FileExtent *) b;

	if (val1->off != val2->off)
		return val1->off > val2->off ? 1 : -1;

	if (val1->len != val2->len)
	{
		/*
		 * The sort order helps in perform_writeback().
		 */
		return val1->len > val2->len ? -1 : 1;
	}
	return 0;
}

/*
 * Sort lists of free blocks in .map file to optimize disk access.
 */
static void
sort_checkpoint_map_file(BTreeDescr *descr, int cur_chkp_index)
{
	Pointer		free_blocks;
	uint64		free_blocks_size;
	File		file;
	char	   *filename;
	CheckpointFileHeader header = {0};
	bool		ferror = false,
				is_compressed = OCompressIsValid(descr->compress);
	int			read_size;

	filename = get_seq_buf_filename(&descr->nextChkp[cur_chkp_index].tag);
	file = PathNameOpenFile(filename, O_RDWR | PG_BINARY);
	if (file < 0)
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("Could not open checkpoint map file: %s",
							   filename)));
	}

	/* reads header and blocks from map file */
	ferror = OFileRead(file, (Pointer) &header,
					   sizeof(header), 0, WAIT_EVENT_DATA_FILE_READ) != sizeof(header);
	if (ferror)
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("Could not read data from checkpoint map file: %s",
							   filename)));
	}

	if (is_compressed || use_device)
	{
		free_blocks_size = sizeof(FileExtent) * header.numFreeBlocks;
	}
	else
	{
		free_blocks_size = sizeof(uint32) * header.numFreeBlocks;
	}

	free_blocks = palloc(free_blocks_size);
	if (free_blocks_size > 0)
	{
		read_size = OFileRead(file, (Pointer) free_blocks, free_blocks_size,
							  sizeof(header), WAIT_EVENT_DATA_FILE_READ);

		if (read_size != free_blocks_size)
		{
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("Could not read data from checkpoint map file: %s",
								   filename)));
		}
	}

	/* sorts blocks */
	if (is_compressed || use_device)
		pg_qsort(free_blocks, header.numFreeBlocks, sizeof(FileExtent), file_extents_len_off_cmp);
	else
		pg_qsort(free_blocks, header.numFreeBlocks, sizeof(uint32), uint32_offsets_cmp);

	/* writes sorted blocks to map file. */
	if (OFileWrite(file, (Pointer) free_blocks, free_blocks_size,
				   sizeof(header), WAIT_EVENT_DATA_FILE_WRITE) != free_blocks_size ||
		FileSync(file, WAIT_EVENT_SLRU_SYNC) != 0)
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("Could not write sorted data to checkpoint map file: %s",
							   filename)));
	}
	FileClose(file);
	pfree(filename);
	pfree(free_blocks);
}

/*
 * Sort lists of free blocks in .map file to optimize disk access.
 */
static void
sort_checkpoint_tmp_file(BTreeDescr *descr, int cur_chkp_index)
{
	Pointer		free_blocks;
	uint64		free_blocks_size;
	File		file;
	char	   *filename;
	bool		is_compressed = OCompressIsValid(descr->compress);
	int			read_size;

	filename = get_seq_buf_filename(&descr->tmpBuf[cur_chkp_index].tag);
	file = PathNameOpenFile(filename, O_RDWR | PG_BINARY);
	if (file < 0)
	{
		/*
		 * *.tmp file does not exist, nothing to sort
		 */
		return;
	}

	free_blocks_size = FileSize(file);
	free_blocks = palloc(free_blocks_size);

	if (free_blocks_size > 0)
	{
		read_size = OFileRead(file, (Pointer) free_blocks,
							  free_blocks_size, 0, WAIT_EVENT_DATA_FILE_READ);
		if (read_size != free_blocks_size)
		{
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("Could not read data from checkpoint tmp file: %s %d %lu",
								   filename, read_size, free_blocks_size)));
		}
	}

	/* sorts blocks */
	if (is_compressed || use_device)
	{
		pg_qsort(free_blocks, free_blocks_size / sizeof(FileExtent),
				 sizeof(FileExtent), file_extents_len_off_cmp);
	}
	else
	{
		pg_qsort(free_blocks, free_blocks_size / sizeof(uint32),
				 sizeof(uint32), uint32_offsets_cmp);
	}

	/* writes sorted blocks to tmp file */
	if (OFileWrite(file, (Pointer) free_blocks,
				   free_blocks_size, 0, WAIT_EVENT_DATA_FILE_WRITE) != free_blocks_size ||
		FileSync(file, WAIT_EVENT_SLRU_SYNC) != 0)
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("Could not write sorted data to checkpoint tmp file: %s",
							   filename)));
	}

	FileClose(file);
	pfree(filename);
	pfree(free_blocks);
}

static inline void
checkpoint_ix_init_state(CheckpointState *state, BTreeDescr *descr)
{
	chkp_inc_changecount_before(checkpoint_state);
	checkpoint_state->treeType = descr->type;
	checkpoint_state->datoid = descr->oids.datoid;
	checkpoint_state->reloid = descr->oids.reloid;
	checkpoint_state->relnode = descr->oids.relnode;
	checkpoint_state->completed = false;
	checkpoint_state->curKeyType = CurKeyLeast;
	chkp_inc_changecount_after(checkpoint_state);
}

/*
 * Marks a offset as free for given checkpoint number, throws an error on failure.
 *
 * It adds the offset to *.map and *.tmp files.
 */
void
free_extent_for_checkpoint(BTreeDescr *desc, FileExtent *extent, uint32 chkp_num)
{
	SeqBufDescPrivate *bufs[2] = {&desc->nextChkp[chkp_num % 2], &desc->tmpBuf[chkp_num % 2]};
	int			i;
	bool		success;

	for (i = 0; i < 2; i++)
	{
		/* Don't have *.map files for BTreeStorageTemporary */
		if (i == 0 && desc->storageType == BTreeStorageTemporary)
			continue;

		if (OCompressIsValid(desc->compress) || use_device)
		{
			success = seq_buf_write_file_extent(bufs[i], *extent);
		}
		else
		{
			uint32		offset = extent->off;

			Assert(extent->off < UINT32_MAX);
			success = seq_buf_write_u32(bufs[i], offset);
		}

		if (!success)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not write offset %lu to file %s",
								   (unsigned long) extent->off,
								   get_seq_buf_filename(&bufs[i]->shared->tag))));
		}
	}
	extent->len = InvalidFileExtentLen;
	extent->off = InvalidFileExtentOff;
}

/*
 * Returns true if page with given page number is under in-progress
 * checkpointing.
 */
bool
page_is_under_checkpoint(BTreeDescr *desc, OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	Oid			datoid,
				relnode;
	int			level = PAGE_GET_LEVEL(p),
				before_changecount,
				after_changecount;
	CurKeyType	cur_key;
	OInMemoryBlkno blkno_on_checkpoint;
	OInMemoryBlkno hikey_blkno_on_checkpoint;
	OIndexType	type;
	bool		result;

	while (true)
	{
		chkp_save_changecount_before(checkpoint_state, before_changecount);
		if (before_changecount & 1)
			continue;

		type = checkpoint_state->treeType;
		datoid = checkpoint_state->datoid;
		relnode = checkpoint_state->relnode;
		blkno_on_checkpoint = checkpoint_state->stack[level].blkno;
		hikey_blkno_on_checkpoint = checkpoint_state->stack[level].hikeyBlkno;
		cur_key = checkpoint_state->curKeyType;

		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount != after_changecount)
			continue;

		if (desc->oids.datoid != datoid ||
			desc->oids.relnode != relnode ||
			desc->type != type)
		{
			/* BTree is not under checkpoint */
			result = false;
		}
		else if (cur_key == CurKeyFinished)
		{
			/* checkpoint already finished */
			result = false;
		}
		else if (blkno_on_checkpoint == blkno ||
				 hikey_blkno_on_checkpoint == blkno)
		{
			/* page is under checkpoint */
			result = true;
		}
		else if (blkno_on_checkpoint == desc->rootInfo.rootPageBlkno && O_PAGE_IS(p, LEFTMOST))
		{
			/* concurrent rootPageBlkno split may happens */
			result = true;
		}
		else
		{
			/* page is not under checkpoint */
			Assert(blkno_on_checkpoint != blkno);
			result = false;
		}

		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount != after_changecount)
			continue;

		return result;
	}
}

/*
 * Returns true if btree is under in-progress checkpointing.
 */
bool
tree_is_under_checkpoint(BTreeDescr *desc)
{
	Oid			datoid,
				relnode;
	int			before_changecount,
				after_changecount;
	OIndexType	type;
	bool		result;

	while (true)
	{
		chkp_save_changecount_before(checkpoint_state, before_changecount);
		if (before_changecount & 1)
			continue;

		type = checkpoint_state->treeType;
		datoid = checkpoint_state->datoid;
		relnode = checkpoint_state->relnode;

		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount != after_changecount)
			continue;

		if (desc->oids.datoid != datoid ||
			desc->oids.relnode != relnode ||
			desc->type != type)
		{
			/* BTree is not under checkpoint */
			result = false;
		}
		else
		{
			result = true;
		}

		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount != after_changecount)
			continue;

		return result;
	}
}

/*
 * Returns -1 if page must be evicted to current in progress checkpoint.
 * Returns 1 if page must be evicted to next checkpoint.
 * Return 0 if page can not be evicted.
 *
 * We can't evict page if its hikey is in range [cur_key, lvl_hikey]:
 *
 * If we evict it to the current checkpoint (last_checkpoint_number + 1),
 * we may lost the page offset on concurrent split case.
 *
 * If we evict it to the next checkpoint (last_checkpoint_number + 2)
 * a page offset will be marked as free for current checkpoint,
 * than if we add downlink with the offset to an autonomous page, and restart
 * we will have the offset both free and busy for the current checkpoint.
 */
static inline int
side_of_checkpoint_bound(BTreeDescr *descr, Page page,
						 OTuple cur_key, CurKeyType cur_key_type,
						 OTuple lvl_hikey, CheckpointBound bound)
{
	int			cmp;
	OTuple		hikey;
	bool		page_is_rightmost = O_PAGE_IS(page, RIGHTMOST);

	Assert(cur_key_type == CurKeyValue || cur_key_type == CurKeyGreatest);

	/* fast checks, helps to exclusive rightmost case */

	if (cur_key_type == CurKeyGreatest)
	{
		/* left bound on rightmost pages */
		if (page_is_rightmost)
			return 0;
		return 1;
	}

	Assert(cur_key_type == CurKeyValue);
	if (page_is_rightmost)
	{
		/* case for rightmost page (no hikey to compare) */
		if (bound == CheckpointBoundRightmost)
			return 0;
		return -1;
	}

	/*
	 * left bound comparison
	 */
	Assert(!page_is_rightmost && cur_key_type == CurKeyValue);
	BTREE_PAGE_GET_HIKEY(hikey, page);
	cmp = o_btree_cmp(descr, &hikey, BTreeKeyNonLeafKey, &cur_key, BTreeKeyNonLeafKey);

	if (cmp == 0)
		return 0;

	if (cmp < 0)
		return 1;

	/* need to check right bound */
	Assert(cmp > 0);
	if (bound == CheckpointBoundNone)
	{
		/* right bound does not exist */
		return -1;
	}

	if (bound == CheckpointBoundRightmost)
	{
		/* right bound to the end of the BTree level */
		return 0;
	}

	/*
	 * right bound comparison
	 */
	Assert(!page_is_rightmost && bound == CheckpointBoundHikey);
	cmp = o_btree_cmp(descr, &hikey, BTreeKeyNonLeafKey, &lvl_hikey, BTreeKeyNonLeafKey);
	if (cmp <= 0)
		return 0;
	return -1;
}

/*
 * Compare tree identifiers in the same order we process them on checkpoint.
 */
static int
chkp_ordering_cmp(OIndexType type1, Oid datoid1, Oid relnode1,
				  OIndexType type2, Oid datoid2, Oid relnode2)
{
	if (datoid1 <= SYS_TREES_DATOID && datoid2 > SYS_TREES_DATOID)
		return -1;

	if (datoid1 > SYS_TREES_DATOID && datoid2 <= SYS_TREES_DATOID)
		return 1;

	if (type1 != type2)
		return type1 < type2 ? -1 : 1;
	if (datoid1 != datoid2)
		return datoid1 < datoid2 ? -1 : 1;
	if (relnode1 != relnode2)
		return relnode1 < relnode2 ? -1 : 1;

	return 0;
}

/*
 * Determine which checkpoint `blkno` should be written to.
 */
bool
get_checkpoint_number(BTreeDescr *desc, OInMemoryBlkno blkno,
					  uint32 *checkpoint_number, bool *copy_blkno)
{
	CheckpointBound bound;
	CurKeyType	cur_key_type;
	OFixedKey	lvl_hikey,
				cur_key;
	Page		page = O_GET_IN_MEMORY_PAGE(blkno);
	Oid			datoid,
				relnode;
	int			level = PAGE_GET_LEVEL(page),
				before_changecount,
				after_changecount,
				cmp;
	uint32		last_checkpoint_number;
	OInMemoryBlkno chkp_lvl_blkno,
				chkp_lvl_hikey_blkno;
	OIndexType	type;
	bool		under_checkpoint;

	while (true)
	{
		chkp_save_changecount_before(checkpoint_state, before_changecount);
		if ((before_changecount & 1) != 0)
			continue;

		last_checkpoint_number = checkpoint_state->lastCheckpointNumber;
		type = checkpoint_state->treeType;
		datoid = checkpoint_state->datoid;
		relnode = checkpoint_state->relnode;
		chkp_lvl_blkno = checkpoint_state->stack[level].blkno;
		chkp_lvl_hikey_blkno = checkpoint_state->stack[level].hikeyBlkno;
		bound = checkpoint_state->stack[level].bound;
		cur_key_type = checkpoint_state->curKeyType;

		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount != after_changecount)
			continue;

		cmp = chkp_ordering_cmp(desc->type, desc->oids.datoid,
								desc->oids.relnode,
								type, datoid, relnode);

		if (cmp != 0)
		{
			/* easy case: BTree is not under checkpoint */
			if (cmp < 0)
			{
				/* Already passed checkpoint */
				*checkpoint_number = last_checkpoint_number + 2;
				*copy_blkno = false;
			}
			else
			{
				/* Not yet passed by checkpoint */
				*checkpoint_number = last_checkpoint_number + 1;
				*copy_blkno = false;
			}

			chkp_save_changecount_after(checkpoint_state, after_changecount);
			if (before_changecount != after_changecount)
				continue;

			return true;
		}

		under_checkpoint = (chkp_lvl_blkno == blkno || chkp_lvl_hikey_blkno == blkno);
		if (!under_checkpoint && O_PAGE_IS(page, LEFTMOST))
		{
			/*
			 * Page can be under checkpoint if concurrent rootPageBlkno split
			 * happens.
			 */
			under_checkpoint = chkp_lvl_blkno == desc->rootInfo.rootPageBlkno;
		}

		/*
		 * Can't evict page which is now under checkpoint: checkpoint
		 * algorithm does not allow this.
		 *
		 * That is reason why this check before curKeyType check.
		 */
		if (under_checkpoint)
		{
			chkp_save_changecount_after(checkpoint_state, after_changecount);
			if (before_changecount != after_changecount)
				continue;
			return false;
		}

		/* the checkpointer does not write any page */
		if (cur_key_type == CurKeyLeast || cur_key_type == CurKeyFinished)
		{
			if (cur_key_type == CurKeyLeast)
				*checkpoint_number = last_checkpoint_number + 1;
			else
				*checkpoint_number = last_checkpoint_number + 2;
			*copy_blkno = false;

			chkp_save_changecount_after(checkpoint_state, after_changecount);
			if (before_changecount != after_changecount)
				continue;

			return true;
		}

		if (cur_key_type == CurKeyValue)
			copy_from_fixed_shmem_key(&cur_key, &checkpoint_state->curKeyValue);

		if (bound == CheckpointBoundHikey)
			copy_from_fixed_shmem_key(&lvl_hikey, &checkpoint_state->stack[level].hikey);

		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount != after_changecount)
			continue;

		cmp = side_of_checkpoint_bound(desc, page, cur_key.tuple, cur_key_type,
									   lvl_hikey.tuple, bound);

		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount != after_changecount)
			continue;

		if (cmp > 0)
		{
			*checkpoint_number = last_checkpoint_number + 2;
			*copy_blkno = true;
			return true;
		}
		else if (cmp < 0)
		{
			*checkpoint_number = last_checkpoint_number + 1;
			*copy_blkno = false;
			return true;
		}
		Assert(cmp == 0);
		return false;
	}
}

/*
 * Sets the autonomous level by backend. It should not be called for leafs.
 */
void
backend_set_autonomous_level(CheckpointState *state, uint32 level)
{
	uint32		cur_level = ORIOLEDB_MAX_DEPTH;

	/* no sense in autonomous level for leafs */
	Assert(level != 0);

	/*
	 * setups a new autonomous level if it less than current value of
	 * state->autonomousLevel
	 */
	while (!pg_atomic_compare_exchange_u32(&state->autonomousLevel,
										   &cur_level,
										   level))
	{
		if (cur_level <= level)
			break;
	}
}

/*
 * Make checkpoint of an index.
 */
static bool
checkpoint_ix(int flags, BTreeDescr *descr)
{
	FileExtentsArray *free_extents = NULL;
	char	   *filename,
			   *finalize_filename;
	BTreeMetaPage *meta_page;
	uint64		map_len,
				offset = 0,
				root_downlink;
	CheckpointFileHeader header = {0};
	File		file = -1;
	SeqBufTag	next_chkp_tag = {0},
				next_tmp_tag = {0},
				free_buf_tag;
	int			cur_chkp_index,
				next_chkp_index;
	Oid			datoid = descr->oids.datoid;
	Oid			relnode = descr->oids.relnode;
	bool		success,
				is_compressed = OCompressIsValid(descr->compress);
	CheckpointWriteBack writeback;
	off_t		file_length;

	/*
	 * TODO: can we make checkpoint on evicted or unloaded tree?
	 */
	checkpoint_ix_init_state(checkpoint_state, descr);

	cur_chkp_index = (checkpoint_state->lastCheckpointNumber + 1) % 2;
	next_chkp_index = (checkpoint_state->lastCheckpointNumber + 2) % 2;
	meta_page = BTREE_GET_META(descr);

	Assert(ORootPageIsValid(descr) && OMetaPageIsValid(descr));

	/* Initialize next checkpoint file and next tmp file */
	memset(&next_chkp_tag, 0, sizeof(next_chkp_tag));
	next_chkp_tag.datoid = datoid;
	next_chkp_tag.relnode = relnode;
	next_chkp_tag.num = checkpoint_state->lastCheckpointNumber + 2;
	next_chkp_tag.type = 'm';

	if (!init_seq_buf_pages(descr, &meta_page->nextChkp[next_chkp_index]) ||
		!init_seq_buf_pages(descr, &meta_page->tmpBuf[next_chkp_index]))
	{
		free_seq_buf_pages(descr, &meta_page->nextChkp[next_chkp_index]);
		free_seq_buf_pages(descr, &meta_page->tmpBuf[next_chkp_index]);
		elog(FATAL, "Unable to get pages for sequence buffers.");
	}

	success = init_seq_buf(&descr->nextChkp[next_chkp_index],
						   &meta_page->nextChkp[next_chkp_index],
						   &next_chkp_tag, true, true, sizeof(CheckpointFileHeader), NULL);
	if (!success)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create a new sequence buffer file %s",
						get_seq_buf_filename(&next_chkp_tag))));

	memset(&next_tmp_tag, 0, sizeof(next_tmp_tag));
	next_tmp_tag.datoid = datoid;
	next_tmp_tag.relnode = relnode;
	next_tmp_tag.num = checkpoint_state->lastCheckpointNumber + 2;
	next_tmp_tag.type = 't';

	success = init_seq_buf(&descr->tmpBuf[next_chkp_index],
						   &meta_page->tmpBuf[next_chkp_index],
						   &next_tmp_tag, true, true, 0, NULL);
	if (!success)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not init a new sequence buffer file %s",
						get_seq_buf_filename(&next_tmp_tag))));

	/* Make checkpoint of the tree itself */
	init_writeback(&writeback, flags, is_compressed);
	root_downlink = checkpoint_btree(&descr, checkpoint_state, &writeback);
	if (!DiskDownlinkIsValid(root_downlink))
	{
		free_writeback(&writeback);
		return false;
	}
	descr = perform_writeback_and_relock(descr, &writeback,
										 checkpoint_state, NULL, 0);
	free_writeback(&writeback);
	if (!descr)
		return false;

	Assert(checkpoint_state->curKeyType == CurKeyGreatest);
	Assert(DiskDownlinkIsValid(root_downlink));

	if (!use_device)
	{
		if (is_compressed)
			file_length = pg_atomic_read_u64(&meta_page->datafileLength) * ORIOLEDB_COMP_BLCKSZ;
		else
			file_length = pg_atomic_read_u64(&meta_page->datafileLength) * ORIOLEDB_BLCKSZ;
		btree_smgr_sync(descr, file_length);
	}

	if (is_compressed)
	{
		free_extents = file_extents_array_init();
		foreach_free_extent(descr, foreach_extent_append, (void *) free_extents);
	}

	STOPEVENT(STOPEVENT_BEFORE_BLKNO_LOCK, NULL);

	/*
	 * Need a lock to be sure, that nobody is concurrently copying block
	 * number from previous checkpoint to current.  See write_page() for
	 * details.
	 */
	LWLockAcquire(&meta_page->copyBlknoLock, LW_EXCLUSIVE);

	chkp_inc_changecount_before(checkpoint_state);
	checkpoint_state->curKeyType = CurKeyFinished;
	chkp_inc_changecount_after(checkpoint_state);

	/* Make header for the map file... */
	header.rootDownlink = root_downlink;
	header.datafileLength = pg_atomic_read_u64(&meta_page->datafileLength);
	header.leafPagesNum = pg_atomic_read_u32(&meta_page->leafPagesNum);
	header.ctid = pg_atomic_read_u64(&meta_page->ctid);

	if (!is_compressed)
	{
		offset = seq_buf_get_offset(&descr->freeBuf);
		free_buf_tag = descr->freeBuf.shared->tag;
	}
	LWLockRelease(&meta_page->copyBlknoLock);

	/* finalizes *.tmp file */
	seq_buf_finalize(&descr->tmpBuf[cur_chkp_index]);
	free_seq_buf_pages(descr, descr->tmpBuf[cur_chkp_index].shared);

	/* finalizes *.map file */
	map_len = seq_buf_finalize(&descr->nextChkp[cur_chkp_index]);
	free_seq_buf_pages(descr, descr->nextChkp[cur_chkp_index].shared);
	filename = get_seq_buf_filename(&descr->nextChkp[cur_chkp_index].tag);
	file = PathNameOpenFile(filename, O_RDWR | PG_BINARY);
	if (file < 0)
	{
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open checkpoint file %s", filename)));
	}

	if (is_compressed && free_extents->size != 0)
	{
		/*
		 * We need to combine a *.map file content and the free_extents array
		 * and remove all intersections
		 */
		FileExtent *map_extents = NULL;
		off_t		map_extents_size,
					write_offset = sizeof(CheckpointFileHeader);

		Assert(free_extents != NULL);

		map_extents_size = (map_len - sizeof(CheckpointFileHeader))
			/ sizeof(FileExtent);
		if (map_extents_size > 0)
		{
			/* read and sort *.map file data */
			off_t		map_extents_bytes;

			map_extents_bytes = sizeof(FileExtent) * map_extents_size;

			map_extents = (FileExtent *) palloc(map_extents_bytes);
			if (OFileRead(file, (char *) map_extents, map_extents_bytes,
						  sizeof(CheckpointFileHeader),
						  WAIT_EVENT_DATA_FILE_READ) != map_extents_bytes)
			{
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not to read extents from file %s",
									   filename)));
			}

			/* sort it */
			pg_qsort(map_extents, map_extents_size,
					 sizeof(FileExtent), file_extents_off_len_cmp);

			/* and truncate the file because it may be less than original */
			if (FileTruncate(file, sizeof(CheckpointFileHeader),
							 WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
			{
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not to truncate file %s",
									   filename)));
			}
		}

		Assert(free_extents->size != 0);	/* checked above */
		if (map_extents_size == 0)
		{
			/* easy case - just write sorted file extents */
			int			write_bytes = free_extents->size * sizeof(FileExtent);

			if (OFileWrite(file, (char *) free_extents->extents,
						   write_bytes, write_offset, WAIT_EVENT_SLRU_WRITE) != write_bytes)
			{
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not to write extents to file %s",
									   filename)));
			}
			header.numFreeBlocks = free_extents->size;
		}
		else
		{
			/* create a new combined *.map file */
			FileExtent *cur = NULL;
			char		write_buf[ORIOLEDB_BLCKSZ];
			int			f_i = 0,
						m_i = 0,
						write_buf_len = 0;

			header.numFreeBlocks = 0;
			while (f_i < free_extents->size || m_i < map_extents_size)
			{
				if (f_i == free_extents->size)
				{
					cur = &map_extents[m_i++];
				}
				else if (m_i == map_extents_size)
				{
					cur = &free_extents->extents[f_i++];
				}
				else if (map_extents[m_i].off < free_extents->extents[f_i].off)
				{
					cur = &map_extents[m_i++];
				}
				else
				{
					cur = &free_extents->extents[f_i++];
					while (m_i < map_extents_size
						   && map_extents[m_i].off < cur->off + cur->len)
					{
						/* skip intersection */
						m_i++;
					}
				}

				if (write_buf_len + sizeof(FileExtent) > ORIOLEDB_BLCKSZ)
				{
					/* flush the buffer */
					if (OFileWrite(file, write_buf, write_buf_len, write_offset,
								   WAIT_EVENT_SLRU_WRITE) != write_buf_len)
					{
						ereport(FATAL, (errcode_for_file_access(),
										errmsg("could not to write extents to file %s",
											   filename)));
					}
					write_offset += write_buf_len;
					write_buf_len = 0;
				}

				memcpy(write_buf + write_buf_len, cur, sizeof(FileExtent));
				write_buf_len += sizeof(FileExtent);
				header.numFreeBlocks++;
			}

			if (write_buf_len > 0)
			{
				if (OFileWrite(file, write_buf, write_buf_len, write_offset,
							   WAIT_EVENT_SLRU_WRITE) != write_buf_len)
				{
					ereport(FATAL, (errcode_for_file_access(),
									errmsg("could not to write extents to file %s",
										   filename)));
				}
			}
		}

		/* free allocated bytes */
		if (map_extents_size > 0)
			pfree(map_extents);
		file_extents_array_free(free_extents);
	}
	else if (!is_compressed)
	{
		Assert(!is_compressed);
		finalize_filename = seq_buf_file_exist(&free_buf_tag)
			? get_seq_buf_filename(&free_buf_tag)
			: NULL;
		map_len = finalize_chkp_map(file, map_len, finalize_filename, offset,
									free_buf_tag.num);
		if (finalize_filename)
			pfree(finalize_filename);
		header.numFreeBlocks = (map_len - sizeof(CheckpointFileHeader)) / (use_device ? sizeof(FileExtent) : sizeof(uint32));
	}
	else
	{
		/* nothing to do */
		Assert(is_compressed && free_extents->size == 0);
		header.numFreeBlocks = (map_len - sizeof(CheckpointFileHeader)) / sizeof(FileExtent);
	}

	if (OFileWrite(file, (Pointer) &header, sizeof(header), 0,
				   WAIT_EVENT_SLRU_WRITE) != sizeof(header) ||
		FileSync(file, WAIT_EVENT_SLRU_SYNC) != 0)
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not write checkpoint header to file %s", filename)));
	}
	FileClose(file);
	pfree(filename);

	chkp_inc_changecount_before(checkpoint_state);
	checkpoint_state->completed = true;
	chkp_inc_changecount_after(checkpoint_state);
	return true;
}


/*
 * Checkpointer walk over particular B-tree. Returns rootPageBlkno page offset.
 */
static uint64
checkpoint_btree(BTreeDescr **descrPtr, CheckpointState *state,
				 CheckpointWriteBack *writeback)
{
	uint64		root_downlink;
	MemoryContext tmp_context,
				prev_context;

	tmp_context = AllocSetContextCreate(CurrentMemoryContext,
										"checkpoint temporary context",
										ALLOCSET_DEFAULT_SIZES);
	prev_context = MemoryContextSwitchTo(tmp_context);

	set_skip_ucm();
	/* Walk the tree recursively starting from rootPageBlkno */
	root_downlink = checkpoint_btree_loop(descrPtr,
										  state,
										  writeback,
										  tmp_context);
	unset_skip_ucm();

	checkpoint_reset_stack(state);

	MemoryContextSwitchTo(prev_context);
	MemoryContextDelete(tmp_context);

	return root_downlink;
}

/*
 * Resets autonomous level to default value and returns previous value.
 */
static inline uint32
checkpointer_reset_autonomous_level(CheckpointState *state)
{
	uint32		cur_level = ORIOLEDB_MAX_DEPTH;

	/*
	 * CAS read of current autonomous level and setup it to default value
	 */
	while (!pg_atomic_compare_exchange_u32(&state->autonomousLevel,
										   &cur_level,
										   ORIOLEDB_MAX_DEPTH));
	return cur_level;
}

/*
 * Setups autonomous flag for stack items from state->autonomousLevel.
 *
 * Should be used only under lock_page() for avoid concurrent issues in
 * min_level == autonomous_level case.
 *
 * min_level used only for assertion.
 */
static inline void
checkpointer_update_autonomous(BTreeDescr *desc, CheckpointState *state)
{
	int			i,
				cur_chkp_num = state->lastCheckpointNumber + 1,
				autonomous_level;

	autonomous_level = checkpointer_reset_autonomous_level(state);
	if (autonomous_level == ORIOLEDB_MAX_DEPTH)
	{
		/*
		 * autonomous level is default value, no need to setup autonomous
		 * flags
		 */
		return;
	}

	if (state->curKeyType == CurKeyLeast)
	{
		/*
		 * we do not make any leaf write, no sense yet.
		 */
		return;
	}

	Assert(autonomous_level > 0);
	Assert(OInMemoryBlknoIsValid(state->stack[autonomous_level].blkno));

	/* go upwards for the stack and setup autonomous flag if needed */
	for (i = autonomous_level; OInMemoryBlknoIsValid(state->stack[i].blkno); i++)
	{
		OrioleDBPageDesc *page_desc;
		BTreePageHeader *header;

		if (!state->stack[i].autonomous)
		{
			state->stack[i].autonomous = true;
			state->stack[i].autonomousLeftmost = state->stack[i].leftmost;

			page_desc = O_GET_IN_MEMORY_PAGEDESC(state->stack[i].blkno);
			header = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(state->stack[i].blkno);

			header->checkpointNum = 0;
			if (FileExtentIsValid(page_desc->fileExtent))
			{
				/* the offset will not be used in current checkpoint */
				free_extent_for_checkpoint(desc, &page_desc->fileExtent, cur_chkp_num);
				MARK_DIRTY(desc->ppool, state->stack[i].blkno);
			}
		}
	}

	/* stack last item must be the rootPageBlkno page */
	Assert(state->stack[i - 1].blkno == desc->rootInfo.rootPageBlkno);
}

/*
 * Locks page, updates autonomous flags and stack after rootPageBlkno split.
 */
static void
checkpoint_lock_page(BTreeDescr *descr, CheckpointState *state,
					 OInMemoryBlkno *blkno, uint32 page_chage_count, int level)
{
	Page		page,
				img;
	int			l,
				page_level;
	OInMemoryBlkno next_blkno;
	bool		autonomous;

	lock_page(*blkno);
	page = O_GET_IN_MEMORY_PAGE(*blkno);
	page_level = PAGE_GET_LEVEL(page);
	if (page_level == level)
	{
		checkpointer_update_autonomous(descr, state);
		return;
	}

	if (*blkno != descr->rootInfo.rootPageBlkno)
	{
		Assert(page_chage_count != InvalidOPageChangeCount);
		Assert(page_chage_count != O_PAGE_GET_CHANGE_COUNT(page));
		return;
	}

	/*
	 * only rootPageBlkno page split increases page level (and only for
	 * rootPageBlkno page)
	 */
	Assert(page_level > level && *blkno == descr->rootInfo.rootPageBlkno);

	/*
	 * Concurrent rootPageBlkno spit happens. We need to fill stack from a new
	 * rootPageBlkno level (page_level) to old rootPageBlkno level (level).
	 */

	/* we need to setup autonomous flag the same to old rootPageBlkno level */
	autonomous = state->stack[level].autonomous;

	chkp_inc_changecount_before(state);
	for (l = page_level; l > level; l--)
	{
		BTreePageItemLocator pageLoc,
					imageLoc;
		LocationIndex itemsize;
		BTreeNonLeafTuphdr *tuphdr;
		uint64		downlink;

		BTREE_PAGE_LOCATOR_FIRST(page, &pageLoc);
		itemsize = BTREE_PAGE_GET_ITEM_SIZE(page, &pageLoc);

		Assert(l > 0);
		Assert(!OInMemoryBlknoIsValid(state->stack[l].blkno));

		state->stack[l].autonomous = autonomous;
		if (!autonomous && !O_PAGE_IS(page, RIGHTMOST))
		{
			/* we did not forget about merge */
			copy_fixed_shmem_hikey(descr, &state->stack[l].hikey, page);
			state->stack[l].bound = CheckpointBoundHikey;
		}

		state->stack[l].blkno = *blkno;
		state->stack[l].hikeyBlkno = *blkno;
		state->stack[l].offset = BTREE_PAGE_LOCATOR_GET_OFFSET(page, &pageLoc) + 1;
		state->stack[l].nextkeyType = NextKeyNone;
		state->stack[l].leftmost = true;
		state->stack[l].autonomousLeftmost = true;
		chkp_inc_changecount_after(state);

		img = state->stack[l].image;
		memset(img, 0, ORIOLEDB_BLCKSZ);
		init_page_first_chunk(descr, img, 0);
		BTREE_PAGE_LOCATOR_FIRST(img, &imageLoc);
		page_locator_insert_item(img, &imageLoc, itemsize);
		memcpy(BTREE_PAGE_LOCATOR_GET_ITEM(img, &imageLoc),
			   BTREE_PAGE_LOCATOR_GET_ITEM(page, &pageLoc),
			   itemsize);
		BTREE_PAGE_SET_ITEM_FLAGS(img, &imageLoc, BTREE_PAGE_GET_ITEM_FLAGS(page, &pageLoc));

		tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(page, &pageLoc);
		downlink = tuphdr->downlink;

		/* Page under checkpoint shouldn't be evicted... */
		Assert(DOWNLINK_IS_IN_MEMORY(downlink));

		next_blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(downlink);
		unlock_page(*blkno);

		*blkno = next_blkno;
		lock_page(*blkno);
		page = O_GET_IN_MEMORY_PAGE(*blkno);
		chkp_inc_changecount_before(state);
	}

	Assert(l == level);
	Assert(!O_PAGE_IS(page, RIGHTMOST));	/* can not be merged */

	state->stack[level].blkno = *blkno;
	state->stack[level].hikeyBlkno = *blkno;

	checkpointer_update_autonomous(descr, state);
	if (!state->stack[level].autonomous)
	{
		copy_fixed_shmem_hikey(descr, &state->stack[level].hikey, page);
		state->stack[level].bound = CheckpointBoundHikey;
	}
	else if (!autonomous)
	{
		/*
		 * autonomous state of the level updated while we traverse down,
		 * update parents state
		 */
		Assert(state->stack[level].bound == CheckpointBoundRightmost);
		for (l = page_level; l > level; l--)
		{
			state->stack[l].autonomous = true;
			state->stack[l].bound = CheckpointBoundRightmost;
		}
	}
	chkp_inc_changecount_after(state);

}

static bool
checkpoint_try_merge_page(BTreeDescr *descr, CheckpointState *state,
						  OInMemoryBlkno blkno, int level)
{
	OInMemoryBlkno parentBlkno = state->stack[level + 1].blkno,
				rightBlkno;
	Page		parentPage = O_GET_IN_MEMORY_PAGE(parentBlkno),
				rightPage,
				page = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageItemLocator loc;
	BTreeNonLeafTuphdr *tuphdr;
	OTuple		key PG_USED_FOR_ASSERTS_ONLY;
	bool		mergeParent = false;

	if (RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(page)))
		return false;

	if (state->stack[level].hikeyBlkno == blkno)
		return false;

	if (!try_lock_page(parentBlkno))
		return false;

	if (state->stack[level + 1].offset < 1 ||
		state->stack[level + 1].offset >= BTREE_PAGE_ITEMS_COUNT(parentPage))
	{
		unlock_page(parentBlkno);
		return false;
	}

	BTREE_PAGE_OFFSET_GET_LOCATOR(parentPage, state->stack[level + 1].offset - 1, &loc);
	Assert(BTREE_PAGE_LOCATOR_IS_VALID(parentPage, &loc));
	BTREE_PAGE_READ_INTERNAL_ITEM(tuphdr, key, parentPage, &loc);
	Assert(!O_TUPLE_IS_NULL(key));

	if (!DOWNLINK_IS_IN_MEMORY(tuphdr->downlink) ||
		DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink) != blkno)
	{
		unlock_page(parentBlkno);
		return false;
	}

	BTREE_PAGE_LOCATOR_NEXT(parentPage, &loc);
	Assert(BTREE_PAGE_LOCATOR_IS_VALID(parentPage, &loc));
	BTREE_PAGE_READ_INTERNAL_ITEM(tuphdr, key, parentPage, &loc);
	Assert(!O_TUPLE_IS_NULL(key));

	if (!DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
	{
		unlock_page(parentBlkno);
		return false;
	}

	rightBlkno = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
	rightPage = O_GET_IN_MEMORY_PAGE(rightBlkno);

	if (!try_lock_page(rightBlkno))
	{
		unlock_page(parentBlkno);
		return false;
	}

	if (RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(rightPage)))
	{
		unlock_page(parentBlkno);
		unlock_page(rightBlkno);
		return false;
	}

	if (btree_try_merge_pages(descr, parentBlkno, NULL, &mergeParent,
							  blkno, loc, rightBlkno))
	{
		release_undo_size(UndoReserveTxn);
		free_retained_undo_location();

		reserve_undo_size(UndoReserveTxn, 2 * O_MERGE_UNDO_IMAGE_SIZE);
		return true;
	}
	else
	{
		unlock_page(parentBlkno);
		unlock_page(rightBlkno);
		return false;
	}
}

/*
 * Locks a given page for safely processing by the checkpointer.
 */
static void
checkpoint_fix_split_and_lock_page(BTreeDescr *descr, CheckpointState *state,
								   OInMemoryBlkno *blkno, uint32 page_chage_count, int level)
{
	OInMemoryBlkno old_blkno;

	reserve_undo_size(UndoReserveTxn, 2 * O_MERGE_UNDO_IMAGE_SIZE);

	while (true)
	{
		bool		relocked = false;

		old_blkno = *blkno;

		checkpoint_lock_page(descr, state, blkno, page_chage_count, level);

		if (old_blkno == *blkno && page_chage_count != InvalidOPageChangeCount &&
			O_GET_IN_MEMORY_PAGE_CHANGE_COUNT(*blkno) != page_chage_count)
			break;

		if (o_btree_split_is_incomplete(*blkno, &relocked))
		{
			o_btree_split_fix_and_unlock(descr, *blkno);
			reserve_undo_size(UndoReserveTxn, 2 * O_MERGE_UNDO_IMAGE_SIZE);
		}
		else if (!(level > 0 && *blkno == state->stack[level].hikeyBlkno) &&
				 is_page_too_sparse(descr, O_GET_IN_MEMORY_PAGE(*blkno)))
		{
			/*
			 * Try merge page to the right.  Skip merge for autonomous pages,
			 * because we could miss the expected hikey then.
			 */

			if (!checkpoint_try_merge_page(descr, state, *blkno, level))
				break;
		}
		else if (relocked)
			unlock_page(*blkno);
		else
			break;
	}

	release_undo_size(UndoReserveTxn);
	free_retained_undo_location();
}

static void
next_key_to_jsonb(JsonbParseState **state, BTreeDescr *descr,
				  NextKeyType keyType, OTuple keyValue)
{
	pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	if (keyType == NextKeyGreatest)
		jsonb_push_string_key(state, "type", "greatest");
	else if (keyType == NextKeyNone)
		jsonb_push_string_key(state, "type", "none");
	else if (keyType == NextKeyValue)
	{
		jsonb_push_string_key(state, "type", "value");
		jsonb_push_key(state, "value");
		o_btree_key_to_jsonb(descr, keyValue, state);
	}
	pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

static Jsonb *
prepare_checkpoint_step_params(BTreeDescr *descr,
							   CheckpointState *chkpState,
							   WalkMessage *message,
							   int level)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(descr, &state);
	jsonb_push_int8_key(&state, "level", level);
	if (!message)
	{
		jsonb_push_string_key(&state, "action", "none");
	}
	else if (message->action == WalkDownwards)
	{
		jsonb_push_string_key(&state, "action", "walkDownwards");
		jsonb_push_int8_key(&state, "blkno", message->content.downwards.blkno);
		jsonb_push_int8_key(&state, "pageChangeCount", message->content.downwards.pageChangeCount);
		if (!O_TUPLE_IS_NULL(message->content.downwards.lokey.tuple))
		{
			jsonb_push_key(&state, "lokey");
			o_btree_key_to_jsonb(descr, message->content.downwards.lokey.tuple, &state);
		}
		else
		{
			jsonb_push_null_key(&state, "lokey");
		}
	}
	else if (message->action == WalkUpwards)
	{
		jsonb_push_string_key(&state, "action", "walkUpwards");
		jsonb_push_bool_key(&state, "parentDirty", message->content.upwards.parentDirty);
		jsonb_push_bool_key(&state, "saveTuple", message->content.upwards.saveTuple);
		jsonb_push_key(&state, "downlink");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		jsonb_push_int8_key(&state, "offset", DOWNLINK_GET_DISK_OFF(message->content.upwards.diskDownlink));
		jsonb_push_int8_key(&state, "length", DOWNLINK_GET_DISK_LEN(message->content.upwards.diskDownlink));
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
		if (message->content.upwards.saveTuple || DiskDownlinkIsValid(message->content.upwards.diskDownlink))
		{
			jsonb_push_key(&state, "nextKey");
			next_key_to_jsonb(&state, descr, message->content.upwards.nextkeyType,
							  message->content.upwards.nextkey.tuple);
		}
	}
	else if (message->action == WalkContinue)
	{
		CheckpointPageInfo *pageInfo = &chkpState->stack[level];

		jsonb_push_string_key(&state, "action", "walkContinue");
		jsonb_push_bool_key(&state, "autonomous", pageInfo->autonomous);
		jsonb_push_bool_key(&state, "leftmost", pageInfo->leftmost);
		jsonb_push_int8_key(&state, "blkno", pageInfo->blkno);
		jsonb_push_int8_key(&state, "offset", pageInfo->offset);
		jsonb_push_key(&state, "nextKey");
		next_key_to_jsonb(&state, descr, pageInfo->nextkeyType,
						  fixed_shmem_key_get_tuple(&pageInfo->nextkey));
	}

	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

static Jsonb *
prepare_checkpoint_table_start_params(ORelOids tableOids, ORelOids treeOids)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_key(&state, "table");
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(&state, "datoid", tableOids.datoid);
	jsonb_push_int8_key(&state, "reloid", tableOids.reloid);
	jsonb_push_int8_key(&state, "relnode", tableOids.relnode);
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	jsonb_push_key(&state, "tree");
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(&state, "datoid", treeOids.datoid);
	jsonb_push_int8_key(&state, "reloid", treeOids.reloid);
	jsonb_push_int8_key(&state, "relnode", treeOids.relnode);
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

static Jsonb *
prepare_checkpoint_tree_start_params(BTreeDescr *desc)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(desc, &state);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

static uint64
checkpoint_btree_loop(BTreeDescr **descrPtr,
					  CheckpointState *state,
					  CheckpointWriteBack *writeback,
					  MemoryContext tmp_context)
{
	WalkMessage message;
	Page		page;
	uint64		downlink;
	int			level,
				i;
	BTreeDescr *descr = *descrPtr;
	OInMemoryBlkno blkno = descr->rootInfo.rootPageBlkno;
	uint32		page_chage_count = InvalidOPageChangeCount;
	uint		blcksz = OCompressIsValid(descr->compress) ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ;
	Jsonb	   *params;

	memset(&message, 0, sizeof(WalkMessage));

	/* Prepare message start walk from the rootPageBlkno */
	page = O_GET_IN_MEMORY_PAGE(blkno);
	lock_page(blkno);
	level = PAGE_GET_LEVEL(page);
	message.action = WalkDownwards;
	message.content.downwards.blkno = blkno;
	message.content.downwards.pageChangeCount = O_PAGE_GET_CHANGE_COUNT(page);
	clear_fixed_key(&message.content.downwards.lokey);

	chkp_inc_changecount_before(state);
	for (i = level; i < ORIOLEDB_MAX_DEPTH; i++)
	{
		state->stack[i].bound = CheckpointBoundRightmost;
		state->stack[i].hikeyBlkno = OInvalidInMemoryBlkno;
	}
	/* avoid fail on first traverse to rootPageBlkno */
	state->stack[level].blkno = blkno;
	chkp_inc_changecount_after(state);

	level++;
	unlock_page(blkno);

	while (true)
	{
		Assert(!have_locked_pages());
		Assert(!have_retained_undo_location());

		MemoryContextReset(tmp_context);

		if (STOPEVENTS_ENABLED())
			params = prepare_checkpoint_step_params(descr, state,
													&message, level);
		STOPEVENT(STOPEVENT_CHECKPOINT_STEP, params);

		if (message.action == WalkDownwards &&
			level >= 4 &&
			writeback->extentsNumber >= checkpoint_flush_after * (BLCKSZ / blcksz))
		{
			descr = perform_writeback_and_relock(descr, writeback, state,
												 &message, level);
			if (!descr)
				return InvalidDiskDownlink;
			*descrPtr = descr;
		}

		if (message.action == WalkDownwards)
		{
			bool		was_dirty,
						parent_dirty;
			Page		img;
			OrioleDBPageDesc *page_desc = NULL;

			Assert(level > 0);
			level--;

			blkno = message.content.downwards.blkno;
			page_chage_count = message.content.downwards.pageChangeCount;

			checkpoint_fix_split_and_lock_page(descr, state, &blkno, page_chage_count, level);

			page = O_GET_IN_MEMORY_PAGE(blkno);

			/*
			 * Check if it not the page we expected, because it might
			 * disappear due to concurrent eviction.
			 */
			if (O_PAGE_GET_CHANGE_COUNT(page) != message.content.downwards.pageChangeCount
				&& blkno == message.content.downwards.blkno)	/* rootPageBlkno level
																 * does not change */
			{
				unlock_page(blkno);
				message.action = WalkUpwards;
				message.content.upwards.parentDirty = false;
				message.content.upwards.diskDownlink = InvalidDiskDownlink;
				message.content.upwards.saveTuple = false;
				continue;
			}

			img = state->stack[level].image;
			if (!state->stack[level].autonomous)
			{
				/* Updates right checkpoint bound. */
				chkp_inc_changecount_before(state);
				if (!O_PAGE_IS(page, RIGHTMOST))
				{
					copy_fixed_shmem_hikey(descr, &state->stack[level].hikey,
										   page);
					state->stack[level].bound = CheckpointBoundHikey;
					state->stack[level].hikeyBlkno = blkno;
				}
				else
				{
					state->stack[level].bound = CheckpointBoundRightmost;
					state->stack[level].hikeyBlkno = OInvalidInMemoryBlkno;
				}
				chkp_inc_changecount_after(state);
			}

			if (level != 0 && state->stack[level].autonomous)
			{
				/*
				 * Downwards to autonomous node, we must free file offset if
				 * it exist.
				 */
				BTreePageHeader *header = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(blkno);

				page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);

				header->checkpointNum = 0;
				if (FileExtentIsValid(page_desc->fileExtent))
				{
					/* the offset will not be used in current checkpoint */
					free_extent_for_checkpoint(descr, &page_desc->fileExtent,
											   checkpoint_state->lastCheckpointNumber + 1);
					MARK_DIRTY(descr->ppool, blkno);
				}
			}

			/*
			 * Leaf pages are going to be written immediately.  So, check
			 * there is no IO in progress.
			 */
			if (level == 0)
			{
				int			ionum;

				page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
				ionum = page_desc->ionum;
				if (ionum >= 0)
				{
					unlock_page(blkno);
					wait_for_io_completion(ionum);
					level++;
					continue;
				}
			}

			chkp_inc_changecount_before(state);
			state->stack[level].blkno = blkno;
			chkp_inc_changecount_after(state);
			state->stack[level].offset = 0;

			if (level == 0)
			{
				memcpy(img, page, ORIOLEDB_BLCKSZ);
				was_dirty = IS_DIRTY(blkno);
				if (was_dirty)
				{
					/* Code above ensured there is no IO in progress */
					Assert(page_desc->ionum < 0);
					page_desc->ionum = assign_io_num(blkno, InvalidOffsetNumber);

					/*
					 * we assume that concurrent eviction of the parent is
					 * forbidden by get_checkpoint_number() in walk_page()
					 */
					CLEAN_DIRTY(descr->ppool, blkno);

					/* prepare_leaf_page() unlocks page */
					prepare_leaf_page(descr, state);

					downlink = perform_page_io(descr,
											   blkno,
											   state->stack[level].image,
											   state->lastCheckpointNumber + 1,
											   false,
											   &parent_dirty);

					if (!DiskDownlinkIsValid(downlink))
					{
						elog(ERROR, "unable to perform page IO for page %d to file %s with offset %lu",
							 blkno,
							 btree_smgr_filename(descr, page_desc->fileExtent.off),
							 (uint64) page_desc->fileExtent.off);
					}

					writeback_put_extent(writeback, &page_desc->fileExtent);
					unlock_io(page_desc->ionum);
					page_desc->ionum = -1;
				}
				else
				{
					Assert(state->stack[level].autonomous == false);
					parent_dirty = false;
					downlink = MAKE_ON_DISK_DOWNLINK(O_GET_IN_MEMORY_PAGEDESC(blkno)->fileExtent);

					/* prepare_leaf_page() unlocks page */
					prepare_leaf_page(descr, state);
				}

				/* Indicate that we've finished that page image */
				message.action = WalkUpwards;
				Assert(DiskDownlinkIsValid(downlink));
				message.content.upwards.parentDirty = parent_dirty;
				message.content.upwards.diskDownlink = downlink;
				message.content.upwards.saveTuple = false;
				if (O_PAGE_IS(img, RIGHTMOST))
				{
					message.content.upwards.nextkeyType = NextKeyGreatest;
				}
				else
				{
					message.content.upwards.nextkeyType = NextKeyValue;
					copy_fixed_hikey(descr, &message.content.upwards.nextkey, img);
				}
				BTREE_PAGE_ITEMS_COUNT(img) = 0;
				state->stack[level].nextkeyType = NextKeyNone;
				continue;
			}
			/* else level != 0 */

			if (BTREE_PAGE_ITEMS_COUNT(img) == 0)
			{
				memset(img, 0, ORIOLEDB_BLCKSZ);
				init_page_first_chunk(descr, img, 0);
			}

			/* saves lokey for the node */
			if (O_TUPLE_IS_NULL(message.content.downwards.lokey.tuple))
			{
				state->stack[level].leftmost = true;
			}
			else
			{
				state->stack[level].leftmost = false;
				copy_fixed_shmem_key(descr,
									 &state->stack[level].lokey,
									 message.content.downwards.lokey.tuple);
			}
		}
		else if (message.action == WalkUpwards)
		{
			BTreeNonLeafTuphdr *tuphdr;
			Page		img;
			bool		save_item,
						valid_doff;

			save_item = message.content.upwards.saveTuple;
			valid_doff = DiskDownlinkIsValid(message.content.upwards.diskDownlink);

			level = level + 1;
			page_chage_count = InvalidOPageChangeCount;

			state->stack[level].autonomousTupleExist = save_item;
			/* Is everything done? */
			if (!OInMemoryBlknoIsValid(state->stack[level].blkno))
			{
				Assert(valid_doff);
				return message.content.upwards.diskDownlink;
			}

			img = state->stack[level].image;
			Assert(BTREE_PAGE_ITEMS_COUNT(img) > 0);

			/* Did we manage to write the page? */
			if (!valid_doff && !save_item)
			{
				/* Setup next key */
				if (BTREE_PAGE_ITEMS_COUNT(img) == 1)
				{
					BTREE_PAGE_ITEMS_COUNT(img) = 0;
					memset(img, 0, ORIOLEDB_BLCKSZ);
					init_page_first_chunk(descr, img, 0);
					state->stack[level].nextkeyType = NextKeyNone;
				}
				else
				{
					BTreePageItemLocator loc;

					BTREE_PAGE_LOCATOR_LAST(img, &loc);
					state->stack[level].nextkeyType = NextKeyValue;
					copy_fixed_shmem_page_key(descr,
											  &state->stack[level].nextkey,
											  img, &loc);
					page_locator_delete_item(img, &loc);
				}

				if (state->stack[level].offset > 0)
					state->stack[level].offset--;
			}
			else
			{
				state->stack[level].nextkeyType = message.content.upwards.nextkeyType;
				if (state->stack[level].nextkeyType == NextKeyValue)
					copy_fixed_shmem_key(descr,
										 &state->stack[level].nextkey,
										 message.content.upwards.nextkey.tuple);

				if (!save_item)
				{
					/* make downlink */
					BTreePageItemLocator loc;

					Assert(valid_doff);
					BTREE_PAGE_LOCATOR_LAST(img, &loc);
					tuphdr = (BTreeNonLeafTuphdr *)
						BTREE_PAGE_LOCATOR_GET_ITEM(img, &loc);
					tuphdr->downlink = message.content.upwards.diskDownlink;
				}
			}

			blkno = state->stack[level].blkno;
			checkpoint_fix_split_and_lock_page(descr, state, &blkno, page_chage_count, level);
			page = O_GET_IN_MEMORY_PAGE(blkno);

			if (message.content.upwards.parentDirty)
				MARK_DIRTY(descr->ppool, blkno);
		}
		else if (message.action == WalkContinue)
		{
			checkpoint_fix_split_and_lock_page(descr, state, &blkno, page_chage_count, level);
		}
		else
		{
			Assert(false);
		}

		checkpoint_internal_pass(descr, state, writeback, level, &message);

	}
}

/*
 * Return true if an item successfully added to checkpoint image.
 * Returns false if no enough space at the image for the item. It may happens
 * only for autonomous images.
 */
static inline bool
checkpoint_image_add_item(CheckpointPageInfo *page_info,
						  StackImageAddType type,
						  OTuple key,
						  uint key_size)
{
	Page		img = page_info->image;
	int			img_count = BTREE_PAGE_ITEMS_COUNT(img);
	uint		item_size;

	if (type == StackImageAddHikey)
	{
		/* hikey insert case */
		Assert(key_size != 0);
		item_size = MAXALIGN(key_size);

		if (!page_info->autonomous || img_count == 0 ||
			page_fits_hikey(img, item_size))
		{
			OTuple		hikey;

			page_resize_hikey(img, item_size);
			BTREE_PAGE_SET_HIKEY_FLAGS(img, key.formatFlags);
			BTREE_PAGE_GET_HIKEY(hikey, img);
			memcpy(hikey.data, key.data, key_size);
			return true;
		}
	}
	else
	{
		BTreePageItemLocator loc;

		/* downlink insert case */
		Assert(type == StackImageAddDownlink);
		/* we need additional space for BTreeNonLeafTuphdr */
		item_size = MAXALIGN(BTreeNonLeafTuphdrSize + key_size);
		BTREE_PAGE_LOCATOR_TAIL(img, &loc);

		if (!page_info->autonomous || img_count == 0 ||
			page_locator_fits_new_item(img, &loc, item_size))
		{
			page_locator_insert_item(img, &loc, item_size);

			if (key_size != 0)
			{
				Pointer		image_key;

				image_key = BTREE_PAGE_LOCATOR_GET_ITEM(img, &loc) + BTreeNonLeafTuphdrSize;
				memcpy(image_key, key.data, key_size);
				BTREE_PAGE_SET_ITEM_FLAGS(img, &loc, key.formatFlags);
			}
			return true;
		}
	}

	Assert(page_info->autonomous);
	return false;
}

/*
 * Splits the autonomous image. Last tuple is divided into two parts:
 *
 * 1. Key will be set as hikey of splitted image.
 * 2. BTreeNonLeafTuphdr will be returned.
 */
static BTreeNonLeafTuphdr
autonomous_image_split(BTreeDescr *descr, CheckpointPageInfo *page_info)
{
	BTreeNonLeafTuphdr result;
	OFixedKey	saved_key;
	OTuple		hikey;
	Page		img = page_info->image;
	int			key_len;
	BTreePageItemLocator loc;

	Assert(page_info->autonomous);
	/* page must contain a full node tuple (downlink + key) */
	Assert(BTREE_PAGE_ITEMS_COUNT(img) > 1);

	BTREE_PAGE_LOCATOR_LAST(img, &loc);

	/* save internal header */
	memcpy(&result,
		   BTREE_PAGE_LOCATOR_GET_ITEM(img, &loc),
		   sizeof(BTreeNonLeafTuphdr));

	/* save key to the buffer */
	copy_fixed_page_key(descr, &saved_key, img, &loc);
	key_len = MAXALIGN(o_btree_len(descr, saved_key.tuple, OKeyLength));

	/* remove tuple */
	page_locator_delete_item(img, &loc);

	/* add a new hikey */
	page_resize_hikey(img, key_len);
	BTREE_PAGE_SET_HIKEY_FLAGS(img, saved_key.tuple.formatFlags);
	BTREE_PAGE_GET_HIKEY(hikey, img);
	memcpy(hikey.data, saved_key.tuple.data, key_len);
	return result;
}

/*
 * Writes the autonomous image of given stack level to disk.
 */
static uint64
autonomous_image_write(BTreeDescr *descr, CheckpointState *state,
					   CheckpointWriteBack *writeback, int level, uint32 flags)
{
	Page		img = state->stack[level].image;
	BTreePageHeader *img_header;
	uint64		downlink;
	FileExtent	extent;

	/* prepare the image header */
	img_header = (BTreePageHeader *) img;
	img_header->checkpointNum = state->lastCheckpointNumber + 1;
	img_header->undoLocation = InvalidUndoLocation;
	img_header->csn = COMMITSEQNO_FROZEN;
	img_header->rightLink = InvalidRightLink;
	img_header->flags = flags;
	PAGE_SET_N_ONDISK(img, BTREE_PAGE_ITEMS_COUNT(img));
	PAGE_SET_LEVEL(img, level);

	extent.len = InvalidFileExtentLen;
	extent.off = InvalidFileExtentOff;

	/* write the image to disk */
	split_page_by_chunks(descr, img);

	downlink = perform_page_io_autonomous(descr, img, &extent);
	writeback_put_extent(writeback, &extent);

	Assert(DiskDownlinkIsValid(downlink));

	/*
	 * The BTree is not contain a page with the offset, so we need to free it
	 * for next checkpoint because it will not be possible in the future.
	 */
	free_extent_for_checkpoint(descr, &extent, state->lastCheckpointNumber + 2);

	/* the next page no more can be leftmost for the current level */
	state->stack[level].autonomousLeftmost = false;
	return downlink;
}

/*
 * Updates lowest level hikeys and reset autonomous flag if needed.
 */
static inline void
update_lowest_level_hikey(BTreeDescr *descr, CheckpointState *state, int to_level,
						  OTuple hikey)
{
	CheckpointPageInfo *page_info;
	bool		autonomous;
	int			i;

	for (i = 0; i < to_level; i++)
	{
		OTuple		pageHikey;

		page_info = &state->stack[i];
		if (O_TUPLE_IS_NULL(hikey))
		{
			/* no more pages on the level */
			page_info->autonomous = false;
			page_info->bound = CheckpointBoundRightmost;
			page_info->hikeyBlkno = OInvalidInMemoryBlkno;
			continue;
		}

		autonomous = page_info->autonomous;
		pageHikey = fixed_shmem_key_get_tuple(&page_info->hikey);
		if (!autonomous || (page_info->bound == CheckpointBoundHikey &&
							o_btree_cmp(descr,
										&pageHikey,
										BTreeKeyNonLeafKey,
										&hikey, BTreeKeyNonLeafKey) <= 0))
		{
			/* update hikey if no need in autonomous flag */
			page_info->bound = CheckpointBoundHikey;
			page_info->autonomous = false;
			page_info->hikeyBlkno = OInvalidInMemoryBlkno;
			copy_fixed_shmem_key(descr, &page_info->hikey, hikey);
		}
	}
}

/*
 * There is no free space left on the current autonomous image. The image
 * should be splitted and then written. Downlink to written image should be
 * inserted into parent image.
 *
 * But there are also may no left free space. All upper images should be
 * splitted until not found image in which downlink to child image
 * will be succesfully inserted.
 */
static void
checkpoint_stack_image_split_flush(BTreeDescr *descr, CheckpointState *state,
								   CheckpointWriteBack *writeback, int level)
{
	uint64		downlink = 0;
	int			cur_level;
	bool		inserted = false;
	BTreeNonLeafTuphdr *header,
				savedHeader;
	OFixedKey	hikey[2];
	LocationIndex hikeySize[2];
	OTuple		curKey;
	LocationIndex curKeySize;

	O_TUPLE_SET_NULL(curKey);
	curKeySize = 0;

	Assert(state->stack[level].autonomous);
	cur_level = level;

	while (true)
	{
		BTreePageItemLocator curLoc;
		CheckpointPageInfo *curItem = &state->stack[cur_level];
		uint32		flags = curItem->autonomousLeftmost ? O_BTREE_FLAG_LEFTMOST : 0;

		/*
		 * It might happen that "checkpointed" tree grow up higher than
		 * original tree.  Thus, we might need to initialize the new
		 * rootPageBlkno here.
		 */
		if (BTREE_PAGE_ITEMS_COUNT(curItem->image) == 0)
		{
			BTreePageItemLocator loc;

			init_page_first_chunk(descr, curItem->image, 0);

			BTREE_PAGE_LOCATOR_FIRST(curItem->image, &loc);
			page_locator_insert_item(curItem->image, &loc, BTreeNonLeafTuphdrSize);
			curItem->autonomous = true;
			curItem->autonomousLeftmost = true;
			curItem->blkno = OInvalidInMemoryBlkno;
			curItem->hikeyBlkno = OInvalidInMemoryBlkno;
		}

		if (cur_level != level)
		{
			BTREE_PAGE_LOCATOR_LAST(curItem->image, &curLoc);
			header = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(curItem->image, &curLoc);
			header->downlink = downlink;

			inserted = checkpoint_image_add_item(curItem,
												 StackImageAddDownlink,
												 curKey,
												 curKeySize);
			if (inserted)
				break;

			/* It still might happen, that we can insert item as a hikey */
			inserted = checkpoint_image_add_item(curItem,
												 StackImageAddHikey,
												 curKey,
												 curKeySize);
			savedHeader.downlink = 0;
		}

		if (!inserted)
			savedHeader = autonomous_image_split(descr, &state->stack[cur_level]);

		downlink = autonomous_image_write(descr, state, writeback, cur_level, flags);

		copy_fixed_hikey(descr, &hikey[cur_level % 2], curItem->image);
		hikeySize[cur_level % 2] = BTREE_PAGE_GET_HIKEY_SIZE(curItem->image);

		init_page_first_chunk(descr, curItem->image, 0);
		BTREE_PAGE_LOCATOR_FIRST(curItem->image, &curLoc);
		page_locator_insert_item(curItem->image, &curLoc, BTreeNonLeafTuphdrSize);
		header = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(curItem->image, &curLoc);
		*header = savedHeader;

		if (cur_level != level && !inserted)
		{
			inserted = checkpoint_image_add_item(curItem,
												 StackImageAddDownlink,
												 curKey,
												 curKeySize);
			Assert(inserted);
		}

		curKey = hikey[cur_level % 2].tuple;
		curKeySize = hikeySize[cur_level % 2];

		cur_level++;
		Assert(cur_level < ORIOLEDB_MAX_DEPTH);
	}
}

/*
 * For regular pages just add a new item to current level image.
 *
 * For autonomous pages it can modify stack if on the page is not enough space.
 */
static void
checkpoint_stack_image_add_item(BTreeDescr *descr, CheckpointState *state,
								CheckpointWriteBack *writeback, int level,
								StackImageAddType type, OTuple item, int item_size)
{
	bool		inserted PG_USED_FOR_ASSERTS_ONLY;

	if (checkpoint_image_add_item(&state->stack[level], type, item, item_size))
		return;

	/* no space for the item */
	checkpoint_stack_image_split_flush(descr, state, writeback, level);

	/* repeat insert must be success */
	inserted = checkpoint_image_add_item(&state->stack[level], type,
										 item, item_size);
	Assert(inserted);
}

/*
 * Flushes autonomous stack to the disk with the hikey to a given level
 * as upper stack limit.
 */
static void
autonomous_stack_flush_to_disk(BTreeDescr *descr, CheckpointState *state,
							   CheckpointWriteBack *writeback,
							   int to_level, OTuple hikey, int hikey_size)
{
	uint64		downlink;
	int			cur_level;

	/*
	 * Finds the lowest level on which images has tuples.
	 */
	for (cur_level = 1; cur_level < to_level; cur_level++)
	{
		if (BTREE_PAGE_ITEMS_COUNT(state->stack[cur_level].image) != 0)
		{
			/* It must be autonomous */
			Assert(state->stack[cur_level].autonomous);
			break;
		}
	}

	/*
	 * Loops had been separated just for simplicity and it can be merged.
	 */
	for (; cur_level < to_level; cur_level++)
	{
		BTreeNonLeafTuphdr *tuphdr;
		Page		parent_img = state->stack[cur_level + 1].image;
		int			flags;
		BTreePageItemLocator loc;

		/* write the autonomous image with given hikey */
		checkpoint_stack_image_add_item(descr, state, writeback, cur_level,
										StackImageAddHikey, hikey, hikey_size);

		flags = state->stack[cur_level].leftmost ? O_BTREE_FLAG_LEFTMOST : 0;
		downlink = autonomous_image_write(descr, state, writeback,
										  cur_level, flags);

		/* update last parent downlink with the offset */

		/* we do not loose a valid downlink here */
		Assert(cur_level + 1 == to_level
			   || state->stack[cur_level + 1].autonomousTupleExist);
		BTREE_PAGE_LOCATOR_LAST(parent_img, &loc);
		tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_img, &loc);
		tuphdr->downlink = downlink;

		/* reset stack values for the current level */

		BTREE_PAGE_ITEMS_COUNT(state->stack[cur_level].image) = 0;
		state->stack[cur_level].nextkeyType = NextKeyNone;
		state->stack[cur_level].autonomousTupleExist = false;
	}
}


static void
checkpoint_internal_pass(BTreeDescr *descr, CheckpointState *state,
						 CheckpointWriteBack *writeback,
						 int level, WalkMessage *message)
{
	OrioleDBPageDesc *page_desc = NULL;
	OTuple		write_hikey;
	Page		page,
				img;
	uint64		downlink;
	int			page_count,
				ionum;
	OInMemoryBlkno blkno;
	bool		was_dirty,
				autonomous,
				write_img,
				write_rightmost,
				prev_less = false,
				tuple_processed;
	BTreePageItemLocator loc;

	autonomous = state->stack[level].autonomous;
	blkno = state->stack[level].blkno;
	page = O_GET_IN_MEMORY_PAGE(blkno);
	BTREE_PAGE_OFFSET_GET_LOCATOR(page, state->stack[level].offset, &loc);
	img = state->stack[level].image;
	page_count = BTREE_PAGE_ITEMS_COUNT(page);

	Assert(level > 0);
	Assert(PAGE_GET_LEVEL(page) == level);

	if (state->stack[level].nextkeyType == NextKeyGreatest)
	{
		/* no sense in the while loop */
		BTREE_PAGE_LOCATOR_SET_INVALID(&loc);
	}

	tuple_processed = false;
	while (BTREE_PAGE_LOCATOR_IS_VALID(page, &loc))
	{
		BTreeNonLeafTuphdr *tuphdr;
		OTuple		key;

		BTREE_PAGE_READ_INTERNAL_ITEM(tuphdr, key, page, &loc);
		Assert(tuphdr != NULL);
		downlink = tuphdr->downlink;

		if (BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc) == 0)
		{
			if (state->stack[level].leftmost)
				O_TUPLE_SET_NULL(key);
			else
				key = fixed_shmem_key_get_tuple(&state->stack[level].lokey);
		}

		if (state->stack[level].nextkeyType != NextKeyNone)
		{
			int			cmp;

			Assert(state->stack[level].nextkeyType == NextKeyValue);
			if (O_TUPLE_IS_NULL(key))
			{
				cmp = -1;
			}
			else
			{
				OTuple		levelNextKey = fixed_shmem_key_get_tuple(&state->stack[level].nextkey);

				cmp = o_btree_cmp(descr,
								  &key, BTreeKeyNonLeafKey,
								  &levelNextKey,
								  BTreeKeyNonLeafKey);
			}

			if (cmp < 0)
			{
				/*
				 * The key we met is less than nextkey.  That may happen due
				 * to concurrent inserts.  So, skip it.
				 */
				OTuple		hikey;
				OTuple		levelHikey;

				BTREE_PAGE_LOCATOR_NEXT(page, &loc);
				prev_less = true;

				if (BTREE_PAGE_LOCATOR_IS_VALID(page, &loc) || !autonomous)
					continue;

				if (!O_PAGE_IS(page, RIGHTMOST))
				{
					BTREE_PAGE_GET_HIKEY(hikey, page);
					levelHikey = fixed_shmem_key_get_tuple(&state->stack[level].nextkey);
				}

				if (O_PAGE_IS(page, RIGHTMOST) ||
					o_btree_cmp(descr, &hikey, BTreeKeyNonLeafKey,
								&levelHikey,
								BTreeKeyNonLeafKey) > 0)
				{
					/*
					 * the page is autonomous and nextkey location is last
					 * page downlink
					 */
					BTREE_PAGE_LOCATOR_PREV(page, &loc);
				}
				else
				{
					continue;
				}
			}
			else if (cmp > 0)
			{
				if (BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc) > 0)
				{
					BTREE_PAGE_LOCATOR_PREV(page, &loc);
					if (!prev_less)
						continue;
				}
				Assert(autonomous);
			}
		}
		prev_less = false;
		tuple_processed = true;

		/* the offset may change */
		if (autonomous)
		{
			tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(page, &loc);
			downlink = tuphdr->downlink;
		}

		if (DOWNLINK_IS_IN_MEMORY(downlink) || DOWNLINK_IS_ON_DISK(downlink))
		{
			if (!state->stack[level].autonomousTupleExist)
			{
				/* we need to add a new downlink to img */
				OTuple		downlink_key;
				uint		downlink_key_size;
				bool		nextkey = state->stack[level].nextkeyType == NextKeyValue,
							first_off = BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc) == 0,
							page_key = !autonomous || !(nextkey || first_off);

				if (BTREE_PAGE_ITEMS_COUNT(img) == 0)
				{
					O_TUPLE_SET_NULL(downlink_key);
					downlink_key_size = 0;
				}
				else if (page_key)
				{
					/* easy case */
					BTREE_PAGE_READ_INTERNAL_TUPLE(downlink_key, page, &loc);
					downlink_key_size = BTREE_PAGE_GET_ITEM_SIZE(page, &loc)
						- BTreeNonLeafTuphdrSize;
				}
				else if (nextkey)
				{
					downlink_key = fixed_shmem_key_get_tuple(&state->stack[level].nextkey);
					downlink_key_size = o_btree_len(descr, downlink_key, OKeyLength);
				}
				else
				{
					Assert(first_off);
					Assert(BTREE_PAGE_ITEMS_COUNT(img) != 0);
					Assert(!state->stack[level].leftmost);
					downlink_key = fixed_shmem_key_get_tuple(&state->stack[level].lokey);
					downlink_key_size = o_btree_len(descr, downlink_key, OKeyLength);
				}

				if (!checkpoint_image_add_item(&state->stack[level],
											   StackImageAddDownlink,
											   downlink_key, downlink_key_size))
				{
					/*
					 * unable to add downlink into the image, we need to write
					 * autonomous image data
					 */
					Assert(autonomous);

					/*
					 * but we need to unlock page first and be ready to
					 * continue
					 */
					if (first_off)
					{
						state->stack[level].nextkeyType = NextKeyNone;
					}
					else
					{
						state->stack[level].nextkeyType = NextKeyValue;
						copy_fixed_shmem_page_key(descr,
												  &state->stack[level].nextkey,
												  page, &loc);
					}
					unlock_page(blkno);

					checkpoint_stack_image_split_flush(descr, state, writeback, level);

					/*
					 * and repeat try to add downlink
					 */
					message->action = WalkContinue;
					state->stack[level].offset = BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc);
					return;
				}
			}
			else
			{
				/* downlink may be saved only for autonomous pages */
				Assert(autonomous);
			}
			state->stack[level].autonomousTupleExist = false;
		}

		if (DOWNLINK_IS_IN_MEMORY(downlink))
		{
			BTreePageItemLocator nextLoc = loc;

			BTREE_PAGE_LOCATOR_NEXT(page, &nextLoc);
			if (BTREE_PAGE_LOCATOR_IS_VALID(page, &nextLoc))
			{
				state->stack[level].nextkeyType = NextKeyValue;
				copy_fixed_shmem_page_key(descr, &state->stack[level].nextkey,
										  page, &nextLoc);
			}
			else
			{
				if (O_PAGE_IS(page, RIGHTMOST))
				{
					state->stack[level].nextkeyType = NextKeyGreatest;
				}
				else
				{
					state->stack[level].nextkeyType = NextKeyValue;
					copy_fixed_shmem_hikey(descr, &state->stack[level].nextkey,
										   page);
				}
			}

			if (BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc) == 0)
			{
				if (state->stack[level].leftmost)
					clear_fixed_key(&message->content.downwards.lokey);
				else
				{
					OFixedKey  *lokey = &message->content.downwards.lokey;

					copy_from_fixed_shmem_key(lokey, &state->stack[level].lokey);
				}
			}
			else
			{
				copy_fixed_page_key(descr, &message->content.downwards.lokey,
									page, &loc);
			}

			unlock_page(blkno);
			message->action = WalkDownwards;
			message->content.downwards.blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(downlink);
			message->content.downwards.pageChangeCount = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(downlink);

			BTREE_PAGE_LOCATOR_NEXT(page, &loc);
			state->stack[level].offset = BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc);
			return;
		}
		else if (DOWNLINK_IS_ON_DISK(downlink))
		{
			BTreePageItemLocator nextLoc,
						imgLastLoc;

			/* copy internal header with downlink */
			BTREE_PAGE_LOCATOR_LAST(img, &imgLastLoc);
			memcpy(BTREE_PAGE_LOCATOR_GET_ITEM(img, &imgLastLoc),
				   BTREE_PAGE_LOCATOR_GET_ITEM(page, &loc),
				   BTreeNonLeafTuphdrSize);

			if (BTREE_PAGE_ITEMS_COUNT(state->stack[level - 1].image) > 0)
			{
				OTuple		hikey;
				int			hikey_size;

				/*
				 * Lowest levels of the stack have autonomous images with
				 * tuples. We need to flush it to disk.
				 */
				Assert(autonomous);

				if (BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc) == 0)
				{
					Assert(!state->stack[level].leftmost);
					state->stack[level].nextkeyType = NextKeyNone;
					hikey = fixed_shmem_key_get_tuple(&state->stack[level].lokey);
				}
				else
				{
					state->stack[level].nextkeyType = NextKeyValue;
					copy_fixed_shmem_page_key(descr,
											  &state->stack[level].nextkey,
											  page, &loc);
					hikey = fixed_shmem_key_get_tuple(&state->stack[level].nextkey);
				}
				unlock_page(blkno);
				hikey_size = MAXALIGN(o_btree_len(descr, hikey, OKeyLength));

				autonomous_stack_flush_to_disk(descr, state, writeback,
											   level, hikey, hikey_size);

				/* after this we can repeat */
				message->action = WalkContinue;
				state->stack[level].offset = BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc);
				return;
			}

			/*
			 * Page is already on the disk, but we have to advance current key
			 * ourselves...
			 */
			nextLoc = loc;
			BTREE_PAGE_LOCATOR_NEXT(page, &nextLoc);
			if (BTREE_PAGE_LOCATOR_IS_VALID(page, &nextLoc) || !O_PAGE_IS(page, RIGHTMOST))
			{
				chkp_inc_changecount_before(state);
				state->curKeyType = CurKeyValue;
				if (BTREE_PAGE_LOCATOR_IS_VALID(page, &nextLoc))
					copy_fixed_shmem_page_key(descr, &state->curKeyValue, page,
											  &nextLoc);
				else
					copy_fixed_shmem_hikey(descr, &state->curKeyValue, page);

				update_lowest_level_hikey(descr, state, level,
										  fixed_shmem_key_get_tuple(&state->curKeyValue));
				chkp_inc_changecount_after(state);
			}
			else
			{
				OTuple		nullTup;

				chkp_inc_changecount_before(state);
				state->curKeyType = CurKeyGreatest;

				O_TUPLE_SET_NULL(nullTup);
				update_lowest_level_hikey(descr, state, level, nullTup);
				chkp_inc_changecount_after(state);
			}

			if (autonomous && BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc) + 1 == page_count)
			{
				if (!O_PAGE_IS(page, RIGHTMOST))
				{
					copy_fixed_shmem_hikey(descr, &state->stack[level].nextkey,
										   page);
					state->stack[level].nextkeyType = NextKeyValue;
				}
				else
				{
					state->stack[level].nextkeyType = NextKeyGreatest;
				}
			}
			else
			{
				state->stack[level].nextkeyType = NextKeyNone;
			}
			BTREE_PAGE_LOCATOR_NEXT(page, &loc);
		}
		else if (DOWNLINK_IS_IN_IO(downlink))
		{
			/* Save the key we need to continue from */
			if (BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc) == 0)
			{
				state->stack[level].nextkeyType = NextKeyNone;
			}
			else
			{
				state->stack[level].nextkeyType = NextKeyValue;
				copy_fixed_shmem_page_key(descr, &state->stack[level].nextkey,
										  page, &loc);
			}

			message->action = WalkContinue;
			state->stack[level].offset = BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc);

			unlock_page(blkno);
			/* IO is in-progress.  So, wait for completeness and retry. */
			wait_for_io_completion(DOWNLINK_GET_IO_LOCKNUM(downlink));
			return;
		}
	}

	write_rightmost = O_PAGE_IS(page, RIGHTMOST) ||
		state->stack[level].nextkeyType == NextKeyGreatest;

	write_img = !autonomous || write_rightmost;
	O_TUPLE_SET_NULL(write_hikey);
	if (autonomous && !write_rightmost && state->stack[level].bound == CheckpointBoundHikey)
	{
		/* we may need to write the autonomous image if hikeys is equal */
		int			cmp;
		OTuple		hikey;
		OTuple		levelHikey;

		BTREE_PAGE_GET_HIKEY(hikey, page);
		levelHikey = fixed_shmem_key_get_tuple(&state->stack[level].hikey);
		cmp = o_btree_cmp(descr, &levelHikey, BTreeKeyNonLeafKey,
						  &hikey, BTreeKeyNonLeafKey);
		Assert(cmp >= 0);
		write_img = cmp == 0;
		if (write_img)
		{
			write_hikey = levelHikey;
		}
		else if (!tuple_processed)
		{
			OTuple		levelNextKey = fixed_shmem_key_get_tuple(&state->stack[level].nextkey);

			cmp = o_btree_cmp(descr, &levelHikey, BTreeKeyNonLeafKey,
							  &levelNextKey, BTreeKeyNonLeafKey);
			Assert(cmp >= 0);
			if (cmp == 0)
			{
				/* we already write the child with the hikey */
				write_hikey = levelHikey;
				write_img = true;
			}
		}
	}

	if (autonomous)
	{
		/* no more need in page data */
		chkp_inc_changecount_before(state);
		state->stack[level].blkno = OInvalidInMemoryBlkno;
		if (write_img)
			state->stack[level].hikeyBlkno = OInvalidInMemoryBlkno;
		chkp_inc_changecount_after(state);
		unlock_page(blkno);

		if (!write_img)
		{
			/* no need to write the autonomous image */
			message->action = WalkUpwards;
			copy_from_fixed_shmem_key(&message->content.upwards.nextkey,
									  &state->stack[level].nextkey);
			message->content.upwards.nextkeyType = NextKeyValue;
			message->content.upwards.parentDirty = false;
			message->content.upwards.diskDownlink = InvalidDiskDownlink;
			message->content.upwards.saveTuple = true;
			state->stack[level].nextkeyType = NextKeyValue;
			return;
		}
	}
	else
	{
		/* If IO is in-progress then wait for its completion */
		page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
		ionum = page_desc->ionum;
		if (ionum >= 0)
		{
			/* Save the key we need to continue from */
			if (O_PAGE_IS(page, RIGHTMOST))
			{
				state->stack[level].nextkeyType = NextKeyGreatest;
			}
			else
			{
				state->stack[level].nextkeyType = NextKeyValue;
				copy_fixed_shmem_hikey(descr, &state->stack[level].nextkey,
									   page);
			}
			message->action = WalkContinue;
			state->stack[level].offset = BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc);
			unlock_page(blkno);
			wait_for_io_completion(ionum);
			return;
		}
	}

	/* we should write the image */
	Assert(write_img);

	/* but first add a hikey to the image if needed */
	if (!write_rightmost)
	{
		int			hikey_len;

		if (!O_TUPLE_IS_NULL(write_hikey))
		{
			Assert(autonomous);
			hikey_len = o_btree_len(descr, write_hikey, OKeyLength);
		}
		else
		{
			Assert(!autonomous);
			BTREE_PAGE_GET_HIKEY(write_hikey, page);
			hikey_len = BTREE_PAGE_GET_HIKEY_SIZE(page);
		}

		checkpoint_stack_image_add_item(descr, state,
										writeback, level, StackImageAddHikey,
										write_hikey,
										hikey_len);
	}
	state->stack[level].autonomous = false;

	/* Indicate that we've finished that page image */
	message->action = WalkUpwards;
	message->content.upwards.saveTuple = false;
	if (autonomous)
	{
		uint64		written_downlink;
		uint32		flags = 0;

		if (state->stack[level].autonomousLeftmost)
			flags |= O_BTREE_FLAG_LEFTMOST;

		if (write_rightmost)
			flags |= O_BTREE_FLAG_RIGHTMOST;

		written_downlink = autonomous_image_write(descr, state, writeback, level, flags);
		message->content.upwards.diskDownlink = written_downlink;
		message->content.upwards.parentDirty = false;
	}
	else
	{
		BTreePageHeader *img_header,
				   *page_header;
		uint64		written_downlink;
		bool		parent_dirty;

		page_header = (BTreePageHeader *) page;
		img_header = (BTreePageHeader *) img;
		img_header->undoLocation = page_header->undoLocation;
		img_header->csn = page_header->csn;
		img_header->checkpointNum = page_header->checkpointNum;
		img_header->flags = page_header->flags;
		img_header->rightLink = InvalidRightLink;
		PAGE_SET_N_ONDISK(img, BTREE_PAGE_ITEMS_COUNT(img));
		PAGE_SET_LEVEL(img, level);

		/*
		 * We don't allow concurrent downlinks inserts into processed part of
		 * the page.  So, just cleaning dirty flag should be correct.
		 */
		was_dirty = IS_DIRTY(blkno);
		if (was_dirty)
		{
			page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
			Assert(page_desc->ionum < 0);	/* already checked above */
			page_desc->ionum = assign_io_num(blkno, InvalidOffsetNumber);

			/*
			 * we assume that concurrent eviction of the parent is forbidden
			 * by get_checkpoint_number() in walk_page()
			 */
			CLEAN_DIRTY(descr->ppool, blkno);
		}

		/* We've finished operation with the page, allow concurrent operations */
		chkp_inc_changecount_before(state);
		state->stack[level].blkno = OInvalidInMemoryBlkno;
		state->stack[level].hikeyBlkno = OInvalidInMemoryBlkno;
		chkp_inc_changecount_after(state);
		unlock_page(blkno);

		if (was_dirty)
		{
			/*
			 * TODO: Non-leaf page isn't modified during checkpoint.  We can
			 * reuse original chunks layout/max key length.
			 */
			split_page_by_chunks(descr, img);

			written_downlink = perform_page_io(descr,
											   blkno,
											   img,
											   state->lastCheckpointNumber + 1,
											   false,
											   &parent_dirty);

			if (!DiskDownlinkIsValid(written_downlink))
			{
				Assert(page_desc != NULL);
				elog(ERROR, "Unable to perform page IO for page %d to file %s with offset %lu",
					 blkno,
					 btree_smgr_filename(descr, page_desc->fileExtent.off),
					 (long unsigned) page_desc->fileExtent.off);
			}

			writeback_put_extent(writeback, &page_desc->fileExtent);

			unlock_io(page_desc->ionum);
			page_desc->ionum = -1;
		}
		else
		{
			parent_dirty = false;
			written_downlink = MAKE_ON_DISK_DOWNLINK(O_GET_IN_MEMORY_PAGEDESC(blkno)->fileExtent);
		}
		Assert(FileExtentIsValid(O_GET_IN_MEMORY_PAGEDESC(blkno)->fileExtent));
		Assert(DiskDownlinkIsValid(written_downlink));
		message->content.upwards.diskDownlink = written_downlink;
		message->content.upwards.parentDirty = parent_dirty;
	}

	if (write_rightmost)
	{
		message->content.upwards.nextkeyType = NextKeyGreatest;
	}
	else
	{
		message->content.upwards.nextkeyType = NextKeyValue;
		copy_fixed_hikey(descr, &message->content.upwards.nextkey, img);
	}

	state->stack[level].nextkeyType = NextKeyNone;
	BTREE_PAGE_ITEMS_COUNT(img) = 0;
}


/*
 * Prepare particular leaf B-tree page for checkpointing.  Checkpointer
 * state stack item is already filled and page is locked.
 *
 * Unlocks page.
 */
static void
prepare_leaf_page(BTreeDescr *descr, CheckpointState *state)
{
	Page		page;
	OInMemoryBlkno blkno = state->stack[0].blkno;

	/*
	 * Update checkpoint bound key.
	 */
	page = O_GET_IN_MEMORY_PAGE(blkno);

	chkp_inc_changecount_before(state);
	if (O_PAGE_IS(page, RIGHTMOST))
	{
		state->curKeyType = CurKeyGreatest;
	}
	else
	{
		state->curKeyType = CurKeyValue;
		copy_fixed_shmem_hikey(descr, &state->curKeyValue, page);
	}
	chkp_inc_changecount_after(state);

	unlock_page(blkno);
}

static void
checkpoint_tables_callback(OIndexType type, ORelOids treeOids,
						   ORelOids tableOids, bool temp_table,
						   void *arg)
{
	CheckpointTablesArg *tbl_arg = (CheckpointTablesArg *) arg;
	OIndexDescr *descr;
	int			cur_chkp_index = (checkpoint_state->lastCheckpointNumber + 1) % 2;
	MemoryContext prev_context;
	Jsonb	   *params;

	if (temp_table)
		return;

	prev_context = MemoryContextSwitchTo(chkp_mem_context);

	if (STOPEVENTS_ENABLED())
		params = prepare_checkpoint_table_start_params(tableOids, treeOids);
	STOPEVENT(STOPEVENT_CHECKPOINT_TABLE_START, params);

	descr = o_fetch_index_descr(treeOids, type, true, NULL);
	if (descr != NULL && o_btree_load_shmem_checkpoint(&descr->desc))
	{
		BTreeDescr *td = &descr->desc;
		bool		success;

		elog(DEBUG3, "CHKP %u, (%u, %u, %u) => (%u, %u, %u)",
			 type, treeOids.datoid, treeOids.reloid, treeOids.relnode,
			 tableOids.datoid, tableOids.reloid, tableOids.relnode);

		Assert(!have_locked_pages());
		Assert(!have_retained_undo_location());

		if (td->type >= oIndexUnique &&
			XLogRecPtrIsInvalid(checkpoint_state->toastConsistentPtr))
		{
			checkpoint_state->toastConsistentPtr = GetXLogInsertRecPtr();
		}

		LWLockRelease(&checkpoint_state->oTablesMetaLock);

		if (STOPEVENTS_ENABLED())
			params = prepare_checkpoint_tree_start_params(td);
		STOPEVENT(STOPEVENT_CHECKPOINT_INDEX_START, params);
		success = checkpoint_ix(tbl_arg->flags, td);
		if (success)
		{
			sort_checkpoint_map_file(td, cur_chkp_index);
			sort_checkpoint_tmp_file(td, cur_chkp_index);
			if (OCompressIsValid(td->compress))
				tbl_arg->freeExtents = add_free_extents_item(tbl_arg->freeExtents, td);
			tbl_arg->cleanupMap = add_map_cleanup_item(tbl_arg->cleanupMap, td);
			o_tables_rel_unlock_extended(&treeOids, AccessShareLock, true);
		}

		LWLockAcquire(&checkpoint_state->oTablesMetaLock, LW_EXCLUSIVE);
	}

	MemoryContextSwitchTo(prev_context);
	MemoryContextResetOnly(chkp_mem_context);
}

char *
get_eviction_filename(ORelOids oids, uint32 chkp_num)
{
	/* this format is used by recovery_cleanup_old_files() */
	return psprintf(ORIOLEDB_DATA_DIR "/%u/%u-%u." ORIOLEDB_EVT_EXTENSION,
					oids.datoid, oids.relnode, chkp_num);
}

/*
 * Returns actual lastCheckpointNumber for current tree.
 */
uint32
get_cur_checkpoint_number(ORelOids *oids, OIndexType type,
						  bool *checkpoint_concurrent)
{
	OIndexType	chkp_tree_type = oIndexInvalid;
	Oid			datoid = InvalidOid,
				relnode = InvalidOid;
	int			before_changecount,
				after_changecount;
	uint32		result,
				completed;

	do
	{
		chkp_save_changecount_before(checkpoint_state, before_changecount);
		if ((before_changecount & 1) != 0)
			continue;

		chkp_tree_type = checkpoint_state->treeType;
		datoid = checkpoint_state->datoid;
		relnode = checkpoint_state->relnode;
		result = checkpoint_state->lastCheckpointNumber;
		completed = checkpoint_state->completed;

		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount != after_changecount)
			continue;

		if (OidIsValid(datoid))
		{
			/* datoid, relnode and ix_num setups inside changecount section */
			Assert(OidIsValid(relnode));

			if (chkp_ordering_cmp(type, oids->datoid, oids->relnode,
								  chkp_tree_type, datoid, relnode) < 0)
			{
				/* BTree is already processed by current checkpoint */
				result += 1;
			}
		}
		/* else checkpoint is not in progress */

		*checkpoint_concurrent = oids->datoid == datoid && oids->relnode == relnode
			&& type == chkp_tree_type && !completed;
		chkp_save_changecount_after(checkpoint_state, after_changecount);
		if (before_changecount == after_changecount)
			break;
	} while (true);

	return result;
}

/*
 * Check if we can already re-use space freed in given checkpoint.
 */
bool
can_use_checkpoint_extents(BTreeDescr *desc, uint32 chkp_num)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);;

	if (chkp_num > checkpoint_state->lastCheckpointNumber)
		return false;

	/*
	 * Prevent situtation when checkpoint was finished and new sequential scan
	 * started between out checks for numSeqScans and lastCheckpointNumber.
	 */
	pg_read_barrier();

	if (pg_atomic_read_u32(&metaPageBlkno->numSeqScans[chkp_num - 1]) != 0)
		return false;
	return true;
}

static inline bool
init_seq_buf_pages(BTreeDescr *desc, SeqBufDescShared *shared)
{
	Assert(!OInMemoryBlknoIsValid(shared->pages[0]));
	Assert(!OInMemoryBlknoIsValid(shared->pages[1]));

	shared->pages[0] = ppool_get_metapage(desc->ppool);
	if (!OInMemoryBlknoIsValid(shared->pages[0]))
		return false;

	shared->pages[1] = ppool_get_metapage(desc->ppool);
	if (!OInMemoryBlknoIsValid(shared->pages[1]))
	{
		ppool_free_page(desc->ppool, shared->pages[0], NULL);
		return false;
	}
	return true;
}

static inline void
free_seq_buf_pages(BTreeDescr *desc, SeqBufDescShared *shared)
{
	FREE_PAGE_IF_VALID(desc->ppool, shared->pages[0]);
	FREE_PAGE_IF_VALID(desc->ppool, shared->pages[1]);
}

static bool
checkpointable_tree_fill_seq_buffers(BTreeDescr *td, bool init,
									 EvictedTreeData *evicted_tree_data,
									 uint32 chkp_num)
{
	EvictedSeqBufData *evicted_free,
			   *evicted_next,
			   *evicted_tmp;
	SeqBufTag	prev_chkp_tag,
				cur_chkp_tag,
				tmp_tag;
	BTreeMetaPage *meta_page = BTREE_GET_META(td);
	int			chkp_index = (chkp_num + 1) % 2;
	bool		is_compressed = OCompressIsValid(td->compress);

	evicted_free = evicted_tree_data == NULL ? NULL : &evicted_tree_data->freeBuf;
	evicted_next = evicted_tree_data == NULL ? NULL : &evicted_tree_data->nextChkp;
	evicted_tmp = evicted_tree_data == NULL ? NULL : &evicted_tree_data->tmpBuf;

	memset(&prev_chkp_tag, 0, sizeof(SeqBufTag));
	memset(&cur_chkp_tag, 0, sizeof(SeqBufTag));
	memset(&tmp_tag, 0, sizeof(SeqBufTag));

	if (evicted_free)
	{
		prev_chkp_tag = evicted_free->tag;
	}
	else
	{
		prev_chkp_tag.datoid = td->oids.datoid;
		prev_chkp_tag.relnode = td->oids.relnode;
		prev_chkp_tag.num = chkp_num;
		prev_chkp_tag.type = 'm';
	}

	cur_chkp_tag.datoid = td->oids.datoid;
	cur_chkp_tag.relnode = td->oids.relnode;
	cur_chkp_tag.num = chkp_num + 1;
	cur_chkp_tag.type = 'm';
	Assert(evicted_next == NULL || SeqBufTagEqual(&cur_chkp_tag, &evicted_next->tag));

	tmp_tag = cur_chkp_tag;
	tmp_tag.type = 't';
	Assert(evicted_tmp == NULL || SeqBufTagEqual(&tmp_tag, &evicted_tmp->tag));

	if (init)
	{
		SeqBufDescShared *shareds[3] = {&meta_page->nextChkp[chkp_index],
			&meta_page->tmpBuf[chkp_index],
		&meta_page->freeBuf};
		int			i;

		for (i = 0; i < (is_compressed ? 2 : 3); i++)
		{
			if (!init_seq_buf_pages(td, shareds[i]))
				return false;
		}
	}

	if (is_compressed)
	{
		/* no need to initialize freeBuf shared memory */
		td->freeBuf.tag = prev_chkp_tag;
		if (init)
			meta_page->freeBuf.tag = prev_chkp_tag;
	}
	else
	{
		if (!init_seq_buf(&td->freeBuf,
						  &meta_page->freeBuf,
						  &prev_chkp_tag, false, init,
						  prev_chkp_tag.type == 'm' ? sizeof(CheckpointFileHeader) : 0,
						  evicted_free))
			return false;
	}

	if (!init_seq_buf(&td->nextChkp[chkp_index],
					  &meta_page->nextChkp[chkp_index],
					  &cur_chkp_tag, true, init, sizeof(CheckpointFileHeader), evicted_next))
		return false;

	if (!init_seq_buf(&td->nextChkp[1 - chkp_index],
					  &meta_page->nextChkp[1 - chkp_index],
					  NULL, true, false, sizeof(CheckpointFileHeader), NULL))
		return false;

	if (!init_seq_buf(&td->tmpBuf[chkp_index],
					  &meta_page->tmpBuf[chkp_index],
					  &tmp_tag, true, init, 0, evicted_tmp))
		return false;

	if (!init_seq_buf(&td->tmpBuf[1 - chkp_index],
					  &meta_page->tmpBuf[1 - chkp_index],
					  NULL, true, false, 0, NULL))
		return false;

	return true;
}

/*
 * Initializes metaPageBlkno information for a B-tree with pages eviction support.
 *
 * We can try to use exist on-disk data for the B-tree metaPageBlkno information or
 * initialize clear tree.
 */
static void
evictable_tree_init_meta(BTreeDescr *desc, EvictedTreeData **evicted_data,
						 uint32 chkp_num, bool clear_tree)
{
	CheckpointFileHeader file_header = {0};
	BTreeMetaPage *meta_page;

	Assert(TREE_HAS_OIDS(desc));

	if (clear_tree)
	{
		/* No need to create or read a map file */
		file_header.rootDownlink = InvalidDiskDownlink;
		file_header.datafileLength = 0;
		file_header.numFreeBlocks = 0;
		file_header.leafPagesNum = 1;
		file_header.ctid = 0;
	}
	else
	{
		char	   *prev_chkp_fname;
		File		prev_chkp_file;
		SeqBufTag	prev_chkp_tag;
		bool		prev_chkp_file_exist,
					ferror = false;

		memset(&prev_chkp_tag, 0, sizeof(prev_chkp_tag));
		prev_chkp_tag.datoid = desc->oids.datoid;
		prev_chkp_tag.relnode = desc->oids.relnode;
		prev_chkp_tag.num = chkp_num;
		prev_chkp_tag.type = 'm';
		prev_chkp_fname = get_seq_buf_filename(&prev_chkp_tag);
		prev_chkp_file = PathNameOpenFile(prev_chkp_fname, O_RDONLY | PG_BINARY);
		prev_chkp_file_exist = prev_chkp_file >= 0;

		if (!prev_chkp_file_exist)
		{
			/*
			 * Creates file with default header
			 */
			file_header.rootDownlink = InvalidDiskDownlink;
			file_header.datafileLength = 0;
			file_header.numFreeBlocks = 0;
			file_header.leafPagesNum = 1;
			file_header.ctid = 0;

			prev_chkp_file = PathNameOpenFile(prev_chkp_fname, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

			if (prev_chkp_file < 0)
			{
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not create map file %s", prev_chkp_fname)));
			}

			ferror = OFileWrite(prev_chkp_file, (Pointer) &file_header, sizeof(file_header), 0,
								WAIT_EVENT_SLRU_WRITE) != sizeof(file_header) ||
				FileSync(prev_chkp_file, WAIT_EVENT_SLRU_SYNC) != 0;

			if (ferror)
				ereport(FATAL, (errcode_for_file_access(),
								errmsg("could not to write header to map file %s",
									   prev_chkp_fname)));
		}
		else					/* if checkpoint file exist */
		{
			/*
			 * Reads header from file.
			 */
			Assert(evicted_data);
			*evicted_data = read_evicted_data(desc->oids, chkp_num + 1);

			if (*evicted_data != NULL)
			{
				char	   *filename = get_eviction_filename(desc->oids, chkp_num + 1);

				if (unlink(filename) < 0)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not unlink eviction file \"%s\": %m", filename)));
				file_header = (*evicted_data)->file_header;
				pfree(filename);
			}
			else
			{
				ferror = OFileRead(prev_chkp_file, (Pointer) &file_header,
								   sizeof(file_header), 0, WAIT_EVENT_SLRU_READ) != sizeof(file_header);
				if (ferror)
				{
					ereport(FATAL, (errcode_for_file_access(),
									errmsg("could not to read header of map file %s",
										   prev_chkp_fname)));
				}
			}
		}
		FileClose(prev_chkp_file);
		pfree(prev_chkp_fname);
	}

	o_btree_init(desc);

	meta_page = BTREE_GET_META(desc);
	pg_atomic_write_u64(&meta_page->numFreeBlocks, file_header.numFreeBlocks);
	pg_atomic_write_u64(&meta_page->datafileLength, file_header.datafileLength);
	pg_atomic_write_u32(&meta_page->leafPagesNum, file_header.leafPagesNum);
	pg_atomic_write_u64(&meta_page->ctid, file_header.ctid);

	VALGRIND_CHECK_MEM_IS_DEFINED(meta_page, ORIOLEDB_BLCKSZ);

	if (DiskDownlinkIsValid(file_header.rootDownlink))
	{
		OrioleDBPageDesc *root_desc;
		char		buf[ORIOLEDB_BLCKSZ];
		bool		rerror;

		lock_page(desc->rootInfo.rootPageBlkno);
		page_block_reads(desc->rootInfo.rootPageBlkno);
		root_desc = O_GET_IN_MEMORY_PAGEDESC(desc->rootInfo.rootPageBlkno);

		rerror = !read_page_from_disk(desc, buf, file_header.rootDownlink, &root_desc->fileExtent);

		if (rerror)
		{
			unlock_page(desc->rootInfo.rootPageBlkno);
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not read rootPageBlkno page from %s",
								   btree_smgr_filename(desc, DOWNLINK_GET_DISK_OFF(file_header.rootDownlink)))));
		}

		put_page_image(desc->rootInfo.rootPageBlkno, buf);
		CLEAN_DIRTY(desc->ppool, desc->rootInfo.rootPageBlkno);
		Assert(root_desc->flags == 0);

		unlock_page(desc->rootInfo.rootPageBlkno);
	}
}

/* TODO: move this method ? */
bool
tbl_data_exists(ORelOids *oids)
{
	char	   *filename;
	File		file;

	/* TODO: more smart check */
	filename = psprintf(ORIOLEDB_DATA_DIR "/%u/%u", oids->datoid, oids->relnode);
	file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	pfree(filename);

	if (file >= 0)
	{
		FileClose(file);
		return true;
	}

	return false;
}

/*
 * Initializes B-tree with a page eviction support, but without checkpoint support.
 */
void
evictable_tree_init(BTreeDescr *desc, bool init_shmem)
{
	uint32		chkp_num;
	int			chkp_index;
	SeqBufTag	tmp_tag = {0};
	bool		checkpoint_concurrent;
	BTreeMetaPage *meta_page;

	btree_open_smgr(desc);

	if (init_shmem)
		evictable_tree_init_meta(desc, NULL, 0, true);

	chkp_num = get_cur_checkpoint_number(&desc->oids, desc->type,
										 &checkpoint_concurrent);
	chkp_index = (chkp_num + 1) % 2;
	tmp_tag.datoid = desc->oids.datoid;
	tmp_tag.relnode = desc->oids.relnode;
	tmp_tag.num = chkp_num + 1;
	tmp_tag.type = 't';
	meta_page = BTREE_GET_META(desc);

	if (init_shmem)
	{
		if (!init_seq_buf_pages(desc,
								&meta_page->freeBuf))
			ereport(FATAL, (errcode_for_file_access(),
							errmsg(
								   "could not init sequence buffer pages.")));

		if (!init_seq_buf_pages(desc, &meta_page->tmpBuf[chkp_index]))
			ereport(FATAL, (errcode_for_file_access(),
							errmsg(
								   "could not init sequence buffer pages.")));
	}

	if (!init_seq_buf(&desc->freeBuf,
					  &meta_page->freeBuf,
					  &tmp_tag, false, init_shmem, 0, NULL))
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not fill sequence buffers.")));
	}

	if (!init_seq_buf(&desc->tmpBuf[chkp_index],
					  &meta_page->tmpBuf[chkp_index],
					  &tmp_tag, true, init_shmem, 0, NULL))
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not fill sequence buffers.")));
	}

	if (!init_seq_buf(&desc->tmpBuf[1 - chkp_index],
					  &meta_page->tmpBuf[1 - chkp_index],
					  NULL, true, false, 0, NULL))
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not fill sequence buffers.")));
	}
}

/*
 * Initializes B-tree with checkpoint support.
 */
void
checkpointable_tree_init(BTreeDescr *desc, bool init_shmem, bool *was_evicted)
{
	bool		checkpoint_concurrent;
	uint32		chkp_num;
	EvictedTreeData *evicted_tree_data = NULL;

	chkp_num = get_cur_checkpoint_number(&desc->oids, desc->type,
										 &checkpoint_concurrent);

	/*
	 * We shouldn't initialize shared memory concurrently to checkpoint.
	 * Checkpointer should have initilized that before start working on this
	 * tree.
	 */
	Assert(!init_shmem || !checkpoint_concurrent);

	btree_open_smgr(desc);

	if (init_shmem)
		evictable_tree_init_meta(desc, &evicted_tree_data, chkp_num, false);

	if (!checkpointable_tree_fill_seq_buffers(desc, init_shmem,
											  evicted_tree_data, chkp_num))
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not fill sequence buffers.")));
	}

	if (init_shmem && was_evicted)
		*was_evicted = evicted_tree_data != NULL;

	if (evicted_tree_data != NULL)
		pfree(evicted_tree_data);
}

void
checkpointable_tree_free(BTreeDescr *desc)
{
	btree_close_smgr(desc);
	seq_buf_close_file(&desc->freeBuf);
	seq_buf_close_file(&desc->nextChkp[0]);
	seq_buf_close_file(&desc->nextChkp[1]);
	seq_buf_close_file(&desc->tmpBuf[0]);
	seq_buf_close_file(&desc->tmpBuf[1]);
	desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
	desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;

}

static FileExtentsArray *
file_extents_array_init()
{
	FileExtentsArray *result = palloc0(sizeof(FileExtentsArray));

	return result;
}

static void
file_extents_array_free(FileExtentsArray *array)
{
	if (array->extents != NULL)
		pfree(array->extents);
	pfree(array);
}

static void
file_extents_array_append(FileExtentsArray *array, FileExtent *extent)
{
	Assert(array != NULL);

	if (array->allocated == array->size)
	{
		if (array->allocated == 0)
		{
			array->allocated = 16;
			array->extents = (FileExtent *) palloc(sizeof(FileExtent) * array->allocated);
		}
		else
		{
			array->allocated *= 2;
			array->extents = (FileExtent *) repalloc(array->extents,
													 sizeof(FileExtent) * array->allocated);
		}
	}

	Assert(array->extents != NULL);
	array->extents[array->size++] = *extent;
}

static void
foreach_extent_append(BTreeDescr *desc, FileExtent extent, void *arg)
{
	FileExtentsArray *array = (FileExtentsArray *) arg;
	FileExtent *prev = array->size > 0 ? &array->extents[array->size - 1] : NULL;
	bool		append;

	if (prev != NULL && prev->off + prev->len > extent.off)
	{
		/* cutting in progress, skip this part */
		Assert(prev->off + prev->len == extent.off + extent.len);
		append = false;
	}
	else
	{
		append = true;
	}

	if (append)
	{
		file_extents_array_append(array, &extent);

		/*
		 * we expect that the array will be sorted in ascending sort order
		 * (off, len)
		 */
		Assert(array->size == 1
			   || (array->extents[array->size - 2].off < array->extents[array->size - 1].off));
	}
}

void
systrees_lock_callback(UndoLocation location, UndoStackItem *baseItem, OXid oxid,
					   bool abort, bool changeCountsValid)
{
	SysTreesLockUndoStackItem *lockItem = (SysTreesLockUndoStackItem *) baseItem;

	Assert(abort);

	if (lockItem->lock)
	{
		LWLockAcquire(&checkpoint_state->oSysTreesLock, LW_SHARED);
	}
	else
	{
		if (LWLockHeldByMe(&checkpoint_state->oSysTreesLock))
			LWLockRelease(&checkpoint_state->oSysTreesLock);
	}
}

static void
add_systrees_lock_undo(bool lock)
{
	UndoLocation location;
	SysTreesLockUndoStackItem *item;
	LocationIndex size = sizeof(SysTreesLockUndoStackItem);

	item = (SysTreesLockUndoStackItem *) get_undo_record_unreserved(UndoReserveTxn,
																	&location,
																	MAXALIGN(size));
	item->lock = lock;
	item->header.type = SysTreesLockUndoItemType;
	item->header.indexType = oIndexPrimary;
	item->header.itemSize = size;

	add_new_undo_stack_item(location);
	release_undo_size(UndoReserveTxn);
}

/*
 * systrees_modify_start() and systrees_modify_end() should surround code
 * blocks, which modifies system trees and shouldn't be done concurrently to
 * checkpoint.
 *
 * oSysTreesLock is held during undo replay.
 */
void
systrees_modify_start(void)
{
	LWLockAcquire(&checkpoint_state->oSysTreesLock, LW_EXCLUSIVE);
	add_systrees_lock_undo(false);
}

void
systrees_modify_end(void)
{
	(void) flush_local_wal(false);
	LWLockRelease(&checkpoint_state->oSysTreesLock);
	add_systrees_lock_undo(true);
}
