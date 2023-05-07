/*-------------------------------------------------------------------------
 *
 * checkpoint.h
 * 		Declarations for checkpoint.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/checkpoint/checkpoint.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __CHECKPOINT_H__
#define __CHECKPOINT_H__

#include "orioledb.h"

#include "btree/page_contents.h"

#include "access/xlogdefs.h"

typedef struct
{
	uint32		lastCheckpointNumber;
	CommitSeqNo lastCSN;
	OXid		lastXid;
	UndoLocation lastUndoLocation;
	XLogRecPtr	toastConsistentPtr;
	XLogRecPtr	replayStartPtr;
	XLogRecPtr	sysTreesStartPtr;
	uint64		mmapDataLength;
	UndoLocation checkpointRetainStartLocation;
	UndoLocation checkpointRetainEndLocation;
	OXid		checkpointRetainXmin;
	OXid		checkpointRetainXmax;
	uint32		binaryVersion;
	pg_crc32c	crc;
} CheckpointControl;

struct CheckpointFileHeader
{
	uint64		ctid;
	uint64		rootDownlink;
	uint64		datafileLength;
	uint64		numFreeBlocks;
	uint32		leafPagesNum;
};

typedef enum
{
	NextKeyNone,
	NextKeyValue,
	NextKeyGreatest
} NextKeyType;

typedef enum
{
	CheckpointBoundNone,
	CheckpointBoundHikey,
	CheckpointBoundRightmost
} CheckpointBound;

typedef struct
{
	/*
	 * Checkpoint bound type for current level, helps to determine the
	 * checkpoint number for eviction
	 */
	CheckpointBound bound;

	/*
	 * Type of next key which will be added to image.
	 */
	NextKeyType nextkeyType;

	/*
	 * Contains data which will be written to disk.
	 */
	char		image[ORIOLEDB_BLCKSZ];

	/*
	 * Hikey of current page. It helps to determine checkpoint number for
	 * eviction and filling bound for autonomous pages.
	 */
	OFixedShmemKey hikey;
	/* Next key which can be added to current node. */
	OFixedShmemKey nextkey;
	/* Lokey of current BTree page */
	OFixedShmemKey lokey;

	/*
	 * Current the BTree page number. OInvalidInMemoryBlkno if no page
	 * processing at now.
	 */
	OInMemoryBlkno blkno;

	/*
	 * Number page containing the relevant hikey. Might be different from
	 * `blkno` for autonomous pages.
	 */
	OInMemoryBlkno hikeyBlkno;

	/* Current valid offset on BTree page. */
	OffsetNumber offset;
	/* Is current in memory page leftmost on a BTree level. */
	bool		leftmost;

	/*
	 * Is current page autonomous.
	 *
	 * Autonomous pages do not equal to in memory BTree pages and written to
	 * disk as far as filling or reach CheckpointPageInfo.hikey.
	 */
	bool		autonomous;

	/*
	 * Is the image already contains an internal tuple with a valid key.
	 *
	 * If autonomous stack[level - 1].image was not written to disk no sense
	 * to reinsert the same internal tuple key to the current level (with
	 * autonomous image too).
	 *
	 * We can avoid reinsert of an internal tuple in this case.
	 *
	 * Although this approach helps to flush autonomous images stack to disk
	 * if needed.
	 */
	bool		autonomousTupleExist;

	/*
	 * Helps to setup O_BTREE_FLAG_LEFTMOST for autonomous pages. We can not
	 * use CheckpointPageInfo.leftmost flag because it used for navigation
	 * through OrioleDB BTree.
	 */
	bool		autonomousLeftmost;
} CheckpointPageInfo;

typedef enum
{
	CurKeyLeast,
	CurKeyValue,
	CurKeyGreatest,
	CurKeyFinished
} CurKeyType;

#define SHARED_ROOT_INFO_INSERT_NUM_LOCKS 128

#define XID_RECS_QUEUE_SIZE			(max_procs * 4)

typedef struct
{
	OXid		oxid;
	UndoStackLocations undoLocation;
} XidFileRec;

typedef struct
{
	uint32		changecount;
	uint32		lastCheckpointNumber;
	OIndexType	treeType;
	Oid			datoid;
	Oid			reloid;
	Oid			relnode;
	bool		completed;
	CurKeyType	curKeyType;
	OFixedShmemKey curKeyValue;
	CheckpointPageInfo stack[ORIOLEDB_MAX_DEPTH];
	/* pid of the worker */
	pid_t		pid;
	double		dirtyPagesEstimate;
	uint64		pagesWritten;
	/* helps to avoid skip a new table for the checkpoint in progress */
	int			oTablesMetaTrancheId;
	LWLock		oTablesMetaLock;
	int			oSysTreesTrancheId;
	LWLock		oSysTreesLock;
	int			oSharedRootInfoInsertTrancheId;
	LWLock		oSharedRootInfoInsertLocks[SHARED_ROOT_INFO_INSERT_NUM_LOCKS];
	struct Latch *checkpointerLatch;
	pg_atomic_uint32 autonomousLevel;
	XLogRecPtr	replayStartPtr;
	XLogRecPtr	controlReplayStartPtr;
	XLogRecPtr	sysTreesStartPtr;
	XLogRecPtr	controlSysTreesStartPtr;
	XLogRecPtr	toastConsistentPtr;
	XLogRecPtr	controlToastConsistentPtr;
	pg_atomic_uint64 mmapDataLength;

	/*
	 * Shared memory queue of records for writing to the xids file.  Backends
	 * write to this queue last undo position on transaction commit/abort.
	 * Checkpoint writes current undo positions for in-progress transactions.
	 */
	uint32		xidQueueCheckpointNum;
	int			oXidQueueTrancheId;
	LWLock		oXidQueueLock;
	int			oXidQueueFlushTrancheId;
	LWLock		oXidQueueFlushLock;
	int			copyBlknoTrancheId;
	int			oMetaTrancheId;
	pg_atomic_uint64 xidRecLastPos;
	pg_atomic_uint64 xidRecFlushPos;
	XidFileRec	xidRecQueue[FLEXIBLE_ARRAY_MEMBER];
} CheckpointState;

#define XID_FILENAME_FORMAT (ORIOLEDB_DATA_DIR"/%u.xid")

#define chkp_inc_changecount_before(state)	\
	do {	\
		state->changecount++;	\
		pg_write_barrier(); \
	} while (0)

#define chkp_inc_changecount_after(state) \
	do {	\
		pg_write_barrier(); \
		state->changecount++;	\
		Assert((state->changecount & 1) == 0); \
	} while (0)

#define chkp_save_changecount_before(state, save_changecount)	\
	do {	\
		save_changecount = state->changecount; \
		pg_read_barrier();	\
	} while (0)

#define chkp_save_changecount_after(state, save_changecount)	\
	do {	\
		pg_read_barrier();	\
		save_changecount = state->changecount; \
	} while (0)

extern CheckpointState *checkpoint_state;

extern Size checkpoint_shmem_size(void);
extern void checkpoint_shmem_init(Pointer ptr, bool found);
extern char *get_eviction_filename(ORelOids oids, uint32 chkp_num);

extern void o_perform_checkpoint(XLogRecPtr redo_pos, int flags);
extern void o_after_checkpoint_cleanup_hook(XLogRecPtr checkPointRedo,
											int flags);

extern bool page_is_under_checkpoint(BTreeDescr *desc, OInMemoryBlkno blkno);
extern bool tree_is_under_checkpoint(BTreeDescr *desc);
extern bool get_checkpoint_number(BTreeDescr *desc, OInMemoryBlkno blkno, uint32 *checkpoint_number, bool *copy_blkno);
extern uint32 get_cur_checkpoint_number(ORelOids *oids, OIndexType type, bool *checkpoint_concurrent);
extern bool can_use_checkpoint_extents(BTreeDescr *desc, uint32 chkp_num);
extern void free_extent_for_checkpoint(BTreeDescr *desc, FileExtent *extent, uint32 chkp_num);
extern void backend_set_autonomous_level(CheckpointState *state, uint32 level);
extern bool tbl_data_exists(ORelOids *oids);
extern void evictable_tree_init(BTreeDescr *desc, bool init_shmem);
extern void checkpointable_tree_init(BTreeDescr *desc, bool init_shmem, bool *was_evicted);
extern void checkpointable_tree_free(BTreeDescr *desc);
extern void systrees_modify_start(void);
extern void systrees_modify_end(void);
extern void systrees_lock_callback(UndoLocation location,
								   UndoStackItem *baseItem, OXid oxid,
								   bool abort, bool changeCountsValid);
extern void before_writing_xids_file(int chkpnum);
extern void write_to_xids_queue(XidFileRec *rec);

#endif							/* __CHECKPOINT_H__ */
