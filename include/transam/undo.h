/*-------------------------------------------------------------------------
 *
 * undo.h
 *		Declarations of undo log routines.
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/transam/undo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __UNDO_H__
#define __UNDO_H__

typedef struct
{
	/*---
	 * lastUsedLocation - max undo location actually used (within in-memory undo
	 *					  buffer)
	 * advanceReservedLocation - undo location for reservation of future
	 * 							 records
	 * minProcReservedLocation - min undo location, where forming undo records
	 * 							 might exist)
	 * minProcTransactionRetainLocation - min undo location retained by
	 * 									  transaction
	 * minProcRetainLocation - min undo location retained by transaction or
	 * 						   snapshot
	 * writeInProgressLocation - writing to this location is currently
	 * 							 in-progress
	 * writtenLocation - already written to this location
	 * cleanedLocation - files behind this location are already cleaned
	 *
	 * cleanedLocation <= minProcRetainLocation <= minProcTransactionRetainLocation <= minProcReservedLocation
	 * cleanedLocation <= writtenLocation <= writeInProgressLocation <= minProcReservedLocation
	 * minProcReservedLocation <= lastUsedLocation <= advanceReservedLocation
	 *
	 * writtenLocation >= lastUsedLocation + undo_circular_buffer_size
	 *
	 * [checkpointRetainStartLocation; checkpointRetainEndLocation) -- range of
	 * undo locations required for recovery from the checkpoint.
	 */
	pg_atomic_uint64 lastUsedLocation;
	pg_atomic_uint64 advanceReservedLocation;
	pg_atomic_uint64 writeInProgressLocation;
	pg_atomic_uint64 writtenLocation;
	pg_atomic_uint64 lastUsedUndoLocationWhenUpdatedMinLocation;
	pg_atomic_uint64 minProcTransactionRetainLocation;
	pg_atomic_uint64 minProcRetainLocation;
	pg_atomic_uint64 minProcReservedLocation;
	pg_atomic_uint64 checkpointRetainStartLocation;
	pg_atomic_uint64 checkpointRetainEndLocation;
	pg_atomic_uint64 cleanedLocation;
	pg_atomic_uint64 cleanedCheckpointStartLocation;
	pg_atomic_uint64 cleanedCheckpointEndLocation;
	slock_t		minUndoLocationsMutex;
	uint32		minUndoLocationsChangeCount;
	int			undoWriteTrancheId;
	LWLock		undoWriteLock;
	int			undoStackLocationsFlushLockTrancheId;

	int			pendingTruncatesTrancheId;
	LWLock		pendingTruncatesLock;
	uint64		pendingTruncatesLocation;
} UndoMeta;

typedef enum
{
	/* Invalid value. */
	UndoReserveNone = -1,

	/*
	 * Undo reserved while a transaction in progress. Undo records may be
	 * added to a undo stack.
	 */
	UndoReserveTxn = 0
} UndoReserveType;

typedef struct UndoStackItem UndoStackItem;

typedef enum
{
	ModifyUndoItemType = 1,
	RowLockUndoItemType,
	RelnodeUndoItemType,
	SysTreesLockUndoItemType,
	InvalidateUndoItemType,
	BranchUndoItemType,
	SubXactUndoItemType
} UndoItemType;

struct UndoStackItem
{
	UndoLocation prev;
	LocationIndex itemSize;
	uint8		type;
	uint8		indexType;
};

typedef struct
{
	UndoStackItem base;
	UndoLocation onCommitLocation;
} OnCommitUndoStackItem;

typedef struct
{
	UndoLocation location;
	UndoLocation branchLocation;
	UndoLocation subxactLocation;
	UndoLocation onCommitLocation;
} UndoStackLocations;

typedef struct
{
	bool		needs_wal_flush;
	bool		has_retained_undo_location;
	bool		local_wal_has_material_changes;
	OXid		oxid;
	TransactionId logicalXid;
} OAutonomousTxState;

/*
 * Branch undo record: when we apply part of undo (for instance, when we do
 * rollback to the savepoint), we still memorize the "long" undo path in the
 * "branch" undo record.
 */
typedef struct
{
	UndoStackItem header;
	UndoLocation longPathLocation;
	UndoLocation prevBranchLocation;
} BranchUndoStackItem;

/*
 * Subxact undo record: memorized undo location for rollback in the future.
 */
typedef struct
{
	UndoStackItem header;
	UndoLocation prevSubLocation;
	SubTransactionId parentSubid;
} SubXactUndoStackItem;

typedef enum
{
	UndoStackFull,
	UndoStackHead,
	UndoStackTail
} UndoStackKind;

extern bool oxid_needs_wal_flush;
extern UndoLocation curRetainUndoLocation;

#define ORIOLEDB_UNDO_FILENAME_TEMPLATE (ORIOLEDB_UNDO_DIR "/%02X%08X")
#define UNDO_FILE_SIZE (0x4000000)

#define UNDO_REC_EXISTS(location) ((location) >= pg_atomic_read_u64(&undo_meta->minProcRetainLocation) || \
								   ((location) >= pg_atomic_read_u64(&undo_meta->checkpointRetainStartLocation) && \
									(location) < pg_atomic_read_u64(&undo_meta->checkpointRetainEndLocation)))
#define UNDO_REC_XACT_RETAIN(location) ((location) >= pg_atomic_read_u64(&undo_meta->minProcTransactionRetainLocation))
#define GET_CUR_UNDO_STACK_LOCATIONS() (AssertMacro(MyProc->pgprocno >= 0 && MyProc->pgprocno < max_procs), \
										&oProcData[MyProc->pgprocno].undoStackLocations[oProcData[MyProc->pgprocno].autonomousNestingLevel])

extern Pointer o_undo_buffers;
extern UndoMeta *undo_meta;

extern Size undo_shmem_needs(void);
extern void undo_shmem_init(Pointer buf, bool found);

/*
 * UndoReserveType used here only for assertions and can be removed,
 * but it may help us to simplify error detection
 */
extern void update_min_undo_locations(bool have_lock, bool do_cleanup);
extern void write_undo(UndoLocation targetUndoLocation,
					   UndoLocation minProcReservedLocation,
					   bool attempt);
extern bool reserve_undo_size_extended(UndoReserveType type, Size size,
									   bool waitForUndoLocation,
									   bool reportError);
extern void fsync_undo_range(UndoLocation fromLoc, UndoLocation toLoc, uint32 wait_event_info);
extern Pointer get_undo_record(UndoReserveType type, UndoLocation *undoLocation,
							   Size size);
extern Pointer get_undo_record_unreserved(UndoReserveType type,
										  UndoLocation *undoLocation,
										  Size size);
extern Size get_reserved_undo_size(UndoReserveType type);
extern void release_undo_size(UndoReserveType type);
extern void add_new_undo_stack_item(UndoLocation location);
extern UndoLocation get_subxact_undo_location(void);
extern void read_shared_undo_locations(UndoStackLocations *to, UndoStackSharedLocations *from);
extern void write_shared_undo_locations(UndoStackSharedLocations *to, UndoStackLocations *from);
extern UndoStackLocations get_cur_undo_locations(void);
extern void set_cur_undo_locations(UndoStackLocations);
extern void reset_cur_undo_locations(void);
extern void undo_xact_callback(XactEvent event, void *arg);
extern void undo_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
								  SubTransactionId parentSubid, void *arg);
extern bool have_current_undo(void);
extern void report_undo_overflow(void);
extern void apply_undo_branches(OXid oxid);
extern void apply_undo_stack(OXid oxid, UndoStackLocations *toLocation,
							 bool changeCountsValid);
extern void on_commit_undo_stack(OXid oxid, bool changeCountsValid);
extern void free_retained_undo_location(void);
extern void start_autonomous_transaction(OAutonomousTxState *state);
extern void abort_autonomous_transaction(OAutonomousTxState *state);
extern void finish_autonomous_transaction(OAutonomousTxState *state);
extern void undo_read(UndoLocation location, Size size, Pointer buf);
extern void undo_write(UndoLocation location, Size size, Pointer buf);
extern void undo_snapshot_register_hook(Snapshot snapshot);
extern void undo_snapshot_deregister_hook(Snapshot snapshot);
extern void orioledb_snapshot_hook(Snapshot snapshot);
extern void add_subxact_undo_item(SubTransactionId parentSubid);
extern void rollback_to_savepoint(UndoStackKind kind,
								  SubTransactionId parentSubid,
								  bool changeCountsValid);
extern bool have_retained_undo_location(void);
extern UndoLocation get_snapshot_retained_undo_location(void);
extern void orioledb_reset_xmin_hook(void);

static inline void
reserve_undo_size(UndoReserveType type, Size size)
{
	(void) reserve_undo_size_extended(type, size, true, true);
}

#endif							/* __UNDO_H__ */
