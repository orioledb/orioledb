/*-------------------------------------------------------------------------
 *
 * undo.h
 *		Declarations of undo log routines.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
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
	pg_atomic_uint64 minRewindRetainLocation;
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
} UndoMeta;

typedef struct
{
	int			pendingTruncatesTrancheId;
	LWLock		pendingTruncatesLock;
	uint64		pendingTruncatesLocation;
} PendingTruncatesMeta;

typedef struct UndoStackItem UndoStackItem;

typedef enum
{
	ModifyUndoItemType = 1,
	RowLockUndoItemType,
	RelnodeUndoItemType,
	SysTreesLockUndoItemType,
	InvalidateUndoItemType,
	BranchUndoItemType,
	SubXactUndoItemType,
	RewindRelFileNodeUndoItemType,
	SysCacheDeleteUndoItemType
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
	bool		has_retained_undo_location[(int) UndoLogsCount];
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
extern UndoLocation curRetainUndoLocations[(int) UndoLogsCount];
extern PendingTruncatesMeta *pending_truncates_meta;

#define ORIOLEDB_UNDO_DATA_ROW_FILENAME_TEMPLATE (ORIOLEDB_UNDO_DIR "/%02X%08Xrow")
#define ORIOLEDB_UNDO_DATA_PAGE_FILENAME_TEMPLATE (ORIOLEDB_UNDO_DIR "/%02X%08Xpage")
#define ORIOLEDB_UNDO_SYSTEM_FILENAME_TEMPLATE (ORIOLEDB_UNDO_DIR "/%02X%08Xsystem")
#define UNDO_FILE_SIZE (0x4000000)

#define UNDO_REC_EXISTS(undoType, location) ((location) >= pg_atomic_read_u64(enable_rewind ? &get_undo_meta_by_type((undoType))->minRewindRetainLocation : &get_undo_meta_by_type((undoType))->minProcRetainLocation) || \
											 ((location) >= pg_atomic_read_u64(&get_undo_meta_by_type((undoType))->checkpointRetainStartLocation) && \
											  (location) < pg_atomic_read_u64(&get_undo_meta_by_type((undoType))->checkpointRetainEndLocation)))
#define UNDO_REC_XACT_RETAIN(undoType, location) ((location) >= pg_atomic_read_u64(&get_undo_meta_by_type((undoType))->minProcTransactionRetainLocation))
#define GET_CUR_UNDO_STACK_LOCATIONS(undoType) (AssertMacro(MYPROCNUMBER >= 0 && MYPROCNUMBER < max_procs), \
										AssertMacro((int) (undoType) >= 0 && (int) (undoType) < (int) UndoLogsCount), \
										&oProcData[MYPROCNUMBER].undoStackLocations[oProcData[MYPROCNUMBER].autonomousNestingLevel][(int) (undoType)])

extern Size undo_shmem_needs(void);
extern void undo_shmem_init(Pointer buf, bool found);
extern UndoMeta *get_undo_meta_by_type(UndoLogType undoType);

extern void update_min_undo_locations(UndoLogType undoType,
									  bool have_lock, bool do_cleanup);
extern void write_undo(UndoLogType undoType,
					   UndoLocation targetUndoLocation,
					   UndoLocation minProcReservedLocation,
					   bool attempt);
extern bool reserve_undo_size_extended(UndoLogType type, Size size,
									   bool waitForUndoLocation,
									   bool reportError);
extern void steal_reserved_undo_size(UndoLogType type, Size size);
extern void giveup_reserved_undo_size(UndoLogType type);
extern void fsync_undo_range(UndoLogType undoType,
							 UndoLocation fromLoc, UndoLocation toLoc,
							 uint32 wait_event_info);
extern Pointer get_undo_record(UndoLogType undoType, UndoLocation *undoLocation,
							   Size size);
extern Pointer get_undo_record_unreserved(UndoLogType type,
										  UndoLocation *undoLocation,
										  Size size);
extern Size get_reserved_undo_size(UndoLogType undoType);
extern void release_undo_size(UndoLogType undoType);
extern void add_new_undo_stack_item(UndoLogType undoType,
									UndoLocation location);
extern UndoLocation get_subxact_undo_location(UndoLogType undoType);
extern void add_new_undo_stack_item_to_process(UndoLogType undoType,
											   UndoLocation location,
											   int pgprocno,
											   LocalTransactionId localXid);
extern void read_shared_undo_locations(UndoStackLocations *to, UndoStackSharedLocations *from);
extern void write_shared_undo_locations(UndoStackSharedLocations *to, UndoStackLocations *from);
extern UndoStackLocations get_cur_undo_locations(UndoLogType undoType);
extern void set_cur_undo_locations(UndoLogType undoType,
								   UndoStackLocations locations);
extern void reset_cur_undo_locations(void);
extern void undo_xact_callback(XactEvent event, void *arg);
extern void undo_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
								  SubTransactionId parentSubid, void *arg);
extern bool have_current_undo(UndoLogType undoType);
extern void report_undo_overflow(void);
extern void apply_undo_branches(UndoLogType undoType, OXid oxid);
extern void apply_undo_stack(UndoLogType undoType, OXid oxid,
							 UndoStackLocations *toLocation,
							 bool changeCountsValid);
extern void on_commit_undo_stack(UndoLogType undoType, OXid oxid,
								 bool changeCountsValid);
extern void free_retained_undo_location(UndoLogType undoType);
extern void start_autonomous_transaction(OAutonomousTxState *state);
extern void abort_autonomous_transaction(OAutonomousTxState *state);
extern void finish_autonomous_transaction(OAutonomousTxState *state);
extern void undo_read(UndoLogType undoType, UndoLocation location,
					  Size size, Pointer buf);
extern void undo_write(UndoLogType undoType, UndoLocation location,
					   Size size, Pointer buf);
extern void undo_snapshot_register_hook(Snapshot snapshot);
extern void undo_snapshot_deregister_hook(Snapshot snapshot);
extern void orioledb_snapshot_hook(Snapshot snapshot);
extern void add_subxact_undo_item(SubTransactionId parentSubid);
extern void rollback_to_savepoint(UndoLogType undoType,
								  UndoStackKind kind,
								  SubTransactionId parentSubid,
								  bool changeCountsValid);
extern bool undo_type_has_retained_location(UndoLogType undoType);
extern bool have_retained_undo_location(void);
extern UndoLocation get_snapshot_retained_undo_location(UndoLogType undoType);
extern void orioledb_reset_xmin_hook(void);
extern void o_add_rewind_relfilenode_undo_item(RelFileNode *onCommit,
											   RelFileNode *onAbort,
											   int nOnCommit, int nOnAbort);

static inline void
reserve_undo_size(UndoLogType type, Size size)
{
	(void) reserve_undo_size_extended(type, size, true, true);
}

extern void reset_command_undo_locations(void);
extern UndoLocation command_get_undo_location(CommandId cid);
extern UndoLocation current_command_get_undo_location(void);
extern void update_command_undo_location(CommandId commandId,
										 UndoLocation undoLocation);

#endif							/* __UNDO_H__ */
