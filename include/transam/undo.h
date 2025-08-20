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

	/*-----
	 * lastUsedLocation is a position within a RAM undo log buffer,
	 * representing a boundary between two areas in  the undo log buffer:
	 * (1) reserved area - location range in undo log buffer, already
	 * reserved by (granted to) some backends for writing; backends are
	 * writing their data to this location range at the moment.
	 * (2) ready-for-reservation area is a pre-reserved, free location range
	 * in undo log buffer, ready to be reserved (occupied) by any backend.
	 *
	 * lastUsedLocation is the first position in the RAM undo log buffer
	 * from which the ready-for-reservation area begins.
	 *
	 * RAM undo log buffer visualization:
	 * <- -----------------|------------------------------ ->
	 *	   <reserved area> | <ready-for-reservation area>
	 *					   \
	 *				lastUsedLocation
	 *
	 * lastUsedLocation gets increased in the get_undo_record() method only.
	 *
	 * Basic algorithm used by each backend for obtaining afree undo log
	 * location for an undo record:
	 * - call get_undo_record()
	 *	   - call set_my_reserved_location()
	 *		   - read current lastUsedLocation - the first location available
	 *			 for the reservation at the moment,
	 *		   - setup the per-process undo locations: reservedUndoLocation
	 *			 and transactionUndoRetainLocation, from read lastUsedLocation,
	 *	   - advance shared meta->lastUsedLocation by the value of the
	 *		 reservation size.
	 */
	pg_atomic_uint64 lastUsedLocation;

	/*
	 * advanceReservedLocation is used for preliminary reservation of RAM undo
	 * log buffer' free locations, ready to be obtained by backends.
	 * advanceReservedLocation is the top value of all monotonically
	 * increasing undo log buffer locations. advanceReservedLocation gets
	 * increased in reserve_undo_size_extended() method only.
	 *
	 * Pre-reservation must be performed well in advance before the actual
	 * obtaining (reserving) of undo log locations, because of eviction
	 * overhead in a case of undo log buffer overflow.
	 *
	 * reserve_undo_size_extended() method may trigger an eviction process in
	 * a case of undo log buffer overflow and waitForUndoLocation == true,
	 * otherwise if waitForUndoLocation == false and there is no place in a
	 * buffer - revert modifications on advanceReservedLocation and return
	 * failure.
	 */
	pg_atomic_uint64 advanceReservedLocation;

	/*-----
	 * Eviction of undo log from the RAM buffer to the file range on disk is
	 * performed by:
	 *  - background writer,
	 *  - any process during reserve_undo_size_extended().
	 *
	 * Eviction metadata is presented by an interval: (writtenLocation,
	 * writeInProgressLocation].  writtenLocation is the last location has
	 * already been successfully evicted to the file.  writeInProgressLocation
	 * is the last location where eviction is still in-progress.  The location
	 * range between writtenLocation and writeInProgressLocation means the
	 * area which is currently being evicted to files, i.e., write-to-file
	 * operation for this area is still in-progress.  When writtenLocation
	 * equals to writeInProgressLocation`, then there is no in-progress
	 * eviction process at the moment.
	 */
	pg_atomic_uint64 writeInProgressLocation;
	pg_atomic_uint64 writtenLocation;

	/*
	 * lastUsedUndoLocationWhenUpdatedMinLocation is modified by the
	 * update_min_undo_locations() method only and represents the last actual
	 * lastUsedLocation that has been seen during update_min_undo_locations().
	 * lastUsedUndoLocationWhenUpdatedMinLocation is used by
	 * orioledb_snapshot_hook() for determining if there is necessary to call
	 * update_min_undo_locations() method to actualize shared meta' locations.
	 * NOTE: The update_min_undo_locations() method is called each time after
	 * 1/10 of the undo log is passed. NOTE: location values in shared meta
	 * may lag behind each process's data' actual undoRetainLocations.
	 */
	pg_atomic_uint64 lastUsedUndoLocationWhenUpdatedMinLocation;

	/*
	 * minProcTransactionRetainLocation is used for transaction rollback. It
	 * is a minimum (among all transactions) location that is needed for
	 * rollback.
	 *
	 * minProcRetainLocation - is a minimum (among all snapshots) location
	 * that is needed for any of the active snapshots.
	 *
	 * minRewindRetainLocation is used for the rewind mechanism. The rewind
	 * mechanism allows rolling back all recent transactions, i.e., allows
	 * moving a database to a previous time point. In the case of rewind, the
	 * undo log is retained for a longer period of time to provide recovery
	 * for more remote timepoints.
	 */
	pg_atomic_uint64 minProcTransactionRetainLocation;
	pg_atomic_uint64 minProcRetainLocation;
	pg_atomic_uint64 minRewindRetainLocation;

	/*
	 * minProcReservedLocation is a minimum location (among all backends)
	 * within the RAM undo log buffer that is actually reserved (obtained) by
	 * a backend for writing its undo log record to a RAM undo log buffer. The
	 * process must retain its reservedUndoLocation only while performing a
	 * write operation to a RAM undo log buffer. When the process finishes the
	 * write operation for an undo log record to the RAM buffer, it must
	 * release its reservedUndoLocation as soon as possible.
	 */
	pg_atomic_uint64 minProcReservedLocation;

	/*
	 * [checkpointRetainStartLocation; checkpointRetainEndLocation) represents
	 * the undo range retained for the checkpoint.  This range allows the
	 * rollback of any transaction that was in-progress during checkpointing.
	 */
	pg_atomic_uint64 checkpointRetainStartLocation;
	pg_atomic_uint64 checkpointRetainEndLocation;

	/*
	 * cleanedLocation - the value of minRetainLocation at the moment of last
	 * cleanup.
	 *
	 * Range [cleanedCheckpointStartLocation, cleanedCheckpointEndLocation]
	 * means an undo log range, which has been retained during the last
	 * cleanup, i.e., the last undo log range that is persisted on disk after
	 * the last cleanup.
	 *
	 * cleanedCheckpointStartLocation - the value of
	 * checkpointRetainStartLocation during the last cleanup.
	 * cleanedCheckpointEndLocation - value of checkpointRetainEndLocation
	 * during the last cleanup.
	 */
	pg_atomic_uint64 cleanedLocation;
	pg_atomic_uint64 cleanedCheckpointStartLocation;
	pg_atomic_uint64 cleanedCheckpointEndLocation;

	/*
	 * minUndoLocationsMutex is primarily used by update_min_undo_locations()
	 * method, also is used by evict_undo_to_disk() method to protect shared
	 * meta' fields, but is released for eviction writes; also is used in some
	 * cases to protect shared meta' retain* locations and write/written
	 * locations.
	 */
	slock_t		minUndoLocationsMutex;

	/*
	 * minUndoLocationsChangeCount gets increased by
	 * update_min_undo_locations() method. minUndoLocationsChangeCount is used
	 * together with wait_for_even_min_undo_locations_changecount() method to
	 * fix concurrency between update_min_undo_locations() method and
	 * set_my_reserved_location() & set_my_retain_location() methods.
	 */
	uint32		minUndoLocationsChangeCount;

	/*
	 * writeInProgressChangeCount is used together with a
	 * wait_for_even_write_in_progress_changecount() method to fix concurrency
	 * between undo_write() and evict_undo_to_disk() methods (protects shared
	 * meta' write/written locations).
	 */
	uint32		writeInProgressChangeCount;

	int			undoWriteTrancheId; /* tranche-group ID of the LWLock */

	/*
	 * undoWriteLock is an LWLock that is used for protecting disk writes
	 * (during the eviction process, evict_undo_to_disk()) or for await
	 * process on in-progress writes.
	 */
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
	SysCacheDeleteUndoItemType,
	InvalidateComparatorUndoItemType,
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
									  bool have_lock,
									  bool do_cleanup);
extern void evict_undo_to_disk(UndoLogType undoType,
							   UndoLocation targetUndoLocation,
							   UndoLocation minProcReservedLocation,
							   bool attempt);
extern bool reserve_undo_size_extended(UndoLogType type, Size size,
									   bool waitForUndoLocation);
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
extern void release_reserved_undo_location(UndoLogType undoType);
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
	(void) reserve_undo_size_extended(type, size, true);
}

extern void reset_command_undo_locations(void);
extern UndoLocation command_get_undo_location(CommandId cid);
extern UndoLocation current_command_get_undo_location(void);
extern void update_command_undo_location(CommandId commandId,
										 UndoLocation undoLocation);

#endif							/* __UNDO_H__ */
