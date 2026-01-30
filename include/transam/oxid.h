/*-------------------------------------------------------------------------
 *
 * oxid.h
 *		Decalarations for transaction management routines.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/transam/oxid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __OXID_H__
#define __OXID_H__

typedef struct
{
	pg_atomic_uint64 csn;
	pg_atomic_uint64 commitPtr;
} OXidMapItem;

typedef struct
{
	pg_atomic_uint64 nextXid;
	pg_atomic_uint64 lastXidWhenUpdatedGlobalXmin;
	pg_atomic_uint64 runXmin;
	pg_atomic_uint64 globalXmin;

	pg_atomic_uint64 writeInProgressXmin;
	pg_atomic_uint64 writtenXmin;
	pg_atomic_uint64 checkpointRetainXmin;
	pg_atomic_uint64 checkpointRetainXmax;
	pg_atomic_uint64 cleanedXmin;
	pg_atomic_uint64 cleanedCheckpointXmin;
	pg_atomic_uint64 cleanedCheckpointXmax;

	slock_t		xminMutex;

	int			xidMapTrancheId;
	LWLock		xidMapWriteLock;

	/*
	 * sysXidUndoLocationChangeCount with locks are used for caching in
	 * read_replication_retain_undo_location()
	 */
	int			sysXidUndoLocationTrancheId;
	LWLock		sysXidUndoLocationLock;
	uint32		sysXidUndoLocationChangeCount;

} XidMeta;

extern XidMeta *xid_meta;

typedef struct
{
	TransactionId xid;			/* a 32-bit transaction id to be used during
								 * logical decoding */
	bool		useHeap;		/* flag indicates if current logical xid was
								 * allocated when heap xid has been already
								 * set */
} LogicalXidCtx;

typedef struct OSnapshot
{
	CommitSeqNo csn;
	XLogRecPtr	xlogptr;
	XLogRecPtr	xmin;
	CommandId	cid;
} OSnapshot;

/*
 * OTableFetchContext
 *
 * Encapsulates MVCC visibility context used to fetch relation and index
 * descriptors from OrioleDB system catalogs: SYS_TREES_O_TABLES & SYS_TREES_O_INDICES.
 *
 * The context combines:
 *  - snapshot: defines transactional visibility rules (xmin/csn/cid/xlogptr)
 *  - version:  explicit schema version of the relation to be fetched (possibly from tuple-level undo chain)
 *
 * This allows callers to retrieve a descriptor corresponding to a specific
 * catalog version as visible at a given snapshot, which is required during
 * logical decoding, recovery, and other multi-version catalog access paths.
 */
typedef struct
{
	OSnapshot  *snapshot;
	uint32		version;
} OTableFetchContext;

static inline OTableFetchContext
build_fetch_context(OSnapshot *snapshot, uint32 version)
{
	OTableFetchContext ctx = {.snapshot = snapshot,.version = version};

	return ctx;
}

extern OSnapshot o_in_progress_snapshot;
extern OSnapshot o_non_deleted_snapshot;

static inline void
o_check_isolation_level(void)
{
	if (XactIsoLevel == XACT_SERIALIZABLE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("orioledb does not support SERIALIZABLE isolation level")),
				errdetail("Stay tuned, it will be added in future releases."));
}

#define O_LOAD_SNAPSHOT(o_snapshot, snapshot) \
	do { \
		o_check_isolation_level(); \
		(o_snapshot)->xmin = (snapshot)->csnSnapshotData.xmin; \
		(o_snapshot)->csn = (snapshot)->csnSnapshotData.snapshotcsn; \
		(o_snapshot)->xlogptr = (snapshot)->csnSnapshotData.xlogptr; \
		(o_snapshot)->cid = (snapshot)->curcid; \
	} while (false)

#define O_LOAD_SNAPSHOT_CSN(o_snapshot, csnValue) \
	do { \
		o_check_isolation_level(); \
		(o_snapshot)->xmin = 0; \
		(o_snapshot)->csn = (csnValue); \
		(o_snapshot)->xlogptr = InvalidXLogRecPtr; \
		(o_snapshot)->cid = 0; \
	} while (false)

#define XLOG_PTR_ALIGN(ptr) ((ptr) + ((ptr) & 1))

extern void oxid_subxact_callback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg);

extern Size oxid_shmem_needs(void);
extern void oxid_init_shmem(Pointer ptr, bool found);
extern bool wait_for_oxid(OXid oxid, bool errorOk);
extern void oxid_notify(OXid oxid);
extern void oxid_notify_all(void);
extern void advance_oxids(OXid new_xid);
extern OXid get_current_oxid(void);
extern void assign_subtransaction_logical_xid(void);
extern void set_oxid_csn(OXid oxid, CommitSeqNo csn);
extern void set_oxid_xlog_ptr(OXid oxid, XLogRecPtr ptr);
extern void set_current_oxid(OXid oxid);
extern void set_current_logical_xid(LogicalXidCtx *in);
extern void parallel_worker_set_oxid(void);
extern void reset_current_oxid(void);
extern OXid get_current_oxid_if_any(void);
extern TransactionId get_current_logical_xid(void);
extern void get_current_logical_xid_ctx(LogicalXidCtx *output);
extern void current_oxid_precommit(void);
extern void current_oxid_xlog_precommit(void);
extern void current_oxid_commit(CommitSeqNo csn);
extern void current_oxid_abort(void);
extern CommitSeqNo oxid_get_csn(OXid oxid, bool getRawCsn);
extern XLogRecPtr oxid_get_xlog_ptr(OXid oxid);
extern void oxid_match_snapshot(OXid oxid, OSnapshot *snapshot,
								CommitSeqNo *outCsn, XLogRecPtr *outPtr);
extern void fill_current_oxid_osnapshot(OXid *oxid, OSnapshot *snapshot);
extern void fill_current_oxid_osnapshot_no_check(OXid *oxid,
												 OSnapshot *snapshot);
extern int	oxid_get_procnum(OXid oxid);
extern bool xid_is_finished(OXid xid);
extern bool xid_is_finished_for_everybody(OXid xid);
extern void fsync_xidmap_range(OXid xmin, OXid xmax, uint32 wait_event_info);
extern void clear_rewind_oxid(OXid oxid);
extern bool csn_is_retained_for_rewind(CommitSeqNo csn);

#endif							/* __OXID_H__ */
