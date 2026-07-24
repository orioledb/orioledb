/*-------------------------------------------------------------------------
 *
 * oxid.c
 *		Management of OrioleDB transaction identifiers.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/transam/oxid.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"

#include "orioledb.h"

#include "recovery/recovery.h"
#include "transam/oxid.h"
#include "utils/dsa.h"
#include "utils/o_buffers.h"

#include "access/transam.h"
#include "access/twophase.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "rewind/rewind.h"
#include "storage/lmgr.h"
#include "storage/sinvaladt.h"
#include "storage/procsignal.h"
#include "storage/proc.h"
#if PG_VERSION_NUM >= 180000
#include "utils/memutils.h"
#endif
#include "utils/snapmgr.h"
#include "utils/stopevent.h"
#include "recovery/wal.h"

#define XID_FILE_SIZE (0x1000000)
#define OXID_BUFFERS_TAG (0)

#define	COMMITSEQNO_SPECIAL_BIT (UINT64CONST(1) << 63)
#define COMMITSEQNO_RETAINED_FOR_REWIND (UINT64CONST(1) << 62)
#define COMMITSEQNO_STATUS_IN_PROGRESS (0x0)
#define COMMITSEQNO_STATUS_CSN_COMMITTING (0x1)
#define COMMITSEQNO_IS_SPECIAL(csn) ((csn) & COMMITSEQNO_SPECIAL_BIT)
#define COMMITSEQNO_GET_STATUS(csn) \
	(AssertMacro(COMMITSEQNO_IS_SPECIAL(csn)), (csn) & 0x1)
#define COMMITSEQNO_GET_LEVEL(csn) \
	(AssertMacro(COMMITSEQNO_IS_SPECIAL(csn)), ((csn) >> 31) & 0xFFFFFFFF)
#define COMMITSEQNO_GET_PROCNUM(csn) \
	(AssertMacro(COMMITSEQNO_IS_SPECIAL(csn)), ((csn) >> 15) & 0xFFFF)
#define COMMITSEQNO_MAKE_SPECIAL(procnum, level, status) \
	(COMMITSEQNO_SPECIAL_BIT | ((uint64) procnum << 15) | \
	 ((uint64) level << 31) | (status))

#define	XLOG_PTR_SPECIAL_BIT (0x1)
#define XLOG_PTR_IN_PROGRESS (0x0)
#define XLOG_PTR_COMMITTING (0x2)
#define XLOG_PTR_IS_SPECIAL(ptr) ((ptr) & XLOG_PTR_SPECIAL_BIT)
#define XLOG_PTR_GET_STATUS(ptr) \
		(AssertMacro(XLOG_PTR_IS_SPECIAL(ptr)), (ptr) & 0x2)
#define XLOG_PTR_GET_LEVEL(ptr) \
		(AssertMacro(XLOG_PTR_IS_SPECIAL(ptr)), ((ptr) >> 31) & 0xFFFFFFFF)
#define XLOG_PTR_GET_PROCNUM(ptr) \
		(AssertMacro(XLOG_PTR_IS_SPECIAL(ptr)), ((ptr) >> 15) & 0xFFFF)
#define XLOG_PTR_MAKE_SPECIAL(procnum, level, status) \
		(XLOG_PTR_SPECIAL_BIT | ((uint64) procnum << 15) | \
		 ((uint64) level << 31) | (status))

PG_FUNCTION_INFO_V1(orioledb_get_current_logical_xid);
PG_FUNCTION_INFO_V1(orioledb_get_current_heap_xid);
PG_FUNCTION_INFO_V1(orioledb_get_xid_meta);

/*
 * OrioleDB uses three transaction id entities:
 *     - [uint32 TransactionId] native PG heap transaction id (heap xid)
 *     - [uint64 OXid] extended OrioleDB transaction id (oxid)
 *     - [uint32 TransactionId] logical transaction id for compatibility with logical decoding PG API (logical xid)
 *
 * Each one of these xids uses an independent sequence / algorithm to allocate a new xid.
 *
 * xact types of xid assignment for logical decoding & replay:
 *     - readonly           - no xid (*)
 *     - heap write         - heap xid - COMMIT heap xact
 *     - oriole write       - oriole xid - COMMIT oriole xact
 *     - heap->oriole write - SWITCH_LOGICAL_XID H2O: \
 *                                                     Oriole txn acts as a sub-txn of a top heap txn
 *     - oriole->heap write - SWITCH_LOGICAL_XID O2H: /
 */
static OXid curOxid = InvalidOXid;	/* a 64-bit OrioleDB oxid */

/*
 * Flags that record whether current_oxid_precommit() /
 * current_oxid_xlog_precommit() have stamped the COMMITTING bit on
 * curOxid's CSN / xlog ptr.  current_oxid_clear_committing() consults
 * them on the abort path to decide whether the bit must be reverted to
 * IN_PROGRESS before apply_undo_stack() runs.  These are kept as
 * backend-local booleans rather than being recovered from xidBuffer
 * because the xidBuffer slot is shared by all oxids that map to it
 * modulo xid_circular_buffer_size and, after run_xmin advances, may be
 * reused for a different oxid.
 */
static bool csn_committing_set = false;
static bool xlog_ptr_committing_set = false;

static LogicalXidCtx logicalXidContext = {InvalidTransactionId, false};

static inline void
reset_logical_xid_ctx(void)
{
	logicalXidContext.xid = InvalidTransactionId;
	logicalXidContext.useHeap = false;
}

Datum
orioledb_get_current_logical_xid(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(logicalXidContext.xid);
}

Datum
orioledb_get_current_heap_xid(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(GetCurrentTransactionIdIfAny());
}

/*
 * Saved logical xid context together with the subtransaction whose start
 * pushed it.  The subid tag makes push and pop symmetric: a subtransaction
 * end must only pop the entry it pushed itself.  A subtransaction that
 * started before the transaction's first OrioleDB modification pushes
 * nothing (see undo_subxact_callback), so its end must leave the current
 * context untouched.
 */
typedef struct
{
	LogicalXidCtx ctx;
	SubTransactionId subid;
} PrevLogicalXidEntry;

static List *prevLogicalXids = NIL; /* stack of PrevLogicalXidEntry for all
									 * xids on subxact's chain, for correct
									 * restore and release */

/*
 * Check whether the top of prevLogicalXids was pushed by the subtransaction
 * identified by subid.
 */
static bool
prev_logical_xid_pushed_by(SubTransactionId subid)
{
	PrevLogicalXidEntry *entry;

	if (prevLogicalXids == NIL)
		return false;

	entry = (PrevLogicalXidEntry *) llast(prevLogicalXids);

	return entry != NULL && entry->subid == subid;
}

static OXidMapItem *xidBuffer;

/*
 * Per-page dirty bitmap for xidBuffer.  One bit per ORIOLEDB_BLCKSZ page
 * (i.e. per ORIOLEDB_BLCKSZ / sizeof(OXidMapItem) slots).  Set by writers
 * whenever they store into a slot; cleared by the checkpoint-time flush
 * (flush_dirty_xidsmap_range) and by the eviction path once the page has
 * been pushed out to o_buffers.
 *
 * The bit signals "this page may differ from the on-disk copy" - it is set
 * AFTER the slot store with a release barrier, so a checkpointer that has
 * just observed it can read the slot data with an acquire barrier and is
 * guaranteed to see the writer's update.  A late-setting writer (set bit
 * after checkpoint cleared it) leaves the bit set so the next flush
 * catches the update; the worst case is one redundant page write.
 */
static pg_atomic_uint32 *xidBufferDirty;

XidMeta    *xid_meta;

static pg_atomic_uint32 *logicalXidsShmemMap;

/* # slots covered by one o_buffers page in the xidmap. */
#define XID_SLOTS_PER_PAGE (ORIOLEDB_BLCKSZ / sizeof(OXidMapItem))
StaticAssertDecl(ORIOLEDB_BLCKSZ % sizeof(OXidMapItem) == 0,
				 "OXidMapItem must tile ORIOLEDB_BLCKSZ");

/* # of pages in the circular buffer (== xid_buffers_count by construction). */
#define XID_BUFFER_NPAGES \
	(xid_circular_buffer_size / XID_SLOTS_PER_PAGE)

/* # of uint32 words holding the dirty bitmap. */
#define XID_BUFFER_DIRTY_WORDS \
	((XID_BUFFER_NPAGES + 31) / 32)

/* Ring-relative page index for oxid (folds modulo the circular buffer). */
#define XID_BUFFER_PAGE_INDEX(oxid) \
	(((oxid) % xid_circular_buffer_size) / XID_SLOTS_PER_PAGE)

static inline void
mark_xid_buffer_dirty(OXid oxid)
{
	uint32		page = XID_BUFFER_PAGE_INDEX(oxid);
	uint32		mask = 1U << (page % 32);

	Assert(page < XID_BUFFER_NPAGES);

	/*
	 * Release fence so the preceding slot store is visible to a checkpointer
	 * that observes the bit set.
	 */
	pg_atomic_fetch_or_u32(&xidBufferDirty[page / 32], mask);
}

/*
 * Atomic test-and-clear of the dirty bit for one page.  Returns true if
 * the bit was set (caller must persist the page to disk).  Acquire fence
 * pairs with the release in mark_xid_buffer_dirty().
 */
static inline bool
test_clear_xid_buffer_page_dirty(uint32 page)
{
	uint32		mask = 1U << (page % 32);
	uint32		old = pg_atomic_fetch_and_u32(&xidBufferDirty[page / 32], ~mask);

	return (old & mask) != 0;
}

/*
 * Non-destructive read of one page's dirty bit.  Used for partial boundary
 * pages, whose bit must be left intact (their tail still covers slots
 * outside the range being written).
 */
static inline bool
xid_buffer_page_dirty(uint32 page)
{
	uint32		mask = 1U << (page % 32);

	return (pg_atomic_read_u32(&xidBufferDirty[page / 32]) & mask) != 0;
}

/*
 * Clear dirty bits for pages fully covered by [xmin, xmax) so a subsequent
 * checkpoint-time flush can skip them.  Used by write_xidsmap() (which has
 * just written the pages out) and advance_global_xmin() (which has just
 * reset them to FROZEN).  Partial boundary pages are left dirty: their
 * tail still belongs to slots outside the range that may be actively
 * written.
 */
static inline void
clear_xid_dirty_range(OXid xmin, OXid xmax)
{
	OXid		fullStart,
				fullEnd,
				p;

	fullStart = ((xmin + XID_SLOTS_PER_PAGE - 1) / XID_SLOTS_PER_PAGE)
		* XID_SLOTS_PER_PAGE;
	fullEnd = (xmax / XID_SLOTS_PER_PAGE) * XID_SLOTS_PER_PAGE;
	for (p = fullStart; p < fullEnd; p += XID_SLOTS_PER_PAGE)
	{
		uint32		page = XID_BUFFER_PAGE_INDEX(p);
		uint32		mask = 1U << (page % 32);

		pg_atomic_fetch_and_u32(&xidBufferDirty[page / 32], ~mask);
	}
}

OSnapshot	o_in_progress_snapshot = {COMMITSEQNO_INPROGRESS, InvalidXLogRecPtr, 0, 0};

/*
 * Snapshot that returns uncommitted transactions data (like
 * o_in_progress_snapshot), but also includes tuples marked as deleted that
 * are still present on the data page.  This is needed for accessing
 * SYS_TREES_O_INDICES chunks for trees that may be deleted in uncommitted
 * (sub-)transactions: on rollback those trees might become visible again.
 *
 * Used as the default snapshot for fetching table/index descriptors
 * (default_table_fetch_context), because processes like the checkpointer or
 * bgwriter may need to access trees being dropped.
 */
OSnapshot	o_non_deleted_snapshot = {COMMITSEQNO_NON_DELETED, InvalidXLogRecPtr, 0, 0};

static OBuffersDesc buffersDesc = {
	.singleFileSize = XID_FILE_SIZE,
	.filenameTemplate = {ORIOLEDB_DATA_DIR "/%02X%08X.xidmap"},
	.groupCtlTrancheName = "xidBuffersGroupCtlTranche",
	.bufferCtlTrancheName = "xidBuffersCtlTranche"
};

static TransactionId acquire_logical_xid_wrapper(bool *isValidHeapXid);
static void advance_global_xmin(OXid newXid);

Size
oxid_shmem_needs(void)
{
	Size		size;

	buffersDesc.buffersCount = xid_buffers_count;

	size = CACHELINEALIGN(sizeof(XidMeta));
	size = add_size(size, mul_size(xid_circular_buffer_size,
								   sizeof(OXidMapItem)));
	size = add_size(size, CACHELINEALIGN(mul_size(XID_BUFFER_DIRTY_WORDS,
												  sizeof(pg_atomic_uint32))));
	size = add_size(size, o_buffers_shmem_needs(&buffersDesc));
	size = add_size(size, mul_size(logical_xid_buffers_guc,
								   ORIOLEDB_BLCKSZ));

	return size;
}

#define NLOCKENTS() \
	mul_size(max_locks_per_xact, add_size(MaxBackends, max_prepared_xacts))

static HTAB *LockMethodLockHash;
static HTAB *LockMethodProcLockHash;

/*
 * Compute the hash code associated with a PROCLOCKTAG.
 *
 * Because we want to use just one set of partition locks for both the
 * LOCK and PROCLOCK hash tables, we have to make sure that PROCLOCKs
 * fall into the same partition number as their associated LOCKs.
 * dynahash.c expects the partition number to be the low-order bits of
 * the hash code, and therefore a PROCLOCKTAG's hash code must have the
 * same low-order bits as the associated LOCKTAG's hash code.  We achieve
 * this with this specialized hash function.
 */
static uint32
proclock_hash(const void *key, Size keysize)
{
	const PROCLOCKTAG *proclocktag = (const PROCLOCKTAG *) key;
	uint32		lockhash;
	Datum		procptr;

	Assert(keysize == sizeof(PROCLOCKTAG));

	/* Look into the associated LOCK object, and compute its hash code */
	lockhash = LockTagHashCode(&proclocktag->myLock->tag);

	/*
	 * To make the hash code also depend on the PGPROC, we xor the proc
	 * struct's address into the hash code, left-shifted so that the
	 * partition-number bits don't change.  Since this is only a hash, we
	 * don't care if we lose high-order bits of the address; use an
	 * intermediate variable to suppress cast-pointer-to-int warnings.
	 */
	procptr = PointerGetDatum(proclocktag->myProc);
	lockhash ^= ((uint32) procptr) << LOG2_NUM_LOCK_PARTITIONS;

	return lockhash;
}

/*
 * Get access to lock system hashes in the shared memory.
 */
static void
init_lock_hashes(void)
{
	HASHCTL		info;
	long		init_table_size,
				max_table_size;

	/*
	 * Compute init/max size to request for lock hashtables.  Note these
	 * calculations must agree with LockShmemSize!
	 */
	max_table_size = NLOCKENTS();
	init_table_size = max_table_size / 2;

	/*
	 * Allocate hash table for LOCK structs.  This stores per-locked-object
	 * information.
	 */
	info.keysize = sizeof(LOCKTAG);
	info.entrysize = sizeof(LOCK);
	info.num_partitions = NUM_LOCK_PARTITIONS;

	LockMethodLockHash = ShmemInitHash("LOCK hash",
									   init_table_size,
									   max_table_size,
									   &info,
									   HASH_ELEM | HASH_BLOBS | HASH_PARTITION);

	/* Assume an average of 2 holders per lock */
	max_table_size *= 2;
	init_table_size *= 2;

	/*
	 * Allocate hash table for PROCLOCK structs.  This stores
	 * per-lock-per-holder information.
	 */
	info.keysize = sizeof(PROCLOCKTAG);
	info.entrysize = sizeof(PROCLOCK);
	info.hash = proclock_hash;
	info.num_partitions = NUM_LOCK_PARTITIONS;

	LockMethodProcLockHash = ShmemInitHash("PROCLOCK hash",
										   init_table_size,
										   max_table_size,
										   &info,
										   HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
}

void
oxid_init_shmem(Pointer ptr, bool found)
{
	xid_meta = (XidMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(XidMeta));
	xidBuffer = (OXidMapItem *) ptr;
	ptr += xid_circular_buffer_size * sizeof(OXidMapItem);
	xidBufferDirty = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(XID_BUFFER_DIRTY_WORDS * sizeof(pg_atomic_uint32));
	o_buffers_shmem_init(&buffersDesc, ptr, found);
	ptr += o_buffers_shmem_needs(&buffersDesc);
	logicalXidsShmemMap = (pg_atomic_uint32 *) ptr;

	if (!found)
	{
		int64		i;

		/* xid_meta fields are initialized in checkpoint_shmem_init() */
		SpinLockInit(&xid_meta->xminMutex);
		for (i = 0; i < xid_circular_buffer_size; i++)
		{
			pg_atomic_init_u64(&xidBuffer[i].csn, COMMITSEQNO_FROZEN);
			pg_atomic_init_u64(&xidBuffer[i].commitPtr, FirstNormalUnloggedLSN);
		}
		for (i = 0; i < XID_BUFFER_DIRTY_WORDS; i++)
			pg_atomic_init_u32(&xidBufferDirty[i], 0);

		xid_meta->xidMapTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&xid_meta->xidMapWriteLock,
						 xid_meta->xidMapTrancheId);
		xid_meta->sysXidUndoLocationTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&xid_meta->sysXidUndoLocationLock,
						 xid_meta->sysXidUndoLocationTrancheId);
		xid_meta->sysXidUndoLocationChangeCount = 0;

		for (i = 0; i < logical_xid_buffers_guc * (BLCKSZ / sizeof(pg_atomic_uint32)); i++)
			pg_atomic_init_u32(&logicalXidsShmemMap[i], 0);

		/* Undo positions are initialized in checkpoint_shmem_init() */
	}
	LWLockRegisterTranche(xid_meta->xidMapTrancheId,
						  "OXidMapWriteTranche");
	LWLockRegisterTranche(xid_meta->sysXidUndoLocationTrancheId,
						  "SysTreesUndoLocationTranche");

	init_lock_hashes();
}

#define	MAX_N_TRIES (16)

static TransactionId
acquire_logical_xid(bool *isValidHeapXid)
{
	TransactionId result,
				sub;
	int			itemsCount = logical_xid_buffers_guc * (BLCKSZ / sizeof(pg_atomic_uint32));
	uint32		divider = itemsCount * 32;
	int			i = MYPROCNUMBER % itemsCount,
				mynum = 0;
	int			nloops = 0;

	Assert(i >= 0 && i < max_procs);

	/*
	 * Check whether any valid heap xid is present at the moment of allocation
	 * of xid Oriole
	 */
	if (isValidHeapXid)
	{
		*isValidHeapXid = TransactionIdIsValid(GetTopTransactionIdIfAny());
	}

	/* Allocate new Oriole logical xid */
	while (true)
	{
		uint32		value = pg_atomic_read_u32(&logicalXidsShmemMap[i]);
		uint32		bit,
					bitnum;

		bit = (~value) & (value + 1);

		if (bit == 0)
		{
			i++;
			if (i >= itemsCount)
			{
				i = 0;
				nloops++;
				if (nloops > MAX_N_TRIES)
					elog(ERROR, "not enough logical xids");
			}
			continue;
		}
		bitnum = pg_ceil_log2_32(bit);
		Assert(bit == (1 << bitnum));

		value = pg_atomic_fetch_or_u32(&logicalXidsShmemMap[i], bit);
		if ((value & bit) == 0)
		{
			mynum = i * 32 + bitnum;
			break;
		}
	}

	result = RecentXmin;
	TransactionIdRetreat(result);
	if (result % divider > mynum)
		sub = (result % divider - mynum);
	else
		sub = (result % divider + (divider - mynum));

	if (result >= FirstNormalTransactionId + sub)
	{
		result -= sub;
	}
	else
	{
		result = MaxTransactionId - MaxTransactionId % divider;
		result -= (divider - mynum);
	}

	Assert(result % divider == mynum);

	elog(DEBUG4, "logical xid acquired: %u", result);

	return result;
}

static void
release_logical_xid(LogicalXidCtx *ctx)
{
	uint32		itemsCount = 0,
				mynum = 0;
	uint32		value PG_USED_FOR_ASSERTS_ONLY;

	if (ctx)
	{
		itemsCount = logical_xid_buffers_guc * (BLCKSZ / sizeof(pg_atomic_uint32));
		mynum = ctx->xid % (itemsCount * 32);

		Assert(TransactionIdIsNormal(ctx->xid));

		value = pg_atomic_fetch_and_u32(&logicalXidsShmemMap[mynum / 32],
										~(1 << (mynum % 32)));

		Assert((value & (1 << (mynum % 32))) != 0);
	}
}

static TransactionId
acquire_logical_xid_wrapper(bool *isValidHeapXid)
{
	TransactionId nextLogicalXid;
	TransactionId heapXid;

	Assert(isValidHeapXid);

	nextLogicalXid = acquire_logical_xid(isValidHeapXid);

	if (*isValidHeapXid)
	{
		/* Support native heap mechanics: assign all sub-txns to a top one */

		heapXid = GetTopTransactionIdIfAny();
		if (TransactionIdIsValid(heapXid))
		{
			elog(DEBUG4, "SWITCH_LOGICAL_XID H2O heap xid %u -> oriole xid %u", heapXid, nextLogicalXid);
			add_switch_logical_xid_wal_record(heapXid, nextLogicalXid);
		}
	}

	return nextLogicalXid;
}

void
assign_subtransaction_logical_xid(SubTransactionId mySubid)
{
	TransactionId nextLogicalXid;
	bool		isValidHeapXid = false;
	PrevLogicalXidEntry *entry;
	MemoryContext mcxt;

	nextLogicalXid = acquire_logical_xid_wrapper(&isValidHeapXid);

	/*
	 * Store the previous logical xid context (even an invalid one) tagged
	 * with the subtransaction id, so that the end of this subtransaction can
	 * recognize its own entry and other subtransaction ends leave the stack
	 * alone.
	 */
	mcxt = MemoryContextSwitchTo(TopMemoryContext);
	elog(DEBUG4, "STORE logical xid %u useHeap %d subid %u",
		 logicalXidContext.xid, logicalXidContext.useHeap, mySubid);
	entry = (PrevLogicalXidEntry *) palloc(sizeof(PrevLogicalXidEntry));
	entry->ctx = logicalXidContext;
	entry->subid = mySubid;
	prevLogicalXids = lappend(prevLogicalXids, entry);
	MemoryContextSwitchTo(mcxt);

	logicalXidContext.xid = nextLogicalXid;
	logicalXidContext.useHeap = isValidHeapXid;
}

static void
setup_prev_logical_xid_ctx(SubTransactionId mySubid)
{
	PrevLogicalXidEntry *entry;

	/*
	 * Pop only the entry this subtransaction pushed itself.  If the
	 * subtransaction started before the transaction's first OrioleDB
	 * modification, nothing was pushed for it, and the current context
	 * (possibly assigned lazily inside this subtransaction) must stay valid
	 * up to the top-level commit record.
	 */
	if (!prev_logical_xid_pushed_by(mySubid))
		return;

	entry = (PrevLogicalXidEntry *) llast(prevLogicalXids);
	logicalXidContext = entry->ctx;
	elog(DEBUG4, "RESTORE logical xid %u useHeap %d subid %u",
		 logicalXidContext.xid, logicalXidContext.useHeap, mySubid);
	pfree(entry);
	prevLogicalXids = list_delete_last(prevLogicalXids);
}

void
oxid_subxact_callback(
					  SubXactEvent event, SubTransactionId mySubid,
					  SubTransactionId parentSubid, void *arg)
{
	TransactionId heapXid;

	switch (event)
	{
		case SUBXACT_EVENT_COMMIT_SUB:
			{
				if (have_retained_undo_location())
				{
					/*
					 * Release and restore only the logical xid this
					 * subtransaction assigned at its start.  If nothing was
					 * pushed for it (the subtransaction started before the
					 * first OrioleDB modification), the current logical xid
					 * belongs to the whole transaction and must survive up to
					 * the top-level commit record.
					 */
					if (TransactionIdIsValid(logicalXidContext.xid) &&
						prev_logical_xid_pushed_by(mySubid))
					{
						release_logical_xid(&logicalXidContext);

						heapXid = GetTopTransactionIdIfAny();
						if (TransactionIdIsValid(heapXid))
						{
							if (!logicalXidContext.useHeap)
							{
								elog(DEBUG4, "SWITCH_LOGICAL_XID O2H heap xid %u -> oriole xid %u", heapXid, logicalXidContext.xid);
								add_switch_logical_xid_wal_record(heapXid, logicalXidContext.xid);
							}

							if (!RecoveryInProgress())
							{
								elog(DEBUG4, "Add wal_joint_commit for oxid " UINT64_FORMAT " logical xid %u top xid %u",
									 get_current_oxid_if_any(),
									 logicalXidContext.xid,
									 GetTopTransactionIdIfAny());

								wal_joint_commit(get_current_oxid_if_any(),
												 logicalXidContext.xid,
												 GetTopTransactionIdIfAny(),
												 true);
								wal_after_commit();
							}
						}

						if (!RecoveryInProgress())
						{
							setup_prev_logical_xid_ctx(mySubid);
						}
					}
				}

				break;
			}

		case SUBXACT_EVENT_ABORT_SUB:
			{
				if (have_retained_undo_location())
				{
					if (TransactionIdIsValid(logicalXidContext.xid))
					{
						if (!RecoveryInProgress())
						{
							elog(DEBUG4, "Rollback for oxid " UINT64_FORMAT " logical xid %u top xid %u",
								 get_current_oxid_if_any(),
								 logicalXidContext.xid,
								 GetTopTransactionIdIfAny());

							setup_prev_logical_xid_ctx(mySubid);
						}
					}
				}

				break;
			}

		default:
			break;
	}
}

/*
 * Set the csn value for particular oxid.
 */
void
set_oxid_csn(OXid oxid, CommitSeqNo csn)
{
	CommitSeqNo oldCsn;
	OXid		writeInProgressXmin;

	oldCsn = pg_atomic_read_u64(&xidBuffer[oxid % xid_circular_buffer_size].csn);
	pg_read_barrier();
	writeInProgressXmin = pg_atomic_read_u64(&xid_meta->writeInProgressXmin);
	if (oxid >= writeInProgressXmin)
	{
		if (pg_atomic_compare_exchange_u64(&xidBuffer[oxid % xid_circular_buffer_size].csn,
										   &oldCsn, csn))
		{
			Assert(oldCsn != COMMITSEQNO_FROZEN);
			mark_xid_buffer_dirty(oxid);
			return;
		}

		/*
		 * We assume that nobody could change the csn value concurrently.
		 * Thus, it could be only wiped out from the circular buffer to
		 * o_buffers.
		 */
		Assert(oxid < pg_atomic_read_u64(&xid_meta->writeInProgressXmin));
	}

	/*
	 * Wait for the concurrent write operation if needed.
	 */
	if (oxid >= pg_atomic_read_u64(&xid_meta->writtenXmin))
	{
		LWLockAcquire(&xid_meta->xidMapWriteLock, LW_SHARED);
		LWLockRelease(&xid_meta->xidMapWriteLock);
	}

	Assert(oxid < pg_atomic_read_u64(&xid_meta->writtenXmin));
	o_buffers_write(&buffersDesc, (Pointer) &csn, OXID_BUFFERS_TAG,
					oxid * sizeof(OXidMapItem) + offsetof(OXidMapItem, csn),
					sizeof(CommitSeqNo), false, false);
}

/*
 * Set the csn value for particular oxid.
 */
static void
set_oxid_xlog_ptr_internal(OXid oxid, XLogRecPtr ptr)
{
	XLogRecPtr	oldPtr;
	OXid		writeInProgressXmin;

	oldPtr = pg_atomic_read_u64(&xidBuffer[oxid % xid_circular_buffer_size].commitPtr);
	pg_read_barrier();
	writeInProgressXmin = pg_atomic_read_u64(&xid_meta->writeInProgressXmin);
	if (oxid >= writeInProgressXmin)
	{
		if (pg_atomic_compare_exchange_u64(&xidBuffer[oxid % xid_circular_buffer_size].commitPtr,
										   &oldPtr, ptr))
		{
			Assert(oldPtr != FirstNormalUnloggedLSN);
			mark_xid_buffer_dirty(oxid);
			return;
		}

		/*
		 * We assume that nobody could change the csn value concurrently.
		 * Thus, it could be only wiped out from the circular buffer to
		 * o_buffers.
		 */
		Assert(oxid < pg_atomic_read_u64(&xid_meta->writeInProgressXmin));
	}

	/*
	 * Wait for the concurrent write operation if needed.
	 */
	if (oxid >= pg_atomic_read_u64(&xid_meta->writtenXmin))
	{
		LWLockAcquire(&xid_meta->xidMapWriteLock, LW_SHARED);
		LWLockRelease(&xid_meta->xidMapWriteLock);
	}

	Assert(oxid < pg_atomic_read_u64(&xid_meta->writtenXmin));
	o_buffers_write(&buffersDesc, (Pointer) &ptr, OXID_BUFFERS_TAG,
					oxid * sizeof(OXidMapItem) + offsetof(OXidMapItem, commitPtr),
					sizeof(XLogRecPtr), false, false);
}

void
set_oxid_xlog_ptr(OXid oxid, XLogRecPtr ptr)
{
	Assert(!XLOG_PTR_IS_SPECIAL(ptr));

	set_oxid_xlog_ptr_internal(oxid, ptr);
}


/*
 * Read csn of given xid from xidmap.
 * If getRawCsn is true outputs raw csn, otherwise clears COMMITSEQNO_RETAINED_FOR_REWIND flag.
 */
static void
map_oxid(OXid oxid, CommitSeqNo *outCsn, XLogRecPtr *outPtr, bool getRawCsn)
{
	OXidMapItem mapItem = {0};

	if (is_recovery_process() && outCsn)
	{
		bool		found;

		*outCsn = recovery_map_oxid_csn(oxid, &found);
		if (found)
		{
			outCsn = NULL;
			if (!outPtr)
				return;
		}
	}

	/* Optimisticly try to read csn and/or xlog ptr from circular buffer */
	if (outCsn)
		*outCsn = pg_atomic_read_u64(&xidBuffer[oxid % xid_circular_buffer_size].csn) & (getRawCsn ? UINT64_MAX : (~COMMITSEQNO_RETAINED_FOR_REWIND));
	if (outPtr)
		*outPtr = pg_atomic_read_u64(&xidBuffer[oxid % xid_circular_buffer_size].commitPtr);
	pg_read_barrier();

	/* Did we manage to read the correct csn? */
	if (oxid >= pg_atomic_read_u64(&xid_meta->writeInProgressXmin))
		return;

	/*
	 * Wait for the concurrent write operation if needed.
	 */
	if (oxid >= pg_atomic_read_u64(&xid_meta->writtenXmin))
	{
		LWLockAcquire(&xid_meta->xidMapWriteLock, LW_SHARED);
		LWLockRelease(&xid_meta->xidMapWriteLock);
	}

	Assert(oxid < pg_atomic_read_u64(&xid_meta->writtenXmin));
	o_buffers_read(&buffersDesc, (Pointer) &mapItem, OXID_BUFFERS_TAG,
				   oxid * sizeof(OXidMapItem),
				   sizeof(OXidMapItem), false);

	/* Recheck if globalXmin was advanced concurrently */
	if (oxid < pg_atomic_read_u64(&xid_meta->globalXmin))
	{
		if (outCsn)
			*outCsn = COMMITSEQNO_FROZEN;
		if (outPtr)
			*outPtr = FirstNormalUnloggedLSN;
		return;
	}

	if (outCsn)
		*outCsn = pg_atomic_read_u64(&mapItem.csn) & (getRawCsn ? UINT64_MAX : (~COMMITSEQNO_RETAINED_FOR_REWIND));
	if (outPtr)
		*outPtr = pg_atomic_read_u64(&mapItem.commitPtr);
}

void
clear_rewind_oxid(OXid oxid)
{
	XLogRecPtr	xlogPtr;
	CommitSeqNo csn;

	map_oxid(oxid, &csn, &xlogPtr, false);
/* 	elog(LOG, "csn unset from rewind %lu -> %lu", csn | COMMITSEQNO_RETAINED_FOR_REWIND, csn); */
	set_oxid_csn(oxid, csn);
}

inline bool
csn_is_retained_for_rewind(CommitSeqNo csn)
{
	return (bool) ((csn) & COMMITSEQNO_RETAINED_FOR_REWIND);
}

/*
 * Write some data from circular buffer to o_buffers
 */
static void
write_xidsmap(OXid targetXmax)
{
	OXid		oxid,
				xmin,
				xmax,
				pageOxid;
	int			bufferLength = ORIOLEDB_BLCKSZ / sizeof(OXidMapItem);
	OXidMapItem buffer[ORIOLEDB_BLCKSZ / sizeof(OXidMapItem)];

	/*
	 * Get the xidmap range to write.
	 */
	SpinLockAcquire(&xid_meta->xminMutex);
	xmin = pg_atomic_read_u64(&xid_meta->writtenXmin);
	xmax = Max(pg_atomic_read_u64(&xid_meta->writtenXmin) + xid_circular_buffer_size / 8,
			   targetXmax);
	xmax = Min(xmax, xmin + xid_circular_buffer_size);
	xmax = Min(xmax, pg_atomic_read_u64(&xid_meta->nextXid));
	for (oxid = xmin; oxid < xmax; oxid++)
	{
		if (pg_atomic_read_u64(&xidBuffer[oxid % xid_circular_buffer_size].csn) == COMMITSEQNO_FROZEN)
			break;
	}
	xmax = oxid;

	if (xmin == xmax)
	{
		SpinLockRelease(&xid_meta->xminMutex);
		return;
	}

	Assert(pg_atomic_read_u64(&xid_meta->writeInProgressXmin) < xmax);
	pg_atomic_write_u64(&xid_meta->writeInProgressXmin, xmax);
	SpinLockRelease(&xid_meta->xminMutex);

	Assert(xmax > xmin);

	for (pageOxid = xmin; pageOxid < xmax;)
	{
		OXid		pageBase = pageOxid - (pageOxid % XID_SLOTS_PER_PAGE);
		OXid		writeStart = pageOxid;
		OXid		writeEnd = Min(pageBase + XID_SLOTS_PER_PAGE, xmax);
		uint32		page = XID_BUFFER_PAGE_INDEX(pageOxid);
		bool		isFull = (writeStart == pageBase &&
							  writeEnd == pageBase + XID_SLOTS_PER_PAGE);
		bool		dirty;

		/*
		 * Drain this page's slots into the local buffer, resetting the ring
		 * slots to FROZEN so the ring space is freed.  This happens for every
		 * page regardless of the dirty bit -- writtenXmin advances past the
		 * whole range below, so the slots must be vacated either way.
		 */
		for (oxid = writeStart; oxid < writeEnd; oxid++)
		{
			Size		idx = oxid % xid_circular_buffer_size;

			pg_atomic_write_u64(&buffer[oxid % bufferLength].csn,
								pg_atomic_exchange_u64(&xidBuffer[idx].csn,
													   COMMITSEQNO_FROZEN));
			pg_atomic_write_u64(&buffer[oxid % bufferLength].commitPtr,
								pg_atomic_exchange_u64(&xidBuffer[idx].commitPtr,
													   FirstNormalUnloggedLSN));
		}

		/*
		 * A page whose dirty bit is set is written to o_buffers dirty as
		 * usual.  A page whose bit is already clear was pushed straight to
		 * disk by a checkpoint-time flush (flush_dirty_xidsmap_range), so its
		 * on-disk copy is current; we still write it into the o_buffers cache
		 * as *clean* rather than skipping it, so a stale partial copy an
		 * earlier eviction may have left resident is refreshed before
		 * writtenXmin advances past it.  Consume the bit for a fully covered
		 * page (test-and-clear); only peek for a partial boundary page,
		 * leaving its bit set since the tail slots beyond [xmin, xmax) may
		 * still be dirty.
		 */
		if (isFull)
			dirty = test_clear_xid_buffer_page_dirty(page);
		else
			dirty = xid_buffer_page_dirty(page);

		o_buffers_write(&buffersDesc,
						(Pointer) &buffer[writeStart % bufferLength],
						OXID_BUFFERS_TAG,
						writeStart * sizeof(OXidMapItem),
						(writeEnd - writeStart) * sizeof(OXidMapItem),
						false, !dirty);

		pageOxid = writeEnd;
	}

	SpinLockAcquire(&xid_meta->xminMutex);
	Assert(pg_atomic_read_u64(&xid_meta->writtenXmin) < xmax);
	pg_atomic_write_u64(&xid_meta->writtenXmin, xmax);
	SpinLockRelease(&xid_meta->xminMutex);

	advance_global_xmin(InvalidOXid);
}

/*
 * Walk pages overlapping [xmin, xmax) and persist any whose dirty bit is
 * set, leaving the ring buffer untouched (slots keep their data, writtenXmin
 * does not advance).  This is the checkpoint-time flush: it makes the
 * on-disk copy current without forcing future reads of the same oxids onto
 * the slow path.
 *
 * Pages whose bit was cleared by the time we look at them are already on
 * disk (or were never written since the last flush) and are skipped.  A
 * writer that sets the bit again after we cleared it leaves the bit set,
 * so the next flush will catch the update.
 *
 * The function works in short batches of XID_FLUSH_BATCH_PAGES under
 * xidMapWriteLock in EXCLUSIVE mode, releasing the lock between batches.
 * This keeps it serialised with the slot-pressure eviction path
 * (write_xidsmap), which mutates the same slots and writtenXmin and would
 * otherwise produce torn page snapshots; the brief release windows let
 * concurrent eviction and waiters (set_oxid_csn / set_oxid_xlog_ptr
 * SHARED-lock waiters) make progress instead of stalling for the whole
 * flush.
 */
#define XID_FLUSH_BATCH_PAGES 128
static void
flush_dirty_xidsmap_range(OXid xmin, OXid xmax)
{
	OXid		pageOxid,
				alignedXmin,
				alignedXmax;
	OXidMapItem buffer[XID_SLOTS_PER_PAGE];

	if (xmin >= xmax)
		return;

	alignedXmin = xmin - (xmin % XID_SLOTS_PER_PAGE);
	alignedXmax = ((xmax + XID_SLOTS_PER_PAGE - 1) / XID_SLOTS_PER_PAGE)
		* XID_SLOTS_PER_PAGE;
	if (alignedXmax - alignedXmin > xid_circular_buffer_size)
		alignedXmax = alignedXmin + xid_circular_buffer_size;

	pageOxid = alignedXmin;
	while (pageOxid < alignedXmax)
	{
		int			processed;

		LWLockAcquire(&xid_meta->xidMapWriteLock, LW_EXCLUSIVE);

		for (processed = 0;
			 processed < XID_FLUSH_BATCH_PAGES && pageOxid < alignedXmax;
			 processed++, pageOxid += XID_SLOTS_PER_PAGE)
		{
			uint32		page = XID_BUFFER_PAGE_INDEX(pageOxid);
			OXid		slot;

			if (!test_clear_xid_buffer_page_dirty(page))
				continue;

			/*
			 * Acquire-fence pair with the writer's release in
			 * mark_xid_buffer_dirty() lives inside the atomic fetch_and that
			 * test_clear_xid_buffer_page_dirty() just performed; the per-slot
			 * reads below see all stores preceding the writer's bit set.
			 *
			 * Assemble the page into a local OXidMapItem buffer (writers do
			 * not take the write lock, so per-slot CAS can race with our
			 * read; the local buffer gives o_buffers_write_page_direct() a
			 * stable page).  Bypass the o_buffers cache: reads of these oxids
			 * stay in the ring (writtenXmin does not advance), so the cache
			 * copy would only duplicate the bytes.
			 *
			 * The (csn, commitPtr) pair of a single slot is not atomic, so a
			 * flush may capture a half-updated slot; WAL replay rebuilds
			 * xidmap on recovery and any later write re-dirties the page.
			 */
			for (slot = 0; slot < XID_SLOTS_PER_PAGE; slot++)
			{
				OXid		slotOxid = pageOxid + slot;
				Size		idx = slotOxid % xid_circular_buffer_size;

				pg_atomic_write_u64(&buffer[slot].csn,
									pg_atomic_read_u64(&xidBuffer[idx].csn));
				pg_atomic_write_u64(&buffer[slot].commitPtr,
									pg_atomic_read_u64(&xidBuffer[idx].commitPtr));
			}

			o_buffers_write_page_direct(&buffersDesc,
										(char *) buffer,
										OXID_BUFFERS_TAG,
										pageOxid * sizeof(OXidMapItem));
		}

		LWLockRelease(&xid_meta->xidMapWriteLock);
	}
}

/*
 * Sync given range of xidmap with disk.
 */
void
fsync_xidmap_range(OXid xmin, OXid xmax, uint32 wait_event_info)
{
	flush_dirty_xidsmap_range(xmin, xmax);

	o_buffers_sync(&buffersDesc,
				   OXID_BUFFERS_TAG,
				   xmin * sizeof(OXidMapItem),
				   xmax * sizeof(OXidMapItem),
				   wait_event_info);
}

/*
 * Wait particular oxid to finish or oxid_notify() call.  Returns true if
 * oxid was finished.
 */
bool
wait_for_oxid(OXid oxid, bool errorOk)
{
	int			procnum;
	CommitSeqNo csn;
	XidVXidMapElement *vxidElem;
	VirtualTransactionId vxid;
	bool		result;

	map_oxid(oxid, &csn, NULL, false);

	if (!COMMITSEQNO_IS_SPECIAL(csn))
		return true;

	procnum = COMMITSEQNO_GET_PROCNUM(csn);
	vxidElem = &oProcData[procnum].vxids[COMMITSEQNO_GET_LEVEL(csn)];
	vxid = vxidElem->vxid;

	pg_read_barrier();

	if (vxidElem->oxid != oxid)
	{
		/*
		 * If transaction isn't already present in its process map, then it
		 * must be concurrently gone.
		 */
		return true;
	}

	if (vxid.BACKENDID < 0 || !VirtualTransactionIdIsValid(vxid))
	{
		Assert(errorOk);
		return true;
	}

	GET_CUR_PROCDATA()->waitingForOxid = true;
	result = VirtualXactLock(vxid, true);
	GET_CUR_PROCDATA()->waitingForOxid = false;

	return result;
}

/*
 * Notify wait_for_oxid() caller only if it is waiting for current process.
 *
 * No existing callers.
 */
void
oxid_notify(OXid oxid)
{
	PGPROC	   *proc;
	LOCKTAG		tag;
	CommitSeqNo csn;
	VirtualTransactionId vxid;
	int			procnum;

	map_oxid(oxid, &csn, NULL, false);

	if (!COMMITSEQNO_IS_SPECIAL(csn))
		return;

	procnum = COMMITSEQNO_GET_PROCNUM(csn);
	proc = GetPGProcByNumber(procnum);
	vxid.localTransactionId = MyProc->LXID;
	vxid.BACKENDID = MyBackendId;

	/* ensure that it is waiting for us */
	SET_LOCKTAG_VIRTUALTRANSACTION(tag, vxid);

	if (
		proc->waitStatus == PROC_WAIT_STATUS_WAITING &&
		proc->waitLock->tag.locktag_field1 == tag.locktag_field1 &&
		proc->waitLock->tag.locktag_field2 == tag.locktag_field2 &&
		proc->waitLock->tag.locktag_field3 == tag.locktag_field3 &&
		proc->waitLock->tag.locktag_field4 == tag.locktag_field4 &&
		proc->waitLock->tag.locktag_lockmethodid == tag.locktag_lockmethodid &&
		proc->waitLock->tag.locktag_type == tag.locktag_type &&
		oProcData[PROCNUMBER(proc)].waitingForOxid)
	{
		/*
		 * It's a hack. We can not just release lock because VirtualXactLock()
		 * used in critical postgres code. We must release it only for the
		 * specific backend.
		 */
		uint32		hashcode = LockTagHashCode(&(proc->waitLock->tag));
		LWLock	   *partitionLock = LockHashPartitionLock(hashcode);

		/*
		 * No need to acquire the lock before check because the backend can be
		 * unlocked only by this code.
		 */
		LWLockAcquire(partitionLock, LW_EXCLUSIVE);
		RemoveFromWaitQueue(proc, hashcode);
		proc->waitStatus = PROC_WAIT_STATUS_OK;
		SetLatch(&proc->procLatch);

		LWLockRelease(partitionLock);
	}
	VirtualXactLock(vxid, true);
}

/*
 * Compute the hash code associated with a PROCLOCKTAG, given the hashcode
 * for its underlying LOCK.
 *
 * We use this just to avoid redundant calls of LockTagHashCode().
 */
static inline uint32
ProcLockHashCode(const PROCLOCKTAG *proclocktag, uint32 hashcode)
{
	uint32		lockhash = hashcode;
	Datum		procptr;

	/*
	 * This must match proclock_hash()!
	 */
	procptr = PointerGetDatum(proclocktag->myProc);
	lockhash ^= ((uint32) procptr) << LOG2_NUM_LOCK_PARTITIONS;

	return lockhash;
}

/*
 * Notify oxid_notify_all() callers who are waiting for current process.
 */
void
oxid_notify_all(void)
{
	PGPROC	   *proc;
	LOCKTAG		tag;
	VirtualTransactionId vxid;
	LWLock	   *partitionLock;
	LOCK	   *lock;
	PROCLOCK   *proclock;
	PROCLOCKTAG proclocktag;
	uint32		hashcode;
	uint32		proclock_hashcode;
	List	   *procs = NIL;
	ListCell   *lc;
	dclist_head *waitQueue;
	dlist_iter	iter;

	vxid.localTransactionId = MyProc->LXID;
	vxid.BACKENDID = MyBackendId;

	/* ensure that it is waiting for us */
	SET_LOCKTAG_VIRTUALTRANSACTION(tag, vxid);

	hashcode = LockTagHashCode(&tag);
	partitionLock = LockHashPartitionLock(hashcode);

	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	/* Find the lock object */
	lock = (LOCK *) hash_search_with_hash_value(LockMethodLockHash,
												(void *) &tag,
												hashcode,
												HASH_FIND,
												NULL);
	if (!lock)
	{
		/* Must be granted with fast path */
		LWLockRelease(partitionLock);
		return;
	}

	/*
	 * Re-find the proclock object (ditto).
	 */
	proclocktag.myLock = lock;
	proclocktag.myProc = MyProc;

	proclock_hashcode = ProcLockHashCode(&proclocktag, hashcode);

	proclock = (PROCLOCK *) hash_search_with_hash_value(LockMethodProcLockHash,
														(void *) &proclocktag,
														proclock_hashcode,
														HASH_FIND,
														NULL);
	if (!proclock)
		elog(PANIC, "failed to re-find shared proclock object");


	waitQueue = &lock->waitProcs;

	dclist_foreach(iter, waitQueue)
	{
		proc = dlist_container(PGPROC, links, iter.cur);
		if (
			proc->waitStatus == PROC_WAIT_STATUS_WAITING &&
			proc->waitLock->tag.locktag_field1 == tag.locktag_field1 &&
			proc->waitLock->tag.locktag_field2 == tag.locktag_field2 &&
			proc->waitLock->tag.locktag_field3 == tag.locktag_field3 &&
			proc->waitLock->tag.locktag_field4 == tag.locktag_field4 &&
			proc->waitLock->tag.locktag_lockmethodid == tag.locktag_lockmethodid &&
			proc->waitLock->tag.locktag_type == tag.locktag_type &&
			oProcData[PROCNUMBER(proc)].waitingForOxid)
		{
			procs = lappend(procs, proc);
		}
	}

	foreach(lc, procs)
	{
		proc = lfirst(lc);
		RemoveFromWaitQueue(proc, hashcode);
		proc->waitStatus = PROC_WAIT_STATUS_OK;
		SetLatch(&proc->procLatch);
	}

	LWLockRelease(partitionLock);
	VirtualXactLock(vxid, true);

	list_free(procs);
}

/*
 * Loop over oProcData[] and update xid_meta accordingly.
 */
static void
advance_global_xmin(OXid newXid)
{
	OXid		globalXmin,
				prevGlobalXmin;
	int			i;
	OXid		writtenXmin,
				writeInProgressXmin;
	OXid		oldCleanedXmin,
				oldCheckpointXmin,
				oldCheckpointXmax,
				newCheckpointXmin,
				newCheckpointXmax;
	bool		doCleanup = false;

	SpinLockAcquire(&xid_meta->xminMutex);

	if (OXidIsValid(newXid))
		pg_atomic_write_u64(&xid_meta->lastXidWhenUpdatedGlobalXmin, newXid);

	globalXmin = pg_atomic_read_u64(&xid_meta->runXmin);

	for (i = 0; i < max_procs; i++)
	{
		OXid		xmin;

		xmin = pg_atomic_read_u64(&oProcData[i].xmin);

		if (OXidIsValid(xmin) && xmin < globalXmin)
			globalXmin = xmin;
	}

	if (enable_rewind)
	{
		OXid		rewindRunXmin = get_rewind_run_xmin();

		if (OXidIsValid(rewindRunXmin) && rewindRunXmin < globalXmin)
			globalXmin = rewindRunXmin;
	}

	prevGlobalXmin = pg_atomic_read_u64(&xid_meta->globalXmin);

	/*
	 * Sometimes, xmin of individual process can go backwards.  However, xids
	 * in the shift should be never accessed.  We never advance globalXmin
	 * backwards.
	 */
	if (globalXmin > prevGlobalXmin)
		pg_atomic_write_u64(&xid_meta->globalXmin, globalXmin);

	/*
	 * Check if we can update writtenXmin without actual writing.
	 */
	writtenXmin = pg_atomic_read_u64(&xid_meta->writtenXmin);
	writeInProgressXmin = pg_atomic_read_u64(&xid_meta->writeInProgressXmin);

	if (writtenXmin == writeInProgressXmin && writtenXmin < globalXmin &&
		LWLockConditionalAcquire(&xid_meta->xidMapWriteLock, LW_EXCLUSIVE))
	{
		OXid		oxid;

		Assert(globalXmin >= pg_atomic_read_u64(&xid_meta->writeInProgressXmin));
		Assert(globalXmin >= pg_atomic_read_u64(&xid_meta->writtenXmin));

		pg_atomic_write_u64(&xid_meta->writeInProgressXmin, globalXmin);

		pg_write_barrier();

		for (oxid = writtenXmin; oxid < globalXmin; oxid++)
		{
			pg_atomic_write_u64(&xidBuffer[oxid % xid_circular_buffer_size].csn,
								COMMITSEQNO_FROZEN);
			pg_atomic_write_u64(&xidBuffer[oxid % xid_circular_buffer_size].commitPtr,
								FirstNormalUnloggedLSN);
		}

		clear_xid_dirty_range(writtenXmin, globalXmin);

		pg_write_barrier();

		pg_atomic_write_u64(&xid_meta->writtenXmin, globalXmin);
		LWLockRelease(&xid_meta->xidMapWriteLock);
	}

	oldCleanedXmin = pg_atomic_read_u64(&xid_meta->cleanedXmin);
	oldCheckpointXmin = pg_atomic_read_u64(&xid_meta->cleanedCheckpointXmin);
	oldCheckpointXmax = pg_atomic_read_u64(&xid_meta->cleanedCheckpointXmax);
	newCheckpointXmin = pg_atomic_read_u64(&xid_meta->checkpointRetainXmin);
	newCheckpointXmax = pg_atomic_read_u64(&xid_meta->checkpointRetainXmax);

	if (oldCleanedXmin != globalXmin ||
		oldCheckpointXmin != newCheckpointXmin ||
		oldCheckpointXmax != newCheckpointXmax)
	{
		pg_atomic_write_u64(&xid_meta->cleanedCheckpointXmin, newCheckpointXmin);
		pg_atomic_write_u64(&xid_meta->cleanedCheckpointXmax, newCheckpointXmax);
		pg_atomic_write_u64(&xid_meta->cleanedXmin, globalXmin);
		doCleanup = true;
	}
	SpinLockRelease(&xid_meta->xminMutex);

	if (doCleanup)
	{
		unlink_unretained_o_buffers(&buffersDesc, OXID_BUFFERS_TAG,
									ORIOLEDB_BLCKSZ / sizeof(OXidMapItem),
									oldCheckpointXmin, oldCheckpointXmax,
									newCheckpointXmin, newCheckpointXmax,
									globalXmin);
		unlink_unretained_o_buffers(&buffersDesc, OXID_BUFFERS_TAG,
									ORIOLEDB_BLCKSZ / sizeof(OXidMapItem),
									oldCleanedXmin, globalXmin,
									newCheckpointXmin, newCheckpointXmax,
									globalXmin);
	}
}

/*
 * Extends xidmap to given value if needed.  New values are filled with
 * COMMITSEQNO_INPROGRESS value.  Used during recovery when procnum from xidmap
 * isn't used (transaction doesn't belong to particular pid).
 *
 * This function is intended to be only called by main recovery process.  So,
 * to locks needed to prevent concurrent execution of the this function by
 * another process.
 */
void
advance_oxids(OXid new_xid)
{
	OXid		xid,
				xmax;

	if (new_xid < pg_atomic_read_u64(&xid_meta->nextXid))
		return;

	/*
	 * We might need to extend xidmap more than xid_circular_buffer_size.  So,
	 * we need a loop here.
	 */
	while (new_xid >= pg_atomic_read_u64(&xid_meta->nextXid))
	{
		advance_global_xmin(new_xid);

		/* Write some xids out of circular buffer if needed. */
		if (new_xid >= pg_atomic_read_u64(&xid_meta->writtenXmin) + xid_circular_buffer_size)
		{
			if (LWLockAcquireOrWait(&xid_meta->xidMapWriteLock, LW_EXCLUSIVE))
			{
				write_xidsmap(new_xid - xid_circular_buffer_size);
				LWLockRelease(&xid_meta->xidMapWriteLock);
			}
		}

		/* Fill xidmap in circular buffer. */
		xid = pg_atomic_read_u64(&xid_meta->nextXid);
		xmax = Min(new_xid + 1, pg_atomic_read_u64(&xid_meta->writtenXmin) + xid_circular_buffer_size);
		for (; xid < xmax; xid++)
		{
			pg_atomic_write_u64(&xidBuffer[xid % xid_circular_buffer_size].csn,
								COMMITSEQNO_INPROGRESS);
			pg_atomic_write_u64(&xidBuffer[xid % xid_circular_buffer_size].commitPtr,
								InvalidXLogRecPtr);
			mark_xid_buffer_dirty(xid);
		}
		pg_atomic_write_u64(&xid_meta->nextXid, xmax);
	}
}


/*
 * Get current OrioleDB xid (oxid).  Assign new oxid it's not done yet.
 */
OXid
get_current_oxid(void)
{
	if (OXidIsValid(recovery_oxid))
		return recovery_oxid;

	if (!OXidIsValid(curOxid))
	{
		OXid		newOxid = pg_atomic_fetch_add_u64(&xid_meta->nextXid, 1);
		XidVXidMapElement *vxidElem;
		int			nestingLevel;

		/*
		 * Advance xmin every 10th part of circular buffer.  That should
		 * prevent unnecessary circular buffer overrun.
		 */
		if (newOxid > pg_atomic_read_u64(&xid_meta->lastXidWhenUpdatedGlobalXmin) + xid_circular_buffer_size / 10)
			advance_global_xmin(newOxid);

		/*
		 * Write some xids out of circular buffer if needed.  We always keep
		 * one COMMITSEQNO_FROZEN in circular buffers as a delimiter between
		 * the future and the past.  This helps protect runXmin from growing
		 * bigger than nextXid.
		 */
		while (newOxid >= pg_atomic_read_u64(&xid_meta->writtenXmin) + xid_circular_buffer_size - 1)
		{
			advance_global_xmin(newOxid);
			if (newOxid >= pg_atomic_read_u64(&xid_meta->writtenXmin) + xid_circular_buffer_size - 1)
			{
				if (LWLockAcquireOrWait(&xid_meta->xidMapWriteLock, LW_EXCLUSIVE))
				{
					write_xidsmap(newOxid - xid_circular_buffer_size);
					LWLockRelease(&xid_meta->xidMapWriteLock);
				}
			}
		}

		/*
		 * Make new xidmap item and write it to the circular buffer.
		 */
		nestingLevel = GET_CUR_PROCDATA()->autonomousNestingLevel;
		Assert(nestingLevel >= 0 && nestingLevel < PROC_XID_ARRAY_SIZE);
		vxidElem = &GET_CUR_PROCDATA()->vxids[nestingLevel];

		vxidElem->oxid = newOxid;
		vxidElem->vxid.BACKENDID = MyProc->PROCBACKENDID;
		vxidElem->vxid.localTransactionId = MyProc->LXID;

		Assert(pg_atomic_read_u64(&xidBuffer[newOxid % xid_circular_buffer_size].csn) == COMMITSEQNO_FROZEN);
		pg_atomic_write_u64(&xidBuffer[newOxid % xid_circular_buffer_size].csn,
							COMMITSEQNO_MAKE_SPECIAL(MYPROCNUMBER,
													 nestingLevel,
													 COMMITSEQNO_STATUS_IN_PROGRESS));
		pg_atomic_write_u64(&xidBuffer[newOxid % xid_circular_buffer_size].commitPtr,
							XLOG_PTR_MAKE_SPECIAL(MYPROCNUMBER,
												  nestingLevel,
												  COMMITSEQNO_STATUS_IN_PROGRESS));
		mark_xid_buffer_dirty(newOxid);
		curOxid = newOxid;

		/* Check if an autonomous transaction is in progress */
		if (nestingLevel > 0)
		{
			/*
			 * Autonomous transactions should be ignored during logical
			 * decoding, so invalidate logical xid info
			 */
			reset_logical_xid_ctx();
		}
		else
		{
			logicalXidContext.xid = acquire_logical_xid_wrapper(&logicalXidContext.useHeap);
		}
	}

	return curOxid;
}

void
set_current_oxid(OXid oxid)
{
	curOxid = oxid;

}

void
set_current_logical_xid(LogicalXidCtx *in)
{
	if (in)
	{
		Assert(!TransactionIdIsValid(logicalXidContext.xid));
		logicalXidContext.xid = in->xid;
		logicalXidContext.useHeap = in->useHeap;
	}
}

void
parallel_worker_set_oxid(void)
{
	XidVXidMapElement *vxidElem;
	ODBProcData *leaderProcData;

	Assert(MyProc->lockGroupLeader);

	leaderProcData = &oProcData[PROCNUMBER(MyProc->lockGroupLeader)];
	vxidElem = &leaderProcData->vxids[leaderProcData->autonomousNestingLevel];

	curOxid = vxidElem->oxid;
}

void
reset_current_oxid(void)
{
	curOxid = InvalidOXid;
	reset_logical_xid_ctx();
}

OXid
get_current_oxid_if_any(void)
{
	if (OXidIsValid(recovery_oxid))
		return recovery_oxid;

	return curOxid;
}

TransactionId
get_current_logical_xid(void)
{
	return logicalXidContext.xid;
}

void
get_current_logical_xid_ctx(LogicalXidCtx *output)
{
	if (output)
	{
		output->xid = logicalXidContext.xid;
		output->useHeap = logicalXidContext.useHeap;
	}
}

void
current_oxid_precommit(void)
{
	if (!OXidIsValid(curOxid))
		return;

	set_oxid_csn(curOxid, COMMITSEQNO_MAKE_SPECIAL(MYPROCNUMBER,
												   GET_CUR_PROCDATA()->autonomousNestingLevel,
												   COMMITSEQNO_STATUS_CSN_COMMITTING));
	csn_committing_set = true;

	pg_write_barrier();
}

void
current_oxid_xlog_precommit(void)
{
	if (!OXidIsValid(curOxid))
		return;

	/*
	 * Use XLOG_PTR_MAKE_SPECIAL (bit 0 set) so XLOG_PTR_IS_SPECIAL()
	 * recognises the marker.  set_oxid_xlog_ptr_internal() bypasses the
	 * wrapper's !XLOG_PTR_IS_SPECIAL() assertion, which is meant for real LSN
	 * writes only.
	 */
	set_oxid_xlog_ptr_internal(curOxid,
							   XLOG_PTR_MAKE_SPECIAL(MYPROCNUMBER,
													 GET_CUR_PROCDATA()->autonomousNestingLevel,
													 XLOG_PTR_COMMITTING));
	xlog_ptr_committing_set = true;

	pg_write_barrier();
}

static void
advance_run_xmin(OXid oxid)
{
	OXid		run_xmin;
	CommitSeqNo csn;

	pg_read_barrier();

	run_xmin = pg_atomic_read_u64(&xid_meta->runXmin);
	if (run_xmin == oxid)
	{
		while (pg_atomic_compare_exchange_u64(&xid_meta->runXmin,
											  &run_xmin, run_xmin + 1))
		{
			run_xmin++;
			map_oxid(run_xmin, &csn, NULL, false);
			if (COMMITSEQNO_IS_SPECIAL(csn) || COMMITSEQNO_IS_FROZEN(csn))
				break;
		}
	}
}

static void
release_assigned_logical_xids(void)
{
	if (TransactionIdIsValid(logicalXidContext.xid))
	{
		release_logical_xid(&logicalXidContext);
		reset_logical_xid_ctx();
	}

	if (GET_CUR_PROCDATA()->autonomousNestingLevel == 0)
	{
		ListCell   *lc = NULL;
		PrevLogicalXidEntry *entry = NULL;

		foreach(lc, prevLogicalXids)
		{
			entry = lfirst(lc);
			if (entry)
			{
				/* Entries saved before the first assignment hold no xid */
				if (TransactionIdIsValid(entry->ctx.xid))
					release_logical_xid(&entry->ctx);
				pfree(entry);
			}
		}
		list_free(prevLogicalXids);
		prevLogicalXids = NIL;
	}
}

void
current_oxid_commit(CommitSeqNo csn)
{
	ODBProcData *my_proc_info = &oProcData[MYPROCNUMBER];

	if (!OXidIsValid(curOxid))
		return;

	set_oxid_csn(curOxid,
				 csn | (enable_rewind ? COMMITSEQNO_RETAINED_FOR_REWIND : 0));
	pg_write_barrier();
	csn_committing_set = false;
	xlog_ptr_committing_set = false;
	my_proc_info->vxids[GET_CUR_PROCDATA()->autonomousNestingLevel].oxid = InvalidOXid;

	advance_run_xmin(curOxid);
	curOxid = InvalidOXid;
	release_assigned_logical_xids();
}

void
current_oxid_abort(void)
{
	ODBProcData *my_proc_info = &oProcData[MYPROCNUMBER];

	if (!OXidIsValid(curOxid))
		return;

	set_oxid_csn(curOxid, COMMITSEQNO_ABORTED);
	pg_write_barrier();
	csn_committing_set = false;
	xlog_ptr_committing_set = false;

	if (STOPEVENTS_ENABLED())
	{
		/*
		 * Deterministic-repro hook for the checkpoint abort-snapshot race We
		 * are past wal_rollback (the WAL_REC_ROLLBACK is already written and,
		 * under synchronous_commit, flushed) but have NOT yet cleared this
		 * oxid's vxids slot.  Pausing here lets a concurrent checkpoint's
		 * finish_write_xids() snapshot this oxid as in-flight (it gates only
		 * on OXidIsValid(vxids[].oxid)) while its resolving rollback sits
		 * below the checkpoint's replayStartPtr -- the exact inconsistency
		 * that makes crash recovery resurrect the oxid and emit a spurious
		 * deferred rollback.
		 */
		Jsonb	   *params;
		JsonbParseState *state = NULL;

		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		jsonb_push_int8_key(&state, "oxid", (int64) curOxid);
		params = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));

		STOPEVENT(STOPEVENT_BEFORE_ABORT_VXIDS_CLEAR, params);
	}

	my_proc_info->vxids[GET_CUR_PROCDATA()->autonomousNestingLevel].oxid = InvalidOXid;

	advance_run_xmin(curOxid);
	curOxid = InvalidOXid;
	release_assigned_logical_xids();
}

/*
 * Revert the COMMITTING bit on our oxid back to IN_PROGRESS if
 * current_oxid_precommit() (and/or current_oxid_xlog_precommit()) ran
 * but the commit aborted before current_oxid_commit() got to write the
 * actual CSN.  Other backends busy-spin in oxid_get_csn() /
 * oxid_match_snapshot() / oxid_get_xlog_ptr() while the bit is set, so
 * the abort path must drop it before doing any work that could acquire
 * locks the spinners are holding (notably apply_undo_stack()'s page
 * locks).  current_oxid_abort() will later overwrite the CSN with
 * COMMITSEQNO_ABORTED at the proper moment in the abort sequence; this
 * is intentionally a smaller, idempotent step.
 *
 * Whether each precommit ran is tracked through backend-local flags
 * (csn_committing_set, xlog_ptr_committing_set) — peeking at xidBuffer
 * is unsafe because its slots are shared by all oxids modulo the
 * buffer size and may already hold someone else's data after run_xmin
 * advances.
 */
void
current_oxid_clear_committing(void)
{
	int			nestingLevel;

	if (!OXidIsValid(curOxid))
		return;

	if (!csn_committing_set && !xlog_ptr_committing_set)
		return;

	nestingLevel = GET_CUR_PROCDATA()->autonomousNestingLevel;

	if (csn_committing_set)
	{
		set_oxid_csn(curOxid,
					 COMMITSEQNO_MAKE_SPECIAL(MYPROCNUMBER,
											  nestingLevel,
											  COMMITSEQNO_STATUS_IN_PROGRESS));
		csn_committing_set = false;
	}

	if (xlog_ptr_committing_set)
	{
		set_oxid_xlog_ptr_internal(curOxid,
								   XLOG_PTR_MAKE_SPECIAL(MYPROCNUMBER,
														 nestingLevel,
														 XLOG_PTR_IN_PROGRESS));
		xlog_ptr_committing_set = false;
	}

	pg_write_barrier();
}

/*
 * Gets csn for given oxid.  Wrapper over map_oxid_csn(), which loops
 * till "committing bit" is not set.
 * If getRawCsn is true outputs raw csn, otherwise clears COMMITSEQNO_RETAINED_FOR_REWIND flag.
 */
CommitSeqNo
oxid_get_csn(OXid oxid, bool getRawCsn)
{
	CommitSeqNo csn;
	SpinDelayStatus status;

	if (oxid == BootstrapTransactionId)
		return COMMITSEQNO_FROZEN;

	init_local_spin_delay(&status);

	while (true)
	{
		if (oxid < pg_atomic_read_u64(&xid_meta->globalXmin))
			return COMMITSEQNO_FROZEN;

		map_oxid(oxid, &csn, NULL, getRawCsn);
		if (COMMITSEQNO_IS_SPECIAL(csn) &&
			COMMITSEQNO_GET_STATUS(csn) & COMMITSEQNO_STATUS_CSN_COMMITTING)
			perform_spin_delay(&status);
		else
			break;
	}

	finish_spin_delay(&status);

	if (COMMITSEQNO_IS_SPECIAL(csn))
		return COMMITSEQNO_INPROGRESS;

	return csn;
}

/*
 * Gets commit ptr for given oxid.  Wrapper over map_oxid_xlog_ptr(), which
 * loops till "committing bit" is not set.
 */
XLogRecPtr
oxid_get_xlog_ptr(OXid oxid)
{
	XLogRecPtr	ptr;
	SpinDelayStatus status;

	if (oxid == BootstrapTransactionId)
		return COMMITSEQNO_FROZEN;

	init_local_spin_delay(&status);

	while (true)
	{
		if (oxid < pg_atomic_read_u64(&xid_meta->globalXmin))
			return COMMITSEQNO_FROZEN;

		map_oxid(oxid, NULL, &ptr, false);
		if (XLOG_PTR_IS_SPECIAL(ptr) &&
			XLOG_PTR_GET_STATUS(ptr) & XLOG_PTR_COMMITTING)
			perform_spin_delay(&status);
		else
			break;
	}

	finish_spin_delay(&status);

	if (XLOG_PTR_IS_SPECIAL(ptr))
		return InvalidXLogRecPtr;

	return ptr;
}

/*
 * Gets csn for given oxid.  Wrapper over map_oxid_csn(), which loops
 * till "committing bit" is not set.
 */
void
oxid_match_snapshot(OXid oxid, OSnapshot *snapshot,
					CommitSeqNo *outCsn, XLogRecPtr *outPtr)
{
	SpinDelayStatus status;

	if (oxid == BootstrapTransactionId || oxid < snapshot->xmin)
	{
		if (outCsn)
			*outCsn = COMMITSEQNO_FROZEN;
		if (outPtr)
			*outPtr = FirstNormalUnloggedLSN;
		return;
	}

	init_local_spin_delay(&status);

	while (true)
	{
		if (oxid < pg_atomic_read_u64(&xid_meta->globalXmin))
		{
			if (outCsn)
				*outCsn = COMMITSEQNO_FROZEN;
			if (outPtr)
				*outPtr = FirstNormalUnloggedLSN;
			return;
		}

		map_oxid(oxid, outCsn, outPtr, false);

		if (outCsn &&
			(!COMMITSEQNO_IS_SPECIAL(*outCsn) ||
			 !(COMMITSEQNO_GET_STATUS(*outCsn) & COMMITSEQNO_STATUS_CSN_COMMITTING)))
		{
			if (COMMITSEQNO_IS_SPECIAL(*outCsn))
				*outCsn = COMMITSEQNO_INPROGRESS;
			outCsn = NULL;
		}

		if (outPtr &&
			(!XLOG_PTR_IS_SPECIAL(*outPtr) ||
			 !(XLOG_PTR_GET_STATUS(*outPtr) & XLOG_PTR_COMMITTING)))
		{
			if (XLOG_PTR_IS_SPECIAL(*outPtr))
				*outPtr = InvalidXLogRecPtr;
			outPtr = NULL;
		}

		if (!outCsn && !outPtr)
			break;

		perform_spin_delay(&status);
	}

	finish_spin_delay(&status);
}

void
fill_current_oxid_osnapshot_no_check(OXid *oxid, OSnapshot *snapshot)
{
	if (ActiveSnapshotSet())
	{
		Snapshot	activeSnapshot = GetActiveSnapshot();

		snapshot->csn = activeSnapshot->csnSnapshotData.snapshotcsn;
		snapshot->xlogptr = activeSnapshot->csnSnapshotData.xlogptr;
		snapshot->xmin = activeSnapshot->csnSnapshotData.xmin;
		snapshot->cid = activeSnapshot->curcid;
	}
	else
	{
		snapshot->csn = COMMITSEQNO_INPROGRESS;
		snapshot->xlogptr = InvalidXLogRecPtr;
		snapshot->xmin = InvalidXLogRecPtr;
		snapshot->cid = 0;
	}
	*oxid = get_current_oxid();
}

void
fill_current_oxid_osnapshot(OXid *oxid, OSnapshot *snapshot)
{
	o_check_isolation_level();
	fill_current_oxid_osnapshot_no_check(oxid, snapshot);
}

int
oxid_get_procnum(OXid oxid)
{
	CommitSeqNo csn;

	map_oxid(oxid, &csn, NULL, false);

	if (COMMITSEQNO_IS_SPECIAL(csn))
		return COMMITSEQNO_GET_PROCNUM(csn);

	return -1;
}

/*
 * Check if xid should be considered as finished for given process.  During
 * recovery we also consult with local map of xids, because workers can go
 * ahead of what is markerd as finished for everybody.
 */
bool
xid_is_finished(OXid xid)
{
	OXid		xmin;
	CommitSeqNo csn;

	if (xid == BootstrapTransactionId)
		return true;

	if (is_recovery_process())
	{
		bool		found;

		csn = recovery_map_oxid_csn(xid, &found);
		if (found)
			return COMMITSEQNO_IS_COMMITTED(csn);
	}

	xmin = pg_atomic_read_u64(&xid_meta->runXmin);

	if (xid < xmin)
		return true;

	csn = oxid_get_csn(xid, false);

	return COMMITSEQNO_IS_COMMITTED(csn);
}

Datum
orioledb_get_xid_meta(PG_FUNCTION_ARGS)
{
#define XID_META_NATTS 9
	Datum		values[XID_META_NATTS];
	bool		nulls[XID_META_NATTS];
	TupleDesc	tupdesc;
	HeapTuple	htup;

	orioledb_check_shmem();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	MemSet(nulls, 0, sizeof(nulls));

	values[0] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->nextXid));
	values[1] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->runXmin));
	values[2] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->globalXmin));
	values[3] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->lastXidWhenUpdatedGlobalXmin));
	values[4] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->writeInProgressXmin));
	values[5] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->writtenXmin));
	values[6] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->checkpointRetainXmin));
	values[7] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->checkpointRetainXmax));
	values[8] = Int64GetDatum(pg_atomic_read_u64(&xid_meta->cleanedXmin));

	htup = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/*
 * Check if xid is finished for every process.  It's nothing special during
 * normal processing, but for recovery means xid is confirmed by main
 * recovery process as processed by all recovery workers.
 */
bool
xid_is_finished_for_everybody(OXid xid)
{
	OXid		xmin;
	CommitSeqNo csn;

	if (xid == BootstrapTransactionId)
		return true;

	if (!enable_rewind)
	{
		xmin = pg_atomic_read_u64(&xid_meta->runXmin);

		if (xid < xmin)
			return true;

		csn = oxid_get_csn(xid, false);
	}
	else
	{
		csn = oxid_get_csn(xid, true);

		if (csn_is_retained_for_rewind(csn))
			return false;
	}

	return COMMITSEQNO_IS_COMMITTED(csn);
}
