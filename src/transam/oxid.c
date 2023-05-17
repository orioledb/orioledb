/*-------------------------------------------------------------------------
 *
 * oxid.c
 *		Management of OrioleDB transaction identifiers.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/transam/oxid.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "recovery/recovery.h"
#include "transam/oxid.h"
#include "utils/o_buffers.h"

#include "access/transam.h"
#include "access/twophase.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/sinvaladt.h"
#include "storage/procsignal.h"
#include "storage/proc.h"
#include "utils/snapmgr.h"

#define XID_FILE_SIZE (0x1000000)

#define	COMMITSEQNO_SPECIAL_BIT (UINT64CONST(1) << 63)
#define COMMITSEQNO_SPECIAL_COMMITTING_BIT (1)
#define COMMITSEQNO_IS_SPECIAL(csn) ((csn) & COMMITSEQNO_SPECIAL_BIT)
#define COMMITSEQNO_GET_LEVEL(csn) (((csn) >> 31) & 0xFFFFFFFF)
#define COMMITSEQNO_GET_PROCNUM(csn) (((csn) >> 15) & 0xFFFF)
#define COMMITSEQNO_MAKE_SPECIAL(procnum, level, committing) \
	(COMMITSEQNO_SPECIAL_BIT | ((uint64) procnum << 15) | \
	 ((uint64) level << 31) | \
	 ((committing) ? COMMITSEQNO_SPECIAL_COMMITTING_BIT : 0))

static OXid curOxid = InvalidOXid;
static pg_atomic_uint64 *xidBuffer;

XidMeta    *xid_meta;

static OBuffersDesc buffersDesc = {
	.singleFileSize = XID_FILE_SIZE,
	.filenameTemplate = ORIOLEDB_DATA_DIR "/%02X%08X.xidmap",
	.groupCtlTrancheName = "xidBuffersGroupCtlTranche",
	.bufferCtlTrancheName = "xidBuffersCtlTranche"
};

static void advance_global_xmin(OXid newXid);

Size
oxid_shmem_needs(void)
{
	Size		size;

	buffersDesc.buffersCount = xid_buffers_count;

	size = CACHELINEALIGN(sizeof(XidMeta));
	size = add_size(size, mul_size(xid_circular_buffer_size,
								   sizeof(pg_atomic_uint64)));
	size = add_size(size, o_buffers_shmem_needs(&buffersDesc));

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
	ptr += MAXALIGN(sizeof(XidMeta));
	xidBuffer = (pg_atomic_uint64 *) ptr;
	ptr += xid_circular_buffer_size * sizeof(pg_atomic_uint64);

	o_buffers_shmem_init(&buffersDesc, ptr, found);

	if (!found)
	{
		int64		i;

		/* xid_meta fields are initialized in checkpoint_shmem_init() */
		SpinLockInit(&xid_meta->xminMutex);
		for (i = 0; i < xid_circular_buffer_size; i++)
			pg_atomic_init_u64(&xidBuffer[i], COMMITSEQNO_FROZEN);

		xid_meta->xidMapTrancheId = LWLockNewTrancheId();
		LWLockInitialize(&xid_meta->xidMapWriteLock,
						 xid_meta->xidMapTrancheId);

		/* Undo positions are initialized in checkpoint_shmem_init() */
	}
	LWLockRegisterTranche(xid_meta->xidMapTrancheId,
						  "OXidMapWriteTranche");

	init_lock_hashes();
}

/*
 * Set the csn value for particular oxid.
 */
void
set_oxid_csn(OXid oxid, CommitSeqNo csn)
{
	CommitSeqNo oldCsn;
	OXid		writeInProgressXmin;

	oldCsn = pg_atomic_read_u64(&xidBuffer[oxid % xid_circular_buffer_size]);
	pg_read_barrier();
	writeInProgressXmin = pg_atomic_read_u64(&xid_meta->writeInProgressXmin);
	if (oxid >= writeInProgressXmin)
	{
		if (pg_atomic_compare_exchange_u64(&xidBuffer[oxid % xid_circular_buffer_size],
										   &oldCsn, csn))
		{
			Assert(oldCsn != COMMITSEQNO_FROZEN);
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
	o_buffers_write(&buffersDesc, (Pointer) &csn,
					oxid * sizeof(CommitSeqNo), sizeof(CommitSeqNo));
}

/*
 * Read csn of given xid from xidmap.
 */
static CommitSeqNo
map_oxid_csn(OXid oxid)
{
	CommitSeqNo csn;

	if (is_recovery_process())
	{
		bool		found;

		csn = recovery_map_oxid_csn(oxid, &found);
		if (found)
			return csn;
	}

	/* Optimisticly try to read csn from circular buffer */
	csn = pg_atomic_read_u64(&xidBuffer[oxid % xid_circular_buffer_size]);
	pg_read_barrier();

	/* Did we manage to read the correct csn? */
	if (oxid >= pg_atomic_read_u64(&xid_meta->writeInProgressXmin))
		return csn;

	/*
	 * Wait for the concurrent write operation if needed.
	 */
	if (oxid >= pg_atomic_read_u64(&xid_meta->writtenXmin))
	{
		LWLockAcquire(&xid_meta->xidMapWriteLock, LW_SHARED);
		LWLockRelease(&xid_meta->xidMapWriteLock);
	}

	Assert(oxid < pg_atomic_read_u64(&xid_meta->writtenXmin));
	o_buffers_read(&buffersDesc, (Pointer) &csn,
				   oxid * sizeof(CommitSeqNo), sizeof(CommitSeqNo));

	/* Recheck if globalXmin was advanced concurrently */
	if (oxid < pg_atomic_read_u64(&xid_meta->globalXmin))
		return COMMITSEQNO_FROZEN;

	return csn;
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
				lastWrittenXmin;
	int			bufferLength = ORIOLEDB_BLCKSZ / sizeof(CommitSeqNo);
	CommitSeqNo buffer[ORIOLEDB_BLCKSZ / sizeof(CommitSeqNo)];

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
		if (pg_atomic_read_u64(&xidBuffer[oxid % xid_circular_buffer_size]) == COMMITSEQNO_FROZEN)
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

	lastWrittenXmin = xmin;
	for (oxid = xmin; oxid < xmax; oxid++)
	{
		if (oxid % bufferLength == 0 && oxid > lastWrittenXmin)
		{
			o_buffers_write(&buffersDesc,
							(Pointer) &buffer[lastWrittenXmin % bufferLength],
							lastWrittenXmin * sizeof(CommitSeqNo),
							(oxid - lastWrittenXmin) * sizeof(CommitSeqNo));
			lastWrittenXmin = oxid;
		}

		buffer[oxid % bufferLength] = pg_atomic_exchange_u64(&xidBuffer[oxid % xid_circular_buffer_size],
															 COMMITSEQNO_FROZEN);
	}

	if (oxid > lastWrittenXmin)
		o_buffers_write(&buffersDesc,
						(Pointer) &buffer[lastWrittenXmin % bufferLength],
						lastWrittenXmin * sizeof(CommitSeqNo),
						(oxid - lastWrittenXmin) * sizeof(CommitSeqNo));

	SpinLockAcquire(&xid_meta->xminMutex);
	Assert(pg_atomic_read_u64(&xid_meta->writtenXmin) < xmax);
	pg_atomic_write_u64(&xid_meta->writtenXmin, xmax);
	SpinLockRelease(&xid_meta->xminMutex);

	advance_global_xmin(InvalidOXid);
}

/*
 * Sync given range of xidmap with disk.
 */
void
fsync_xidmap_range(OXid xmin, OXid xmax, uint32 wait_event_info)
{
	while (xmax > pg_atomic_read_u64(&xid_meta->writtenXmin))
	{
		if (LWLockAcquireOrWait(&xid_meta->xidMapWriteLock, LW_EXCLUSIVE))
		{
			write_xidsmap(xmax);
			LWLockRelease(&xid_meta->xidMapWriteLock);
		}
	}

	o_buffers_sync(&buffersDesc,
				   xmin * sizeof(CommitSeqNo),
				   xmax * sizeof(CommitSeqNo),
				   wait_event_info);
}

/*
 * Wait particular oxid to finish or oxid_notify() call.  Returns true if
 * oxid was finished.
 */
bool
wait_for_oxid(OXid oxid)
{
	int			procnum;
	CommitSeqNo csn;
	XidVXidMapElement *vxidElem;
	VirtualTransactionId vxid;
	bool		result;

	csn = map_oxid_csn(oxid);

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

	Assert(VirtualTransactionIdIsValid(vxid));
	GET_CUR_PROCDATA()->waitingForOxid = true;
	result = VirtualXactLock(vxid, true);
	GET_CUR_PROCDATA()->waitingForOxid = false;

	return result;
}

/*
 * Notify wait_for_oxid() caller only if it is waiting for current process.
 */
void
oxid_notify(OXid oxid)
{
	PGPROC	   *proc;
	LOCKTAG		tag;
	CommitSeqNo csn;
	VirtualTransactionId vxid;
	int			procnum;

	csn = map_oxid_csn(oxid);

	if (!COMMITSEQNO_IS_SPECIAL(csn))
		return;

	procnum = COMMITSEQNO_GET_PROCNUM(csn);
	proc = GetPGProcByNumber(procnum);
	vxid.localTransactionId = MyProc->lxid;
	vxid.backendId = MyBackendId;

	/* ensure that it is waiting for us */
	SET_LOCKTAG_VIRTUALTRANSACTION(tag, vxid);

	if (
#if PG_VERSION_NUM >= 140000
		proc->waitStatus == PROC_WAIT_STATUS_WAITING &&
#else
		proc->waitStatus == STATUS_WAITING &&
#endif
		proc->waitLock->tag.locktag_field1 == tag.locktag_field1 &&
		proc->waitLock->tag.locktag_field2 == tag.locktag_field2 &&
		proc->waitLock->tag.locktag_field3 == tag.locktag_field3 &&
		proc->waitLock->tag.locktag_field4 == tag.locktag_field4 &&
		proc->waitLock->tag.locktag_lockmethodid == tag.locktag_lockmethodid &&
		proc->waitLock->tag.locktag_type == tag.locktag_type &&
		oProcData[proc->pgprocno].waitingForOxid)
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
		proc->waitStatus = STATUS_OK;
		SendProcSignal(proc->pid, PROCSIG_NOTIFY_INTERRUPT, proc->backendId);

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
#if PG_VERSION_NUM >= 160000
	dclist_head *waitQueue;
	dlist_iter	iter;
#else
	PROC_QUEUE *waitQueue;
	int			i;
#endif

	vxid.localTransactionId = MyProc->lxid;
	vxid.backendId = MyBackendId;

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

#if PG_VERSION_NUM >= 160000
	dclist_foreach(iter, waitQueue)
	{
		proc = dlist_container(PGPROC, links, iter.cur);
#else
	Assert(waitQueue->size >= 0);

	proc = (PGPROC *) waitQueue->links.next;
	for (i = 0; i < waitQueue->size; i++)
	{
#endif
		if (
#if PG_VERSION_NUM >= 140000
			proc->waitStatus == PROC_WAIT_STATUS_WAITING &&
#else
			proc->waitStatus == STATUS_WAITING &&
#endif
			proc->waitLock->tag.locktag_field1 == tag.locktag_field1 &&
			proc->waitLock->tag.locktag_field2 == tag.locktag_field2 &&
			proc->waitLock->tag.locktag_field3 == tag.locktag_field3 &&
			proc->waitLock->tag.locktag_field4 == tag.locktag_field4 &&
			proc->waitLock->tag.locktag_lockmethodid == tag.locktag_lockmethodid &&
			proc->waitLock->tag.locktag_type == tag.locktag_type &&
			oProcData[proc->pgprocno].waitingForOxid)
		{
			procs = lappend(procs, proc);
		}

#if PG_VERSION_NUM < 160000
		proc = (PGPROC *) proc->links.next;
#endif
	}

	foreach(lc, procs)
	{
		proc = lfirst(lc);
		RemoveFromWaitQueue(proc, hashcode);
		proc->waitStatus = STATUS_OK;
		SendProcSignal(proc->pid, PROCSIG_NOTIFY_INTERRUPT, proc->backendId);
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
			pg_atomic_write_u64(&xidBuffer[oxid % xid_circular_buffer_size],
								COMMITSEQNO_FROZEN);

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
		int64		xidsPerFile = XID_FILE_SIZE / sizeof(CommitSeqNo);
		int64		oldCleanedNum = oldCleanedXmin / xidsPerFile,
					newCleanedNum = globalXmin / xidsPerFile,
					oldCheckpointXminNum = oldCheckpointXmin / xidsPerFile,
					oldCheckpointXmaxNum = oldCheckpointXmax / xidsPerFile,
					newCheckpointXminNum = newCheckpointXmin / xidsPerFile,
					newCheckpointXmaxNum = newCheckpointXmax / xidsPerFile;

		if (oldCheckpointXmaxNum % xidsPerFile == 0)
			oldCheckpointXmaxNum--;
		if (newCheckpointXmaxNum % xidsPerFile == 0)
			newCheckpointXmaxNum--;

		o_buffers_unlink_files_range(&buffersDesc,
									 oldCheckpointXminNum,
									 Min(oldCheckpointXmaxNum,
										 Min(newCheckpointXminNum - 1,
											 newCleanedNum - 1)));

		o_buffers_unlink_files_range(&buffersDesc,
									 Max(oldCheckpointXminNum,
										 newCheckpointXmaxNum + 1),
									 Min(oldCheckpointXmaxNum,
										 newCleanedNum - 1));

		o_buffers_unlink_files_range(&buffersDesc,
									 oldCleanedNum,
									 Min(newCheckpointXminNum - 1,
										 newCleanedNum - 1));

		o_buffers_unlink_files_range(&buffersDesc,
									 Max(oldCleanedNum,
										 newCheckpointXmaxNum + 1),
									 newCleanedNum - 1);
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
			pg_atomic_write_u64(&xidBuffer[xid % xid_circular_buffer_size],
								COMMITSEQNO_INPROGRESS);
		}
		pg_atomic_write_u64(&xid_meta->nextXid, xmax);
	}
}


/*
 * Get curent OrioleDB xid (oxid).  Assign new oxid it's not done yet.
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
		 * prevent unnecessry circular buffer overrun.
		 */
		if (newOxid > pg_atomic_read_u64(&xid_meta->lastXidWhenUpdatedGlobalXmin) + xid_circular_buffer_size / 10)
			advance_global_xmin(newOxid);

		/*
		 * Write some xids out of circular buffer if needed.  We always keep
		 * one COMMITSEQNO_FROZEN in circular buffers as a delimited beween
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
		vxidElem->vxid.backendId = MyProc->backendId;
		vxidElem->vxid.localTransactionId = MyProc->lxid;

		Assert(pg_atomic_read_u64(&xidBuffer[newOxid % xid_circular_buffer_size]) == COMMITSEQNO_FROZEN);
		pg_atomic_write_u64(&xidBuffer[newOxid % xid_circular_buffer_size],
							COMMITSEQNO_MAKE_SPECIAL(MyProc->pgprocno,
													 nestingLevel,
													 false));
		curOxid = newOxid;
	}

	return curOxid;
}

void
set_current_oxid(OXid oxid)
{
	curOxid = oxid;

}

void
parallel_worker_set_oxid(void)
{
	XidVXidMapElement *vxidElem;
	ODBProcData *leaderProcData;

	Assert(MyProc->lockGroupLeader);

	leaderProcData = &oProcData[MyProc->lockGroupLeader->pgprocno];
	vxidElem = &leaderProcData->vxids[leaderProcData->autonomousNestingLevel];

	curOxid = vxidElem->oxid;
	saved_undo_location = InvalidUndoLocation;
}

void
reset_current_oxid(void)
{
	curOxid = InvalidOXid;
}

OXid
get_current_oxid_if_any(void)
{
	if (OXidIsValid(recovery_oxid))
		return recovery_oxid;

	return curOxid;
}

void
current_oxid_precommit(void)
{
	if (!OXidIsValid(curOxid))
		return;

	set_oxid_csn(curOxid, COMMITSEQNO_MAKE_SPECIAL(MyProc->pgprocno,
												   GET_CUR_PROCDATA()->autonomousNestingLevel,
												   true));

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
			csn = map_oxid_csn(run_xmin);
			if (COMMITSEQNO_IS_SPECIAL(csn) || COMMITSEQNO_IS_FROZEN(csn))
				break;
		}
	}
}

void
current_oxid_commit(CommitSeqNo csn)
{
	ODBProcData *my_proc_info = &oProcData[MyProc->pgprocno];

	pg_atomic_write_u64(&my_proc_info->xmin, InvalidOXid);

	if (!OXidIsValid(curOxid))
		return;

	set_oxid_csn(curOxid, csn);
	pg_write_barrier();
	my_proc_info->vxids[GET_CUR_PROCDATA()->autonomousNestingLevel].oxid = InvalidOXid;

	advance_run_xmin(curOxid);
	curOxid = InvalidOXid;
}

void
current_oxid_abort(void)
{
	ODBProcData *my_proc_info = &oProcData[MyProc->pgprocno];

	pg_atomic_write_u64(&my_proc_info->xmin, InvalidOXid);

	if (!OXidIsValid(curOxid))
		return;

	set_oxid_csn(curOxid, COMMITSEQNO_ABORTED);
	pg_write_barrier();
	my_proc_info->vxids[GET_CUR_PROCDATA()->autonomousNestingLevel].oxid = InvalidOXid;

	advance_run_xmin(curOxid);
	curOxid = InvalidOXid;
}

/*
 * Gets csn for given oxid.  Wrapper over map_oxid_csn(), which loops
 * till "committing bit" is not set.
 */
CommitSeqNo
oxid_get_csn(OXid oxid)
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

		csn = map_oxid_csn(oxid);
		if (COMMITSEQNO_IS_SPECIAL(csn) && (csn & COMMITSEQNO_SPECIAL_COMMITTING_BIT))
			perform_spin_delay(&status);
		else
			break;
	}

	finish_spin_delay(&status);

	if (COMMITSEQNO_IS_SPECIAL(csn))
		return COMMITSEQNO_INPROGRESS;

	return csn;
}

void
fill_current_oxid_csn(OXid *oxid, CommitSeqNo *csn)
{
	if (XactIsoLevel == XACT_SERIALIZABLE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("orioledb does not support SERIALIZABLE isolation level")),
				errdetail("Stay tuned, it will be added in future releases."));

	if (ActiveSnapshotSet())
		*csn = GetActiveSnapshot()->snapshotcsn;
	else
		*csn = COMMITSEQNO_INPROGRESS;
	*oxid = get_current_oxid();
}

int
oxid_get_procnum(OXid oxid)
{
	CommitSeqNo csn;

	csn = map_oxid_csn(oxid);

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

	csn = oxid_get_csn(xid);

	return COMMITSEQNO_IS_COMMITTED(csn);
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

	xmin = pg_atomic_read_u64(&xid_meta->runXmin);

	if (xid < xmin)
		return true;

	csn = oxid_get_csn(xid);

	return COMMITSEQNO_IS_COMMITTED(csn);
}
