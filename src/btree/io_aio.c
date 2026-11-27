/*-------------------------------------------------------------------------
 *
 * io_aio.c
 *		PG18 AIO integration for orioledb btree leaf reads.
 *
 * Registers an extension-reserved PgAioTargetID + PgAioHandleCallbackID with
 * the PG18 AIO subsystem.  This lets a btree sequential scan submit async
 * reads of leaf segment files through PG's existing io_workers / io_uring
 * machinery, without touching the shared buffer manager.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/io_aio.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/io.h"
#include "btree/io_aio.h"

/*
 * This file integrates with the PostgreSQL 18 AIO subsystem.  On older
 * majors it compiles to an empty translation unit so the Makefile entry
 * stays version-agnostic.
 */
#if PG_VERSION_NUM >= 180000

#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/aio_types.h"
#include "port/pg_iovec.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/procnumber.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include <fcntl.h>
#include <unistd.h>


static void btree_aio_reopen(PgAioHandle *ioh);
static char *btree_aio_describe_identity(const PgAioTargetData *sd);
static PgAioResult btree_aio_readv_complete_shared(PgAioHandle *ioh,
												   PgAioResult prior_result,
												   uint8 cb_data);
static void btree_aio_readv_report(PgAioResult result,
								   const PgAioTargetData *target_data,
								   int elevel);


static const PgAioTargetInfo orioledb_btree_target_info = {
	.name = "orioledb_btree",
	.reopen = btree_aio_reopen,
	.describe_identity = btree_aio_describe_identity,
};

static const PgAioHandleCallbacks orioledb_btree_readv_cb = {
	.complete_shared = btree_aio_readv_complete_shared,
	.report = btree_aio_readv_report,
};


void
orioledb_aio_init(void)
{
	pgaio_register_target(ORIOLEDB_AIO_TID_BTREE,
						  &orioledb_btree_target_info);
	pgaio_register_handle_callbacks(ORIOLEDB_AIO_HCB_BTREE_READV,
									&orioledb_btree_readv_cb,
									"orioledb_btree_readv");
}

/*
 * Build the path of the segment file described by target_data.
 *
 * Returned string is palloc'd in the current memory context.
 */
static char *
btree_aio_segment_path(const OrioleBtreeAioTargetData *otd)
{
	OIndexKey	key;

	key.oids.datoid = otd->datoid;
	key.oids.reloid = otd->reloid;
	key.oids.relnode = otd->relnode;
	key.oids.spcoid = otd->tablespace;

	return btree_filename(key, otd->segno, otd->chkpNum);
}

/*
 * Callback used when the IO is executed by an io worker: reopen the segment
 * file in the worker's process and stash the resulting OS file descriptor in
 * the handle's op_data.  The matching close happens in
 * btree_aio_readv_complete_shared().
 */
static void
btree_aio_reopen(PgAioHandle *ioh)
{
	PgAioTargetData *td = pgaio_io_get_target_data(ioh);
	OrioleBtreeAioTargetData *otd =
		(OrioleBtreeAioTargetData *) td->extension_bytes;
	PgAioOpData *od = pgaio_io_get_op_data(ioh);
	char	   *path;
	int			fd;

	Assert(!INTERRUPTS_CAN_BE_PROCESSED());

	path = btree_aio_segment_path(otd);
	fd = open(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not reopen orioledb segment file \"%s\": %m",
						path)));
	pfree(path);

	od->read.fd = fd;
}

static char *
btree_aio_describe_identity(const PgAioTargetData *td)
{
	const OrioleBtreeAioTargetData *otd =
		(const OrioleBtreeAioTargetData *) td->extension_bytes;

	return psprintf(_("orioledb segment db=%u rel=%u relnode=%u seg=%u chkp=%u blocks=%u"),
					otd->datoid, otd->reloid, otd->relnode,
					(uint32) otd->segno, otd->chkpNum, (uint32) otd->nblocks);
}

/*
 * Completion in the executing process.  Close the file descriptor we opened
 * in btree_aio_reopen(), if any; the submitter's transient fd (used for the
 * synchronous fallback path) is closed by the submitter, not here.
 */
static PgAioResult
btree_aio_readv_complete_shared(PgAioHandle *ioh,
								PgAioResult prior_result,
								uint8 cb_data)
{
	PgAioOpData *od = pgaio_io_get_op_data(ioh);
	PgAioResult result = prior_result;

	if (pgaio_io_get_owner(ioh) != MyProcNumber)
	{
		/*
		 * We're executing in a different process than the submitter (an io
		 * worker).  Close the fd we opened in btree_aio_reopen().
		 */
		if (od->read.fd >= 0)
		{
			close(od->read.fd);
			od->read.fd = -1;
		}
	}

	if (result.status != PGAIO_RS_ERROR &&
		result.result != od->read.iov_length * ORIOLEDB_BLCKSZ)
	{
		result.status = (result.result < 0) ? PGAIO_RS_ERROR : PGAIO_RS_PARTIAL;
		result.id = ORIOLEDB_AIO_HCB_BTREE_READV;
		result.error_data = (result.result < 0) ? errno : 0;
	}

	return result;
}

static void
btree_aio_readv_report(PgAioResult result,
					   const PgAioTargetData *target_data,
					   int elevel)
{
	char	   *desc = btree_aio_describe_identity(target_data);

	if (result.status == PGAIO_RS_ERROR)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("orioledb async read of %s failed: %s",
						desc, strerror(result.error_data))));
	else
		ereport(elevel,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("orioledb async read of %s returned %d of %u bytes",
						desc, result.result,
						((const OrioleBtreeAioTargetData *) target_data->extension_bytes)->nblocks * ORIOLEDB_BLCKSZ)));
}

/*
 * Can leaf reads of this tree go through AIO at all?  Only the simplest
 * configuration is handled: non-compressed btree, non-S3 mode, non-mmap /
 * non-device storage.
 */
static bool
btree_aio_possible(BTreeDescr *desc)
{
	return !use_mmap && !use_device && !orioledb_s3_mode &&
		!OCompressIsValid(desc->compress);
}

/*
 * Submit one async read of nblocks consecutive leaf pages of a btree segment
 * file, starting at byteOffset, each page into its own buffer of dests.
 * localBuffers says whether the buffers live in this backend's memory, which
 * keeps the read in this process; otherwise they are in shared memory, which
 * an io worker can write to.  *ret receives the outcome once the read is
 * reclaimed.  Returns false if the read cannot be submitted, in which case
 * the caller falls back to the synchronous path.
 */
static bool
orioledb_aio_submit_btree_read(BTreeDescr *desc,
							   uint32 chkpNum,
							   off_t byteOffset,
							   char **dests,
							   int nblocks,
							   bool localBuffers,
							   PgAioWaitRef *wref_out,
							   PgAioReturn *ret)
{
	PgAioHandle *ioh;
	PgAioTargetData *td;
	OrioleBtreeAioTargetData *otd;
	struct iovec *iov;
	off_t		segOffset;
	int			segno;
	int			fd;
	char	   *path;

	pgaio_wref_clear(wref_out);

	Assert(btree_aio_possible(desc));
	Assert(nblocks > 0 && nblocks <= PG_UINT8_MAX);

	segno = byteOffset / ORIOLEDB_SEGMENT_SIZE;
	segOffset = byteOffset % ORIOLEDB_SEGMENT_SIZE;
	Assert(segOffset + (off_t) nblocks * ORIOLEDB_BLCKSZ <= ORIOLEDB_SEGMENT_SIZE);

	{
		OIndexKey	key = {.oids = desc->oids};

		path = btree_filename(key, segno, chkpNum);
	}
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	pfree(path);
	if (fd < 0)
		return false;

	/*
	 * No resource owner: the ring that owns *ret waits for every read it
	 * submitted before it goes away, on error too.
	 */
	ioh = pgaio_io_acquire(NULL, ret);

	/*
	 * From here through pgaio_io_start_readv() we must keep interrupts held:
	 * the AIO subsystem asserts on this so that the submitter's fd cannot be
	 * yanked out from under it (e.g. by FD recycling triggered during
	 * interrupt processing).
	 */
	HOLD_INTERRUPTS();

	/*
	 * A process-local destination must not be handed to an io worker, which
	 * would write to its own copy of the page.  In worker mode this forces
	 * synchronous in-process execution; in io_uring mode the IO remains async
	 * because the kernel uses the submitter's address space directly.
	 */
	if (localBuffers)
		pgaio_io_set_flag(ioh, PGAIO_HF_REFERENCES_LOCAL);

	pgaio_io_set_target(ioh, ORIOLEDB_AIO_TID_BTREE);
	td = pgaio_io_get_target_data(ioh);
	otd = (OrioleBtreeAioTargetData *) td->extension_bytes;
	otd->datoid = desc->oids.datoid;
	otd->reloid = desc->oids.reloid;
	otd->relnode = desc->oids.relnode;
	otd->tablespace = desc->oids.spcoid;
	otd->chkpNum = chkpNum;
	otd->segno = (uint16) segno;
	otd->index_type = (uint8) desc->type;
	otd->nblocks = (uint8) nblocks;

	pgaio_io_register_callbacks(ioh, ORIOLEDB_AIO_HCB_BTREE_READV, 0);
	pgaio_io_get_wref(ioh, wref_out);

	(void) pgaio_io_get_iovec(ioh, &iov);
	for (int i = 0; i < nblocks; i++)
	{
		iov[i].iov_base = dests[i];
		iov[i].iov_len = ORIOLEDB_BLCKSZ;
	}

	pgaio_io_start_readv(ioh, fd, nblocks, segOffset);

	RESUME_INTERRUPTS();

	/*
	 * The submitter-side transient fd is no longer needed once the IO has
	 * been started: an io worker, if used, reopens the file in its own
	 * process, and CloseTransientFile() submits a read still staged on this
	 * fd before closing it.
	 */
	CloseTransientFile(fd);

	return true;
}

/* --------------------------------------------------------------------------
 * Shared pool of prefetch buffers, for io_method = worker.
 *
 * An io worker can only write into shared memory, so with io_method = worker
 * the ring reads into buffers taken from this pool.  Only io_workers reads
 * proceed at once, so the pool is sized by its own setting rather than per
 * backend; a ring that finds no free buffer reads synchronously.  A buffer
 * is held from submit until its page is copied out, or until the read it is
 * the target of has finished, never longer.
 * -------------------------------------------------------------------------- */

typedef struct BTreePrefetchPool
{
	int			nbuffers;
	pg_atomic_uint32 nextSearch;	/* where the next free-buffer search
									 * starts */
	/* owner of each buffer: 0 if free, else the owner's ProcNumber + 1 */
	pg_atomic_uint32 owner[FLEXIBLE_ARRAY_MEMBER];
} BTreePrefetchPool;

static BTreePrefetchPool *prefetchPool = NULL;
static char *prefetchPoolBuffers = NULL;

/*
 * Wait reference of the read each buffer this backend holds is the target
 * of, so that the buffers can be released safely when the backend exits.
 */
static PgAioWaitRef *heldBufferWrefs = NULL;
static bool prefetchPoolExitRegistered = false;

static bool
prefetch_pool_enabled(void)
{
	return io_method == IOMETHOD_WORKER && btree_prefetch_buffers > 0;
}

Size
btree_prefetch_shmem_needs(void)
{
	Size		size;

	if (!prefetch_pool_enabled())
		return 0;

	size = add_size(offsetof(BTreePrefetchPool, owner),
					mul_size(btree_prefetch_buffers, sizeof(pg_atomic_uint32)));
	size = add_size(size, PG_IO_ALIGN_SIZE);
	size = add_size(size, mul_size(btree_prefetch_buffers, ORIOLEDB_BLCKSZ));
	return size;
}

void
btree_prefetch_shmem_init(Pointer ptr, bool found)
{
	Size		headerSize;

	if (!prefetch_pool_enabled())
		return;

	headerSize = offsetof(BTreePrefetchPool, owner) +
		btree_prefetch_buffers * sizeof(pg_atomic_uint32);
	prefetchPool = (BTreePrefetchPool *) ptr;
	prefetchPoolBuffers = (char *) TYPEALIGN(PG_IO_ALIGN_SIZE, ptr + headerSize);

	if (!found)
	{
		prefetchPool->nbuffers = btree_prefetch_buffers;
		pg_atomic_init_u32(&prefetchPool->nextSearch, 0);
		for (int i = 0; i < btree_prefetch_buffers; i++)
			pg_atomic_init_u32(&prefetchPool->owner[i], 0);
	}
}

#define POOL_BUFFER(i) (prefetchPoolBuffers + (Size) (i) * ORIOLEDB_BLCKSZ)

/*
 * Gives a buffer back.  At backend exit prefetch_pool_at_exit() may have
 * done so already, before the scan's ring gets destroyed, and someone else
 * may hold the buffer by then, so only a buffer still ours is released.
 * The compare-and-exchange is a full barrier: the page is copied out before
 * another backend can reuse the buffer.
 */
static void
prefetch_pool_release(int i)
{
	uint32		expected = (uint32) MyProcNumber + 1;

	if (pg_atomic_compare_exchange_u32(&prefetchPool->owner[i], &expected, 0))
		pgaio_wref_clear(&heldBufferWrefs[i]);
}

/*
 * Releases the buffers a backend still holds when it exits, waiting first
 * for the reads that target them.  Runs before AIO shuts down for this
 * backend, as it is registered later -- and also before an open transaction
 * is aborted, so before the rings of its scans release the same buffers.
 */
static void
prefetch_pool_at_exit(int code, Datum arg)
{
	for (int i = 0; i < prefetchPool->nbuffers; i++)
	{
		if (pg_atomic_read_u32(&prefetchPool->owner[i]) !=
			(uint32) MyProcNumber + 1)
			continue;
		if (pgaio_wref_valid(&heldBufferWrefs[i]))
			pgaio_wref_wait(&heldBufferWrefs[i]);
		prefetch_pool_release(i);
	}
}

/* Takes a free buffer of the pool, or returns -1 if there is none. */
static int
prefetch_pool_acquire(void)
{
	int			n = prefetchPool->nbuffers;
	uint32		start;

	if (!prefetchPoolExitRegistered)
	{
		heldBufferWrefs = MemoryContextAllocZero(TopMemoryContext,
												 sizeof(PgAioWaitRef) * n);
		for (int i = 0; i < n; i++)
			pgaio_wref_clear(&heldBufferWrefs[i]);
		before_shmem_exit(prefetch_pool_at_exit, (Datum) 0);
		prefetchPoolExitRegistered = true;
	}

	start = pg_atomic_fetch_add_u32(&prefetchPool->nextSearch, 1) % n;
	for (int k = 0; k < n; k++)
	{
		int			i = (start + k) % n;
		uint32		expected = 0;

		if (pg_atomic_read_u32(&prefetchPool->owner[i]) == 0 &&
			pg_atomic_compare_exchange_u32(&prefetchPool->owner[i], &expected,
										   (uint32) MyProcNumber + 1))
			return i;
	}
	return -1;
}


/* --------------------------------------------------------------------------
 * Prefetch ring buffer for btree seqscan.
 * -------------------------------------------------------------------------- */

typedef enum
{
	PREFETCH_SLOT_EMPTY,
	PREFETCH_SLOT_PENDING,		/* in the group not submitted yet */
	PREFETCH_SLOT_ASYNC,		/* its group's read submitted, waiting on wref */
	PREFETCH_SLOT_DONE,			/* its group's read finished and checked */
	PREFETCH_SLOT_DEFERRED,		/* no AIO possible: read it at consume */
} PrefetchSlotState;

typedef struct PrefetchSlot
{
	uint64		downlink;
	CommitSeqNo csn;
	bool		fullyInRange;
	uint32		chkpNum;
	PrefetchSlotState state;
	int			poolBuffer;		/* pool buffer read into, or -1 */

	/*
	 * One read covers a group of slots holding consecutive pages.  Its first
	 * slot keeps the outcome and the group size; the wait reference is in
	 * every slot of the group.
	 */
	int			groupSize;		/* first slot of a group only, else 0 */
	PgAioWaitRef wref;
	PgAioReturn ret;
} PrefetchSlot;

struct BTreePrefetchRing
{
	int			depth;
	int			head;			/* next slot to submit into */
	int			tail;			/* next slot to consume from */
	int			inflight;		/* number of populated slots */
	int			refillBatch;	/* refill once this many slots are free */
	int			combineLimit;	/* pages per read at most */
	bool		usePool;		/* read into the shared pool */
	char	   *buffers;		/* depth * ORIOLEDB_BLCKSZ, unless usePool */
	PrefetchSlot *slots;
	MemoryContext mctx;

	/* the group of pending slots, to be read together */
	BTreeDescr *pendingDesc;
	int			pendingFirst;	/* slot index */
	int			pendingCount;
	uint64		pendingNextBlock;	/* disk block that may extend it */
};

#define RING_BUFFER(ring, i) ((ring)->buffers + (i) * ORIOLEDB_BLCKSZ)
#define RING_NEXT(ring, i) (((i) + 1) % (ring)->depth)

static char *
prefetch_slot_buffer(BTreePrefetchRing *ring, int i)
{
	PrefetchSlot *slot = &ring->slots[i];

	return slot->poolBuffer >= 0 ? POOL_BUFFER(slot->poolBuffer) :
		RING_BUFFER(ring, i);
}

/*
 * Creates a ring of the given depth, or returns NULL when prefetching cannot
 * help: io_method = sync runs every read in this process anyway, and
 * io_method = worker needs the shared pool.
 */
BTreePrefetchRing *
btree_prefetch_ring_create(int depth, MemoryContext mctx)
{
	BTreePrefetchRing *ring;
	MemoryContext oldcxt;

	Assert(depth > 0);

	if (io_method == IOMETHOD_SYNC)
		return NULL;
	if (io_method == IOMETHOD_WORKER && !prefetchPool)
		return NULL;

	oldcxt = MemoryContextSwitchTo(mctx);
	ring = palloc0(sizeof(BTreePrefetchRing));
	ring->depth = depth;
	ring->mctx = mctx;
	ring->usePool = (io_method == IOMETHOD_WORKER);
	ring->combineLimit = Max(1, Min(io_combine_limit, PG_UINT8_MAX));

	/*
	 * Refilling a ring after every page it hands out would submit one page at
	 * a time.  Wait until a read's worth of slots is free, but keep at least
	 * half of the ring in flight.
	 */
	ring->refillBatch = Max(1, Min(ring->combineLimit, depth / 2));
	if (!ring->usePool)
		ring->buffers = MemoryContextAllocAligned(mctx,
												  (Size) depth * ORIOLEDB_BLCKSZ,
												  PG_IO_ALIGN_SIZE, 0);
	ring->slots = palloc0(sizeof(PrefetchSlot) * depth);
	for (int i = 0; i < depth; i++)
	{
		ring->slots[i].state = PREFETCH_SLOT_EMPTY;
		ring->slots[i].poolBuffer = -1;
		pgaio_wref_clear(&ring->slots[i].wref);
	}
	MemoryContextSwitchTo(oldcxt);

	return ring;
}

/* Empties a slot, first waiting for the read targeting its buffer, if any */
static void
prefetch_slot_clear(PrefetchSlot *slot)
{
	if (slot->state == PREFETCH_SLOT_ASYNC && pgaio_wref_valid(&slot->wref))
		pgaio_wref_wait(&slot->wref);
	pgaio_wref_clear(&slot->wref);
	if (slot->poolBuffer >= 0)
	{
		prefetch_pool_release(slot->poolBuffer);
		slot->poolBuffer = -1;
	}
	slot->groupSize = 0;
	slot->state = PREFETCH_SLOT_EMPTY;
}

/*
 * Submits the pending group as one read.  If that is impossible, its slots
 * are read synchronously when consumed.
 */
static void
prefetch_ring_flush(BTreePrefetchRing *ring)
{
	PrefetchSlot *first;
	char	   *dests[PG_IOV_MAX];
	int			i,
				n;
	bool		submitted;

	if (ring->pendingCount == 0)
		return;

	first = &ring->slots[ring->pendingFirst];
	for (i = ring->pendingFirst, n = 0; n < ring->pendingCount;
		 i = RING_NEXT(ring, i), n++)
		dests[n] = prefetch_slot_buffer(ring, i);

	submitted = orioledb_aio_submit_btree_read(ring->pendingDesc,
											   first->chkpNum,
											   (off_t) DOWNLINK_GET_DISK_OFF(first->downlink) * ORIOLEDB_BLCKSZ,
											   dests, n, !ring->usePool,
											   &first->wref, &first->ret);
	for (i = ring->pendingFirst, n = 0; n < ring->pendingCount;
		 i = RING_NEXT(ring, i), n++)
	{
		PrefetchSlot *slot = &ring->slots[i];

		Assert(slot->state == PREFETCH_SLOT_PENDING);
		if (submitted)
		{
			slot->state = PREFETCH_SLOT_ASYNC;
			slot->wref = first->wref;
			if (slot->poolBuffer >= 0)
				heldBufferWrefs[slot->poolBuffer] = first->wref;
		}
		else
		{
			slot->state = PREFETCH_SLOT_DEFERRED;
			if (slot->poolBuffer >= 0)
			{
				prefetch_pool_release(slot->poolBuffer);
				slot->poolBuffer = -1;
			}
		}
	}
	if (submitted)
		first->groupSize = ring->pendingCount;
	ring->pendingCount = 0;
}

void
btree_prefetch_ring_destroy(BTreePrefetchRing *ring)
{
	if (ring == NULL)
		return;

	/*
	 * Drain any in-flight reads so the AIO handles and buffers are released.
	 * Pending slots were never submitted, so clearing them waits for nothing.
	 */
	while (ring->inflight > 0)
	{
		prefetch_slot_clear(&ring->slots[ring->tail]);
		ring->tail = RING_NEXT(ring, ring->tail);
		ring->inflight--;
	}

	pfree(ring->slots);
	if (ring->buffers)
		pfree(ring->buffers);
	pfree(ring);
}

int
btree_prefetch_ring_inflight(const BTreePrefetchRing *ring)
{
	return ring->inflight;
}

int
btree_prefetch_ring_depth(const BTreePrefetchRing *ring)
{
	return ring->depth;
}

bool
btree_prefetch_ring_needs_refill(const BTreePrefetchRing *ring)
{
	return ring->depth - ring->inflight >= ring->refillBatch;
}

bool
btree_prefetch_ring_submit(BTreePrefetchRing *ring,
						   BTreeDescr *desc,
						   uint64 downlink,
						   CommitSeqNo csn,
						   bool fullyInRange,
						   uint32 chkpNum)
{
	PrefetchSlot *slot;
	uint64		block = DOWNLINK_GET_DISK_OFF(downlink);
	int			idx;

	if (ring->inflight >= ring->depth)
		return false;

	idx = ring->head;
	slot = &ring->slots[idx];
	Assert(slot->state == PREFETCH_SLOT_EMPTY);
	Assert(slot->poolBuffer < 0);

	slot->downlink = downlink;
	slot->csn = csn;
	slot->fullyInRange = fullyInRange;
	slot->chkpNum = chkpNum;
	slot->groupSize = 0;
	pgaio_wref_clear(&slot->wref);
	ring->head = RING_NEXT(ring, idx);
	ring->inflight++;

	if (!btree_aio_possible(desc))
	{
		slot->state = PREFETCH_SLOT_DEFERRED;
		return true;
	}

	if (ring->usePool)
	{
		slot->poolBuffer = prefetch_pool_acquire();
		if (slot->poolBuffer < 0)
		{
			/* no buffer to read into: read this page at consume */
			prefetch_ring_flush(ring);
			slot->state = PREFETCH_SLOT_DEFERRED;
			return true;
		}
	}

	/* Extend the pending group if this page continues it on disk */
	if (ring->pendingCount > 0 &&
		(block != ring->pendingNextBlock ||
		 ring->pendingCount >= ring->combineLimit ||
		 ring->slots[ring->pendingFirst].chkpNum != chkpNum ||
		 (block * ORIOLEDB_BLCKSZ) / ORIOLEDB_SEGMENT_SIZE !=
		 (DOWNLINK_GET_DISK_OFF(ring->slots[ring->pendingFirst].downlink) *
		  ORIOLEDB_BLCKSZ) / ORIOLEDB_SEGMENT_SIZE))
		prefetch_ring_flush(ring);

	if (ring->pendingCount == 0)
	{
		ring->pendingFirst = idx;
		ring->pendingDesc = desc;
	}
	ring->pendingCount++;
	ring->pendingNextBlock = block + 1;
	slot->state = PREFETCH_SLOT_PENDING;
	return true;
}

bool
btree_prefetch_ring_consume(BTreePrefetchRing *ring,
							BTreeDescr *desc,
							Pointer dest,
							uint64 *downlink_out,
							CommitSeqNo *csn_out,
							bool *fullyInRange_out,
							OReadPageResult *result_out)
{
	PrefetchSlot *slot;
	int			idx;

	if (ring->inflight == 0)
		return false;

	/* whatever is pending gets submitted before we wait for anything */
	prefetch_ring_flush(ring);

	idx = ring->tail;
	slot = &ring->slots[idx];
	ring->tail = RING_NEXT(ring, idx);
	ring->inflight--;

	*downlink_out = slot->downlink;
	*csn_out = slot->csn;
	*fullyInRange_out = slot->fullyInRange;

	if (slot->state == PREFETCH_SLOT_ASYNC)
	{
		PgAioResult aioResult;

		/*
		 * Groups are consumed in order, so this is the first slot of its
		 * group: the rest gets marked done once the read is checked here.
		 */
		Assert(slot->groupSize > 0);
		pgaio_wref_wait(&slot->wref);
		aioResult = slot->ret.result;
		if (aioResult.status == PGAIO_RS_OK)
		{
			for (int i = RING_NEXT(ring, idx), n = 1; n < slot->groupSize;
				 i = RING_NEXT(ring, i), n++)
			{
				Assert(ring->slots[i].state == PREFETCH_SLOT_ASYNC);
				ring->slots[i].state = PREFETCH_SLOT_DONE;
			}
			memcpy(dest, prefetch_slot_buffer(ring, idx), ORIOLEDB_BLCKSZ);
		}
		prefetch_slot_clear(slot);

		if (aioResult.status == PGAIO_RS_UNKNOWN)
			elog(ERROR, "orioledb async read finished with no result");
		if (aioResult.status != PGAIO_RS_OK)
			pgaio_result_report(aioResult, &slot->ret.target_data, ERROR);

		*result_out = btree_finish_plain_page_read(dest);
	}
	else if (slot->state == PREFETCH_SLOT_DONE)
	{
		memcpy(dest, prefetch_slot_buffer(ring, idx), ORIOLEDB_BLCKSZ);
		prefetch_slot_clear(slot);
		*result_out = btree_finish_plain_page_read(dest);
	}
	else
	{
		FileExtent	extent;

		Assert(slot->state == PREFETCH_SLOT_DEFERRED);
		prefetch_slot_clear(slot);
		*result_out = read_page_from_disk(desc, dest, slot->downlink, &extent);
	}

	return true;
}

#endif							/* PG_VERSION_NUM >= 180000 */

#ifdef IS_DEV
/*
 * Number of shared prefetch buffers held by any backend, for tests that a
 * scan releases them however it ends.  Zero when there is no pool.
 */
PG_FUNCTION_INFO_V1(orioledb_prefetch_buffers_held);

Datum
orioledb_prefetch_buffers_held(PG_FUNCTION_ARGS)
{
	int32		held = 0;

#if PG_VERSION_NUM >= 180000
	if (prefetchPool)
	{
		for (int i = 0; i < prefetchPool->nbuffers; i++)
			if (pg_atomic_read_u32(&prefetchPool->owner[i]) != 0)
				held++;
	}
#endif
	PG_RETURN_INT32(held);
}
#endif
