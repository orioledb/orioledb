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

	return psprintf(_("orioledb segment db=%u rel=%u relnode=%u seg=%u chkp=%u"),
					otd->datoid, otd->reloid, otd->relnode,
					(uint32) otd->segno, otd->chkpNum);
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
		result.result != ORIOLEDB_BLCKSZ)
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
						desc, result.result, ORIOLEDB_BLCKSZ)));
}

/*
 * Submit an async read of one leaf-page from a btree segment file into dest.
 * localBuffer says whether dest lives in this backend's memory, which keeps
 * the read in this process; otherwise it is in shared memory, which an io
 * worker can write to.  *ret receives the outcome once the read is reclaimed.
 *
 * Currently only handles the simplest configuration: non-compressed btree,
 * non-S3 mode, non-mmap / non-device storage, a single block read that fits
 * inside one segment file.  Returns false if those preconditions are not
 * met (in which case the caller should fall back to the synchronous path).
 */
static bool
orioledb_aio_submit_btree_read(BTreeDescr *desc,
							   uint32 chkpNum,
							   off_t byteOffset,
							   char *dest,
							   bool localBuffer,
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

	/* Bail out for configurations we do not yet support. */
	if (use_mmap || use_device || orioledb_s3_mode)
		return false;
	if (OCompressIsValid(desc->compress))
		return false;

	segno = byteOffset / ORIOLEDB_SEGMENT_SIZE;
	segOffset = byteOffset % ORIOLEDB_SEGMENT_SIZE;
	if (segOffset + ORIOLEDB_BLCKSZ > ORIOLEDB_SEGMENT_SIZE)
		return false;			/* spans segments — fall back */

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
	if (localBuffer)
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
	otd->flags = 0;

	pgaio_io_register_callbacks(ioh, ORIOLEDB_AIO_HCB_BTREE_READV, 0);
	pgaio_io_get_wref(ioh, wref_out);

	(void) pgaio_io_get_iovec(ioh, &iov);
	iov[0].iov_base = dest;
	iov[0].iov_len = ORIOLEDB_BLCKSZ;

	pgaio_io_start_readv(ioh, fd, 1, segOffset);

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
	PREFETCH_SLOT_ASYNC,		/* AIO submitted, waiting on wref */
	PREFETCH_SLOT_DEFERRED,		/* no AIO possible: read it at consume */
} PrefetchSlotState;

typedef struct PrefetchSlot
{
	uint64		downlink;
	CommitSeqNo csn;
	bool		fullyInRange;
	uint32		chkpNum;
	PgAioWaitRef wref;
	PgAioReturn ret;
	int			poolBuffer;		/* pool buffer read into, or -1 */
	PrefetchSlotState state;
} PrefetchSlot;

struct BTreePrefetchRing
{
	int			depth;
	int			head;			/* next slot to submit into */
	int			tail;			/* next slot to consume from */
	int			inflight;		/* number of populated slots */
	bool		usePool;		/* read into the shared pool */
	char	   *buffers;		/* depth * ORIOLEDB_BLCKSZ, unless usePool */
	PrefetchSlot *slots;
	MemoryContext mctx;
};

#define RING_BUFFER(ring, i) ((ring)->buffers + (i) * ORIOLEDB_BLCKSZ)

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
	slot->state = PREFETCH_SLOT_EMPTY;
}

void
btree_prefetch_ring_destroy(BTreePrefetchRing *ring)
{
	if (ring == NULL)
		return;

	/* Drain any in-flight reads so the AIO handles and buffers are released */
	while (ring->inflight > 0)
	{
		prefetch_slot_clear(&ring->slots[ring->tail]);
		ring->tail = (ring->tail + 1) % ring->depth;
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
btree_prefetch_ring_submit(BTreePrefetchRing *ring,
						   BTreeDescr *desc,
						   uint64 downlink,
						   CommitSeqNo csn,
						   bool fullyInRange,
						   uint32 chkpNum)
{
	PrefetchSlot *slot;
	char	   *buf = NULL;
	off_t		byteOffset;

	if (ring->inflight >= ring->depth)
		return false;

	slot = &ring->slots[ring->head];
	Assert(slot->state == PREFETCH_SLOT_EMPTY);
	Assert(slot->poolBuffer < 0);

	slot->downlink = downlink;
	slot->csn = csn;
	slot->fullyInRange = fullyInRange;
	slot->chkpNum = chkpNum;
	slot->state = PREFETCH_SLOT_DEFERRED;

	if (ring->usePool)
	{
		slot->poolBuffer = prefetch_pool_acquire();
		if (slot->poolBuffer >= 0)
			buf = POOL_BUFFER(slot->poolBuffer);
	}
	else
		buf = RING_BUFFER(ring, ring->head);

	byteOffset = (off_t) DOWNLINK_GET_DISK_OFF(downlink) * ORIOLEDB_BLCKSZ;
	if (buf &&
		orioledb_aio_submit_btree_read(desc, chkpNum, byteOffset, buf,
									   !ring->usePool, &slot->wref,
									   &slot->ret))
	{
		slot->state = PREFETCH_SLOT_ASYNC;
		if (slot->poolBuffer >= 0)
			heldBufferWrefs[slot->poolBuffer] = slot->wref;
	}
	else if (slot->poolBuffer >= 0)
	{
		prefetch_pool_release(slot->poolBuffer);
		slot->poolBuffer = -1;
	}

	ring->head = (ring->head + 1) % ring->depth;
	ring->inflight++;
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

	if (ring->inflight == 0)
		return false;

	slot = &ring->slots[ring->tail];
	ring->tail = (ring->tail + 1) % ring->depth;
	ring->inflight--;

	*downlink_out = slot->downlink;
	*csn_out = slot->csn;
	*fullyInRange_out = slot->fullyInRange;

	if (slot->state == PREFETCH_SLOT_ASYNC)
	{
		PgAioResult aioResult;

		pgaio_wref_wait(&slot->wref);
		aioResult = slot->ret.result;
		if (aioResult.status == PGAIO_RS_OK)
			memcpy(dest, slot->poolBuffer >= 0 ?
				   POOL_BUFFER(slot->poolBuffer) :
				   RING_BUFFER(ring, slot - ring->slots),
				   ORIOLEDB_BLCKSZ);
		prefetch_slot_clear(slot);

		if (aioResult.status == PGAIO_RS_UNKNOWN)
			elog(ERROR, "orioledb async read finished with no result");
		if (aioResult.status != PGAIO_RS_OK)
			pgaio_result_report(aioResult, &slot->ret.target_data, ERROR);

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
