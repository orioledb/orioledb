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
	key.tablespace = otd->tablespace;

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
 * Submit an async read of one leaf-page from a btree segment file.
 *
 * Currently only handles the simplest configuration: non-compressed btree,
 * non-S3 mode, non-mmap / non-device storage, a single block read that fits
 * inside one segment file.  Returns false if those preconditions are not
 * met (in which case the caller should fall back to the synchronous path).
 */
bool
orioledb_aio_submit_btree_read(BTreeDescr *desc,
							   uint32 chkpNum,
							   off_t byteOffset,
							   char *dest,
							   PgAioWaitRef *wref_out)
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

	ioh = pgaio_io_acquire(NULL, NULL);

	{
		OIndexKey	key = {.oids = desc->oids,.tablespace = desc->tablespace};

		path = btree_filename(key, segno, chkpNum);
	}
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	pfree(path);
	if (fd < 0)
	{
		pgaio_io_release(ioh);
		return false;
	}

	/*
	 * From here through pgaio_io_start_readv() we must keep interrupts held:
	 * the AIO subsystem asserts on this so that the submitter's fd cannot be
	 * yanked out from under it (e.g. by FD recycling triggered during
	 * interrupt processing).
	 */
	HOLD_INTERRUPTS();

	/*
	 * The ring buffer lives in this backend's process-local memory; mark the
	 * IO accordingly so the AIO subsystem doesn't dispatch it to a worker
	 * that would write to its own copy of the page.  In worker mode this
	 * forces synchronous in-process execution; in io_uring mode the IO
	 * remains async because the kernel uses the submitter's address space
	 * directly.
	 */
	pgaio_io_set_flag(ioh, PGAIO_HF_REFERENCES_LOCAL);

	pgaio_io_set_target(ioh, ORIOLEDB_AIO_TID_BTREE);
	td = pgaio_io_get_target_data(ioh);
	otd = (OrioleBtreeAioTargetData *) td->extension_bytes;
	otd->datoid = desc->oids.datoid;
	otd->reloid = desc->oids.reloid;
	otd->relnode = desc->oids.relnode;
	otd->tablespace = desc->tablespace;
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
	 * been started: an io worker, if used, has reopened the file in its own
	 * process; the synchronous fallback already executed the read by the time
	 * pgaio_io_start_readv() returns.  Either way we can release the
	 * descriptor here.
	 */
	CloseTransientFile(fd);

	return true;
}


/* --------------------------------------------------------------------------
 * Prefetch ring buffer for btree seqscan.
 * -------------------------------------------------------------------------- */

typedef enum
{
	PREFETCH_SLOT_EMPTY,
	PREFETCH_SLOT_ASYNC,		/* AIO submitted, waiting on wref */
	PREFETCH_SLOT_READY,		/* Buffer already contains page data */
} PrefetchSlotState;

typedef struct PrefetchSlot
{
	uint64		downlink;
	CommitSeqNo csn;
	uint32		chkpNum;
	PgAioWaitRef wref;
	PrefetchSlotState state;
} PrefetchSlot;

struct BTreePrefetchRing
{
	int			depth;
	int			head;			/* next slot to submit into */
	int			tail;			/* next slot to consume from */
	int			inflight;		/* number of populated slots */
	char	   *buffers;		/* depth * ORIOLEDB_BLCKSZ contiguous */
	PrefetchSlot *slots;
	MemoryContext mctx;
};

#define RING_BUFFER(ring, i) ((ring)->buffers + (i) * ORIOLEDB_BLCKSZ)

BTreePrefetchRing *
btree_prefetch_ring_create(int depth, MemoryContext mctx)
{
	BTreePrefetchRing *ring;
	MemoryContext oldcxt;

	Assert(depth > 0);

	oldcxt = MemoryContextSwitchTo(mctx);
	ring = palloc0(sizeof(BTreePrefetchRing));
	ring->depth = depth;
	ring->mctx = mctx;
	ring->buffers = MemoryContextAllocAligned(mctx,
											  (Size) depth * ORIOLEDB_BLCKSZ,
											  ORIOLEDB_BLCKSZ, 0);
	ring->slots = palloc0(sizeof(PrefetchSlot) * depth);
	for (int i = 0; i < depth; i++)
	{
		ring->slots[i].state = PREFETCH_SLOT_EMPTY;
		pgaio_wref_clear(&ring->slots[i].wref);
	}
	MemoryContextSwitchTo(oldcxt);

	return ring;
}

void
btree_prefetch_ring_destroy(BTreePrefetchRing *ring)
{
	if (ring == NULL)
		return;

	/* Drain any in-flight reads so the AIO handles are released. */
	while (ring->inflight > 0)
	{
		PrefetchSlot *slot = &ring->slots[ring->tail];

		if (slot->state == PREFETCH_SLOT_ASYNC &&
			pgaio_wref_valid(&slot->wref))
			pgaio_wref_wait(&slot->wref);

		slot->state = PREFETCH_SLOT_EMPTY;
		pgaio_wref_clear(&slot->wref);
		ring->tail = (ring->tail + 1) % ring->depth;
		ring->inflight--;
	}

	pfree(ring->slots);
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
						   uint32 chkpNum)
{
	PrefetchSlot *slot;
	uint64		offset;
	off_t		byteOffset;
	char	   *buf;
	bool		async_ok = false;

	if (ring->inflight >= ring->depth)
		return false;

	slot = &ring->slots[ring->head];
	Assert(slot->state == PREFETCH_SLOT_EMPTY);
	buf = RING_BUFFER(ring, ring->head);

	slot->downlink = downlink;
	slot->csn = csn;
	slot->chkpNum = chkpNum;

	if (!orioledb_s3_mode)
	{
		offset = DOWNLINK_GET_DISK_OFF(downlink);
		byteOffset = (off_t) offset * (off_t) ORIOLEDB_BLCKSZ;
		async_ok = orioledb_aio_submit_btree_read(desc, chkpNum, byteOffset,
												  buf, &slot->wref);
	}

	if (async_ok)
	{
		slot->state = PREFETCH_SLOT_ASYNC;
	}
	else
	{
		FileExtent	tmp;

		/*
		 * AIO path unavailable for this configuration -- fall back to a
		 * synchronous read into the ring buffer so the consumer interface
		 * stays uniform.
		 */
		if (!read_page_from_disk(desc, buf, downlink, &tmp))
			elog(ERROR,
				 "could not read leaf page from disk during prefetch fallback");
		slot->state = PREFETCH_SLOT_READY;
	}

	ring->head = (ring->head + 1) % ring->depth;
	ring->inflight++;
	return true;
}

bool
btree_prefetch_ring_consume(BTreePrefetchRing *ring,
							uint64 *downlink_out,
							CommitSeqNo *csn_out,
							char **page_out)
{
	PrefetchSlot *slot;
	char	   *buf;

	if (ring->inflight == 0)
		return false;

	slot = &ring->slots[ring->tail];
	buf = RING_BUFFER(ring, ring->tail);

	if (slot->state == PREFETCH_SLOT_ASYNC)
	{
		pgaio_wref_wait(&slot->wref);
		pgaio_wref_clear(&slot->wref);
	}

	*downlink_out = slot->downlink;
	*csn_out = slot->csn;
	*page_out = buf;

	slot->state = PREFETCH_SLOT_EMPTY;
	ring->tail = (ring->tail + 1) % ring->depth;
	ring->inflight--;

	return true;
}

#endif							/* PG_VERSION_NUM >= 180000 */
