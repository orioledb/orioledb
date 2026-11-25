/*-------------------------------------------------------------------------
 *
 * io_aio.h
 *		PG18 AIO integration for orioledb btree leaf reads.
 *
 * Registers an extension-reserved PgAioTargetID + PgAioHandleCallbackID with
 * the PG18 AIO subsystem so that orioledb's btree seqscan can prefetch leaf
 * pages through PG's io_workers / io_uring without going through the shared
 * buffer manager.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/io_aio.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_IO_AIO_H__
#define __BTREE_IO_AIO_H__

#include "postgres.h"

#include "btree.h"
#include "io.h"
#include "orioledb.h"

/*
 * Everything below depends on the AIO subsystem introduced in PostgreSQL 18.
 * On older majors this header is intentionally empty so it can be safely
 * #included unconditionally from the orioledb sources.
 */
#if PG_VERSION_NUM >= 180000

#include "storage/aio.h"
#include "storage/aio_types.h"

/*
 * Slot assignments inside PG's extension-reserved AIO ID ranges.  Centralized
 * here so that future orioledb subsystems claiming additional slots can avoid
 * collisions.
 */
#define ORIOLEDB_AIO_TID_BTREE		PGAIO_TID_EXT_1
#define ORIOLEDB_AIO_HCB_BTREE_READV PGAIO_HCB_EXT_1

/*
 * View overlay for PgAioTargetData.extension_bytes used by the orioledb btree
 * AIO target.  Carries everything pgaio_io_reopen() needs to recover the
 * underlying segment file without holding a pointer to the BTreeDescr (which
 * is per-process).
 */
typedef struct OrioleBtreeAioTargetData
{
	Oid			datoid;
	Oid			reloid;
	Oid			relnode;
	Oid			tablespace;
	uint32		chkpNum;
	uint16		segno;			/* segment file index */
	uint8		index_type;		/* OIndexType */
	uint8		flags;			/* reserved for future use */
} OrioleBtreeAioTargetData;

StaticAssertDecl(sizeof(OrioleBtreeAioTargetData) <=
				 sizeof(((PgAioTargetData *) 0)->extension_bytes),
				 "OrioleBtreeAioTargetData does not fit in PgAioTargetData "
				 "extension_bytes");

/* Initialisation: registers the target + callback descriptors. */
extern void orioledb_aio_init(void);

/* Shared pool of prefetch buffers, allocated for io_method = worker only */
extern Size btree_prefetch_shmem_needs(void);
extern void btree_prefetch_shmem_init(Pointer ptr, bool found);


/*
 * Per-scan prefetch ring buffer used by btree seqscan to issue async reads
 * of leaf pages ahead of consumption.  Treat the struct as opaque.
 */
typedef struct BTreePrefetchRing BTreePrefetchRing;

extern BTreePrefetchRing *btree_prefetch_ring_create(int depth,
													 MemoryContext mctx);
extern void btree_prefetch_ring_destroy(BTreePrefetchRing *ring);
extern int	btree_prefetch_ring_inflight(const BTreePrefetchRing *ring);
extern int	btree_prefetch_ring_depth(const BTreePrefetchRing *ring);

/*
 * Queue a read of the page referenced by `downlink` (raw form from
 * BTreeSeqScanDiskDownlink) belonging to checkpoint `chkpNum`.  Starts an
 * async read when the configuration allows one; otherwise the page is read
 * synchronously when consumed.  Returns false only when the ring is full.
 */
extern bool btree_prefetch_ring_submit(BTreePrefetchRing *ring,
									   BTreeDescr *desc,
									   uint64 downlink,
									   CommitSeqNo csn,
									   bool fullyInRange,
									   uint32 chkpNum);

/*
 * Consume the oldest queued page into dest, waiting for its read.  Returns
 * false if the ring is empty.  On success, *downlink_out, *csn_out and
 * *fullyInRange_out are the values passed to submit, and *result_out tells
 * whether the page read and verified as read_page_from_disk() would report
 * it.  A failed async read raises an error.
 */
extern bool btree_prefetch_ring_consume(BTreePrefetchRing *ring,
										BTreeDescr *desc,
										Pointer dest,
										uint64 *downlink_out,
										CommitSeqNo *csn_out,
										bool *fullyInRange_out,
										OReadPageResult *result_out);

#endif							/* PG_VERSION_NUM >= 180000 */

#endif							/* __BTREE_IO_AIO_H__ */
