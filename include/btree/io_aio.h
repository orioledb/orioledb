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
 * Submit a prefetch read of the page referenced by `downlink` (raw form from
 * BTreeSeqScanDiskDownlink) belonging to checkpoint `chkpNum`.  Returns true
 * if the slot has been claimed (whether the underlying AIO went async or was
 * served synchronously by btree_smgr_read).  Returns false only when the
 * ring is already full.
 */
extern bool btree_prefetch_ring_submit(BTreePrefetchRing *ring,
									   BTreeDescr *desc,
									   uint64 downlink,
									   CommitSeqNo csn,
									   uint32 chkpNum);

/*
 * Consume the oldest slot.  Waits for any outstanding AIO.  Returns false
 * if the ring is empty.  On success, *downlink_out and *csn_out are filled
 * with the values originally passed to submit, and *page_out points to a
 * ring-owned buffer that stays valid until the next consume call or until
 * the ring is destroyed.
 */
extern bool btree_prefetch_ring_consume(BTreePrefetchRing *ring,
										uint64 *downlink_out,
										CommitSeqNo *csn_out,
										char **page_out);

/*
 * Submit an async read of one page from btree segment file.
 *
 * - desc       : btree being read (used to derive datoid/reloid/relnode/tspc)
 * - chkpNum    : checkpoint number associated with the downlink
 * - byteOffset : absolute byte offset within the logical btree file
 * - dest       : destination buffer, must be ORIOLEDB_BLCKSZ bytes
 * - wref_out   : populated with a wait reference the caller can later wait on
 *
 * Returns true if the read was successfully submitted asynchronously, or
 * false if the call falls back to / completed synchronously (e.g. because
 * the AIO subsystem chose to execute the IO in the issuing process).  In the
 * synchronous case `*wref_out` is left invalid.
 */
extern bool orioledb_aio_submit_btree_read(BTreeDescr *desc,
										   uint32 chkpNum,
										   off_t byteOffset,
										   char *dest,
										   PgAioWaitRef *wref_out);

#endif							/* PG_VERSION_NUM >= 180000 */

#endif							/* __BTREE_IO_AIO_H__ */
