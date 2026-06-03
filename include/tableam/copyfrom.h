/*-------------------------------------------------------------------------
 *
 * copyfrom.h
 *		Bulk-insert path for COPY FROM.
 *
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/copyfrom.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __ORIOLEDB_COPYFROM_H__
#define __ORIOLEDB_COPYFROM_H__

#include "postgres.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "utils/relcache.h"

/* GUC */
extern bool orioledb_bulk_insert;

/* True while a COPY FROM (or other bulk path) is running in this backend. */
extern bool orioledb_bulk_insert_mode;

/*
 * Called by the split path when it allocates a fresh right page during a
 * bulk insert.  Records the blkno so this backend's btree_insert_skip_undo
 * can recognise it, and arms the txn-end callback that clears the marker.
 */
extern void orioledb_register_bulk_fresh_blkno(OInMemoryBlkno blkno);

/* True if blkno was marked bulk-fresh by *this* backend in the current txn. */
extern bool orioledb_is_my_bulk_fresh_blkno(OInMemoryBlkno blkno);

/* The multi_insert table AM callback. */
extern void orioledb_multi_insert(Relation rel, TupleTableSlot **slots,
								  int ntuples, CommandId cid, int options,
								  BulkInsertState bistate);

#endif							/* __ORIOLEDB_COPYFROM_H__ */
