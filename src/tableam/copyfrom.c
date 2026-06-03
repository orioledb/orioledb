/*-------------------------------------------------------------------------
 *
 * copyfrom.c
 *		Bulk-insert path for COPY FROM with minimal undo.
 *
 * orioledb_multi_insert sets a per-backend "bulk insert mode" flag around
 * the inserts.  When set, the leaf-split path marks every newly allocated
 * right page as bulk-fresh: pages that were just split off can only be
 * reached via a parent downlink set during the same transaction, so
 * subsequent inserts onto them can skip undo (rollback by removing the
 * downlink suffices).  The set of fresh blknos is recorded per backend so
 * a concurrent backend, lacking the entry, still emits normal undo; the
 * PAGE_DESC_FLAG_BULK_FRESH bit is just a cheap O(1) "maybe fresh" filter.
 *
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/copyfrom.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "tableam/copyfrom.h"
#include "tableam/handler.h"
#include "tableam/operations.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/page_pool.h"

#include "access/xact.h"

bool		orioledb_bulk_insert = true;
bool		orioledb_bulk_insert_mode = false;

/*
 * Fixed-size; overflow is benign — pages past the limit don't get tracked
 * per-backend, so inserts on them just emit normal undo.  4096 leaves at
 * ORIOLEDB_BLCKSZ covers a ~32 MB run between commits.
 */
#define ORIOLEDB_MAX_BULK_FRESH 4096
static OInMemoryBlkno bulk_fresh_blknos[ORIOLEDB_MAX_BULK_FRESH];
static int	n_bulk_fresh = 0;
static bool xact_callback_registered = false;

static void
bulk_insert_xact_callback(XactEvent event, void *arg)
{
	int			i;

	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT &&
		event != XACT_EVENT_PARALLEL_COMMIT &&
		event != XACT_EVENT_PARALLEL_ABORT &&
		event != XACT_EVENT_PREPARE)
		return;

	for (i = 0; i < n_bulk_fresh; i++)
	{
		OInMemoryBlkno blkno = bulk_fresh_blknos[i];

		if (OInMemoryBlknoIsValid(blkno))
			O_GET_IN_MEMORY_PAGEDESC(blkno)->flags &= ~PAGE_DESC_FLAG_BULK_FRESH;
	}
	n_bulk_fresh = 0;
	orioledb_bulk_insert_mode = false;
}

bool
orioledb_is_my_bulk_fresh_blkno(OInMemoryBlkno blkno)
{
	int			i;

	for (i = 0; i < n_bulk_fresh; i++)
		if (bulk_fresh_blknos[i] == blkno)
			return true;
	return false;
}

/*
 * Safe to call inside a critical section: no palloc, no error paths.
 * RegisterXactCallback is hit lazily; it must be called from outside a crit
 * section, so we arm it from orioledb_multi_insert below before any split
 * can fire.
 */
void
orioledb_register_bulk_fresh_blkno(OInMemoryBlkno blkno)
{
	if (n_bulk_fresh >= ORIOLEDB_MAX_BULK_FRESH)
		return;
	bulk_fresh_blknos[n_bulk_fresh++] = blkno;
	O_GET_IN_MEMORY_PAGEDESC(blkno)->flags |= PAGE_DESC_FLAG_BULK_FRESH;
}

void
orioledb_multi_insert(Relation rel, TupleTableSlot **slots, int ntuples,
					  CommandId cid, int options, BulkInsertState bistate)
{
	OTableDescr *descr;
	OSnapshot	oSnapshot;
	OXid		oxid;
	bool		saved_mode = orioledb_bulk_insert_mode;
	int			i;

	if (OidIsValid(rel->rd_rel->relrewrite))
		return;

	if (orioledb_bulk_insert)
		orioledb_bulk_insert_mode = true;

	if (!xact_callback_registered)
	{
		RegisterXactCallback(bulk_insert_xact_callback, NULL);
		xact_callback_registered = true;
	}

	o_serializable_lock_relation(RelationGetRelid(rel));
	o_set_current_command(cid);
	descr = relation_get_descr(rel);
	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	for (i = 0; i < ntuples; i++)
		(void) o_tbl_insert(descr, rel, slots[i], oxid, oSnapshot.csn);

	orioledb_bulk_insert_mode = saved_mode;
}
