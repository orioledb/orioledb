/*-------------------------------------------------------------------------
 *
 * vacuum.c
 *		Implementation of VACUUM for OrioleDB bridged indexes.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/vacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/page_chunks.h"
#include "btree/page_contents.h"
#include "recovery/wal.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "storage/off.h"
#include "tableam/descr.h"
#include "tableam/key_range.h"
#include "tableam/vacuum.h"
#include "utils/page_pool.h"

#include "access/multixact.h"
#include "commands/dbcommands.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/pg_rusage.h"

#if PG_VERSION_NUM >= 170000
#define NUM_ITEMS(vacrel) ((vacrel)->dead_items_info->num_items)
#else
#define AmAutoVacuumWorkerProcess() IsAutoVacuumWorkerProcess()
#define NUM_ITEMS(vacrel) ((vacrel)->dead_items->num_items)
#define MAXDEADITEMS(avail_mem) \
	(((avail_mem) - offsetof(VacDeadItems, items)) / sizeof(ItemPointerData))
#endif

/* Phases of vacuum during which we report error context. */
typedef enum
{
	VACUUM_ERRCB_PHASE_UNKNOWN,
	VACUUM_ERRCB_PHASE_SCAN_HEAP,
	VACUUM_ERRCB_PHASE_VACUUM_INDEX,
	VACUUM_ERRCB_PHASE_VACUUM_HEAP,
	VACUUM_ERRCB_PHASE_INDEX_CLEANUP,
	VACUUM_ERRCB_PHASE_TRUNCATE,
} VacErrPhase;

/*
 * Threshold that controls whether we bypass index vacuuming and heap
 * vacuuming as an optimization
 */
#define BYPASS_THRESHOLD_PAGES	0.02	/* i.e. 2% of rel_pages */

/*
 * Macro to check if we are in a parallel vacuum.  If true, we are in the
 * parallel mode and the DSM segment is initialized.
 */
#define ParallelVacuumIsActive(vacrel) ((vacrel)->pvs != NULL)

typedef struct LVRelState
{
	/* Target heap relation and its indexes */
	Relation	rel;
	OTableDescr *descr;
	Relation   *indrels;
	int			nindexes;

	/* Buffer access strategy and parallel vacuum state */
	BufferAccessStrategy bstrategy;
	ParallelVacuumState *pvs;

	/* Consider index vacuuming bypass optimization? */
	bool		consider_bypass_optimization;

	/* Doing index vacuuming, index cleanup, rel truncation? */
	bool		do_index_vacuuming;
	bool		do_index_cleanup;

	/* Error reporting state */
	char	   *dbname;
	char	   *relnamespace;
	char	   *relname;
	char	   *indname;		/* Current index name */
	BlockNumber blkno;			/* used only for heap operations */
	OffsetNumber offnum;		/* used only for heap operations */
	VacErrPhase phase;
	bool		verbose;		/* VACUUM VERBOSE? */

	/*
	 * dead_items stores TIDs whose index tuples are deleted by index
	 * vacuuming. Each TID points to an LP_DEAD line pointer from a heap page
	 * that has been processed by lazy_scan_prune.  Also needed by
	 * lazy_vacuum_heap_rel, which marks the same LP_DEAD line pointers as
	 * LP_UNUSED during second heap pass.
	 *
	 * Both dead_items and dead_items_info are allocated in shared memory in
	 * parallel vacuum cases.
	 */
#if PG_VERSION_NUM >= 170000
	TidStore   *dead_items;		/* TIDs whose index tuples we'll delete */
	VacDeadItemsInfo *dead_items_info;
#else
	VacDeadItems *dead_items;	/* TIDs whose index tuples we'll delete */
#endif

	BlockNumber rel_pages;		/* total number of pages */
	BlockNumber scanned_pages;	/* # pages examined (not skipped via VM) */
	BlockNumber removed_pages;	/* # pages removed by relation truncation */

	BlockNumber lpdead_item_pages;	/* # pages with LP_DEAD items */
	BlockNumber missed_dead_pages;	/* # pages with missed dead tuples */
	BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */

	/* Statistics output by us, for table */
	double		new_rel_tuples; /* new estimated total # of tuples */
	double		new_live_tuples;	/* new estimated total # of live tuples */
	/* Statistics output by index AMs */
	IndexBulkDeleteResult **indstats;

	/* Instrumentation counters */
	int			num_index_scans;
	/* Counters that follow are only for scanned_pages */
	int64		tuples_deleted; /* # deleted from table */
	int64		lpdead_items;	/* # deleted from indexes */
	int64		live_tuples;	/* # live tuples remaining */
	int64		recently_dead_tuples;	/* # dead, but not yet removable */
	int64		missed_dead_tuples; /* # removable, but not removed */

#if PG_VERSION_NUM >= 170000
	/* State maintained by lazy_scan_heap() */
	BlockNumber current_block;
	int			offsets_count;
	OffsetNumber current_offsets[MaxOffsetNumber];
#endif
} LVRelState;

/* Struct for saving and restoring vacuum error information. */
typedef struct LVSavedErrInfo
{
	BlockNumber blkno;
	OffsetNumber offnum;
	VacErrPhase phase;
} LVSavedErrInfo;

static void dead_items_reset(LVRelState *vacrel);
static void dead_items_cleanup(LVRelState *vacrel);
static void update_vacuum_error_info(LVRelState *vacrel,
									 LVSavedErrInfo *saved_vacrel,
									 int phase, BlockNumber blkno,
									 OffsetNumber offnum);
static void restore_vacuum_error_info(LVRelState *vacrel,
									  const LVSavedErrInfo *saved_vacrel);

/*
 * Error context callback for errors occurring during vacuum.  The error
 * context messages for index phases should match the messages set in parallel
 * vacuum.  If you change this function for those phases, change
 * parallel_vacuum_error_callback() as well.
 */
static void
vacuum_error_callback(void *arg)
{
	LVRelState *errinfo = arg;

	switch (errinfo->phase)
	{
		case VACUUM_ERRCB_PHASE_SCAN_HEAP:
			if (BlockNumberIsValid(errinfo->blkno))
			{
				if (OffsetNumberIsValid(errinfo->offnum))
					errcontext("while scanning block %u offset %u of relation \"%s.%s\"",
							   errinfo->blkno, errinfo->offnum, errinfo->relnamespace, errinfo->relname);
				else
					errcontext("while scanning block %u of relation \"%s.%s\"",
							   errinfo->blkno, errinfo->relnamespace, errinfo->relname);
			}
			else
				errcontext("while scanning relation \"%s.%s\"",
						   errinfo->relnamespace, errinfo->relname);
			break;

		case VACUUM_ERRCB_PHASE_VACUUM_HEAP:
			if (BlockNumberIsValid(errinfo->blkno))
			{
				if (OffsetNumberIsValid(errinfo->offnum))
					errcontext("while vacuuming block %u offset %u of relation \"%s.%s\"",
							   errinfo->blkno, errinfo->offnum, errinfo->relnamespace, errinfo->relname);
				else
					errcontext("while vacuuming block %u of relation \"%s.%s\"",
							   errinfo->blkno, errinfo->relnamespace, errinfo->relname);
			}
			else
				errcontext("while vacuuming relation \"%s.%s\"",
						   errinfo->relnamespace, errinfo->relname);
			break;

		case VACUUM_ERRCB_PHASE_VACUUM_INDEX:
			errcontext("while vacuuming index \"%s\" of relation \"%s.%s\"",
					   errinfo->indname, errinfo->relnamespace, errinfo->relname);
			break;

		case VACUUM_ERRCB_PHASE_INDEX_CLEANUP:
			errcontext("while cleaning up index \"%s\" of relation \"%s.%s\"",
					   errinfo->indname, errinfo->relnamespace, errinfo->relname);
			break;

		case VACUUM_ERRCB_PHASE_TRUNCATE:
			if (BlockNumberIsValid(errinfo->blkno))
				errcontext("while truncating relation \"%s.%s\" to %u blocks",
						   errinfo->relnamespace, errinfo->relname, errinfo->blkno);
			break;

		case VACUUM_ERRCB_PHASE_UNKNOWN:
		default:
			return;				/* do nothing; the errinfo may not be
								 * initialized */
	}
}

static void
vac_open_bridged_indexes(Relation relation, LOCKMODE lockmode,
						 int *nindexes, Relation **Irel)
{
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	int			i;

	Assert(lockmode != NoLock);

	indexoidlist = RelationGetIndexList(relation);

	/* allocate enough memory for all indexes */
	i = list_length(indexoidlist);

	if (i > 0)
		*Irel = (Relation *) palloc(i * sizeof(Relation));
	else
		*Irel = NULL;

	/* collect just the ready indexes */
	i = 0;
	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		Relation	indrel;

		indrel = index_open(indexoid, lockmode);

		if (indrel->rd_amhandler == F_BTHANDLER)
		{
			OBTOptions *options = (OBTOptions *) indrel->rd_options;

			if (!(options && !options->orioledb_index))
			{
				index_close(indrel, lockmode);
				continue;
			}
		}

		if (!indrel->rd_index->indisready)
		{
			index_close(indrel, lockmode);
			continue;
		}

		(*Irel)[i++] = indrel;
	}

	*nindexes = i;

	list_free(indexoidlist);
}

/*
 * Allocate dead_items and dead_items_info (either using palloc, or in dynamic
 * shared memory). Sets both in vacrel for caller.
 *
 * Also handles parallel initialization as part of allocating dead_items in
 * DSM when required.
 */
static void
dead_items_alloc(LVRelState *vacrel, int nworkers)
{
#if PG_VERSION_NUM >= 170000
	VacDeadItemsInfo *dead_items_info;
	int			vac_work_mem = AmAutoVacuumWorkerProcess() &&
		autovacuum_work_mem != -1 ?
		autovacuum_work_mem : maintenance_work_mem;
#else
	VacDeadItems *dead_items;
	int64		max_items;
	int			vac_work_mem = IsAutoVacuumWorkerProcess() &&
		autovacuum_work_mem != -1 ?
		autovacuum_work_mem : maintenance_work_mem;

	if (vacrel->nindexes > 0)
	{
		BlockNumber rel_pages = vacrel->rel_pages;

		max_items = MAXDEADITEMS(vac_work_mem * 1024L);
		max_items = Min(max_items, INT_MAX);
		max_items = Min(max_items, MAXDEADITEMS(MaxAllocSize));

		/* curious coding here to ensure the multiplication can't overflow */
		if ((BlockNumber) (max_items / MaxHeapTuplesPerPage) > rel_pages)
			max_items = rel_pages * MaxHeapTuplesPerPage;

		/* stay sane if small maintenance_work_mem */
		max_items = Max(max_items, MaxHeapTuplesPerPage);
	}
	else
	{
		/* One-pass case only stores a single heap page's TIDs at a time */
		max_items = MaxHeapTuplesPerPage;
	}
#endif

	/*
	 * Initialize state for a parallel vacuum.  As of now, only one worker can
	 * be used for an index, so we invoke parallelism only if there are at
	 * least two indexes on a table.
	 */
	if (nworkers >= 0 && vacrel->nindexes > 1 && vacrel->do_index_vacuuming)
	{
		/*
		 * Since parallel workers cannot access data in temporary tables, we
		 * can't perform parallel vacuum on them.
		 */
		if (RelationUsesLocalBuffers(vacrel->rel))
		{
			/*
			 * Give warning only if the user explicitly tries to perform a
			 * parallel vacuum on the temporary table.
			 */
			if (nworkers > 0)
				ereport(WARNING,
						(errmsg("disabling parallel option of vacuum on \"%s\" --- cannot vacuum temporary tables in parallel",
								vacrel->relname)));
		}
		else
			vacrel->pvs = parallel_vacuum_init(vacrel->rel, vacrel->indrels,
											   vacrel->nindexes, nworkers,
											   vac_work_mem,
											   vacrel->verbose ? INFO : DEBUG2,
											   vacrel->bstrategy);

		/*
		 * If parallel mode started, dead_items and dead_items_info spaces are
		 * allocated in DSM.
		 */
		if (ParallelVacuumIsActive(vacrel))
		{
#if PG_VERSION_NUM >= 170000
			vacrel->dead_items = parallel_vacuum_get_dead_items(vacrel->pvs,
																&vacrel->dead_items_info);
#else
			vacrel->dead_items = parallel_vacuum_get_dead_items(vacrel->pvs);
#endif
			return;
		}
	}

#if PG_VERSION_NUM >= 170000

	/*
	 * Serial VACUUM case. Allocate both dead_items and dead_items_info
	 * locally.
	 */
	dead_items_info = (VacDeadItemsInfo *) palloc(sizeof(VacDeadItemsInfo));
	dead_items_info->max_bytes = vac_work_mem * 1024L;
	dead_items_info->num_items = 0;
	vacrel->dead_items_info = dead_items_info;

	vacrel->dead_items = TidStoreCreateLocal(dead_items_info->max_bytes, true);
#else
	/* Serial VACUUM case */
	dead_items = (VacDeadItems *) palloc(vac_max_items_to_alloc_size(max_items));
	dead_items->max_items = max_items;
	dead_items->num_items = 0;

	vacrel->dead_items = dead_items;
#endif
}

#if PG_VERSION_NUM >= 170000
static void
finish_dead_items(LVRelState *vacrel)
{
	const int	prog_index[2] = {
		PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS,
		PROGRESS_VACUUM_DEAD_TUPLE_BYTES
	};
	int64		prog_val[2];

	if (!BlockNumberIsValid(vacrel->current_block))
		return;

	TidStoreSetBlockOffsets(vacrel->dead_items, vacrel->current_block,
							vacrel->current_offsets, vacrel->offsets_count);
	vacrel->dead_items_info->num_items += vacrel->offsets_count;

	/* update the progress information */
	prog_val[0] = vacrel->dead_items_info->num_items;
	prog_val[1] = TidStoreMemoryUsage(vacrel->dead_items);
	pgstat_progress_update_multi_param(2, prog_index, prog_val);
}

static void
add_dead_item(LVRelState *vacrel, ItemPointer item)
{
	if (vacrel->current_block != ItemPointerGetBlockNumber(item))
	{
		finish_dead_items(vacrel);
		vacrel->current_block = ItemPointerGetBlockNumber(item);
		vacrel->offsets_count = 0;
	}
	vacrel->current_offsets[vacrel->offsets_count++] = ItemPointerGetOffsetNumber(item);
}
#else
static void
add_dead_item(LVRelState *vacrel, ItemPointer item)
{
	VacDeadItems *dead_items = vacrel->dead_items;

	dead_items->items[dead_items->num_items++] = *item;
}
#endif

/*
 *	lazy_cleanup_one_index() -- do post-vacuum cleanup for index relation.
 *
 *		Calls index AM's amvacuumcleanup routine.  reltuples is the number
 *		of heap tuples and estimated_count is true if reltuples is an
 *		estimated value.  See indexam.sgml for more info.
 *
 * Returns bulk delete stats derived from input stats
 */
static IndexBulkDeleteResult *
lazy_cleanup_one_index(Relation indrel, IndexBulkDeleteResult *istat,
					   double reltuples, bool estimated_count,
					   LVRelState *vacrel)
{
	IndexVacuumInfo ivinfo;
	LVSavedErrInfo saved_err_info;

	ivinfo.index = indrel;
	ivinfo.heaprel = vacrel->rel;
	ivinfo.analyze_only = false;
	ivinfo.report_progress = false;
	ivinfo.estimated_count = estimated_count;
	ivinfo.message_level = DEBUG2;

	ivinfo.num_heap_tuples = reltuples;
	ivinfo.strategy = vacrel->bstrategy;

	/*
	 * Update error traceback information.
	 *
	 * The index name is saved during this phase and restored immediately
	 * after this phase.  See vacuum_error_callback.
	 */
	Assert(vacrel->indname == NULL);
	vacrel->indname = pstrdup(RelationGetRelationName(indrel));
	update_vacuum_error_info(vacrel, &saved_err_info,
							 VACUUM_ERRCB_PHASE_INDEX_CLEANUP,
							 InvalidBlockNumber, InvalidOffsetNumber);

	istat = vac_cleanup_one_index(&ivinfo, istat);

	/* Revert to the previous phase information for error traceback */
	restore_vacuum_error_info(vacrel, &saved_err_info);
	pfree(vacrel->indname);
	vacrel->indname = NULL;

	return istat;
}

/*
 *	lazy_cleanup_all_indexes() -- cleanup all indexes of relation.
 */
static void
lazy_cleanup_all_indexes(LVRelState *vacrel)
{
	double		reltuples = vacrel->new_rel_tuples;
	bool		estimated_count = vacrel->scanned_pages < vacrel->rel_pages;
#if PG_VERSION_NUM >= 170000
	const int	progress_start_index[] = {
		PROGRESS_VACUUM_PHASE,
		PROGRESS_VACUUM_INDEXES_TOTAL
	};
	const int	progress_end_index[] = {
		PROGRESS_VACUUM_INDEXES_TOTAL,
		PROGRESS_VACUUM_INDEXES_PROCESSED
	};
	int64		progress_start_val[2];
	int64		progress_end_val[2] = {0, 0};
#endif

	Assert(vacrel->do_index_cleanup);
	Assert(vacrel->nindexes > 0);

#if PG_VERSION_NUM >= 170000

	/*
	 * Report that we are now cleaning up indexes and the number of indexes to
	 * cleanup.
	 */
	progress_start_val[0] = PROGRESS_VACUUM_PHASE_INDEX_CLEANUP;
	progress_start_val[1] = vacrel->nindexes;
	pgstat_progress_update_multi_param(2, progress_start_index, progress_start_val);
#else
	/* Report that we are now cleaning up indexes */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_INDEX_CLEANUP);
#endif

	if (!ParallelVacuumIsActive(vacrel))
	{
		for (int idx = 0; idx < vacrel->nindexes; idx++)
		{
			Relation	indrel = vacrel->indrels[idx];
			IndexBulkDeleteResult *istat = vacrel->indstats[idx];

			vacrel->indstats[idx] =
				lazy_cleanup_one_index(indrel, istat, reltuples,
									   estimated_count, vacrel);

#if PG_VERSION_NUM >= 170000
			/* Report the number of indexes cleaned up */
			pgstat_progress_update_param(PROGRESS_VACUUM_INDEXES_PROCESSED,
										 idx + 1);
#endif
		}
	}
	else
	{
		/* Outsource everything to parallel variant */
		parallel_vacuum_cleanup_all_indexes(vacrel->pvs, reltuples,
											vacrel->num_index_scans,
											estimated_count);
	}

#if PG_VERSION_NUM >= 170000
	/* Reset the progress counters */
	pgstat_progress_update_multi_param(2, progress_end_index, progress_end_val);
#endif
}

/*
 *	lazy_vacuum_one_index() -- vacuum index relation.
 *
 *		Delete all the index tuples containing a TID collected in
 *		vacrel->dead_items.  Also update running statistics. Exact
 *		details depend on index AM's ambulkdelete routine.
 *
 *		reltuples is the number of heap tuples to be passed to the
 *		bulkdelete callback.  It's always assumed to be estimated.
 *		See indexam.sgml for more info.
 *
 * Returns bulk delete stats derived from input stats
 */
static IndexBulkDeleteResult *
lazy_vacuum_one_index(Relation indrel, IndexBulkDeleteResult *istat,
					  double reltuples, LVRelState *vacrel)
{
	IndexVacuumInfo ivinfo;
	LVSavedErrInfo saved_err_info;

	ivinfo.index = indrel;
	ivinfo.heaprel = vacrel->rel;
	ivinfo.analyze_only = false;
	ivinfo.report_progress = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = DEBUG2;
	ivinfo.num_heap_tuples = reltuples;
	ivinfo.strategy = vacrel->bstrategy;

	/*
	 * Update error traceback information.
	 *
	 * The index name is saved during this phase and restored immediately
	 * after this phase.  See vacuum_error_callback.
	 */
	Assert(vacrel->indname == NULL);
	vacrel->indname = pstrdup(RelationGetRelationName(indrel));
	update_vacuum_error_info(vacrel, &saved_err_info,
							 VACUUM_ERRCB_PHASE_VACUUM_INDEX,
							 InvalidBlockNumber, InvalidOffsetNumber);

	/* Do bulk deletion */
#if PG_VERSION_NUM >= 170000
	istat = vac_bulkdel_one_index(&ivinfo, istat, vacrel->dead_items,
								  vacrel->dead_items_info);
#else
	istat = vac_bulkdel_one_index(&ivinfo, istat, vacrel->dead_items);
#endif

	/* Revert to the previous phase information for error traceback */
	restore_vacuum_error_info(vacrel, &saved_err_info);
	pfree(vacrel->indname);
	vacrel->indname = NULL;

	return istat;
}

/*
 *	lazy_vacuum_all_indexes() -- Main entry for index vacuuming
 *
 * Returns true in the common case when all indexes were successfully
 * vacuumed.  Returns false in rare cases where we determined that the ongoing
 * VACUUM operation is at risk of taking too long to finish, leading to
 * wraparound failure.
 */
static void
lazy_vacuum_all_indexes(LVRelState *vacrel)
{
	double		old_live_tuples = vacrel->rel->rd_rel->reltuples;
#if PG_VERSION_NUM >= 170000
	const int	progress_start_index[] = {
		PROGRESS_VACUUM_PHASE,
		PROGRESS_VACUUM_INDEXES_TOTAL
	};
	const int	progress_end_index[] = {
		PROGRESS_VACUUM_INDEXES_TOTAL,
		PROGRESS_VACUUM_INDEXES_PROCESSED,
		PROGRESS_VACUUM_NUM_INDEX_VACUUMS
	};
	int64		progress_start_val[2];
	int64		progress_end_val[3];
#endif

	Assert(vacrel->nindexes > 0);
	Assert(vacrel->do_index_vacuuming);
	Assert(vacrel->do_index_cleanup);

#if PG_VERSION_NUM >= 170000

	/*
	 * Report that we are now vacuuming indexes and the number of indexes to
	 * vacuum.
	 */
	progress_start_val[0] = PROGRESS_VACUUM_PHASE_VACUUM_INDEX;
	progress_start_val[1] = vacrel->nindexes;
	pgstat_progress_update_multi_param(2, progress_start_index, progress_start_val);
#else
	/* Report that we are now vacuuming indexes */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_VACUUM_INDEX);
#endif

	if (!ParallelVacuumIsActive(vacrel))
	{
		for (int idx = 0; idx < vacrel->nindexes; idx++)
		{
			Relation	indrel = vacrel->indrels[idx];
			IndexBulkDeleteResult *istat = vacrel->indstats[idx];

			vacrel->indstats[idx] = lazy_vacuum_one_index(indrel, istat,
														  old_live_tuples,
														  vacrel);
#if PG_VERSION_NUM >= 170000
			/* Report the number of indexes vacuumed */
			pgstat_progress_update_param(PROGRESS_VACUUM_INDEXES_PROCESSED,
										 idx + 1);
#endif
		}
	}
	else
	{
		/* Outsource everything to parallel variant */
		parallel_vacuum_bulkdel_all_indexes(vacrel->pvs, old_live_tuples,
											vacrel->num_index_scans);
	}

	/*
	 * We delete all LP_DEAD items from the first heap pass in all indexes on
	 * each call here (except calls where we choose to do the failsafe). This
	 * makes the next call to lazy_vacuum_heap_rel() safe (except in the event
	 * of the failsafe triggering, which prevents the next call from taking
	 * place).
	 */
	Assert(vacrel->num_index_scans > 0 ||
		   NUM_ITEMS(vacrel) == vacrel->lpdead_items);

	/*
	 * Increase and report the number of index scans.  Also, we reset
	 * PROGRESS_VACUUM_INDEXES_TOTAL and PROGRESS_VACUUM_INDEXES_PROCESSED.
	 *
	 * We deliberately include the case where we started a round of bulk
	 * deletes that we weren't able to finish due to the failsafe triggering.
	 */
	vacrel->num_index_scans++;
#if PG_VERSION_NUM >= 170000
	progress_end_val[0] = 0;
	progress_end_val[1] = 0;
	progress_end_val[2] = vacrel->num_index_scans;
	pgstat_progress_update_multi_param(3, progress_end_index, progress_end_val);
#else
	pgstat_progress_update_param(PROGRESS_VACUUM_NUM_INDEX_VACUUMS,
								 vacrel->num_index_scans);
#endif
}

/*
 *	lazy_vacuum_heap_rel() -- second pass over the heap for two pass strategy
 *
 * This routine marks LP_DEAD items in vacrel->dead_items as LP_UNUSED. Pages
 * that never had lazy_scan_prune record LP_DEAD items are not visited at all.
 *
 * We may also be able to truncate the line pointer array of the heap pages we
 * visit.  If there is a contiguous group of LP_UNUSED items at the end of the
 * array, it can be reclaimed as free space.  These LP_UNUSED items usually
 * start out as LP_DEAD items recorded by lazy_scan_prune (we set items from
 * each page to LP_UNUSED, and then consider if it's possible to truncate the
 * page's line pointer array).
 *
 * Note: the reason for doing this as a second pass is we cannot remove the
 * tuples until we've removed their index entries, and we want to process
 * index entry removal in batches as large as possible.
 */
static void
lazy_vacuum_brige_index(LVRelState *vacrel)
{
	OBTreeFindPageContext context;
	OTableDescr *descr = vacrel->descr;
	OIndexDescr *bridge = descr->bridge;
	BlockNumber vacuumed_pages = 0;
	LVSavedErrInfo saved_err_info;
#if PG_VERSION_NUM >= 170000
	TidStoreIter *iter;
	TidStoreIterResult *iter_result;
#endif
	OBTreeKeyBound bound;
	ItemPointerData walBuffer[BTREE_PAGE_MAX_ITEMS];
	int			walBufferIndex = 0;
	bool		have_page = false;
	int			i;

	init_page_find_context(&context, &bridge->desc,
						   COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY);

	bound.nkeys = 1;
	bound.n_row_keys = 0;
	bound.row_keys = NULL;
	bound.keys[0].type = TIDOID;
	bound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
	bound.keys[0].comparator = bridge->fields[0].comparator;

	Assert(vacrel->do_index_vacuuming);
	Assert(vacrel->do_index_cleanup);

	/* Report that we are now vacuuming the heap */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_VACUUM_HEAP);

	/* Update error traceback information */
	update_vacuum_error_info(vacrel, &saved_err_info,
							 VACUUM_ERRCB_PHASE_VACUUM_HEAP,
							 InvalidBlockNumber, InvalidOffsetNumber);
	vacrel->blkno = InvalidBlockNumber;

#if PG_VERSION_NUM >= 170000
	iter = TidStoreBeginIterate(vacrel->dead_items);
	while ((iter_result = TidStoreIterateNext(iter)) != NULL)
	{
		ItemPointerData iptr;
		BlockNumber blkno;
		OTuple		tuple;

		vacuum_delay_point();

		blkno = iter_result->blkno;
		vacrel->blkno = blkno;

		ItemPointerSetBlockNumber(&iptr, blkno);
		for (i = 0; i < iter_result->num_offsets; i++)
		{
			OBtreePageFindItem *item;
			Page		p;

			ItemPointerSetOffsetNumber(&iptr, iter_result->offsets[i]);
			bound.keys[0].value = ItemPointerGetDatum(&iptr);
#else
	for (i = 0; i < vacrel->dead_items->num_items; i++)
	{
		ItemPointerData iptr = vacrel->dead_items->items[i];
		BlockNumber blkno;
		OTuple		tuple;

		blkno = ItemPointerGetBlockNumber(&iptr);
		vacrel->blkno = blkno;

		bound.keys[0].value = ItemPointerGetDatum(&iptr);

		{
			OBtreePageFindItem *item;
			Page		p;
#endif

			if (!have_page)
			{
				OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;

				findResult = find_page(&context, &bound, BTreeKeyBound, 0);
				Assert(findResult == OFindPageResultSuccess);
				have_page = true;
				item = &context.items[context.index];
				p = O_GET_IN_MEMORY_PAGE(item->blkno);
			}
			else
			{
				bool		pageMatch;

				item = &context.items[context.index];
				p = O_GET_IN_MEMORY_PAGE(item->blkno);
				if (O_PAGE_IS(p, RIGHTMOST))
				{
					pageMatch = true;
				}
				else
				{
					OTuple		pageHiKey;

					BTREE_PAGE_GET_HIKEY(pageHiKey, p);
					pageMatch = (o_btree_cmp(&bridge->desc,
											 &bound, BTreeKeyBound,
											 &pageHiKey, BTreeKeyNonLeafKey) < 0);
				}

				if (pageMatch)
				{
					bool		found;

					found = btree_page_search(&bridge->desc, p,
											  (Pointer) &bound, BTreeKeyBound,
											  NULL, &item->locator);

					if (!found)
						continue;
				}
				else
				{
					OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;

					if (have_page)
					{
						int			j;

						for (j = 0; j < walBufferIndex; j++)
							add_bridge_erase_wal_record(&bridge->desc,
														&walBuffer[j]);
						walBufferIndex = 0;
						vacuumed_pages++;
						unlock_page(context.items[context.index].blkno);
					}

					findResult = find_page(&context, &bound, BTreeKeyBound, 0);
					Assert(findResult == OFindPageResultSuccess);

					have_page = true;
					item = &context.items[context.index];
					p = O_GET_IN_MEMORY_PAGE(item->blkno);
				}
			}

			if (!BTREE_PAGE_LOCATOR_IS_VALID(p, &item->locator))
				continue;

			BTREE_PAGE_READ_TUPLE(tuple, p, &item->locator);

			if (o_btree_cmp(&bridge->desc,
							&bound, BTreeKeyBound,
							&tuple, BTreeKeyLeafTuple) != 0)
				continue;

			START_CRIT_SECTION();
			page_block_reads(item->blkno);
			page_locator_delete_item(p, &item->locator);
			MARK_DIRTY(&bridge->desc, item->blkno);
			END_CRIT_SECTION();
			Assert(walBufferIndex < BTREE_PAGE_MAX_ITEMS);
			walBuffer[walBufferIndex++] = iptr;
		}

#if PG_VERSION_NUM >= 170000
	}
	TidStoreEndIterate(iter);
#else
	}
#endif

	if (have_page)
	{
		int			j;

		for (j = 0; j < walBufferIndex; j++)
			add_bridge_erase_wal_record(&bridge->desc,
										&walBuffer[j]);
		walBufferIndex = 0;
		vacuumed_pages++;
		unlock_page(context.items[context.index].blkno);
	}

	/*
	 * We set all LP_DEAD items from the first heap pass to LP_UNUSED during
	 * the second heap pass.  No more, no less.
	 */
	Assert(vacrel->num_index_scans > 1 ||
		   (NUM_ITEMS(vacrel) == vacrel->lpdead_items &&
			vacuumed_pages == vacrel->lpdead_item_pages));

	ereport(DEBUG2,
			(errmsg("table \"%s\": removed %lld dead item identifiers in %u pages",
					vacrel->relname, (long long) NUM_ITEMS(vacrel),
					vacuumed_pages)));

	/* Revert to the previous phase information for error traceback */
	restore_vacuum_error_info(vacrel, &saved_err_info);
}

/*
 * Main entry point for index vacuuming and heap vacuuming.
 *
 * Removes items collected in dead_items from table's indexes, then marks the
 * same items LP_UNUSED in the heap.  See the comments above lazy_scan_heap
 * for full details.
 *
 * Also empties dead_items, freeing up space for later TIDs.
 *
 * We may choose to bypass index vacuuming at this point, though only when the
 * ongoing VACUUM operation will definitely only have one index scan/round of
 * index vacuuming.
 */
static void
lazy_vacuum(LVRelState *vacrel)
{
	bool		bypass;

	/*
	 * Consider bypassing index vacuuming (and heap vacuuming) entirely.
	 *
	 * We currently only do this in cases where the number of LP_DEAD items
	 * for the entire VACUUM operation is close to zero.  This avoids sharp
	 * discontinuities in the duration and overhead of successive VACUUM
	 * operations that run against the same table with a fixed workload.
	 * Ideally, successive VACUUM operations will behave as if there are
	 * exactly zero LP_DEAD items in cases where there are close to zero.
	 *
	 * This is likely to be helpful with a table that is continually affected
	 * by UPDATEs that can mostly apply the HOT optimization, but occasionally
	 * have small aberrations that lead to just a few heap pages retaining
	 * only one or two LP_DEAD items.  This is pretty common; even when the
	 * DBA goes out of their way to make UPDATEs use HOT, it is practically
	 * impossible to predict whether HOT will be applied in 100% of cases.
	 * It's far easier to ensure that 99%+ of all UPDATEs against a table use
	 * HOT through careful tuning.
	 */
	bypass = false;
	if (vacrel->consider_bypass_optimization && vacrel->rel_pages > 0)
	{
		BlockNumber threshold;

		Assert(vacrel->num_index_scans == 0);
		Assert(vacrel->lpdead_items == NUM_ITEMS(vacrel));
		Assert(vacrel->do_index_vacuuming);
		Assert(vacrel->do_index_cleanup);

		/*
		 * This crossover point at which we'll start to do index vacuuming is
		 * expressed as a percentage of the total number of heap pages in the
		 * table that are known to have at least one LP_DEAD item.  This is
		 * much more important than the total number of LP_DEAD items, since
		 * it's a proxy for the number of heap pages whose visibility map bits
		 * cannot be set on account of bypassing index and heap vacuuming.
		 *
		 * We apply one further precautionary test: the space currently used
		 * to store the TIDs (TIDs that now all point to LP_DEAD items) must
		 * not exceed 32MB.  This limits the risk that we will bypass index
		 * vacuuming again and again until eventually there is a VACUUM whose
		 * dead_items space is not CPU cache resident.
		 *
		 * We don't take any special steps to remember the LP_DEAD items (such
		 * as counting them in our final update to the stats system) when the
		 * optimization is applied.  Though the accounting used in analyze.c's
		 * acquire_sample_rows() will recognize the same LP_DEAD items as dead
		 * rows in its own stats report, that's okay. The discrepancy should
		 * be negligible.  If this optimization is ever expanded to cover more
		 * cases then this may need to be reconsidered.
		 */
		threshold = (double) vacrel->rel_pages * BYPASS_THRESHOLD_PAGES;
#if PG_VERSION_NUM >= 170000
		bypass = (vacrel->lpdead_item_pages < threshold &&
				  (TidStoreMemoryUsage(vacrel->dead_items) < (32L * 1024L * 1024L)));
#else
		bypass = (vacrel->lpdead_item_pages < threshold &&
				  vacrel->lpdead_items < MAXDEADITEMS(32L * 1024L * 1024L));
#endif
	}

	if (bypass)
	{
		/*
		 * There are almost zero TIDs.  Behave as if there were precisely
		 * zero: bypass index vacuuming, but do index cleanup.
		 *
		 * We expect that the ongoing VACUUM operation will finish very
		 * quickly, so there is no point in considering speeding up as a
		 * failsafe against wraparound failure. (Index cleanup is expected to
		 * finish very quickly in cases where there were no ambulkdelete()
		 * calls.)
		 */
		vacrel->do_index_vacuuming = false;
	}
	else
	{
		if (vacrel->nindexes > 0)
			lazy_vacuum_all_indexes(vacrel);
		lazy_vacuum_brige_index(vacrel);
	}

	/*
	 * Forget the LP_DEAD items that we just vacuumed (or just decided to not
	 * vacuum)
	 */
	dead_items_reset(vacrel);
}

static void
lazy_scan_bridge_index(LVRelState *vacrel)
{
	OBTreeFindPageContext context;
	OTableDescr *descr = vacrel->descr;
	OIndexDescr *bridge = descr->bridge;
	OFixedKey	hikey;
	BTreePageItemLocator loc;
	int64		blocksScanned = 0;
	OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;

	Assert(bridge != NULL);

#if PG_VERSION_NUM >= 170000
	vacrel->current_block = InvalidBlockNumber;
	vacrel->offsets_count = 0;
#endif

	init_page_find_context(&context, &bridge->desc,
						   COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_IMAGE);

	findResult = find_page(&context, NULL, BTreeKeyNone, 0);
	Assert(findResult == OFindPageResultSuccess);

	do
	{
		Page		p = context.img;
		bool		firstDead = true;

		/* Report as block scanned, update error traceback information */

		/*
		 * pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED,
		 * blkno); update_vacuum_error_info(vacrel, NULL,
		 * VACUUM_ERRCB_PHASE_SCAN_HEAP, blkno, InvalidOffsetNumber);
		 *
		 * vacuum_delay_point();
		 */
		for (loc = context.items[context.index].locator;
			 BTREE_PAGE_LOCATOR_IS_VALID(p, &loc);
			 BTREE_PAGE_LOCATOR_NEXT(p, &loc))
		{
			BTreeLeafTuphdr *tupHdr;
			OTuple		tup;
			ItemPointer iptr;
			bool		isnull;
			bool		tuple_can_be_vaccumed;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tup, p, &loc);

			tuple_can_be_vaccumed = XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo);

			if (tupHdr->deleted != BTreeLeafTupleNonDeleted &&
				tuple_can_be_vaccumed)
			{
				Assert(tupHdr->deleted == BTreeLeafTupleDeleted);

				/* cppcheck-suppress unknownEvaluationOrder */
				iptr = DatumGetItemPointer(o_fastgetattr(tup, 1, bridge->leafTupdesc, &bridge->leafSpec, &isnull));
				add_dead_item(vacrel, iptr);
				vacrel->lpdead_items++;

#if PG_VERSION_NUM >= 170000
				if (TidStoreMemoryUsage(vacrel->dead_items) > vacrel->dead_items_info->max_bytes)
#else
				if (vacrel->dead_items->num_items >= vacrel->dead_items->max_items)
#endif
				{
					/* Perform a round of index and heap vacuuming */
					vacrel->consider_bypass_optimization = false;
					lazy_vacuum(vacrel);

					/* Report that we are once again scanning the heap */
					pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
												 PROGRESS_VACUUM_PHASE_SCAN_HEAP);
				}

				if (firstDead)
				{
					vacrel->lpdead_item_pages++;
					firstDead = false;
				}
			}
			else
			{
				vacrel->live_tuples++;
			}
		}
		blocksScanned++;
	} while (find_right_page(&context, &hikey));

#if PG_VERSION_NUM >= 170000
	finish_dead_items(vacrel);
#endif

	/* report that everything is now scanned */
	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blocksScanned);

	/*
	 * Also compute the total number of surviving heap entries.  In the
	 * (unlikely) scenario that new_live_tuples is -1, take it as zero.
	 */
	vacrel->new_rel_tuples =
		Max(vacrel->new_live_tuples, 0) + vacrel->recently_dead_tuples +
		vacrel->missed_dead_tuples;

	/*
	 * Do index vacuuming (call each index's ambulkdelete routine), then do
	 * related heap vacuuming
	 */
	if (NUM_ITEMS(vacrel) > 0)
		lazy_vacuum(vacrel);

	/* report all blocks vacuumed */
	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blocksScanned);

	/* Do final index cleanup (call each index's amvacuumcleanup routine) */
	if (vacrel->nindexes > 0 && vacrel->do_index_cleanup)
		lazy_cleanup_all_indexes(vacrel);
}

/*
 * Update index statistics in pg_class if the statistics are accurate.
 */
static void
update_relstats_all_indexes(LVRelState *vacrel)
{
	Relation   *indrels = vacrel->indrels;
	int			nindexes = vacrel->nindexes;
	IndexBulkDeleteResult **indstats = vacrel->indstats;

	Assert(vacrel->do_index_cleanup);

	for (int idx = 0; idx < nindexes; idx++)
	{
		Relation	indrel = indrels[idx];
		IndexBulkDeleteResult *istat = indstats[idx];

		if (istat == NULL || istat->estimated_count)
			continue;

		/* Update index statistics */
		vac_update_relstats(indrel,
							istat->num_pages,
							istat->num_index_tuples,
							0,
							false,
							InvalidTransactionId,
							InvalidMultiXactId,
							NULL, NULL, false);
	}
}

void
orioledb_vacuum_bridged_indexes(Relation rel, OTableDescr *descr,
								struct VacuumParams *params,
								BufferAccessStrategy bstrategy)
{
	LVRelState *vacrel;
	char	  **indnames = NULL;
	bool		verbose;
	bool		instrument;
	PGRUsage	ru0;
	TimestampTz starttime = 0;
	PgStat_Counter startreadtime = 0,
				startwritetime = 0;
	WalUsage	startwalusage = pgWalUsage;
	BufferUsage startbufferusage = pgBufferUsage;
	BlockNumber orig_rel_pages,
				new_rel_pages;
	OIndexDescr *bridge;
	ErrorContextCallback errcallback;

	verbose = (params->options & VACOPT_VERBOSE) != 0;
	instrument = (verbose || (AmAutoVacuumWorkerProcess() &&
							  params->log_min_duration >= 0));
	if (instrument)
	{
		pg_rusage_init(&ru0);
		starttime = GetCurrentTimestamp();
		if (track_io_timing)
		{
			startreadtime = pgStatBlockReadTime;
			startwritetime = pgStatBlockWriteTime;
		}
	}

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
								  RelationGetRelid(rel));

	/*
	 * Setup error traceback support for ereport() first.  The idea is to set
	 * up an error context callback to display additional information on any
	 * error during a vacuum.  During different phases of vacuum, we update
	 * the state so that the error context callback always display current
	 * information.
	 *
	 * Copy the names of heap rel into local memory for error reporting
	 * purposes, too.  It isn't always safe to assume that we can get the name
	 * of each rel.  It's convenient for code in lazy_scan_heap to always use
	 * these temp copies.
	 */
	vacrel = (LVRelState *) palloc0(sizeof(LVRelState));
	vacrel->dbname = get_database_name(MyDatabaseId);
	vacrel->relnamespace = get_namespace_name(RelationGetNamespace(rel));
	vacrel->relname = pstrdup(RelationGetRelationName(rel));
	vacrel->indname = NULL;
	vacrel->phase = VACUUM_ERRCB_PHASE_UNKNOWN;
	vacrel->verbose = verbose;
	errcallback.callback = vacuum_error_callback;
	errcallback.arg = vacrel;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Set up high level stuff about rel and its indexes */
	vacrel->rel = rel;
	vacrel->descr = descr;
	vac_open_bridged_indexes(vacrel->rel, RowExclusiveLock, &vacrel->nindexes,
							 &vacrel->indrels);
	vacrel->bstrategy = bstrategy;
	if (instrument && vacrel->nindexes > 0)
	{
		/* Copy index names used by instrumentation (not error reporting) */
		indnames = palloc(sizeof(char *) * vacrel->nindexes);
		for (int i = 0; i < vacrel->nindexes; i++)
			indnames[i] = pstrdup(RelationGetRelationName(vacrel->indrels[i]));
	}

	Assert(params->index_cleanup != VACOPTVALUE_UNSPECIFIED);
	Assert(params->index_cleanup != VACOPTVALUE_DISABLED);

	vacrel->do_index_vacuuming = true;
	vacrel->do_index_cleanup = true;
	vacrel->consider_bypass_optimization = true;

	if (params->index_cleanup == VACOPTVALUE_ENABLED)
	{
		/* Force index vacuuming.  Note that failsafe can still bypass. */
		vacrel->consider_bypass_optimization = false;
	}
	else
	{
		/* Default/auto, make all decisions dynamically */
		Assert(params->index_cleanup == VACOPTVALUE_AUTO);
	}

	/* Initialize page counters explicitly (be tidy) */
	vacrel->scanned_pages = 0;
	vacrel->removed_pages = 0;
	vacrel->lpdead_item_pages = 0;
	vacrel->missed_dead_pages = 0;
	vacrel->nonempty_pages = 0;
	/* dead_items_alloc allocates vacrel->dead_items later on */

	/* Allocate/initialize output statistics state */
	vacrel->new_rel_tuples = 0;
	vacrel->new_live_tuples = 0;
	vacrel->indstats = (IndexBulkDeleteResult **)
		palloc0(vacrel->nindexes * sizeof(IndexBulkDeleteResult *));

	/* Initialize remaining counters (be tidy) */
	vacrel->num_index_scans = 0;
	vacrel->tuples_deleted = 0;
	vacrel->lpdead_items = 0;
	vacrel->live_tuples = 0;
	vacrel->recently_dead_tuples = 0;
	vacrel->missed_dead_tuples = 0;

	bridge = vacrel->descr->bridge;
	Assert(bridge != NULL);
	o_btree_load_shmem(&bridge->desc);
	vacrel->rel_pages = orig_rel_pages = pg_atomic_read_u32(&BTREE_GET_META(&bridge->desc)->leafPagesNum);

	if (verbose)
	{
		ereport(INFO,
				(errmsg("vacuuming bridged indexes \"%s.%s.%s\"",
						vacrel->dbname, vacrel->relnamespace,
						vacrel->relname)));
	}

	/*
	 * Allocate dead_items memory using dead_items_alloc.  This handles
	 * parallel VACUUM initialization as part of allocating shared memory
	 * space used for dead_items.
	 */
	dead_items_alloc(vacrel, params->nworkers);

	/*
	 * Call lazy_scan_heap to perform all required heap pruning, index
	 * vacuuming, and heap vacuuming (plus related processing)
	 */
	lazy_scan_bridge_index(vacrel);

	if (!local_wal_is_empty())
		flush_local_wal(false);

	/*
	 * Free resources managed by dead_items_alloc.  This ends parallel mode in
	 * passing when necessary.
	 */
	dead_items_cleanup(vacrel);
	Assert(!IsInParallelMode());

	/*
	 * Update pg_class entries for each of rel's indexes where appropriate.
	 *
	 * Unlike the later update to rel's pg_class entry, this is not critical.
	 * Maintains relpages/reltuples statistics used by the planner only.
	 */
	if (vacrel->do_index_cleanup)
		update_relstats_all_indexes(vacrel);

	/* Done with rel's indexes */
	vac_close_indexes(vacrel->nindexes, vacrel->indrels, NoLock);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	/* Report that we are now doing final cleanup */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_FINAL_CLEANUP);

	new_rel_pages = vacrel->rel_pages;	/* After possible rel truncation */

	/*
	 * Not calling vac_update_relstats here, because we only analyze bridged
	 * index;
	 */
	/* Reporting to pgstat of this info should not break anything */
	pgstat_report_vacuum(RelationGetRelid(rel),
						 rel->rd_rel->relisshared,
						 vacrel->live_tuples,
						 vacrel->lpdead_items);

	pgstat_progress_end_command();

	if (instrument)
	{
		TimestampTz endtime = GetCurrentTimestamp();

		if (verbose || params->log_min_duration == 0 ||
			TimestampDifferenceExceeds(starttime, endtime,
									   params->log_min_duration))
		{
			long		secs_dur;
			int			usecs_dur;
			WalUsage	walusage;
			BufferUsage bufferusage;
			StringInfoData buf;
			char	   *msgfmt;
			double		read_rate = 0,
						write_rate = 0;

			TimestampDifference(starttime, endtime, &secs_dur, &usecs_dur);
			memset(&walusage, 0, sizeof(WalUsage));
			WalUsageAccumDiff(&walusage, &pgWalUsage, &startwalusage);
			memset(&bufferusage, 0, sizeof(BufferUsage));
			BufferUsageAccumDiff(&bufferusage, &pgBufferUsage, &startbufferusage);

			initStringInfo(&buf);
			if (verbose)
			{
				/*
				 * Aggressiveness already reported earlier, in dedicated
				 * VACUUM VERBOSE ereport
				 */
				Assert(!params->is_wraparound);
				msgfmt = _("finished vacuuming bridged indexes \"%s.%s.%s\": index scans: %d\n");
			}
			else
			{
				msgfmt = _("automatic vacuum of bridged indexes \"%s.%s.%s\": index scans: %d\n");
			}
			appendStringInfo(&buf, msgfmt,
							 vacrel->dbname,
							 vacrel->relnamespace,
							 vacrel->relname,
							 vacrel->num_index_scans);
			appendStringInfo(&buf, _("pages: %u removed, %u remain, %u scanned (%.2f%% of total)\n"),
							 vacrel->removed_pages,
							 new_rel_pages,
							 vacrel->scanned_pages,
							 orig_rel_pages == 0 ? 100.0 :
							 100.0 * vacrel->scanned_pages / orig_rel_pages);

			if (vacrel->nindexes == 0 || vacrel->num_index_scans == 0)
				appendStringInfoString(&buf, _("index scan not needed: "));
			else
				appendStringInfoString(&buf, _("index scan needed: "));

			msgfmt = _("%u pages from table (%.2f%% of total) had %lld dead item identifiers removed\n");

			appendStringInfo(&buf, msgfmt,
							 vacrel->lpdead_item_pages,
							 orig_rel_pages == 0 ? 100.0 :
							 100.0 * vacrel->lpdead_item_pages / orig_rel_pages,
							 (long long) vacrel->lpdead_items);
			for (int i = 0; i < vacrel->nindexes; i++)
			{
				IndexBulkDeleteResult *istat = vacrel->indstats[i];

				if (!istat)
					continue;

				Assert(indnames);
				appendStringInfo(&buf,
								 _("index \"%s\": pages: %u in total, %u newly deleted, %u currently deleted, %u reusable\n"),
								 indnames[i],
								 istat->num_pages,
								 istat->pages_newly_deleted,
								 istat->pages_deleted,
								 istat->pages_free);
			}
			if (track_io_timing)
			{
				double		read_ms = (double) (pgStatBlockReadTime - startreadtime) / 1000;
				double		write_ms = (double) (pgStatBlockWriteTime - startwritetime) / 1000;

				appendStringInfo(&buf, _("I/O timings: read: %.3f ms, write: %.3f ms\n"),
								 read_ms, write_ms);
			}
			if (secs_dur > 0 || usecs_dur > 0)
			{
				read_rate = (double) BLCKSZ * (bufferusage.shared_blks_read + bufferusage.local_blks_read) /
					(1024 * 1024) / (secs_dur + usecs_dur / 1000000.0);
				write_rate = (double) BLCKSZ * (bufferusage.shared_blks_dirtied + bufferusage.local_blks_dirtied) /
					(1024 * 1024) / (secs_dur + usecs_dur / 1000000.0);
			}
			appendStringInfo(&buf, _("avg read rate: %.3f MB/s, avg write rate: %.3f MB/s\n"),
							 read_rate, write_rate);
			appendStringInfo(&buf,
							 _("buffer usage: %lld hits, %lld misses, %lld dirtied\n"),
							 (long long) (bufferusage.shared_blks_hit + bufferusage.local_blks_hit),
							 (long long) (bufferusage.shared_blks_read + bufferusage.local_blks_read),
							 (long long) (bufferusage.shared_blks_dirtied + bufferusage.local_blks_dirtied));
			appendStringInfo(&buf,
							 _("WAL usage: %lld records, %lld full page images, %llu bytes\n"),
							 (long long) walusage.wal_records,
							 (long long) walusage.wal_fpi,
							 (unsigned long long) walusage.wal_bytes);
			appendStringInfo(&buf, _("system usage: %s"), pg_rusage_show(&ru0));

			ereport(verbose ? INFO : LOG,
					(errmsg_internal("%s", buf.data)));
			pfree(buf.data);
		}
	}

	/* Cleanup index statistics and index names */
	for (int i = 0; i < vacrel->nindexes; i++)
	{
		if (vacrel->indstats[i])
			pfree(vacrel->indstats[i]);
		if (instrument)
			pfree(indnames[i]);
	}
}

/*
 * Forget all collected dead items.
 */
static void
dead_items_reset(LVRelState *vacrel)
{
#if PG_VERSION_NUM >= 170000
	if (ParallelVacuumIsActive(vacrel))
	{
		parallel_vacuum_reset_dead_items(vacrel->pvs);
		return;
	}

	/* Recreate the tidstore with the same max_bytes limitation */
	TidStoreDestroy(vacrel->dead_items);
	vacrel->dead_items = TidStoreCreateLocal(vacrel->dead_items_info->max_bytes, true);

	/* Reset the counter */
	vacrel->dead_items_info->num_items = 0;
#else
	vacrel->dead_items->num_items = 0;
#endif
}

/*
 * Perform cleanup for resources allocated in dead_items_alloc
 */
static void
dead_items_cleanup(LVRelState *vacrel)
{
	if (!ParallelVacuumIsActive(vacrel))
	{
		/* Don't bother with pfree here */
		return;
	}

	/* End parallel mode */
	parallel_vacuum_end(vacrel->pvs, vacrel->indstats);
	vacrel->pvs = NULL;
}

/*
 * Updates the information required for vacuum error callback.  This also saves
 * the current information which can be later restored via restore_vacuum_error_info.
 */
static void
update_vacuum_error_info(LVRelState *vacrel, LVSavedErrInfo *saved_vacrel,
						 int phase, BlockNumber blkno, OffsetNumber offnum)
{
	if (saved_vacrel)
	{
		saved_vacrel->offnum = vacrel->offnum;
		saved_vacrel->blkno = vacrel->blkno;
		saved_vacrel->phase = vacrel->phase;
	}

	vacrel->blkno = blkno;
	vacrel->offnum = offnum;
	vacrel->phase = phase;
}

/*
 * Restores the vacuum information saved via a prior call to update_vacuum_error_info.
 */
static void
restore_vacuum_error_info(LVRelState *vacrel,
						  const LVSavedErrInfo *saved_vacrel)
{
	vacrel->blkno = saved_vacrel->blkno;
	vacrel->offnum = saved_vacrel->offnum;
	vacrel->phase = saved_vacrel->phase;
}
